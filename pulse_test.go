package synx

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestPulseSize(t *testing.T) {
	var b Pulse
	// 8 (state) + 4 (sema) + 4 (padding) = 16 typically
	size := unsafe.Sizeof(b)
	if size > 16 {
		t.Errorf("Pulse size = %d, expected <= 16", size)
	}
}

func TestPulseCyclic(t *testing.T) {
	var b Pulse
	var count int32
	var wg sync.WaitGroup

	// Round 1
	n := 5
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			b.Wait()
			atomic.AddInt32(&count, 1)
		}()
	}

	// Wait a bit to ensure they are blocked
	time.Sleep(50 * time.Millisecond)
	if c := atomic.LoadInt32(&count); c != 0 {
		t.Errorf("Round 1: Waiters passed early: %d", c)
	}

	b.Beat() // Pulse 1
	wg.Wait()

	if c := atomic.LoadInt32(&count); c != int32(n) {
		t.Errorf("Round 1: Waiters didn't wake: %d", c)
	}

	// Round 2 (Recycle)
	atomic.StoreInt32(&count, 0)
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			b.Wait() // Should block for Beat 2
			atomic.AddInt32(&count, 1)
		}()
	}

	time.Sleep(50 * time.Millisecond)
	if c := atomic.LoadInt32(&count); c != 0 {
		t.Errorf("Round 2: Waiters passed early (barrier didn't close): %d", c)
	}

	b.Beat() // Pulse 2
	wg.Wait()

	if c := atomic.LoadInt32(&count); c != int32(n) {
		t.Errorf("Round 2: Waiters didn't wake: %d", c)
	}
}

func TestPulseLateArrival(t *testing.T) {
	var b Pulse

	// 1. Beat first (Gen 0 -> 1)
	b.Beat()

	// 2. Waiter arrives late (Should join Gen 1)
	done := make(chan struct{})
	go func() {
		b.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Errorf("Late waiter didn't block!")
	case <-time.After(50 * time.Millisecond):
		// Correct, blocked.
	}

	// 3. Beat second time (Gen 1 -> 2)
	b.Beat()

	select {
	case <-done:
		// Success
	case <-time.After(50 * time.Millisecond):
		t.Errorf("Late waiter didn't wake on second beat!")
	}
}
