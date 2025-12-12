package synx

import (
	"sync"
	"testing"
	"time"
)

func TestSemaphore_Simple(t *testing.T) {
	s := NewSemaphore(1)

	// Acquire
	s.Acquire(1)

	// TryAcquire fail
	if s.TryAcquire(1) {
		t.Error("TryAcquire succeeded when empty")
	}

	// Release
	s.Release(1)

	// Acquire again
	s.Acquire(1)
}

func TestSemaphore_Ordering(t *testing.T) {
	s := NewSemaphore(0)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		s.Acquire(1)
	}()

	go func() {
		defer wg.Done()
		s.Acquire(1)
	}()

	time.Sleep(10 * time.Millisecond) // Wait for them to block

	s.Release(2)
	wg.Wait()
}

func TestSemaphore_Batch(t *testing.T) {
	s := NewSemaphore(0)

	go func() {
		time.Sleep(10 * time.Millisecond)
		s.Release(5)
	}()

	s.Acquire(5)
}

func TestSemaphore_Race(t *testing.T) {
	s := NewSemaphore(0)
	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)

	for range N {
		go func() {
			defer wg.Done()
			s.Acquire(1)
			// critical section
			s.Release(1)
		}()
	}

	s.Release(1) // Start the chain
	wg.Wait()

	// Should have 1 permit left
	if !s.TryAcquire(1) {
		t.Error("Race finished but semaphore empty")
	}
}
