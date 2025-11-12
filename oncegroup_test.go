package synx

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOnceGroup_DoDuplicates(t *testing.T) {
	var g OnceGroup[string, int]

	var calls int32
	key := "same"
	n := 64

	var wg sync.WaitGroup
	wg.Add(n)
	sharedCount := int32(0)
	for range n {
		go func() {
			defer wg.Done()
			v, err, shared := g.Do(key, func() (int, error) {
				atomic.AddInt32(&calls, 1)
				time.Sleep(2 * time.Millisecond)
				return 42, nil
			})
			if err != nil || v != 42 {
				t.Errorf("bad result: %v, %v", v, err)
			}
			if shared {
				atomic.AddInt32(&sharedCount, 1)
			}
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Fatalf("fn executed %d times, want 1", calls)
	}
	if sharedCount != int32(n) {
		t.Fatalf("shared=%d, want %d", sharedCount, n)
	}
}

func TestOnceGroup_DoChan(t *testing.T) {
	var g OnceGroup[string, string]
	key := "dup"
	n := 32

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			<-start
			ch := g.DoChan(key, func() (string, error) {
				time.Sleep(2 * time.Millisecond)
				return "ok", nil
			})
			r := <-ch
			if r.Err != nil || r.Val != "ok" {
				t.Errorf("bad: %v, %v", r.Val, r.Err)
			}
			if !r.Shared {
				t.Errorf("expected shared=true")
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestOnceGroup_Forget(t *testing.T) {
	var g OnceGroup[string, any]
	key := "work"

	firstStarted := make(chan struct{})
	unblockFirst := make(chan struct{})
	firstFinished := make(chan struct{})

	// First in-flight call: signal when registered, then block until unblocked.
	go func() {
		_, _, _ = g.Do(key, func() (any, error) {
			close(firstStarted)
			<-unblockFirst
			close(firstFinished)
			return 1, nil
		})
	}()

	// Ensure first call has registered in the group before Forget.
	<-firstStarted
	g.Forget(key)

	// Second call should not join the first; it must run independently.
	v, err, shared := g.Do(key, func() (any, error) { return 2, nil })
	if err != nil || v.(int) != 2 || shared {
		t.Fatalf("unexpected: v=%v err=%v shared=%v", v, err, shared)
	}

	// Let the first call complete.
	close(unblockFirst)
	<-firstFinished
}

func TestOnceGroup_ForgetUnshared(t *testing.T) {
	var g OnceGroup[string, any]
	key := "k"

	// No duplicates: call in-flight, should forget.
	block := make(chan struct{})
	ch := g.DoChan(key, func() (any, error) {
		<-block
		return 1, nil
	})
	if !g.ForgetUnshared(key) {
		t.Fatalf("ForgetUnshared should succeed without dups")
	}
	close(block)
	<-ch

	// With duplicates: should not forget.
	block = make(chan struct{})
	ch = g.DoChan(key, func() (any, error) {
		<-block
		return 2, nil
	})
	_ = g.DoChan(key, func() (any, error) { return 0, nil })
	if g.ForgetUnshared(key) {
		t.Fatalf("ForgetUnshared should fail with dups")
	}
	close(block)
	<-ch
}

// Panic should propagate to all Do callers (including duplicates).
func TestOnceGroup_Do_Panic(t *testing.T) {
	var g OnceGroup[string, any]
	key := "panic"
	n := 16

	var wg sync.WaitGroup
	wg.Add(n)
	panics := int32(0)
	start := make(chan struct{})
	for range n {
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					atomic.AddInt32(&panics, 1)
				}
			}()
			<-start
			_, _, _ = g.Do(key, func() (any, error) {
				panic("boom")
			})
		}()
	}
	close(start)
	wg.Wait()
	if panics != int32(n) {
		t.Fatalf("expected %d panics, got %d", n, panics)
	}
}

// Goexit should propagate to all Do callers.
func TestOnceGroup_Do_Goexit(t *testing.T) {
	var g OnceGroup[string, any]
	key := "goexit"
	n := 16

	var wg sync.WaitGroup
	wg.Add(n)
	exited := int32(0)
	start := make(chan struct{})
	for range n {
		go func() {
			defer wg.Done()
			// runtime.Goexit executes deferred funcs.
			defer atomic.AddInt32(&exited, 1)
			<-start
			_, _, _ = g.Do(key, func() (any, error) {
				runtime.Goexit()
				return nil, nil
			})
		}()
	}
	close(start)
	wg.Wait()
	if exited != int32(n) {
		t.Fatalf("expected %d goexits, got %d", n, exited)
	}
}
