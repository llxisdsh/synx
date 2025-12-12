package synx

import (
	"sync/atomic"
)

// Semaphore is a counting semaphore synchronization primitive.
// It allows a fixed number of concurrent accesses to a resource.
//
// It is zero-value usable (starts with 0 permits, effectively a mutex that starts locked
// if you Wait(), but usually you initialize it or Release() into it).
//
// However, unlike sync.Mutex, it does not have an owner.
//
// Size: 16 bytes (8 byte permits + 4 byte sema).
type Semaphore struct {
	_ noCopy
	// permits is the number of available permits.
	// Positive: Available permits.
	// Negative: Number of waiters (approximately).
	permits atomic.Int64

	sema uint32
}

// NewSemaphore creates a new Semaphore with a given number of initial permits.
func NewSemaphore(permits int64) *Semaphore {
	s := &Semaphore{}
	s.permits.Store(permits)
	return s
}

// Acquire acquires n permits.
// It blocks until n permits are available.
func (s *Semaphore) Acquire(n int64) {
	if n <= 0 {
		return
	}

	// Strict Dijkstra Semaphore implementation:
	// Decrement the counter.
	// If negative, it means we must wait.
	if s.permits.Add(-n) < 0 {
		runtime_semacquire(&s.sema)
	}
}

// TryAcquire attempts to acquire n permits without blocking.
// Returns true on success.
func (s *Semaphore) TryAcquire(n int64) bool {
	for {
		p := s.permits.Load()
		if p < n {
			return false
		}
		if s.permits.CompareAndSwap(p, p-n) {
			return true
		}
	}
}

// Release releases n permits.
func (s *Semaphore) Release(n int64) {
	if n <= 0 {
		return
	}

	// Atomic increment the permits.
	// We capture the value assuming no concurrent ops for a moment to reason about it,
	// but atomic.Add is safe.
	v := s.permits.Add(n)

	// We need to wake up waiters if there are any.
	// "v" is the value after we added n.
	// Waiters exist if the value BEFORE add was negative.
	// value_before = v - n

	// If value_before >= 0, then there were no waiters (permits were available or 0).
	// If value_before < 0, then there were waiters.
	// The number of waiters was roughly -value_before.

	// Example: P = -5. Release(2). P becomes -3.
	// We added 2. We should wake up 2 waiters to consume these 2 permits.

	// Example: P = -1. Release(5). P becomes 4.
	// We added 5. Only 1 waiter needed a permit. We wake 1.
	// The other 4 remain as available permits (count=4).

	// So we wake min(n, waiters_count).

	valBefore := v - n
	if valBefore < 0 {
		waiters := -valBefore
		toWake := min(waiters, n)
		for range toWake {
			runtime_semrelease(&s.sema, false, 0)
		}
	}
}
