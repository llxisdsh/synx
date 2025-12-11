package synx

import (
	"sync/atomic"
)

// Pulse is a reusable synchronization primitive (Pulse / Auto-Closing Door).
// It separates waiters into "generations".
//
// Behavior:
//   - Wait(): Blocks until the NEXT Beat() call.
//   - Beat(): Wakes up all currently waiting goroutines.
//     IMMEDIATELY closes the door for any new Wait() calls (they will wait for the NEXT Beat).
//
// Size: 12-16 bytes (8 byte state + 4 byte sema).
type Pulse struct {
	_ noCopy
	// state 64-bit:
	//   High 32: Generation
	//   Low 32: Waiter Count
	state atomic.Uint64

	// sema used for parking.
	sema uint32
}

// Beat wakes up all threads currently waiting on the barrier.
// It advances the generation, ensuring that any subsequent calls to Wait()
// will block until the *next* Beat().
func (b *Pulse) Beat() {
	for {
		s := b.state.Load()
		gen := s >> 32
		waiters := uint32(s) // Cast drops high bits

		// Next state: Gen+1, Waiters=0
		nextState := (gen + 1) << 32
		if b.state.CompareAndSwap(s, nextState) {
			for range waiters {
				runtime_semrelease(&b.sema, false, 0)
			}
			return
		}
	}
}

// Wait blocks until Beat() is called.
func (b *Pulse) Wait() {
	for {
		s := b.state.Load()
		if b.state.CompareAndSwap(s, s+1) {
			runtime_semacquire(&b.sema)
			return
		}
	}
}
