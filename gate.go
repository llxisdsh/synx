package synx

import (
	"sync/atomic"
)

// Gate is a synchronization primitive that can be manually set and reset.
// It is also known as a ManualResetEvent.
//
// State:
//   - Signaled (Set): Wait returns immediately.
//   - Unsignaled (Reset): Wait blocks.
//
// It is zero-value usable (starts Unsignaled).
//
// Size: 16 bytes (8 byte state + 2*4 byte sema).
type Gate struct {
	_ noCopy
	// state 64-bit:
	//   Bit 63:    IsSet (1 = Signaled, 0 = Unsignaled)
	//   Bit 32-62: Generation
	//   Bit 0-31:  Waiter Count
	state atomic.Uint64

	// sema is a double-buffered semaphore to prevent signal stealing
	// during rapid Set/Reset cycles.
	sema [2]uint32
}

const (
	gateSetBit = 1 << 63
	// gateGenOne = 1 << 32
	gateCntMsk = 0xFFFFFFFF
)

// Open signals the gate (sets state to true).
// All current waiters are woken up.
// Future calls to Wait() return immediately until Reset() is called.
func (e *Gate) Open() {
	for {
		s := e.state.Load()
		if s&gateSetBit != 0 {
			// Already set
			return
		}

		gen := (s >> 32) & 0x7FFFFFFF
		cnt := s & gateCntMsk

		// New state: Set=1, Gen=Same, Count=0
		// We clear count because we are about to wake them all up.
		next := gateSetBit | (gen << 32)

		if e.state.CompareAndSwap(s, next) {
			if cnt > 0 {
				semaPtr := &e.sema[gen%2]
				for i := 0; i < int(cnt); i++ {
					runtime_semrelease(semaPtr, false, 0)
				}
			}
			return
		}
	}
}

// Close signals the gate (sets state to false).
// Future calls to Wait() will block.
func (e *Gate) Close() {
	for {
		s := e.state.Load()
		if s&gateSetBit == 0 {
			// Already reset (unsignaled)
			return
		}

		// Preserve generation, but increment it for the NEW phase.
		// Old generation was 'gen'. New unsignaled phase is 'gen+1'.
		gen := (s >> 32) & 0x7FFFFFFF

		// New state: Set=0, Gen=Gen+1, Count=0
		// Note: Count should be 0 here anyway if it was Set, but we force 0.
		// Handle wrapping of 31-bit generation
		nextGen := (gen + 1) & 0x7FFFFFFF
		next := nextGen << 32

		if e.state.CompareAndSwap(s, next) {
			return
		}
	}
}

// Wait blocks until the gate is opened (Set).
// If the gate is already opened, it returns immediately.
func (e *Gate) Wait() {
	for {
		s := e.state.Load()

		// If Set bit is 1, return immediately
		if s&gateSetBit != 0 {
			return
		}

		// Not set. Add to waiter count.
		if e.state.CompareAndSwap(s, s+1) {
			gen := (s >> 32) & 0x7FFFFFFF
			runtime_semacquire(&e.sema[gen%2])
			// Upon wakeup, we loop again to double-check state or
			// mostly just return because we were woken by Set().
			// But since Set() leaves it Open, returning is correct.
			//
			// However, if Reset() happened quickly after Set(),
			// we technically satisfied the "Wait until Set" condition
			// even if it is now Reset.
			return
		}
	}
}

// IsOpen returns true if the gate is currently opened.
func (e *Gate) IsOpen() bool {
	return e.state.Load()&gateSetBit != 0
}
