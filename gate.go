package synx

import (
	"sync/atomic"
)

// Gate is a synchronization primitive that can be manually opened and closed.
//
// State:
//   - Open: Wait returns immediately.
//   - Close: Wait blocks.
//
// It is zero-value usable (starts Close).
//
// Size: 16 bytes (8 byte state + 2*4 byte sema).
type Gate struct {
	_ noCopy
	// state 64-bit:
	//   Bit 63:    IsOpen (1 = Open, 0 = Close)
	//   Bit 32-62: Generation
	//   Bit 0-31:  Waiter Count
	state atomic.Uint64

	// sema is a double-buffered semaphore to prevent signal stealing
	// during rapid Open/Close cycles.
	sema [2]uint32
}

const (
	gateOpenBit = 1 << 63
	// gateGenOne = 1 << 32
	gateCntMsk = 0xFFFFFFFF
)

// Open signals the gate (sets state to Open).
// All current waiters are woken up.
// Future calls to Wait() return immediately until Close() is called.
func (e *Gate) Open() {
	for {
		s := e.state.Load()
		if s&gateOpenBit != 0 {
			// Already Open
			return
		}

		gen := (s >> 32) & 0x7FFFFFFF
		cnt := s & gateCntMsk

		// New state: Open=1, Gen=Same, Count=0
		// We clear count because we are about to wake them all up.
		next := gateOpenBit | (gen << 32)

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

// Close signals the gate (sets state to Close).
// Future calls to Wait() will block.
func (e *Gate) Close() {
	for {
		s := e.state.Load()
		if s&gateOpenBit == 0 {
			// Already Close
			return
		}

		// Preserve generation, but increment it for the NEW phase.
		// Old generation was 'gen'. New Close phase is 'gen+1'.
		gen := (s >> 32) & 0x7FFFFFFF

		// New state: Open=0, Gen=Gen+1, Count=0
		// Note: Count should be 0 here anyway if it was Open, but we force 0.
		// Handle wrapping of 31-bit generation
		nextGen := (gen + 1) & 0x7FFFFFFF
		next := nextGen << 32

		if e.state.CompareAndSwap(s, next) {
			return
		}
	}
}

// Wait blocks until the gate is opened (Open).
// If the gate is already opened, it returns immediately.
func (e *Gate) Wait() {
	for {
		s := e.state.Load()

		// If Open bit is 1, return immediately
		if s&gateOpenBit != 0 {
			return
		}

		// Not Open. Add to waiter count.
		if e.state.CompareAndSwap(s, s+1) {
			gen := (s >> 32) & 0x7FFFFFFF
			runtime_semacquire(&e.sema[gen%2])
			// Upon wakeup, we loop again to double-check state or
			// mostly just return because we were woken by Open().
			// But since Open() leaves it Open, returning is correct.
			//
			// However, if Close() happened quickly after Open(),
			// we technically satisfied the "Wait until Open" condition
			// even if it is now Close.
			return
		}
	}
}

// IsOpen returns true if the gate is currently opened.
func (e *Gate) IsOpen() bool {
	return e.state.Load()&gateOpenBit != 0
}
