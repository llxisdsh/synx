package synx

import (
	"sync/atomic"
)

// RWLock is a spin-based Reader-Writer lock backed by a uintptr.
// It is writer-preferred to prevent reader starvation.
type RWLock uintptr

const (
	rwWriteMask = 1
	rwReadShift = 2
	rwReadUnit  = 1 << rwReadShift
	rwFreeState = 2 // 2 (binary 10) means initialized, no writer, no readers
)

// Lock acquires the write lock.
// It spins until the lock is free.
//
//go:nosplit
func (l *RWLock) Lock() {
	var spins int
	for {
		// 1. Acquire Write Bit (Bit 0). This blocks NEW readers.
		s := atomic.LoadUintptr((*uintptr)(l))
		if s&rwWriteMask == 0 {
			if atomic.CompareAndSwapUintptr((*uintptr)(l), s, s|rwWriteMask) {
				// Acquired Write Bit.
				// 2. Wait for existing Readers to drain.
				// Note: Readers are bits 2+. Bit 1 is the "initialized" bit (value 2).
				// So we wait until s >> rwReadShift == 0.
				for {
					s2 := atomic.LoadUintptr((*uintptr)(l))
					if s2>>rwReadShift == 0 {
						return
					}
					delay(&spins)
				}
			}
		}
		delay(&spins)
	}
}

// Unlock releases the write lock.
// It resets the state to rwFreeState (2), indicating "initialized and free".
//
//go:nosplit
func (l *RWLock) Unlock() {
	atomic.StoreUintptr((*uintptr)(l), rwFreeState)
}

// CanRead reports whether the lock allows reading (no writer holding or waiting)
// and the value is non-zero (initialized).
//
//go:nosplit
func (l *RWLock) CanRead() bool {
	s := atomic.LoadUintptr((*uintptr)(l))
	return s != 0 && s&rwWriteMask == 0
}

// RLock acquires a read lock.
//
//go:nosplit
func (l *RWLock) RLock() {
	var spins int
	for {
		s := atomic.LoadUintptr((*uintptr)(l))
		if s&rwWriteMask == 0 { // No writer
			if atomic.CompareAndSwapUintptr((*uintptr)(l), s, s+rwReadUnit) {
				return
			}
		}
		delay(&spins)
	}
}

// RUnlock releases a read lock.
//
//go:nosplit
func (l *RWLock) RUnlock() {
	atomic.AddUintptr((*uintptr)(l), ^uintptr(rwReadUnit-1))
}

// RWLock32 is a spin-based Reader-Writer lock backed by a uint32.
// It is writer-preferred to prevent reader starvation.
type RWLock32 uint32

// Lock acquires the write lock.
//
//go:nosplit
func (l *RWLock32) Lock() {
	var spins int
	for {
		s := atomic.LoadUint32((*uint32)(l))
		if s&rwWriteMask == 0 {
			if atomic.CompareAndSwapUint32((*uint32)(l), s, s|rwWriteMask) {
				for {
					s2 := atomic.LoadUint32((*uint32)(l))
					if s2>>rwReadShift == 0 {
						return
					}
					delay(&spins)
				}
			}
		}
		delay(&spins)
	}
}

// Unlock releases the write lock.
//
//go:nosplit
func (l *RWLock32) Unlock() {
	atomic.StoreUint32((*uint32)(l), uint32(rwFreeState))
}

// RLock acquires a read lock.
//
//go:nosplit
func (l *RWLock32) RLock() {
	var spins int
	for {
		s := atomic.LoadUint32((*uint32)(l))
		if s&rwWriteMask == 0 {
			if atomic.CompareAndSwapUint32((*uint32)(l), s, s+uint32(rwReadUnit)) {
				return
			}
		}
		delay(&spins)
	}
}

// RUnlock releases a read lock.
//
//go:nosplit
func (l *RWLock32) RUnlock() {
	atomic.AddUint32((*uint32)(l), ^uint32(rwReadUnit-1))
}

// CanRead reports whether the lock allows reading (no writer holding or waiting)
// and the value is non-zero (initialized).
//
//go:nosplit
func (l *RWLock32) CanRead() bool {
	s := atomic.LoadUint32((*uint32)(l))
	return s != 0 && s&rwWriteMask == 0
}
