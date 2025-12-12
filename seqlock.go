package synx

import (
	"sync/atomic"
	"unsafe"
)

// seqlock coordinates tear-free publication for a single slot.
//
// Role:
//   - Sequence guard only: holds odd/even counter, no payload.
//   - Stable-window reads: readers copy from seqlockSlot[T] when seq is
//     even and unchanged.
//   - Non-blocking publishes: writers CAS to odd, copy, then publish by
//     storing seq+2 (even).
//
// Differences:
//   - vs atomic.Pointer[T]: avoids heap indirection; improves locality.
//   - vs atomic.Value: typed publication without boxing; supports
//     multi-word snapshots on weak memory via the sequence.
//
// Usage:
//
//	Pair with seqlockSlot[T]. Use Read/Write helpers or the Locked
//	variants when an external lock is held.
type seqlock struct {
	seq uintptr
}

// BeginRead enters the reader window if seq is even.
// Returns the observed sequence and ok=true when even.
//
//go:nosplit
func (sl *seqlock) BeginRead() (s1 uintptr, ok bool) {
	s1 = atomic.LoadUintptr(&sl.seq)
	return s1, s1&1 == 0
}

// EndRead verifies the window stability: returns true if seq unchanged.
//
//go:nosplit
func (sl *seqlock) EndRead(s1 uintptr) (ok bool) {
	s2 := atomic.LoadUintptr(&sl.seq)
	return s1 == s2
}

// BeginWrite enters the writer window by CASing seq to odd.
// Returns previous sequence and ok=true on success.
//
//go:nosplit
func (sl *seqlock) BeginWrite() (s1 uintptr, ok bool) {
	s1 = atomic.LoadUintptr(&sl.seq)
	if s1&1 != 0 {
		return s1, false
	}
	return s1, atomic.CompareAndSwapUintptr(&sl.seq, s1, s1|1)
}

// EndWrite exits the writer window by storing seq+2 (even).
//
//go:nosplit
func (sl *seqlock) EndWrite(s1 uintptr) {
	atomic.StoreUintptr(&sl.seq, s1+2)
}

// BeginWriteLocked enters the writer window using add (odd).
// Only safe when an external lock is held.
//
//go:nosplit
func (sl *seqlock) BeginWriteLocked() {
	atomic.AddUintptr(&sl.seq, 1)
}

// EndWriteLocked exits the writer window using add (even).
// Only safe when an external lock is held.
//
//go:nosplit
func (sl *seqlock) EndWriteLocked() {
	atomic.AddUintptr(&sl.seq, 1)
}

// WriteCompleted returns true if seq is non-zero and even.
//
//go:nosplit
func (sl *seqlock) WriteCompleted() (ok bool) {
	s1 := atomic.LoadUintptr(&sl.seq)
	return s1 != 0 && s1&1 == 0
}

// seqlock32 coordinates tear-free publication for a single slot.
type seqlock32 struct {
	seq uint32
}

// BeginRead enters the reader window if seq is even.
// Returns the observed sequence and ok=true when even.
//
//go:nosplit
func (sl *seqlock32) BeginRead() (s1 uint32, ok bool) {
	s1 = atomic.LoadUint32(&sl.seq)
	return s1, s1&1 == 0
}

// EndRead verifies the window stability: returns true if seq unchanged.
//
//go:nosplit
func (sl *seqlock32) EndRead(s1 uint32) (ok bool) {
	s2 := atomic.LoadUint32(&sl.seq)
	return s1 == s2
}

// BeginWrite enters the writer window by CASing seq to odd.
// Returns previous sequence and ok=true on success.
//
//go:nosplit
func (sl *seqlock32) BeginWrite() (s1 uint32, ok bool) {
	s1 = atomic.LoadUint32(&sl.seq)
	if s1&1 != 0 {
		return s1, false
	}
	return s1, atomic.CompareAndSwapUint32(&sl.seq, s1, s1|1)
}

// EndWrite exits the writer window by storing seq+2 (even).
//
//go:nosplit
func (sl *seqlock32) EndWrite(s1 uint32) {
	atomic.StoreUint32(&sl.seq, s1+2)
}

// BeginWriteLocked enters the writer window using add (odd).
// Only safe when an external lock is held.
//
//go:nosplit
func (sl *seqlock32) BeginWriteLocked() {
	atomic.AddUint32(&sl.seq, 1)
}

// EndWriteLocked exits the writer window using add (even).
// Only safe when an external lock is held.
//
//go:nosplit
func (sl *seqlock32) EndWriteLocked() {
	atomic.AddUint32(&sl.seq, 1)
}

// WriteCompleted returns true if seq is non-zero and even.
//
//go:nosplit
func (sl *seqlock32) WriteCompleted() (ok bool) {
	s1 := atomic.LoadUint32(&sl.seq)
	return s1 != 0 && s1&1 == 0
}

// seqlockSlot[T] holds an inline buffer of T. Used with seqlock to
// publish tear-free snapshots.
//
// Copy semantics:
//   - Reads: On TSO (amd64/386/s390x), plain typed copies are sufficient.
//     On weak models, uintptr-sized atomic loads are used inside the stable
//     window when alignment/size permit; otherwise a typed copy is used.
//   - Writes: Always plain typed assignment to preserve GC write barriers
//     and avoid publishing pointers via uintptr/unsafe stores.
//
// Safety:
//   - ReadUnfenced/WriteUnfenced must run under a seqlock-stable window or
//     an external lock; otherwise torn reads/writes are possible.
type seqlockSlot[T any] struct {
	_   [0]atomic.Uintptr
	buf T
}

// ReadUnfenced copies buf into v using uintptr-sized atomic loads when
// alignment and size permit on weak memory models to avoid reordering;
// otherwise falls back to a typed copy.
// Must be called under a lock or within a seqlock-stable window.
//
//go:nosplit
func (slot *seqlockSlot[T]) ReadUnfenced() (v T) {
	if !isTSO_ {
		if unsafe.Sizeof(slot.buf) == 0 {
			return v
		}

		ws := unsafe.Sizeof(uintptr(0))
		sz := unsafe.Sizeof(slot.buf)
		al := unsafe.Alignof(slot.buf)
		if al >= ws && sz%ws == 0 {
			n := sz / ws
			switch n {
			case 1:
				u := atomic.LoadUintptr((*uintptr)(unsafe.Pointer(&slot.buf)))
				*(*uintptr)(unsafe.Pointer(&v)) = u
			case 2:
				p := (*[2]uintptr)(unsafe.Pointer(&slot.buf))
				q := (*[2]uintptr)(unsafe.Pointer(&v))
				q[0] = atomic.LoadUintptr(&p[0])
				q[1] = atomic.LoadUintptr(&p[1])
			case 3:
				p := (*[3]uintptr)(unsafe.Pointer(&slot.buf))
				q := (*[3]uintptr)(unsafe.Pointer(&v))
				q[0] = atomic.LoadUintptr(&p[0])
				q[1] = atomic.LoadUintptr(&p[1])
				q[2] = atomic.LoadUintptr(&p[2])
			case 4:
				p := (*[4]uintptr)(unsafe.Pointer(&slot.buf))
				q := (*[4]uintptr)(unsafe.Pointer(&v))
				q[0] = atomic.LoadUintptr(&p[0])
				q[1] = atomic.LoadUintptr(&p[1])
				q[2] = atomic.LoadUintptr(&p[2])
				q[3] = atomic.LoadUintptr(&p[3])
			default:
				for i := range n {
					off := i * ws
					src := (*uintptr)(unsafe.Pointer(
						uintptr(unsafe.Pointer(&slot.buf)) + off,
					))
					dst := (*uintptr)(unsafe.Pointer(
						uintptr(unsafe.Pointer(&v)) + off,
					))
					*dst = atomic.LoadUintptr(src)
				}
			}
			return v
		}
	}
	return slot.buf
}

// WriteUnfenced writes v into buf via a plain typed assignment.
// This preserves Go's GC write barriers and avoids publishing pointers
// through uintptr/unsafe atomic stores.
// Must be called under a lock or within a seqlock-stable window.
//
//go:nosplit
func (slot *seqlockSlot[T]) WriteUnfenced(v T) {
	slot.buf = v
}

// Ptr returns the address of the inline buffer.
// Mutations through this pointer must be guarded by an external lock or
// odd/even sequence; otherwise readers may observe torn data.
//
//go:nosplit
func (slot *seqlockSlot[T]) Ptr() *T {
	return &slot.buf
}

// seqRead atomically loads a tear-free snapshot using the external seqlock.
// Spins until seq is even and unchanged across two reads; copies the value
// within the stable window.
//
//nolint:unused
func seqRead[T any](sl *seqlock, slot *seqlockSlot[T]) (v T) {
	if s1, ok := sl.BeginRead(); ok {
		v = slot.ReadUnfenced()
		if ok = sl.EndRead(s1); ok {
			return v
		}
	}
	return seqSlowRead(sl, slot)
}

//nolint:unused
func seqSlowRead[T any](sl *seqlock, slot *seqlockSlot[T]) (v T) {
	var spins int
	for {
		if s1, ok := sl.BeginRead(); ok {
			v = slot.ReadUnfenced()
			if ok = sl.EndRead(s1); ok {
				return v
			}
			continue
		}
		delay(&spins)
	}
}

// seqWrite publishes v guarded by the external seqlock.
// Enters odd, copies v, then exits to even to publish a stable snapshot.
//
//nolint:unused
func seqWrite[T any](sl *seqlock, slot *seqlockSlot[T], v T) {
	if s1, ok := sl.BeginWrite(); ok {
		slot.WriteUnfenced(v)
		sl.EndWrite(s1)
		return
	}
	seqSlowWrite(sl, slot, v)
}

//nolint:unused
func seqSlowWrite[T any](sl *seqlock, slot *seqlockSlot[T], v T) {
	var spins int
	for {
		if s1, ok := sl.BeginWrite(); ok {
			slot.WriteUnfenced(v)
			sl.EndWrite(s1)
			return
		}
		delay(&spins)
	}
}

// seqRead32 atomically loads a tear-free snapshot using the external seqlock.
// Spins until seq is even and unchanged across two reads; copies the value
// within the stable window.
//
//nolint:unused
func seqRead32[T any](sl *seqlock32, slot *seqlockSlot[T]) (v T) {
	if s1, ok := sl.BeginRead(); ok {
		v = slot.ReadUnfenced()
		if ok = sl.EndRead(s1); ok {
			return v
		}
	}
	return seqSlowRead32(sl, slot)
}

//nolint:unused
func seqSlowRead32[T any](sl *seqlock32, slot *seqlockSlot[T]) (v T) {
	var spins int
	for {
		if s1, ok := sl.BeginRead(); ok {
			v = slot.ReadUnfenced()
			if ok = sl.EndRead(s1); ok {
				return v
			}
			continue
		}
		delay(&spins)
	}
}

// seqWrite32 publishes v guarded by the external seqlock.
// Enters odd, copies v, then exits to even to publish a stable snapshot.
//
//nolint:unused
func seqWrite32[T any](sl *seqlock32, slot *seqlockSlot[T], v T) {
	if s1, ok := sl.BeginWrite(); ok {
		slot.WriteUnfenced(v)
		sl.EndWrite(s1)
		return
	}
	seqSlowWrite32(sl, slot, v)
}

//nolint:unused
func seqSlowWrite32[T any](sl *seqlock32, slot *seqlockSlot[T], v T) {
	var spins int
	for {
		if s1, ok := sl.BeginWrite(); ok {
			slot.WriteUnfenced(v)
			sl.EndWrite(s1)
			return
		}
		delay(&spins)
	}
}

//nolint:unused
//go:nosplit
func seqWriteLocked[T any](sl *seqlock, slot *seqlockSlot[T], v T) {
	sl.BeginWriteLocked()
	slot.WriteUnfenced(v)
	sl.EndWriteLocked()
}

//nolint:unused
//go:nosplit
func seqWriteLocked32[T any](sl *seqlock32, slot *seqlockSlot[T], v T) {
	sl.BeginWriteLocked()
	slot.WriteUnfenced(v)
	sl.EndWriteLocked()
}
