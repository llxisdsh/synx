//go:build race

package opt

import (
	"sync/atomic"
	"unsafe"
)

// IsTSO_ under race detector, disable TSO optimizations and use conservative
// atomic loads/stores
const IsTSO_ = false

// LoadPtr conservative: atomic pointer load to satisfy race detector
//
//go:nosplit
func LoadPtr(addr *unsafe.Pointer) unsafe.Pointer {
	return atomic.LoadPointer(addr)
}

// StorePtr conservative: atomic pointer store to satisfy race detector
//
//go:nosplit
func StorePtr(addr *unsafe.Pointer, val unsafe.Pointer) {
	atomic.StorePointer(addr, val)
}

// LoadInt conservative: atomic integer load to satisfy race detector
//
//go:nosplit
func LoadInt[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	if unsafe.Sizeof(T(0)) == 4 {
		return T(atomic.LoadUint32((*uint32)(unsafe.Pointer(addr))))
	} else {
		return T(atomic.LoadUint64((*uint64)(unsafe.Pointer(addr))))
	}
}

// StoreInt conservative: atomic integer store to satisfy race detector
//
//go:nosplit
func StoreInt[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	if unsafe.Sizeof(T(0)) == 4 {
		atomic.StoreUint32((*uint32)(unsafe.Pointer(addr)), uint32(val))
	} else {
		atomic.StoreUint64((*uint64)(unsafe.Pointer(addr)), uint64(val))
	}
}

// LoadIntFast conservative: atomic integer load to satisfy race detector
//
//go:nosplit
func LoadIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	return LoadInt(addr)
}

// StoreIntFast conservative: atomic integer store to satisfy race detector
//
//go:nosplit
func StoreIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	StoreInt(addr, val)
}
