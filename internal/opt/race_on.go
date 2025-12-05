//go:build race

package opt

import (
	"sync/atomic"
	"unsafe"
)

const IsTSO_ = false

//go:nosplit
func LoadPtr(addr *unsafe.Pointer) unsafe.Pointer {
	return atomic.LoadPointer(addr)
}

//go:nosplit
func StorePtr(addr *unsafe.Pointer, val unsafe.Pointer) {
	atomic.StorePointer(addr, val)
}

//go:nosplit
func LoadInt[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	if unsafe.Sizeof(T(0)) == 4 {
		return T(atomic.LoadUint32((*uint32)(unsafe.Pointer(addr))))
	} else {
		return T(atomic.LoadUint64((*uint64)(unsafe.Pointer(addr))))
	}
}

//go:nosplit
func StoreInt[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	if unsafe.Sizeof(T(0)) == 4 {
		atomic.StoreUint32((*uint32)(unsafe.Pointer(addr)), uint32(val))
	} else {
		atomic.StoreUint64((*uint64)(unsafe.Pointer(addr)), uint64(val))
	}
}

//go:nosplit
func LoadIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	return LoadInt(addr)
}

//go:nosplit
func StoreIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	StoreInt(addr, val)
}
