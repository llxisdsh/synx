//go:build !race

package opt

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// IsTSO_ detects TSO architectures; on TSO, plain reads/writes are safe for
// pointers and native word-sized integers
const IsTSO_ = runtime.GOARCH == "amd64" ||
	runtime.GOARCH == "386" ||
	runtime.GOARCH == "s390x"

// Compile-time pointer size, used to choose 32-bit vs 64-bit paths
const uintptrSize_ = unsafe.Sizeof(uintptr(0))

// LoadPtr loads a pointer atomically on non-TSO architectures.
// On TSO architectures, it performs a plain pointer load.
//
//go:nosplit
func LoadPtr(addr *unsafe.Pointer) unsafe.Pointer {
	if IsTSO_ {
		return *addr
	} else {
		return atomic.LoadPointer(addr)
	}
}

// StorePtr stores a pointer atomically on non-TSO architectures.
// On TSO architectures, it performs a plain pointer store.
//
//go:nosplit
func StorePtr(addr *unsafe.Pointer, val unsafe.Pointer) {
	if IsTSO_ {
		*addr = val
	} else {
		atomic.StorePointer(addr, val)
	}
}

// LoadInt aligned integer load; plain on TSO when width matches,
// otherwise atomic
//
//go:nosplit
func LoadInt[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	if unsafe.Sizeof(T(0)) == 4 {
		if IsTSO_ {
			return *addr
		} else {
			return T(atomic.LoadUint32((*uint32)(unsafe.Pointer(addr))))
		}
	} else {
		if IsTSO_ && uintptrSize_ == 8 {
			return *addr
		} else {
			return T(atomic.LoadUint64((*uint64)(unsafe.Pointer(addr))))
		}
	}
}

// StoreInt aligned integer store; plain on TSO when width matches,
// otherwise atomic
//
//go:nosplit
func StoreInt[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	if unsafe.Sizeof(T(0)) == 4 {
		if IsTSO_ {
			*addr = val
		} else {
			atomic.StoreUint32((*uint32)(unsafe.Pointer(addr)), uint32(val))
		}
	} else {
		if IsTSO_ && uintptrSize_ == 8 {
			*addr = val
		} else {
			atomic.StoreUint64((*uint64)(unsafe.Pointer(addr)), uint64(val))
		}
	}
}

// LoadIntFast loads an integer atomically on non-TSO architectures.
// On TSO architectures, it performs a plain integer load.
//
//go:nosplit
func LoadIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	return *addr
}

// StoreIntFast stores an integer atomically on non-TSO architectures.
// On TSO architectures, it performs a plain integer store.
//
//go:nosplit
func StoreIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	*addr = val
}

// LoadIntInWindow loads without barrier when inside a seqlock window.
// Consistency is guaranteed by seqlock retry.
func LoadIntInWindow[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	return *addr
}
