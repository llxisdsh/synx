//go:build !race

package opt

import (
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// IsTSO detects TSO architectures; on TSO, plain reads/writes are safe for
// pointers and native word-sized integers
const IsTSO = runtime.GOARCH == "amd64" ||
	runtime.GOARCH == "386" ||
	runtime.GOARCH == "s390x"

// LoadPtr loads a pointer atomically on non-TSO architectures.
// On TSO architectures, it performs a plain pointer load.
//
//go:nosplit
func LoadPtr(addr *unsafe.Pointer) unsafe.Pointer {
	//goland:noinspection ALL
	if IsTSO {
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
	//goland:noinspection ALL
	if IsTSO {
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
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(uint32(0)) {
		//goland:noinspection ALL
		if IsTSO {
			return *addr
		} else {
			return T(atomic.LoadUint32((*uint32)(unsafe.Pointer(addr))))
		}
	} else {
		//goland:noinspection ALL
		if IsTSO && bits.UintSize >= 64 {
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
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(uint32(0)) {
		//goland:noinspection ALL
		if IsTSO {
			*addr = val
		} else {
			atomic.StoreUint32((*uint32)(unsafe.Pointer(addr)), uint32(val))
		}
	} else {
		//goland:noinspection ALL
		if IsTSO && bits.UintSize >= 64 {
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
