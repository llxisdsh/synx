//go:build !synx_cachelinesize_32 && !synx_cachelinesize_64 && !synx_cachelinesize_128 && !synx_cachelinesize_256

package opt

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

// CacheLineSize_ is used in structure padding to prevent false sharing.
// It's automatically calculated using the `golang.org/x/sys` package.
const CacheLineSize_ = unsafe.Sizeof(cpu.CacheLinePad{})
