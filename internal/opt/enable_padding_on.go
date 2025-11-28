//go:build synx_enable_padding

package opt

import (
	"unsafe"
)

// CounterStripe_ represents a striped counter to reduce contention.
type CounterStripe_ struct {
	C uintptr // Counter value, accessed atomically
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		C uintptr
	}{})%CacheLineSize) % CacheLineSize]byte
}
