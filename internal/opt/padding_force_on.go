//go:build synx_enable_padding

package opt

import (
	"unsafe"
)

// CounterStripe_ represents a striped counter to reduce contention.
// Padding is force-enabled via the synx_enable_padding build tag.
// Use: go build -tags=synx_enable_padding
type CounterStripe_ struct {
	C uintptr // Counter value, accessed atomically
	_ [(CacheLineSize_ - unsafe.Sizeof(struct {
		C uintptr
	}{})%CacheLineSize_) % CacheLineSize_]byte
}
