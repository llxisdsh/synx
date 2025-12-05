//go:build !(amd64 || 386 || arm || mips || mipsle || wasm) && !synx_disable_padding && !synx_enable_padding

package opt

import (
	"unsafe"
)

// CounterStripe_ represents a striped counter to reduce contention.
// Padding is automatically enabled for architectures that are NOT:
// - amd64 (x86_64): Hardware optimizations often make padding less critical
// - 32-bit architectures (386, arm, mips, mipsle, wasm): Smaller cache lines/memory constraints
//
// Enabled for: arm64, s390x, ppc64, ppc64le, riscv64, loong64, mips64, mips64le, etc.
type CounterStripe_ struct {
	C uintptr // Counter value, accessed atomically
	_ [(CacheLineSize_ - unsafe.Sizeof(struct {
		C uintptr
	}{})%CacheLineSize_) % CacheLineSize_]byte
}
