//go:build (amd64 || 386 || arm || mips || mipsle || wasm) && !synx_disable_padding && !synx_enable_padding

package opt

// CounterStripe_ represents a striped counter to reduce contention.
// Padding is disabled by default for:
// - amd64
// - 32-bit architectures (386, arm, mips, mipsle, wasm)
type CounterStripe_ struct {
	C uintptr // Counter value, accessed atomically
}
