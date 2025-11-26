//go:build !synx_enable_padding

package opt

// CounterStripe represents a striped counter to reduce contention.
type CounterStripe struct {
	C uintptr // Counter value, accessed atomically
}
