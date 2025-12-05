//go:build synx_disable_padding

package opt

// CounterStripe_ represents a striped counter to reduce contention.
// Padding is force-disabled via the synx_disable_padding build tag.
// Use: go build -tags=synx_disable_padding
type CounterStripe_ struct {
	C uintptr // Counter value, accessed atomically
}
