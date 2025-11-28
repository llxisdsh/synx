//go:build !synx_embedded_hash

package opt

const EmbeddedHash_ = false

// Entry_ is an immutable key-value entry type for [Map]
type Entry_[K comparable, V any] struct {
	Key   K
	Value V
}

//go:nosplit
func (e *Entry_[K, V]) GetHash() uintptr {
	return 0
}

//go:nosplit
func (e *Entry_[K, V]) SetHash(_ uintptr) {
}

// FlatEntry_ is an entry type for [FlatMap]
type FlatEntry_[K comparable, V any] struct {
	Key   K
	Value V
}

//go:nosplit
func (e *FlatEntry_[K, V]) GetHash() uintptr {
	return 0
}

//go:nosplit
func (e *FlatEntry_[K, V]) SetHash(_ uintptr) {
}
