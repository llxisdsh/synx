//go:build synx_embedded_hash

package opt

const EmbeddedHash_ = true

// Entry_ is an immutable key-value entry type for [Map]
type Entry_[K comparable, V any] struct {
	Hash  uintptr
	Key   K
	Value V
}

//go:nosplit
func (e *Entry_[K, V]) GetHash() uintptr {
	return e.Hash
}

//go:nosplit
func (e *Entry_[K, V]) SetHash(h uintptr) {
	e.Hash = h
}

// FlatEntry_ is an entry type for [FlatMap]
type FlatEntry_[K comparable, V any] struct {
	Hash  uintptr
	Key   K
	Value V
}

//go:nosplit
func (e *FlatEntry_[K, V]) GetHash() uintptr {
	return e.Hash
}

//go:nosplit
func (e *FlatEntry_[K, V]) SetHash(h uintptr) {
	e.Hash = h
}
