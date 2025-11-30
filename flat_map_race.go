//go:build race

package synx

type FlatMap[K comparable, V any] = Map[K, V]

func NewFlatMap[K comparable, V any](options ...func(*MapConfig)) *FlatMap[K, V] {
	return NewMap[K, V](options...)
}

// Stubs to satisfy size-related tests under race build.
type flatRebuildState[K comparable, V any] struct{}
type flatTable[K comparable, V any] struct{}
type flatBucket[K comparable, V any] struct{}
