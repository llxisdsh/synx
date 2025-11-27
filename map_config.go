package synx

import (
	"fmt"
	"strings"
	"unsafe"
)

// ============================================================================
// Configuration
// ============================================================================

// MapConfig defines configurable options for Map initialization.
// This structure contains all the configuration parameters that can be used
// to customize the behavior and performance characteristics of a Map
// instance.
type MapConfig struct {
	// KeyHash specifies a custom hash function for keys.
	// If nil, the built-in hash function will be used.
	// Custom hash functions can improve performance for specific key types
	// or provide better hash distribution.
	KeyHash HashFunc

	// ValEqual specifies a custom equality function for values.
	// If nil, the built-in equality comparison will be used.
	// This is primarily used for compare-and-swap operations.
	// Note: Using Compare* methods with non-comparable value types
	// will panic if ValEqual is nil.
	ValEqual EqualFunc

	// SizeHint provides an estimate of the expected number of entries.
	// This is used to pre-allocate the underlying hash table with appropriate
	// capacity, reducing the need for resizing during initial population.
	// If zero or negative, the default minimum capacity will be used.
	// The actual capacity will be rounded up to the next power of 2.
	SizeHint int

	// AutoShrink controls whether the map can automatically shrink
	// when the load factor falls below the shrink threshold.
	// When false (default), the map will only grow and never shrink,
	// which provides better performance but may use more memory.
	// When true, the map will shrink when occupancy < 1/shrinkFraction.
	AutoShrink bool

	// HashOpt specifies the hash distribution optimization strategies to use.
	// These options control how hash values are converted to bucket indices
	// in the h1 function. Different strategies work better for different
	// key patterns and distributions.
	// If zero, AutoDistribution will be used (recommended for most cases).
	HashOpt HashOptimization
}

// WithCapacity configuring new Map instance with capacity enough
// to hold sizeHint entries. The capacity is treated as the minimal
// capacity, meaning that the underlying hash table will never shrink
// to a smaller capacity. If sizeHint is zero or negative, the value
// is ignored.
func WithCapacity(sizeHint int) func(*MapConfig) {
	return func(c *MapConfig) {
		c.SizeHint = sizeHint
	}
}

// WithAutoShrink configures automatic map shrinking when the load factor
// falls below the threshold (default: 1/shrinkFraction).
// Disabled by default to prioritize performance.
func WithAutoShrink() func(*MapConfig) {
	return func(c *MapConfig) {
		c.AutoShrink = true
	}
}

// HashOptimization defines hash distribution optimization strategies.
// These strategies control how hash values are converted to bucket indices
// in the h1 function, affecting performance for different key patterns.
type HashOptimization uint8

const (
	// AutoDistribution automatically selects the most suitable distribution
	// strategy (default).
	// Based on key type analysis: integer types use linear distribution,
	// other types use shift distribution. This provides optimal performance
	// for most use cases without manual tuning.
	AutoDistribution HashOptimization = iota

	// LinearDistribution uses division-based bucket index calculation.
	// Formula: h / entriesPerBucket
	// Optimal for: sequential integer keys (1,2,3,4...), ordered data,
	// auto-incrementing IDs, or any pattern with continuous key values.
	// Provides better cache locality and reduces hash collisions for such
	// patterns.
	LinearDistribution

	// ShiftDistribution uses bit-shifting for bucket index calculation.
	// Formula: h >> 7
	// Optimal for: randomly distributed keys, string keys, complex types,
	// or any pattern with pseudo-random hash distribution.
	// Faster computation but may have suboptimal distribution for sequential
	// keys.
	ShiftDistribution
)

// WithKeyHasher sets a custom key hashing function for the map.
// This allows you to optimize hash distribution for specific key types
// or implement custom hashing strategies.
//
// Parameters:
//   - keyHash: custom hash function that takes a key and seed,
//     returns hash value. Pass nil to use the default built-in hasher
//   - opt: optional hash distribution optimization strategies.
//     Controls how hash values are converted to bucket indices.
//     If not specified, AutoDistribution will be used.
//
// Usage:
//
//	// Basic custom hasher
//	m := NewMap[string, int](WithKeyHasher(myCustomHashFunc))
//
//	// Custom hasher with linear distribution for sequential keys
//	m := NewMap[int, string](WithKeyHasher(myIntHasher, LinearDistribution))
//
//	// Custom hasher with shift distribution for random keys
//	m := NewMap[string, int](WithKeyHasher(myStringHasher, ShiftDistribution))
//
// Use cases:
//   - Optimize hash distribution for specific data patterns
//   - Implement case-insensitive string hashing
//   - Custom hashing for complex key types
//   - Performance tuning for known key distributions
//   - Combine with distribution strategies for optimal performance
func WithKeyHasher[K comparable](
	keyHash func(key K, seed uintptr) uintptr,
	opt ...HashOptimization,
) func(*MapConfig) {
	return func(c *MapConfig) {
		if keyHash != nil {
			c.KeyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
				return keyHash(*(*K)(pointer), u)
			}
			if len(opt) != 0 {
				c.HashOpt = opt[0]
			}
		}
	}
}

// WithKeyHasherUnsafe sets a low-level unsafe key hashing function.
// This is the high-performance version that operates directly on memory
// pointers. Use this when you need maximum performance and are comfortable with
// unsafe operations.
//
// Parameters:
//   - hs: unsafe hash function that operates on raw unsafe.Pointer.
//     The pointer points to the key data in memory.
//     Pass nil to use the default built-in hasher
//   - opt: optional hash distribution optimization strategies.
//     Controls how hash values are converted to bucket indices.
//     If not specified, AutoDistribution will be used.
//
// Usage:
//
//	// Basic unsafe hasher
//	unsafeHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
//		// Cast ptr to your key type and implement hashing
//		key := *(*string)(ptr)
//		return uintptr(len(key)) // example hash
//	}
//	m := NewMap[string, int](WithKeyHasherUnsafe(unsafeHasher))
//
//	// Unsafe hasher with specific distribution strategy
//	m := NewMap[int, string](WithKeyHasherUnsafe(fastIntHasher,
//
// LinearDistribution))
//
// Notes:
//   - You must correctly cast unsafe.Pointer to the actual key type
//   - Incorrect pointer operations will cause crashes or memory corruption
//   - Only use if you understand Go's unsafe package
//   - Distribution strategies still apply to the hash output
func WithKeyHasherUnsafe(
	hs HashFunc,
	opt ...HashOptimization,
) func(*MapConfig) {
	return func(c *MapConfig) {
		c.KeyHash = hs
		if len(opt) != 0 {
			c.HashOpt = opt[0]
		}
	}
}

// WithValueEqual sets a custom value equality function for the map.
// This is essential for CompareAndSwap and CompareAndDelete operations
// when working with non-comparable value types or custom equality logic.
//
// Parameters:
//   - valEqual: custom equality function that compares two values
//     Pass nil to use the default built-in comparison (for comparable types)
//
// Usage:
//
//	// For custom structs with specific equality logic
//	EqualFunc := func(a, b MyStruct) bool {
//		return a.ID == b.ID && a.Name == b.Name
//	}
//	m := NewMap[string, MyStruct](WithValueEqual(EqualFunc))
//
// Use cases:
//   - Custom equality for structs (compare specific fields)
//   - Case-insensitive string comparison
//   - Floating-point comparison with tolerance
//   - Deep equality for slices/maps
//   - Required for non-comparable types (slices, maps, functions)
func WithValueEqual[V any](
	valEqual func(val, val2 V) bool,
) func(*MapConfig) {
	return func(c *MapConfig) {
		if valEqual != nil {
			c.ValEqual = func(val unsafe.Pointer, val2 unsafe.Pointer) bool {
				return valEqual(*(*V)(val), *(*V)(val2))
			}
		}
	}
}

// WithValueEqualUnsafe sets a low-level unsafe value equality function.
// This is the high-performance version that operates directly on memory
// pointers. Use this when you need maximum performance and are comfortable with
// unsafe operations.
//
// Parameters:
//   - eq: unsafe equality function that operates on raw unsafe.Pointer
//     Both pointers point to value data in memory
//     Pass nil to use the default built-in comparison
//
// Usage:
//
//	unsafeEqual := func(ptr, other unsafe.Pointer) bool {
//		// Cast pointers to your value type and implement comparison
//		val1 := *(*MyStruct)(ptr)
//		val2 := *(*MyStruct)(other)
//		return val1.ID == val2.ID // example comparison
//	}
//	m := NewMap[string, MyStruct](WithValueEqualUnsafe(unsafeEqual))
//
// Notes:
//   - You must correctly cast unsafe.Pointer to the actual value type
//   - Both pointers must point to valid memory of the same type
//   - Incorrect pointer operations will cause crashes or memory corruption
//   - Only use if you understand Go's unsafe package
func WithValueEqualUnsafe(eq EqualFunc) func(*MapConfig) {
	return func(c *MapConfig) {
		c.ValEqual = eq
	}
}

// WithBuiltInHasher returns a MapConfig option that explicitly sets the
// built-in hash function for the specified type.
//
// This option is useful when you want to explicitly use Go's built-in hasher
// instead of any optimized variants. It ensures that the map uses the same
// hashing strategy as Go's native map implementation.
//
// Performance characteristics:
// - Provides consistent performance across all key sizes
// - Uses Go's optimized internal hash functions
// - Guaranteed compatibility with future Go versions
// - May be slower than specialized hashers for specific use cases
//
// Usage:
//
//	m := NewMap[string, int](WithBuiltInHasher[string]())
func WithBuiltInHasher[T comparable]() func(*MapConfig) {
	return func(c *MapConfig) {
		c.KeyHash = GetBuiltInHasher[T]()
	}
}

// IHashCode defines a custom hash function interface for key types.
// Key types implementing this interface can provide their own hash computation,
// serving as an alternative to WithKeyHasher for type-specific optimization.
//
// This interface is automatically detected during Map initialization and
// takes
// precedence over the default built-in hasher but is overridden by explicit
// WithKeyHasher configuration.
//
// Usage:
//
//	type UserID struct {
//		ID int64
//		Tenant string
//	}
//
//	func (u *UserID) HashCode(seed uintptr) uintptr {
//		return uintptr(u.ID) ^ seed
//	}
type IHashCode interface {
	HashCode(seed uintptr) uintptr
}

// IHashOpt defines hash distribution optimization interface for key types.
// Key types implementing this interface can specify their preferred hash
// distribution strategy, serving as an alternative to WithKeyHasher's opts
// parameter.
//
// Note: IHashOpts only works if Key implements IHashCode.
//
// This interface is automatically detected during Map initialization and
// provides fine-grained control over hash-to-bucket mapping strategies.
//
// Usage:
//
//	type SequentialID int64
//
//	func (*SequentialID) HashOpt() HashOptimization {
//		return LinearDistribution
//	}
type IHashOpt interface {
	HashOpt() HashOptimization
}

// IEqual defines a custom equality comparison interface for value types.
// Value types implementing this interface can provide their own equality logic,
// serving as an alternative to WithValueEqual for type-specific comparison.
//
// This interface is automatically detected during Map initialization and is
// essential for non-comparable value types or custom equality semantics.
// It takes precedence over the default built-in comparison but is overridden
// by explicit WithValueEqual configuration.
//
// Usage:
//
//	type UserProfile struct {
//		Name string
//		Tags []string // slice makes this non-comparable
//	}
//
//	func (u *UserProfile) Equal(other UserProfile) bool {
//		return u.Name == other.Name && slices.Equal(u.Tags, other.Tags)
//	}
type IEqual[T any] interface {
	Equal(other T) bool
}

func parseKeyInterface[K comparable]() (keyHash HashFunc, hashOpt HashOptimization) {
	var k *K
	if _, ok := any(k).(IHashCode); ok {
		keyHash = func(ptr unsafe.Pointer, seed uintptr) uintptr {
			return any((*K)(ptr)).(IHashCode).HashCode(seed)
		}
		if _, ok = any(k).(IHashOpt); ok {
			hashOpt = any(*new(K)).(IHashOpt).HashOpt()
		}
	}
	return
}

func parseValueInterface[V any]() (valEqual EqualFunc) {
	var v *V
	if _, ok := any(v).(IEqual[V]); ok {
		valEqual = func(ptr unsafe.Pointer, other unsafe.Pointer) bool {
			return any((*V)(ptr)).(IEqual[V]).Equal(*(*V)(other))
		}
	}
	return
}

func (cfg *MapConfig) parseIntKey(intKey *bool) {
	switch cfg.HashOpt {
	case LinearDistribution:
		*intKey = true
	case ShiftDistribution:
		*intKey = false
	default:
		// AutoDistribution: default distribution
	}
}

// ============================================================================
// Statistics
// ============================================================================

// MapStats is Map statistics.
//
// Notes:
//   - map statistics are intended to be used for diagnostic
//     purposes, not for production code. This means that breaking changes
//     may be introduced into this struct even between minor releases.
type MapStats struct {
	// RootBuckets is the number of root buckets in the hash table.
	// Each bucket holds a few entries.
	RootBuckets int
	// TotalBuckets is the total number of buckets in the hash table,
	// including root and their chained buckets. Each bucket holds
	// a few entries.
	TotalBuckets int
	// EmptyBuckets is the number of buckets that hold no entries.
	EmptyBuckets int
	// Capacity is the Map capacity, i.e., the total number of
	// entries that all buckets can physically hold. This number
	// does not consider the loadEntry factor.
	Capacity int
	// Size is the exact number of entries stored in the map.
	Size int
	// Counter is the number of entries stored in the map according
	// to the internal atomic counter. In the case of concurrent map
	// modifications, this number may be different from Size.
	Counter int
	// CounterLen is the number of internal atomic counter stripes.
	// This number may grow with the map capacity to improve
	// multithreaded scalability.
	CounterLen int
	// MinEntries is the minimum number of entries per a chain of
	// buckets, i.e., a root bucket and its chained buckets.
	MinEntries int
	// MinEntries is the maximum number of entries per a chain of
	// buckets, i.e., a root bucket and its chained buckets.
	MaxEntries int
	// TotalGrowths is the number of times the hash table grew.
	TotalGrowths uint32
	// TotalGrowths is the number of times the hash table shrunk.
	TotalShrinks uint32
}

// String returns string representation of map stats.
func (s *MapStats) String() string {
	var sb strings.Builder
	sb.WriteString("MapStats{\n")
	sb.WriteString(fmt.Sprintf("RootBuckets:  %d\n", s.RootBuckets))
	sb.WriteString(fmt.Sprintf("TotalBuckets: %d\n", s.TotalBuckets))
	sb.WriteString(fmt.Sprintf("EmptyBuckets: %d\n", s.EmptyBuckets))
	sb.WriteString(fmt.Sprintf("Capacity:     %d\n", s.Capacity))
	sb.WriteString(fmt.Sprintf("Size:         %d\n", s.Size))
	sb.WriteString(fmt.Sprintf("Counter:      %d\n", s.Counter))
	sb.WriteString(fmt.Sprintf("CounterLen:   %d\n", s.CounterLen))
	sb.WriteString(fmt.Sprintf("MinEntries:   %d\n", s.MinEntries))
	sb.WriteString(fmt.Sprintf("MaxEntries:   %d\n", s.MaxEntries))
	sb.WriteString(fmt.Sprintf("TotalGrowths: %d\n", s.TotalGrowths))
	sb.WriteString(fmt.Sprintf("TotalShrinks: %d\n", s.TotalShrinks))
	sb.WriteString("}\n")
	return sb.String()
}

// ============================================================================
// Hash Utilities
// ============================================================================

// GetBuiltInHasher returns Go's built-in hash function for the specified type.
// This function provides direct access to the same hash function that Go's
// built-in map uses internally, ensuring optimal performance and compatibility.
//
// The returned hash function is type-specific and optimized for the given
// comparable type T. It uses Go's internal type representation to access
// the most efficient hashing implementation available.
//
// Usage:
//
//	hashFunc := GetBuiltInHasher[string]()
//	m := NewMap[string, int](WithKeyHasherUnsafe(GetBuiltInHasher[string]()))
func GetBuiltInHasher[T comparable]() HashFunc {
	keyHash, _ := defaultHasherUsingBuiltIn[T, struct{}]()
	return keyHash
}
