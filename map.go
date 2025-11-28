package synx

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	. "github.com/llxisdsh/synx/internal/opt" // nolint:staticcheck
)

// Map is a high-performance concurrent map implementation that is fully
// compatible with sync.Map API and significantly outperforms sync.Map in
// most scenarios.
//
// Core advantages:
//   - Lock-free reads, fine-grained locking for writes
//   - Zero-value ready with lazy initialization
//   - Custom hash and value comparison function support
//   - Rich batch operations and functional extensions
//
// Usage recommendations:
//   - Direct declaration: var m Map[string, int]
//   - Pre-allocate capacity: NewMap(WithCapacity(1000))
//
// Notes:
//   - Map must not be copied after first use.
type Map[K comparable, V any] struct {
	_        noCopy
	table    unsafe.Pointer // *mapTable
	rs       unsafe.Pointer // *rebuildState
	growths  uint32
	shrinks  uint32
	seed     uintptr
	keyHash  HashFunc  // WithKeyHasher
	valEqual EqualFunc // WithValueEqual
	minLen   int       // WithCapacity
	shrinkOn bool      // WithAutoShrink
	intKey   bool
}

// rebuildState represents the current state of a resizing operation
type rebuildState struct {
	hint      mapRebuildHint
	wg        sync.WaitGroup
	table     unsafe.Pointer // *mapTable
	newTable  unsafe.Pointer // *mapTable
	process   int32
	completed int32
}

// mapTable represents the internal hash table structure.
type mapTable struct {
	buckets  unsafeSlice[bucket]
	mask     int
	size     unsafeSlice[CounterStripe_]
	sizeMask int
	// number of chunks and chunks size for resizing
	chunks  int
	chunkSz int
}

// bucket represents a hash table bucket with cache-line alignment.
type bucket struct {
	// meta: SWAR-optimized metadata for fast entry lookups
	// (must be 64-bit aligned)
	_    [0]atomic.Uint64
	meta uint64

	// Cache line layout is optimal;
	// omit padding fields to reduce memory footprint.
	// _ [(CacheLineSize - unsafe.Sizeof(struct {
	// 	_       [0]int64
	// 	meta    uint64
	// 	entries [entriesPerBucket]unsafe.Pointer
	// 	next    unsafe.Pointer
	// }{})%CacheLineSize) % CacheLineSize]byte

	entries [entriesPerBucket]unsafe.Pointer // *opt.Entry_
	next    unsafe.Pointer                   // *bucket
}

// NewMap creates a new Map instance. Direct initialization is also
// supported.
//
// Parameters:
//   - options: configuration options (WithCapacity, WithKeyHasher, etc.)
func NewMap[K comparable, V any](
	options ...func(*MapConfig),
) *Map[K, V] {
	m := &Map[K, V]{}
	m.withOptions(options...)
	return m
}

// withOptions initializes the Map instance using variadic option
// parameters. This is a convenience method that allows configuring Map
// through the functional options pattern.
//
// Configuration Priority (highest to lowest):
//   - Explicit With* functions (WithKeyHasher, WithValueEqual)
//   - Interface implementations (IHashFunc, IIntKey, IEqualFunc)
//   - Default built-in implementations (defaultHasher) - fallback
//
// Parameters:
//   - options: configuration option functions such as WithCapacity,
//     WithAutoShrink, WithKeyHasher, WithValueEqual, etc.
//
// Usage example:
//
//	m.withOptions(WithCapacity(1000), WithAutoShrink())
//
// Notes:
//   - This function is not thread-safe and should only be called before Map
//     is used
//   - If this function is not called, Map will use default configuration
//   - The behavior of calling this function multiple times is undefined
func (m *Map[K, V]) withOptions(
	options ...func(*MapConfig),
) {
	var cfg MapConfig

	// parse options
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m.init(noEscape(&cfg))
}

func (m *Map[K, V]) init(
	cfg *MapConfig,
) *mapTable {
	// parse interface
	if cfg.keyHash == nil {
		cfg.keyHash, cfg.intKey = parseKeyInterface[K]()
	}
	if cfg.valEqual == nil {
		cfg.valEqual = parseValueInterface[V]()
	}
	// perform initialization
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	if cfg.keyHash != nil {
		m.keyHash = cfg.keyHash
		if cfg.intKey {
			m.intKey = true
		}
	}
	if cfg.valEqual != nil {
		m.valEqual = cfg.valEqual
	}

	m.seed = uintptr(rand.Uint64())
	m.minLen = calcTableLen(cfg.capacity)
	m.shrinkOn = cfg.autoShrink

	table := newMapTable(m.minLen, runtime.GOMAXPROCS(0))
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	return table
}

// slowInit may be called concurrently by multiple goroutines, so it requires
// synchronization with a "lock" mechanism.
//
//go:noinline
func (m *Map[K, V]) slowInit() *mapTable {
	rs := (*rebuildState)(LoadPtr(&m.rs))
	if rs != nil {
		rs.wg.Wait()
		// Now the table should be initialized
		return (*mapTable)(LoadPtr(&m.table))
	}

	rs, ok := m.beginRebuild(mapRebuildBlockWritersHint)
	if !ok {
		// Another goroutine is initializing, wait for it to complete
		rs = (*rebuildState)(LoadPtr(&m.rs))
		if rs != nil {
			rs.wg.Wait()
		}
		// Now the table should be initialized
		return (*mapTable)(LoadPtr(&m.table))
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := (*mapTable)(LoadPtr(&m.table))
	if table != nil {
		m.endRebuild(rs)
		return table
	}

	// Perform initialization
	var cfg MapConfig
	table = m.init(&cfg)
	m.endRebuild(rs)
	return table
}

// Load retrieves a value for the given key, compatible with `sync.Map`.
//
//go:nosplit
func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	if e := m.loadEntry(table, hash, &key); e != nil {
		return e.Value, true
	}
	return
}

// LoadOrStore retrieves an existing value or stores a new one if the key
// doesn't exist, compatible with `sync.Map`.
func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		table = m.slowInit()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		if e := m.loadEntry(table, hash, &key); e != nil {
			return e.Value, true
		}
	}

	return m.computeEntry(table, hash, &key,
		func(e *Entry_[K, V]) (*Entry_[K, V], V, bool) {
			if e != nil {
				return e, e.Value, true
			}
			return &Entry_[K, V]{Value: value}, value, false
		},
	)
}

// LoadOrStoreFn returns the existing value for the key if
// present. Otherwise, it tries to compute the value using the
// provided function and, if successful, stores and returns
// the computed value. The loaded result is true if the value was
// loaded, or false if computed.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the newValueFn executes. Consider
// this when the function includes long-running operations.
func (m *Map[K, V]) LoadOrStoreFn(
	key K,
	newValueFn func() V,
) (actual V, loaded bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		table = m.slowInit()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		if e := m.loadEntry(table, hash, &key); e != nil {
			return e.Value, true
		}
	}

	return m.computeEntry(table, hash, &key,
		func(e *Entry_[K, V]) (*Entry_[K, V], V, bool) {
			if e != nil {
				return e, e.Value, true
			}
			newValue := newValueFn()
			return &Entry_[K, V]{Value: newValue}, newValue, false
		},
	)
}

// LoadAndUpdate retrieves the value associated with the given key and updates
// it if the key exists.
//
// Parameters:
//   - key: The key to look up in the map.
//   - value: The new value to set if the key exists.
//
// Returns:
//   - previous: The loaded value associated with the key (if it existed),
//     otherwise a zero-value of V.
//   - loaded: True if the key existed and the value was updated,
//     false otherwise.
func (m *Map[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.loadEntry(table, hash, &key)
		if e == nil {
			return
		}

		// deduplicates identical values
		if m.valEqual != nil {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.Value)),
				noescape(unsafe.Pointer(&value)),
			) {
				return value, true
			}
		}
	}

	return m.computeEntry(table, hash, &key,
		func(e *Entry_[K, V]) (*Entry_[K, V], V, bool) {
			if e != nil {
				return &Entry_[K, V]{Value: value}, e.Value, true
			}
			return nil, *(new(V)), false
		},
	)
}

// LoadAndDelete retrieves the value for a key and deletes it from the map.
// compatible with `sync.Map`.
func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.loadEntry(table, hash, &key)
		if e == nil {
			return
		}
	}

	return m.computeEntry(table, hash, &key,
		func(e *Entry_[K, V]) (*Entry_[K, V], V, bool) {
			if e != nil {
				return nil, e.Value, true
			}
			return nil, *new(V), false
		},
	)
}

// Store inserts or updates a key-value pair, compatible with `sync.Map`.
func (m *Map[K, V]) Store(key K, value V) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		table = m.slowInit()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		// deduplicates identical values
		if m.valEqual != nil {
			if e := m.loadEntry(table, hash, &key); e != nil {
				if m.valEqual(
					noescape(unsafe.Pointer(&e.Value)),
					noescape(unsafe.Pointer(&value)),
				) {
					return
				}
			}
		}
	}

	m.computeEntry(table, hash, &key,
		func(*Entry_[K, V]) (*Entry_[K, V], V, bool) {
			return &Entry_[K, V]{Value: value}, *new(V), false
		},
	)
}

// Swap stores a key-value pair and returns the previous value if any,
// compatible with `sync.Map`.
func (m *Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		table = m.slowInit()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		// deduplicates identical values
		if m.valEqual != nil {
			if e := m.loadEntry(table, hash, &key); e != nil {
				if m.valEqual(
					noescape(unsafe.Pointer(&e.Value)),
					noescape(unsafe.Pointer(&value)),
				) {
					return value, true
				}
			}
		}
	}

	return m.computeEntry(table, hash, &key,
		func(e *Entry_[K, V]) (*Entry_[K, V], V, bool) {
			if e != nil {
				return &Entry_[K, V]{Value: value}, e.Value, true
			}
			return &Entry_[K, V]{Value: value}, *new(V), false
		},
	)
}

// Delete removes a key-value pair, compatible with `sync.Map`.
func (m *Map[K, V]) Delete(key K) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.loadEntry(table, hash, &key)
		if e == nil {
			return
		}
	}

	m.computeEntry(table, hash, &key,
		func(*Entry_[K, V]) (*Entry_[K, V], V, bool) {
			return nil, *new(V), false
		},
	)
}

// CompareAndSwap atomically replaces an existing value with a new value
// if the existing value matches the expected value, compatible with `sync.Map`.
func (m *Map[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return false
	}

	if m.valEqual == nil {
		panic("called CompareAndSwap when value is not of comparable type")
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.loadEntry(table, hash, &key)
		if e == nil {
			return false
		}

		if !m.valEqual(
			noescape(unsafe.Pointer(&e.Value)),
			noescape(unsafe.Pointer(&old)),
		) {
			return false
		}
		// deduplicates identical values
		if m.valEqual(
			noescape(unsafe.Pointer(&e.Value)),
			noescape(unsafe.Pointer(&new)),
		) {
			return true
		}
	}

	_, swapped = m.computeEntry(table, hash, &key,
		func(e *Entry_[K, V]) (*Entry_[K, V], V, bool) {
			var zero V
			if e != nil &&
				m.valEqual(
					noescape(unsafe.Pointer(&e.Value)),
					noescape(unsafe.Pointer(&old)),
				) {
				return &Entry_[K, V]{Value: new}, zero, true
			}
			return e, zero, false
		},
	)
	return swapped
}

// CompareAndDelete atomically deletes an existing entry
// if its value matches the expected value, compatible with `sync.Map`.
func (m *Map[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return false
	}

	if m.valEqual == nil {
		panic("called CompareAndDelete when value is not of comparable type")
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.loadEntry(table, hash, &key)
		if e == nil {
			return false
		}
		if !m.valEqual(
			noescape(unsafe.Pointer(&e.Value)),
			noescape(unsafe.Pointer(&old)),
		) {
			return false
		}
	}

	_, deleted = m.computeEntry(table, hash, &key,
		func(e *Entry_[K, V]) (*Entry_[K, V], V, bool) {
			var zero V
			if e != nil &&
				m.valEqual(
					noescape(unsafe.Pointer(&e.Value)),
					noescape(unsafe.Pointer(&old)),
				) {
				return nil, zero, true
			}
			return e, zero, false
		},
	)
	return deleted
}

// MapEntry represents an entry being processed in Compute/ComputeRange operations.
// It provides methods to update or delete the entry during iteration.
// WARNING: Transient view valid only within the callback. Never store or retain
// this struct or its pointer beyond the callback; doing so may lead to dangling
// pointers and invalid references.
// 警告：此结构体仅在回调期间有效；不要保存或持有其指针，避免悬垂指针。
type MapEntry[K comparable, V any] struct {
	entry  Entry_[K, V]
	loaded bool
	op     computeOp
}

// Key returns the key of the current entry.
//
//go:nosplit
func (e *MapEntry[K, V]) Key() K {
	return e.entry.Key
}

// Value returns the value of the current entry.
//
//go:nosplit
func (e *MapEntry[K, V]) Value() V {
	return e.entry.Value
}

// Loaded returns true if the entry is loaded, false otherwise.
//
//go:nosplit
func (e *MapEntry[K, V]) Loaded() bool {
	return e.loaded
}

// Update performs an upsert on the current entry.
// If Loaded()==true, replaces the value;
// if false, inserts the key with newValue.
//
//go:nosplit
func (e *MapEntry[K, V]) Update(newValue V) {
	e.entry.Value = newValue
	e.op = updateOp
}

// Delete marks the current entry for deletion.
// The entry will be removed from the map.
//
//go:nosplit
func (e *MapEntry[K, V]) Delete() {
	e.entry.Value = *new(V)
	e.op = deleteOp
}

// Compute performs a compute-style, atomic update for the given key.
//
// Concurrency model:
//   - Acquires the root-bucket lock to serialize write/resize cooperation.
//   - If a resize is observed, cooperates to finish copying and restarts on
//     the latest table.
//
// Callback signature:
//
//		fn(e *MapEntry[K, V])
//
//	  - Use e.Loaded() and e.Value() to inspect the current state
//	  - Use e.Update(newV) to upsert; Use e.Delete() to remove
//
// Parameters:
//
//   - key: The key to process
//   - fn: Callback function (called regardless of value existence)
//
// Returns:
//   - actual: The value as returned by the callback.
//   - loaded: True if the key existed before the callback, false otherwise.
func (m *Map[K, V]) Compute(
	key K,
	fn func(e *MapEntry[K, V]),
) (actual V, loaded bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		table = m.slowInit()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	return m.computeEntry(table, hash, &key,
		func(e *Entry_[K, V]) (*Entry_[K, V], V, bool) {
			var it MapEntry[K, V]
			if e != nil {
				it.entry = *e
				it.loaded = true
			}
			fn(noEscape(&it))
			switch it.op {
			case updateOp:
				return &Entry_[K, V]{Value: it.Value()}, it.Value(), it.loaded
			case deleteOp:
				return nil, it.Value(), it.loaded
			default:
				return e, it.Value(), it.loaded
			}
		},
	)
}

// Range compatible with `sync.Map`.
//
//go:nosplit
func (m *Map[K, V]) Range(yield func(key K, value V) bool) {
	m.rangeEntry(func(e *Entry_[K, V]) bool {
		return yield(e.Key, e.Value)
	})
}

// All compatible with `sync.Map`.
//
//go:nosplit
func (m *Map[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// ComputeRange iterates all entries and applies a user callback.
//
// Callback signature:
//
//		fn(e *MapEntry[K, V]) bool
//
//	  - e.Update(newV): update the entry to newV
//	  - e.Delete(): delete the entry
//	  - default (no op): keep the entry unchanged
//	  - return true to continue; return false to stop iteration immediately
//	    (the entry passed to the final callback is not modified when stopping)
//
// Concurrency & consistency:
//   - Cooperates with concurrent grow/shrink; if a resize is detected, it
//     helps complete copying, then continues on the latest table.
//   - Holds the root-bucket lock while processing its bucket chain to
//     coordinate with writers/resize operations.
//   - Uses per-bucket seqlock discipline internally to minimize write windows
//     and preserve reader consistency.
//
// Parameters:
//   - fn: user function applied to each key-value pair.
//   - blockWriters: optional flag (default false). If true, concurrent writers
//     are blocked during iteration; resize operations are always exclusive.
//
// Recommendation: keep fn lightweight to reduce lock hold time.
func (m *Map[K, V]) ComputeRange(
	fn func(e *MapEntry[K, V]) bool,
	blockWriters ...bool,
) {
	it := MapEntry[K, V]{loaded: true}
	m.computeRangeEntry(func(e *Entry_[K, V]) (*Entry_[K, V], bool) {
		it.entry = *e
		it.op = cancelOp
		shouldContinue := fn(noEscape(&it))
		switch it.op {
		case updateOp:
			return &Entry_[K, V]{Value: it.entry.Value}, shouldContinue
		case deleteOp:
			return nil, shouldContinue
		default:
			return e, shouldContinue
		}
	}, blockWriters...)
}

// Entries returns an iterator function for use with range-over-func.
// It provides the same functionality as ComputeRange but in iterator form.
//
//go:nosplit
func (m *Map[K, V]) Entries(
	blockWriters ...bool,
) func(yield func(e *MapEntry[K, V]) bool) {
	return func(yield func(e *MapEntry[K, V]) bool) {
		m.ComputeRange(yield, blockWriters...)
	}
}

// Size returns the number of key-value pairs in the map.
// This is an O(1) operation.
//
//go:nosplit
func (m *Map[K, V]) Size() int {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return 0
	}
	return table.SumSize()
}

// IsZero checks zero values, faster than Size().
//
//go:nosplit
func (m *Map[K, V]) IsZero() bool {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return true
	}
	return !table.SumSizeExceeds(0)
}

// Clear compatible with `sync.Map`
func (m *Map[K, V]) Clear() {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return
	}
	m.rebuild(mapRebuildBlockWritersHint, func() {
		cpus := runtime.GOMAXPROCS(0)
		newTable := newMapTable(m.minLen, cpus)
		atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	})
}

// Grow increases the map's capacity by sizeAdd entries to accommodate future
// growth. This pre-allocation avoids rehashing when adding new entries up to
// the new capacity.
//
// Parameters:
//   - sizeAdd specifies the number of additional entries the map should be able
//     to hold.
//
// Notes:
//   - If the current remaining capacity already exceeds sizeAdd, no growth will
//     be triggered.
func (m *Map[K, V]) Grow(sizeAdd int) {
	if sizeAdd <= 0 {
		return
	}
	if LoadPtr(&m.table) == nil {
		m.slowInit()
	}
	m.doResize(mapGrowHint, sizeAdd)
}

// Shrink reduces the capacity to fit the current size,
// always executes regardless of WithAutoShrink.
func (m *Map[K, V]) Shrink() {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return
	}
	m.doResize(mapShrinkHint, -1)
}

func (m *Map[K, V]) doResize(
	hint mapRebuildHint,
	sizeAdd int,
) {
	var size int
	for {
		// Resize check
		table := (*mapTable)(LoadPtr(&m.table))
		tableLen := table.mask + 1
		if hint == mapGrowHint {
			if sizeAdd <= 0 {
				return
			}
			size = table.SumSize()
			newTableLen := calcTableLen(size + sizeAdd)
			if tableLen >= newTableLen {
				return
			}
		} else {
			// mapShrinkHint
			if tableLen <= m.minLen {
				return
			}
			// Recalculate the shrink size to avoid over-shrinking
			size = table.SumSize()
			newTableLen := calcTableLen(size)
			if tableLen <= newTableLen {
				return
			}
		}

		// Help finishing rebuild if needed
		if rs := (*rebuildState)(LoadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if LoadPtr(&rs.table) != nil /*skip init*/ &&
					LoadPtr(&rs.newTable) != nil /*skip newTable is nil*/ {
					m.helpCopyAndWait(rs)
				} else {
					runtime.Gosched()
					continue
				}
			default:
				rs.wg.Wait()
			}
		}

		m.tryResize(hint, size, sizeAdd)
	}
}

// String implement the formatting output interface fmt.Stringer
func (m *Map[K, V]) String() string {
	const limit = 1024
	return strings.Replace(
		fmt.Sprint(m.ToMap(limit)),
		"map[",
		"Map[",
		1,
	)
}

// ToMap collect up to limit entries into a map[K]V, limit < 0 is no limit
func (m *Map[K, V]) ToMap(limit ...int) map[K]V {
	l := math.MaxInt
	if len(limit) != 0 {
		l = limit[0]
		if l <= 0 {
			return map[K]V{}
		}
	}

	a := make(map[K]V, min(m.Size(), l))
	m.rangeEntry(func(e *Entry_[K, V]) bool {
		a[e.Key] = e.Value
		l--
		return l > 0
	})
	return a
}

// CloneTo copies all key-value pairs from this map to the destination map.
// The destination map is cleared before copying.
//
// Parameters:
//   - clone: The destination map to copy into. Must not be nil.
//
// Notes:
//
//   - This operation is not atomic with respect to concurrent modifications.
//
//   - The destination map will have the same configuration as the source.
//
//   - The destination map is cleared before copying to ensure a clean state.
func (m *Map[K, V]) CloneTo(clone *Map[K, V]) {
	clone.Clear()
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return
	}

	clone.seed = m.seed
	clone.keyHash = m.keyHash
	clone.valEqual = m.valEqual
	clone.minLen = m.minLen
	clone.shrinkOn = m.shrinkOn
	clone.intKey = m.intKey
	atomic.StorePointer(&clone.table,
		unsafe.Pointer(newMapTable(clone.minLen, runtime.GOMAXPROCS(0))),
	)

	// Pre-fetch size to optimize initial capacity
	clone.Grow(m.Size())
	m.rangeEntry(func(e *Entry_[K, V]) bool {
		cloneTable := (*mapTable)(LoadPtr(&clone.table))
		hash := m.keyHash(noescape(unsafe.Pointer(&e.Key)), m.seed)
		clone.computeEntry(cloneTable, hash, &e.Key,
			func(*Entry_[K, V]) (*Entry_[K, V], V, bool) {
				return e, e.Value, false
			},
		)
		return true
	})
}

//go:nosplit
func (m *Map[K, V]) loadEntry(
	table *mapTable,
	hash uintptr,
	key *K,
) *Entry_[K, V] {
	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1(hash, m.intKey)
	for b := table.buckets.At(idx); b != nil; b = (*bucket)(LoadPtr(&b.next)) {
		meta := LoadInt(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if e := (*Entry_[K, V])(LoadPtr(b.At(j))); e != nil {
				if EmbeddedHash_ {
					if e.GetHash() == hash && e.Key == *key {
						return e
					}
				} else {
					if e.Key == *key {
						return e
					}
				}
			}
		}
	}
	return nil
}

// computeEntry processes a key-value pair using the provided function.
//
// This method is the foundation for all modification operations in Map.
// It provides. Complete control over key-value pairs, allowing atomic reading,
// modification, deletion, or insertion of entries.
//
// Callback signature:
//
//	fn(e *Entry_[K, V]) (newEntry *Entry_[K, V], ret V, status bool)
//
//	 - e *Entry_[K, V]: current entry (nil if key does not exist).
//	 - newEntry: Executed only when the key is missing. It returns a new entry.
//	   If it returns nil, the map will not store any value.
//	 - ret/status: values returned to the caller of Compute, allowing the
//	   callback to provide computed results (e.g., final value and hit status)
//
// Parameters:
//
//   - key: The key to process
//   - fn: Callback function (called regardless of value existence)
//
// Returns:
//   - value: The ret value from fn
//   - status: The status value from fn
//
// Notes:
//   - The input parameter 'e' is immutable and should not be modified
//     directly.
//   - This method internally ensures goroutine safety and consistency
//   - If you need to modify a value, return a new Entry_ instance
//   - The fn function is executed while holding an internal lock.
//     Keep the execution time short to avoid blocking other operations.
//   - Avoid calling other map methods inside fn to prevent deadlocks.
//   - Do not perform expensive computations or I/O operations inside fn.
func (m *Map[K, V]) computeEntry(
	table *mapTable,
	hash uintptr,
	key *K,
	fn func(e *Entry_[K, V]) (*Entry_[K, V], V, bool),
) (V, bool) {
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	for {
		idx := table.mask & h1v
		root := table.buckets.At(idx)

		root.Lock()

		// This is the first check, checking if there is a rebuild operation in
		// progress before acquiring the bucket lock
		if rs := (*rebuildState)(LoadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if LoadPtr(&rs.table) != nil /*skip init*/ &&
					LoadPtr(&rs.newTable) != nil /*skip newTable is nil*/ {
					root.Unlock()
					m.helpCopyAndWait(rs)
					table = (*mapTable)(LoadPtr(&m.table))
					continue
				}
			case mapRebuildBlockWritersHint:
				root.Unlock()
				rs.wg.Wait()
				table = (*mapTable)(LoadPtr(&m.table))
				continue
			default:
				// mapRebuildWithWritersHint: allow concurrent writers
			}
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := (*mapTable)(LoadPtr(&m.table)); table != newTable {
			root.Unlock()
			table = newTable
			continue
		}

		var (
			oldEntry *Entry_[K, V]
			oldB     *bucket
			oldIdx   int
			oldMeta  uint64
			emptyB   *bucket
			emptyIdx int
			lastB    *bucket
		)

	findLoop:
		for b := root; b != nil; b = (*bucket)(b.next) {
			meta := LoadIntFast(&b.meta)
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*Entry_[K, V])(*b.At(j)); e != nil {
					if EmbeddedHash_ {
						if e.GetHash() == hash && e.Key == *key {
							oldEntry, oldB, oldIdx, oldMeta = e, b, j, meta
							break findLoop
						}
					} else {
						if e.Key == *key {
							oldEntry, oldB, oldIdx, oldMeta = e, b, j, meta
							break findLoop
						}
					}
				}
			}
			if emptyB == nil {
				if empty := (^meta) & metaMask; empty != 0 {
					emptyB = b
					emptyIdx = firstMarkedByteIndex(empty)
				}
			}
			lastB = b
		}

		// --- Compute Logic ---
		newEntry, value, status := fn(oldEntry)

		if oldEntry != nil {
			if newEntry == oldEntry {
				// No entry to update or delete
				root.Unlock()
				return value, status
			}
			if newEntry != nil {
				// Update
				if EmbeddedHash_ {
					newEntry.SetHash(hash)
				}
				newEntry.Key = *key
				StorePtr(
					oldB.At(oldIdx),
					unsafe.Pointer(newEntry),
				)
				root.Unlock()
				return value, status
			}
			// Delete
			StorePtr(oldB.At(oldIdx), nil)
			newMeta := setByte(oldMeta, emptySlot, oldIdx)
			if oldB == root {
				root.UnlockWithMeta(newMeta)
			} else {
				StoreInt(&oldB.meta, newMeta)
				root.Unlock()
			}
			table.AddSize(idx, -1)

			// Check if table shrinking is needed
			if m.shrinkOn && newMeta&metaDataMask == emptyMeta &&
				LoadPtr(&m.rs) == nil {
				tableLen := table.mask + 1
				if m.minLen < tableLen {
					size := table.SumSize()
					if size < tableLen*entriesPerBucket/shrinkFraction {
						m.tryResize(mapShrinkHint, size, 0)
					}
				}
			}
			return value, status
		}

		if newEntry == nil {
			// No entry to insert or delete
			root.Unlock()
			return value, status
		}

		// Insert
		if EmbeddedHash_ {
			newEntry.SetHash(hash)
		}
		newEntry.Key = *key
		if emptyB != nil {
			// publish pointer first, then meta; readers check meta before
			// pointer so they won't observe a partially-initialized entry,
			// and this reduces the window where meta is visible but pointer is
			// still nil
			StorePtr(emptyB.At(emptyIdx), unsafe.Pointer(newEntry))
			newMeta := setByte(LoadIntFast(&emptyB.meta), h2v, emptyIdx)
			if emptyB == root {
				root.UnlockWithMeta(newMeta)
			} else {
				StoreInt(&emptyB.meta, newMeta)
				root.Unlock()
			}
			table.AddSize(idx, 1)
			return value, status
		}

		// No empty slot, create new bucket and insert
		StorePtr(&lastB.next, unsafe.Pointer(&bucket{
			meta: setByte(emptyMeta, h2v, 0),
			entries: [entriesPerBucket]unsafe.Pointer{
				unsafe.Pointer(newEntry),
			},
		}))
		root.Unlock()
		table.AddSize(idx, 1)

		// Check if the table needs to grow
		if LoadPtr(&m.rs) == nil {
			tableLen := table.mask + 1
			size := table.SumSize()
			const capFactor = float64(entriesPerBucket) * loadFactor
			if size >= int(float64(tableLen)*capFactor) {
				m.tryResize(mapGrowHint, size, 0)
			}
		}

		return value, status
	}
}

// rangeEntry iterates over all entries in the map.
//   - yield: callback that processes each entry and return a boolean
//     to control iteration.
//     Return true to continue iteration, false to stop early.
//     The 'e' parameter is guaranteed to be non-nil during iteration.
//
// Notes:
//   - Never modify the Key or Value in an Entry_ under any circumstances.
//   - The iteration directly traverses bucket data. The data is not guaranteed
//     to be real-time but provides eventual consistency.
//     In extreme cases, the same value may be traversed twice
//     (if it gets deleted and re-added later during iteration).
func (m *Map[K, V]) rangeEntry(yield func(e *Entry_[K, V]) bool) {
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return
	}
	for i := 0; i <= table.mask; i++ {
		for b := table.buckets.At(i); b != nil; b = (*bucket)(LoadPtr(&b.next)) {
			meta := LoadInt(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*Entry_[K, V])(LoadPtr(b.At(j))); e != nil {
					if !yield(e) {
						return
					}
				}
			}
		}
	}
}

// computeRangeEntry iterates through all map entries while holding the
// bucket lock, applying fn to each entry. The iteration is thread-safe
// due to bucket-level locking.
//
// The fn callback (with the same signature as unsafeCompute) controls entry
// modification:
//   - Return modified entry: updates the value
//   - Return nil: deletes the entry
//   - Return original entry: no change
//
// Ideal for batch operations requiring atomic read-modify-write semantics.
//
// Parameters:
//   - fn: callback that processes each entry.
//     The 'e' parameter is guaranteed to be non-nil during iteration.
//   - blockWriters: optional flag (default false).
//     If true, concurrent writers are blocked; otherwise they are allowed.
//     Resize operations (grow/shrink) are always exclusive.
//
// Notes:
//   - The input parameter 'e' is immutable and should not be modified
//     directly
//   - If a resize/rebuild is detected, it cooperates to completion, then
//     iterates the new table while blocking subsequent resize/rebuild.
//   - Holds bucket lock for entire iteration - avoid long operations/deadlock
//     risks
func (m *Map[K, V]) computeRangeEntry(
	fn func(e *Entry_[K, V]) (*Entry_[K, V], bool),
	blockWriters ...bool,
) {
	if (*mapTable)(LoadPtr(&m.table)) == nil {
		return
	}

	hint := mapRebuildAllowWritersHint
	if len(blockWriters) != 0 && blockWriters[0] {
		hint = mapRebuildBlockWritersHint
	}

	m.rebuild(hint, func() {
		table := (*mapTable)(LoadPtr(&m.table))
		for i := 0; i <= table.mask; i++ {
			root := table.buckets.At(i)
			root.Lock()
			for b := root; b != nil; b = (*bucket)(b.next) {
				meta := LoadIntFast(&b.meta)
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					if e := (*Entry_[K, V])(*b.At(j)); e != nil {
						newEntry, shouldContinue := fn(e)

						if newEntry != nil {
							if newEntry != e {
								if EmbeddedHash_ {
									newEntry.SetHash(e.GetHash())
								}
								newEntry.Key = e.Key
								StorePtr(b.At(j), unsafe.Pointer(newEntry))
							}
						} else {
							StorePtr(b.At(j), nil)
							meta = setByte(meta, emptySlot, j)
							StoreInt(&b.meta, meta)
							table.AddSize(i, -1)
						}

						if !shouldContinue {
							root.Unlock()
							return
						}
					}
				}
			}
			root.Unlock()
		}
	})
}

func (m *Map[K, V]) beginRebuild(hint mapRebuildHint) (*rebuildState, bool) {
	rs := new(rebuildState)
	rs.hint = hint
	rs.wg.Add(1)
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return nil, false
	}
	return rs, true
}

func (m *Map[K, V]) endRebuild(rs *rebuildState) {
	atomic.StorePointer(&m.rs, nil)
	rs.wg.Done()
}

// rebuild reorganizes the map. Only these hints are supported:
//   - mapRebuildWithWritersHint: allows concurrent reads/writes
//   - mapExclusiveRebuildHint: allows concurrent reads
func (m *Map[K, V]) rebuild(
	hint mapRebuildHint,
	fn func(),
) {
	for {
		// Help finishing rebuild if needed
		if rs := (*rebuildState)(LoadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if LoadPtr(&rs.table) != nil /*skip init*/ &&
					LoadPtr(&rs.newTable) != nil /*skip newTable is nil*/ {
					m.helpCopyAndWait(rs)
				} else {
					runtime.Gosched()
					continue
				}
			default:
				rs.wg.Wait()
			}
		}

		if rs, ok := m.beginRebuild(hint); ok {
			fn()
			m.endRebuild(rs)
			return
		}
	}
}

//go:noinline
func (m *Map[K, V]) tryResize(
	hint mapRebuildHint,
	size, sizeAdd int,
) {
	rs, ok := m.beginRebuild(hint)
	if !ok {
		return
	}

	table := (*mapTable)(LoadPtr(&m.table))
	tableLen := table.mask + 1
	var newLen int
	if hint == mapGrowHint {
		if sizeAdd == 0 {
			newLen = max(calcTableLen(size), tableLen<<1)
		} else {
			newLen = calcTableLen(size + sizeAdd)
			if newLen <= tableLen {
				m.endRebuild(rs)
				return
			}
		}
		atomic.AddUint32(&m.growths, 1)
	} else {
		// mapShrinkHint
		if sizeAdd == 0 {
			newLen = tableLen >> 1
		} else {
			newLen = calcTableLen(size)
		}
		if newLen < m.minLen {
			m.endRebuild(rs)
			return
		}
		atomic.AddUint32(&m.shrinks, 1)
	}

	cpus := runtime.GOMAXPROCS(0)
	if cpus > 1 &&
		newLen*int(unsafe.Sizeof(bucket{})) >= asyncThreshold {
		// The big table, use goroutines to create new table and copy entries
		go m.finalizeResize(table, newLen, rs, cpus)
	} else {
		m.finalizeResize(table, newLen, rs, cpus)
	}
}

func (m *Map[K, V]) finalizeResize(
	table *mapTable,
	newLen int,
	rs *rebuildState,
	cpus int,
) {
	atomic.StorePointer(&rs.table, unsafe.Pointer(table))
	newTable := newMapTable(newLen, cpus)
	atomic.StorePointer(&rs.newTable, unsafe.Pointer(newTable))
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *Map[K, V]) helpCopyAndWait(rs *rebuildState) {
	table := (*mapTable)(LoadPtr(&rs.table))
	tableLen := table.mask + 1
	chunks := int32(table.chunks)
	chunkSz := table.chunkSz
	newTable := (*mapTable)(LoadPtr(&rs.newTable))
	isGrowth := (newTable.mask + 1) > tableLen
	for {
		process := atomic.AddInt32(&rs.process, 1)
		if process > chunks {
			// Wait copying completed
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSz
		end := min(start+chunkSz, tableLen)
		if isGrowth {
			m.copyBucket(table, start, end, newTable, false)
		} else {
			m.copyBucket(table, start, end, newTable, true)
		}
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			// Copying completed
			atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
			m.endRebuild(rs)
			return
		}
	}
}

func (m *Map[K, V]) copyBucket(
	table *mapTable,
	start, end int,
	newTable *mapTable,
	lockBucket bool,
) {
	seed := m.seed
	keyHash := m.keyHash
	intKey := m.intKey
	copied := 0
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*bucket)(b.next) {
			meta := LoadIntFast(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*Entry_[K, V])(*b.At(j)); e != nil {
					var hash uintptr
					if EmbeddedHash_ {
						hash = e.GetHash()
					} else {
						hash = keyHash(noescape(unsafe.Pointer(&e.Key)), seed)
					}
					idx := newTable.mask & h1(hash, intKey)
					destBucket := newTable.buckets.At(idx)
					h2v := h2(hash)

					if lockBucket {
						destBucket.Lock()
					}
					b := destBucket
				appendToBucket:
					for {
						meta := LoadIntFast(&b.meta)
						empty := (^meta) & metaMask
						if empty != 0 {
							emptyIdx := firstMarkedByteIndex(empty)
							StoreIntFast(&b.meta, setByte(meta, h2v, emptyIdx))
							*b.At(emptyIdx) = unsafe.Pointer(e)
							break appendToBucket
						}
						next := (*bucket)(b.next)
						if next == nil {
							b.next = unsafe.Pointer(&bucket{
								meta:    setByte(emptyMeta, h2v, 0),
								entries: [entriesPerBucket]unsafe.Pointer{unsafe.Pointer(e)},
							})
							break appendToBucket
						}
						b = next
					}
					if lockBucket {
						destBucket.Unlock()
					}
					copied++
				}
			}
		}
		srcBucket.Unlock()
	}
	if copied != 0 {
		// copyBucket is used during multithreaded growth, requiring a
		// thread-safe AddSize.
		newTable.AddSize(start, copied)
	}
}

func newMapTable(tableLen, cpus int) *mapTable {
	overCpus := cpus * resizeOverPartition
	chunkSz, chunks := calcParallelism(tableLen, minBucketsPerCPU, overCpus)
	sizeLen := calcSizeLen(tableLen, cpus)
	return &mapTable{
		buckets:  makeUnsafeSlice(make([]bucket, tableLen)),
		mask:     tableLen - 1,
		size:     makeUnsafeSlice(make([]CounterStripe_, sizeLen)),
		sizeMask: sizeLen - 1,
		chunks:   chunks,
		chunkSz:  chunkSz,
	}
}

// AddSize atomically adds delta to the size counter for the given bucket index.
//
//go:nosplit
func (t *mapTable) AddSize(idx, delta int) {
	atomic.AddUintptr(&t.size.At(t.sizeMask&idx).C, uintptr(delta))
}

// SumSize calculates the total number of entries in the table
// by summing all counter-stripes.
//
//go:nosplit
func (t *mapTable) SumSize() int {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += LoadInt(&t.size.At(i).C)
	}
	return int(sum)
}

//go:nosplit
func (t *mapTable) SumSizeExceeds(limit int) bool {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += LoadInt(&t.size.At(i).C)
		if int(sum) > limit {
			return true
		}
	}
	return false
}

// Lock acquires a spinlock for the bucket using embedded metadata.
// Uses atomic operations on the meta field to avoid false sharing overhead.
// Implements optimistic locking with fallback to spinning.
//
//go:nosplit
func (b *bucket) Lock() {
	cur := LoadInt(&b.meta)
	if atomic.CompareAndSwapUint64(&b.meta, cur&(^opLockMask), cur|opLockMask) {
		return
	}
	b.slowLock()
}

func (b *bucket) slowLock() {
	var spins int
	for !b.tryLock() {
		delay(&spins)
	}
}

//go:nosplit
func (b *bucket) tryLock() bool {
	for {
		cur := LoadInt(&b.meta)
		if cur&opLockMask != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64(&b.meta, cur, cur|opLockMask) {
			return true
		}
	}
}

//go:nosplit
func (b *bucket) Unlock() {
	atomic.StoreUint64(&b.meta, LoadIntFast(&b.meta)&^opLockMask)
}

//go:nosplit
func (b *bucket) UnlockWithMeta(meta uint64) {
	atomic.StoreUint64(&b.meta, meta&^opLockMask)
}

//go:nosplit
func (b *bucket) At(i int) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(
		unsafe.Pointer(&b.entries),
		uintptr(i)*unsafe.Sizeof(unsafe.Pointer(nil))),
	)
}

// stats returns statistics for the Map. Just like other map
// methods, this one is thread-safe. Yet it's an O(N) operation,
// so it should be used only for diagnostics or debugging purposes.
func (m *Map[K, V]) stats() *mapStats {
	stats := &mapStats{
		TotalGrowths: LoadInt(&m.growths),
		TotalShrinks: LoadInt(&m.shrinks),
		MinEntries:   math.MaxInt,
	}
	table := (*mapTable)(LoadPtr(&m.table))
	if table == nil {
		return stats
	}
	stats.RootBuckets = table.mask + 1
	stats.Counter = table.SumSize()
	stats.CounterLen = table.sizeMask + 1
	for i := 0; i <= table.mask; i++ {
		entries := 0
		for b := table.buckets.At(i); b != nil; b = (*bucket)(LoadPtr(&b.next)) {
			stats.TotalBuckets++
			entriesLocal := 0
			stats.Capacity += entriesPerBucket

			meta := LoadInt(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*Entry_[K, V])(LoadPtr(b.At(j))); e != nil {
					stats.Size++
					entriesLocal++
				}
			}
			entries += entriesLocal
			if entriesLocal == 0 {
				stats.EmptyBuckets++
			}
		}

		if entries < stats.MinEntries {
			stats.MinEntries = entries
		}
		if entries > stats.MaxEntries {
			stats.MaxEntries = entries
		}
	}
	return stats
}
