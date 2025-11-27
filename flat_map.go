package synx

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	. "github.com/llxisdsh/synx/internal/opt" // nolint:staticcheck
)

// FlatMap implements a flat hash map using seqlock.
// Table and key/value pairs are stored inline (flat).
// Value size is not limited by the CPU word size.
// Readers use per-bucket seqlock: even sequence means stable; writers
// flip the bucket sequence to odd during mutation, then even again.
//
// Concurrency model:
//   - Readers: read s1=seq (must be even), then meta/entries, then s2=seq;
//     if s1!=s2 or s1 is odd, retry the bucket.
//   - Writers: take the root-bucket lock (opLock in meta), then on the target
//     bucket: seq++, apply changes, seq++, finally release the root lock.
//   - Resize: copy under the root-bucket lock using the same discipline.
//
// Notes:
//   - Reuses Map constants and compile-time bucket sizing (entriesPerBucket).
//   - Buckets are packed without padding for cache-friendly layout.
type FlatMap[K comparable, V any] struct {
	_        noCopy
	table    seqlockSlot[flatTable[K, V]]
	rs       unsafe.Pointer // *flatRebuildState[K,V]
	seed     uintptr
	keyHash  HashFunc
	tableSeq seqlock[uint32, flatTable[K, V]] // seqlock of table
	shrinkOn bool                             // WithAutoShrink
	intKey   bool
}

type flatRebuildState[K comparable, V any] struct {
	hint        mapRebuildHint
	chunks      int32
	newTable    seqlockSlot[flatTable[K, V]]
	newTableSeq seqlock[uint32, flatTable[K, V]] // seqlock of new table
	process     int32                            // atomic
	completed   int32                            // atomic
	wg          sync.WaitGroup
}

type flatTable[K comparable, V any] struct {
	buckets  unsafeSlice[flatBucket[K, V]]
	mask     int
	size     unsafeSlice[CounterStripe]
	sizeMask uint32
	// The inline size has minimal effect on reducing cache misses,
	// so we will not use it for now.
	// smallSz  uintptr
}

type flatBucket[K comparable, V any] struct {
	_       [0]atomic.Uint64
	meta    uint64                            // op byte + h2 bytes
	seq     seqlock[uintptr, FlatEntry[K, V]] // seqlock of bucket
	next    unsafe.Pointer                    // *flatBucket[K,V]
	entries [entriesPerBucket]seqlockSlot[FlatEntry[K, V]]
}

// NewFlatMap creates a new seqlock-based flat hash map.
//
// Highlights:
//   - Optimistic reads via per-bucket seqlock; brief spinning under
//     contention.
//   - Writes coordinate via a lightweight root-bucket lock and per-bucket
//     seqlock fencing.
//   - Parallel resize (grow/shrink) with cooperative copying by readers and
//     writers.
//
// Configuration options (aligned with Map):
//   - WithCapacity(sizeHint): pre-allocate capacity to reduce early resizes.
//   - WithAutoShrink(): enable automatic shrinking when load drops.
//   - WithKeyHasher / WithKeyHasherUnsafe / WithBuiltInHasher: custom or
//     built-in hashing.
//   - HashOptimization: control h1 distribution strategy (Linear/Shift/Auto).
//
// Example:
//
//	m := NewFlatMap[string,int](WithCapacity(1024), WithAutoShrink())
//	m.Store("a", 1)
//	v, ok := m.Load("a")
func NewFlatMap[K comparable, V any](
	options ...func(*MapConfig),
) *FlatMap[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(&cfg)
	}
	m := &FlatMap[K, V]{}
	m.init(&cfg)
	return m
}

func (m *FlatMap[K, V]) init(
	cfg *MapConfig,
) {
	// parse interface
	if cfg.KeyHash == nil {
		cfg.KeyHash, cfg.HashOpt = parseKeyInterface[K]()
	}
	// perform initialization
	m.keyHash, _, m.intKey = defaultHasher[K, V]()
	if cfg.KeyHash != nil {
		m.keyHash = cfg.KeyHash
		cfg.parseIntKey(&m.intKey)
	}

	m.seed = uintptr(rand.Uint64())
	m.shrinkOn = cfg.AutoShrink
	var newTable flatTable[K, V]
	tableLen := calcTableLen(cfg.SizeHint)
	newTable.makeTable(tableLen, runtime.GOMAXPROCS(0))
	m.tableSeq.WriteLocked(&m.table, newTable)
}

//go:noinline
func (m *FlatMap[K, V]) slowInit() {
	rs := (*flatRebuildState[K, V])(atomic.LoadPointer(&m.rs))
	if rs != nil {
		rs.wg.Wait()
		return
	}
	rs, ok := m.beginRebuild(mapRebuildBlockWritersHint)
	if !ok {
		rs = (*flatRebuildState[K, V])(atomic.LoadPointer(&m.rs))
		if rs != nil {
			rs.wg.Wait()
		}
		return
	}
	// The table may have been altered prior to our changes.
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr != nil {
		m.endRebuild(rs)
		return
	}
	cfg := &MapConfig{}
	m.init(cfg)
	m.endRebuild(rs)
}

// Load retrieves the value for a key.
//
//   - Fast path: per-bucket seqlock read; an even and stable sequence yields
//     a consistent snapshot.
//   - Contention handling: short spinning on observed writes (odd seq) or
//     instability; no locked fallback path.
//   - Provides stable latency under high concurrency.
func (m *FlatMap[K, V]) Load(key K) (value V, ok bool) {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1(hash, m.intKey)
	root := table.buckets.At(idx)
	for b := root; b != nil; b = (*flatBucket[K, V])(atomic.LoadPointer(&b.next)) {
		meta := atomic.LoadUint64(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			e := b.seq.Read(b.At(j))
			if EmbeddedHash {
				if e.GetHash() == hash && e.Key == key {
					return e.Value, true
				}
			} else {
				if e.Key == key {
					return e.Value, true
				}
			}
		}
	}
	return
}

// Range iterates all entries using per-bucket seqlock reads.
//
//   - Copies a consistent snapshot from each bucket when the sequence is
//     stable; otherwise briefly spins and retries.
//   - Yields outside of locks to minimize contention.
//   - Returning false from the callback stops iteration early.
func (m *FlatMap[K, V]) Range(yield func(K, V) bool) {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return
	}

	var meta uint64
	var cache [entriesPerBucket]FlatEntry[K, V]
	var cacheCount int
	for i := 0; i <= table.mask; i++ {
		root := table.buckets.At(i)
		for b := root; b != nil; b = (*flatBucket[K, V])(atomic.LoadPointer(&b.next)) {
			var spins int
			for {
				if s1, ok := b.seq.BeginRead(); ok {
					meta = atomic.LoadUint64(&b.meta)
					cacheCount = 0
					for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
						j := firstMarkedByteIndex(marked)
						cache[cacheCount] = b.At(j).ReadUnfenced()
						cacheCount++
					}
					if b.seq.EndRead(s1) {
						for j := range cacheCount {
							kv := &cache[j]
							if !yield(kv.Key, kv.Value) {
								return
							}
						}
						break
					}
				}
				delay(&spins)
			}
		}
	}
}

// ComputeRange iterates all entries and applies a user callback.
//
// Callback signature:
//
//		fn(e *FlatMapIter[K, V]) bool
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
//   - Uses per-bucket seqlock to minimize write windows and preserve reader
//     consistency.
//
// Parameters:
//   - fn: user function applied to each key-value pair.
//   - blockWriters: optional flag (default false). If true, concurrent writers
//     are blocked during iteration; resize operations are always exclusive.
//
// Recommendation: keep fn lightweight to reduce lock hold time.
func (m *FlatMap[K, V]) ComputeRange(
	fn func(e *FlatMapIter[K, V]) bool,
	blockWriters ...bool,
) {
	hint := mapRebuildAllowWritersHint
	if len(blockWriters) != 0 && blockWriters[0] {
		hint = mapRebuildBlockWritersHint
	}

	m.rebuild(hint, func() {
		table := m.tableSeq.Read(&m.table)
		if table.buckets.ptr == nil {
			return
		}
		it := FlatMapIter[K, V]{
			loaded: true,
		}
		for i := 0; i <= table.mask; i++ {
			root := table.buckets.At(i)
			root.Lock()
			for b := root; b != nil; b = (*flatBucket[K, V])(b.next) {
				meta := LoadIntFast(&b.meta)
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j)
					it.entry = *e.Ptr()
					it.op = cancelOp
					shouldContinue := fn(&it)
					switch it.op {
					case updateOp:
						b.seq.WriteLocked(e, it.entry)
					case deleteOp:
						b.seq.BeginWriteLocked()
						e.WriteUnfenced(FlatEntry[K, V]{})
						meta = setByte(meta, emptySlot, j)
						atomic.StoreUint64(&b.meta, meta)
						b.seq.EndWriteLocked()
						table.AddSize(i, -1)
					default:
						// cancelOp: No-op
					}
					if !shouldContinue {
						root.Unlock()
						return
					}
				}
			}
			root.Unlock()
		}
	})
}

// Compute performs a compute-style, atomic update for the given key.
//
// Concurrency model:
//   - Acquires the root-bucket lock to serialize write/resize cooperation.
//   - Performs per-bucket seqlock writes (odd/even sequence) to minimize the
//     write window and preserve reader consistency.
//   - If a resize is observed, cooperates to finish copying and restarts on
//     the latest table.
//
// Callback signature:
//
//		fn(e *FlatMapIter[K, V])
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
//   - actual: the current value in the map after the operation
//   - loaded: true if the key existed before the operation
func (m *FlatMap[K, V]) Compute(
	key K,
	fn func(e *FlatMapIter[K, V]),
) (actual V, loaded bool) {
	for {
		table := m.tableSeq.Read(&m.table)
		if table.buckets.ptr == nil {
			m.slowInit()
			continue
		}
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h1v := h1(hash, m.intKey)
		h2v := h2(hash)
		h2w := broadcast(h2v)
		idx := table.mask & h1v
		root := table.buckets.At(idx)
		root.Lock()

		// Help finishing rebuild if needed
		if rs := (*flatRebuildState[K, V])(atomic.LoadPointer(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if rs.newTableSeq.WriteCompleted() {
					root.Unlock()
					m.helpCopyAndWait(rs)
					continue
				}
			case mapRebuildBlockWritersHint:
				root.Unlock()
				rs.wg.Wait()
				continue
			default:
				// mapRebuildWithWritersHint: allow concurrent writers
			}
		}
		if m.tableSeq.Read(&m.table).buckets.ptr != table.buckets.ptr {
			root.Unlock()
			continue
		}

		var (
			oldB     *flatBucket[K, V]
			oldIdx   int
			oldMeta  uint64
			it       FlatMapIter[K, V]
			emptyB   *flatBucket[K, V]
			emptyIdx int
			lastB    *flatBucket[K, V]
		)

	findLoop:
		for b := root; b != nil; b = (*flatBucket[K, V])(b.next) {
			meta := LoadIntFast(&b.meta)
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).Ptr()
				if EmbeddedHash {
					if e.GetHash() == hash && e.Key == key {
						oldB, oldIdx, oldMeta, it.entry, it.loaded = b, j, meta, *e, true
						break findLoop
					}
				} else {
					if e.Key == key {
						oldB, oldIdx, oldMeta, it.entry, it.loaded = b, j, meta, *e, true
						break findLoop
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

		fn(&it)
		actual = it.Value()
		loaded = it.Loaded()
		switch it.op {
		case updateOp:
			if loaded {
				e := oldB.At(oldIdx)
				oldB.seq.WriteLocked(e, it.entry)
				root.Unlock()
				return
			}
			it.entry.Key = key
			if EmbeddedHash {
				it.entry.SetHash(hash)
			}
			// insert new
			if emptyB != nil {
				emptyB.seq.BeginWriteLocked()
				emptyB.At(emptyIdx).WriteUnfenced(it.entry)
				newMeta := setByte(LoadIntFast(&emptyB.meta), h2v, emptyIdx)
				atomic.StoreUint64(&emptyB.meta, newMeta)
				emptyB.seq.EndWriteLocked()

				root.Unlock()
				table.AddSize(idx, 1)
				return
			}
			// append new bucket
			bucket := &flatBucket[K, V]{
				meta: setByte(emptyMeta, h2v, 0),
				entries: [entriesPerBucket]seqlockSlot[FlatEntry[K, V]]{
					{buf: it.entry},
				},
			}

			atomic.StorePointer(&lastB.next, unsafe.Pointer(bucket))
			root.Unlock()
			table.AddSize(idx, 1)
			// Auto-grow check (parallel resize)
			if atomic.LoadPointer(&m.rs) == nil {
				tableLen := table.mask + 1
				size := table.SumSize()
				const sizeHintFactor = float64(entriesPerBucket) * loadFactor
				if size >= int(float64(tableLen)*sizeHintFactor) {
					m.tryResize(mapGrowHint, size, 0)
				}
			}
			return
		case deleteOp:
			if !loaded {
				root.Unlock()
				return
			}
			oldB.seq.BeginWriteLocked()
			oldB.At(oldIdx).WriteUnfenced(FlatEntry[K, V]{})
			newMeta := setByte(oldMeta, emptySlot, oldIdx)
			atomic.StoreUint64(&oldB.meta, newMeta)
			oldB.seq.EndWriteLocked()
			root.Unlock()
			table.AddSize(idx, -1)
			// Check if table shrinking is needed
			if m.shrinkOn && newMeta&metaDataMask == emptyMeta &&
				atomic.LoadPointer(&m.rs) == nil {
				tableLen := table.mask + 1
				if minTableLen < tableLen {
					size := table.SumSize()
					if size < tableLen*entriesPerBucket/shrinkFraction {
						m.tryResize(mapShrinkHint, size, 0)
					}
				}
			}
			return
		default:
			// cancelOp: No-op
			root.Unlock()
			return
		}
	}
}

// Store sets the value for a key.
func (m *FlatMap[K, V]) Store(key K, value V) {
	m.Compute(key, func(e *FlatMapIter[K, V]) {
		e.Update(value)
	})
}

// Swap stores value for key and returns the previous value if any.
// The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	_, loaded = m.Compute(key, func(e *FlatMapIter[K, V]) {
		previous = e.Value()
		e.Update(value)
	})
	return
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMap[K, V]) LoadOrStore(
	key K,
	value V,
) (actual V, loaded bool) {
	if v, ok := m.Load(key); ok {
		return v, true
	}
	return m.Compute(key, func(e *FlatMapIter[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(value)
	})
}

// LoadOrStoreFn loads the value for a key if present.
// Otherwise, it stores and returns the value returned by valueFn.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMap[K, V]) LoadOrStoreFn(
	key K,
	valueFn func() V,
) (actual V, loaded bool) {
	if v, ok := m.Load(key); ok {
		return v, true
	}
	return m.Compute(key, func(e *FlatMapIter[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(valueFn())
	})
}

// Delete deletes the value for a key.
func (m *FlatMap[K, V]) Delete(key K) {
	m.Compute(key, func(e *FlatMapIter[K, V]) {
		e.Delete()
	})
}

// LoadAndDelete deletes the value for a key, returning the previous value.
// The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	_, loaded = m.Compute(key, func(e *FlatMapIter[K, V]) {
		if e.Loaded() {
			previous = e.Value()
			e.Delete()
		}
	})
	return
}

// LoadAndUpdate updates the value for key if it exists, returning the previous
// value. The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	_, loaded = m.Compute(key, func(e *FlatMapIter[K, V]) {
		if e.Loaded() {
			previous = e.Value()
			e.Update(value)
		}
	})
	return
}

// Clear clears all key-value pairs from the map.
func (m *FlatMap[K, V]) Clear() {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return
	}

	m.rebuild(mapRebuildBlockWritersHint, func() {
		var newTable flatTable[K, V]
		cpus := runtime.GOMAXPROCS(0)
		newTable.makeTable(minTableLen, cpus)
		m.tableSeq.WriteLocked(&m.table, newTable)
	})
}

// All returns an iterator function for use with range-over-func.
// It provides the same functionality as Range but in iterator form.
//
//go:nosplit
func (m *FlatMap[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// ComputeAll returns an iterator function for use with range-over-func.
// It provides the same functionality as ComputeRange but in iterator form.
//
//go:nosplit
func (m *FlatMap[K, V]) ComputeAll(
	blockWriters ...bool,
) func(yield func(e *FlatMapIter[K, V]) bool) {
	return func(yield func(e *FlatMapIter[K, V]) bool) {
		m.ComputeRange(yield, blockWriters...)
	}
}

// Size returns the number of key-value pairs in the map.
// This operation sums counters across all size stripes for an approximate
// count.
//
//go:nosplit
func (m *FlatMap[K, V]) Size() int {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return 0
	}

	return table.SumSize()
}

// IsZero checks if the map is empty.
// This is faster than checking Size() == 0 as it can return early.
//
//go:nosplit
func (m *FlatMap[K, V]) IsZero() bool {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return true
	}

	return !table.SumSizeExceeds(0)
}

func (m *FlatMap[K, V]) beginRebuild(hint mapRebuildHint) (*flatRebuildState[K, V], bool) {
	rs := new(flatRebuildState[K, V])
	rs.hint = hint
	rs.wg.Add(1)
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return nil, false
	}
	return rs, true
}

func (m *FlatMap[K, V]) endRebuild(rs *flatRebuildState[K, V]) {
	atomic.StorePointer(&m.rs, nil)
	rs.wg.Done()
}

// rebuild reorganizes the map. Only these hints are supported:
//   - mapRebuildWithWritersHint: allows concurrent reads/writes
//   - mapExclusiveRebuildHint: allows concurrent reads
func (m *FlatMap[K, V]) rebuild(
	hint mapRebuildHint,
	fn func(),
) {
	for {
		// Help finishing rebuild if needed
		if rs := (*flatRebuildState[K, V])(atomic.LoadPointer(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if rs.newTableSeq.WriteCompleted() {
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
func (m *FlatMap[K, V]) tryResize(hint mapRebuildHint, size, sizeAdd int) {
	rs, ok := m.beginRebuild(hint)
	if !ok {
		return
	}

	table := m.table.Ptr()
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
	} else {
		// mapShrinkHint
		if sizeAdd == 0 {
			newLen = tableLen >> 1
		} else {
			newLen = calcTableLen(size)
		}
		if newLen < minTableLen {
			m.endRebuild(rs)
			return
		}
	}

	cpus := runtime.GOMAXPROCS(0)
	if cpus > 1 &&
		newLen*int(unsafe.Sizeof(flatBucket[K, V]{})) >= asyncThreshold {
		go m.finalizeResize(table, newLen, rs, cpus)
	} else {
		m.finalizeResize(table, newLen, rs, cpus)
	}
}

func (m *FlatMap[K, V]) finalizeResize(
	table *flatTable[K, V],
	newLen int,
	rs *flatRebuildState[K, V],
	cpus int,
) {
	overCpus := cpus * resizeOverPartition
	_, chunks := calcParallelism(table.mask+1, minBucketsPerCPU, overCpus)
	rs.chunks = int32(chunks)
	var newTable flatTable[K, V]
	newTable.makeTable(newLen, cpus)
	rs.newTableSeq.WriteLocked(&rs.newTable, newTable) // Release rs
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *FlatMap[K, V]) helpCopyAndWait(rs *flatRebuildState[K, V]) {
	table := m.tableSeq.Read(&m.table)
	newTable := rs.newTableSeq.Read(&rs.newTable) // Acquire rs
	if newTable.buckets.ptr == table.buckets.ptr {
		rs.wg.Wait()
		return
	}
	tableLen := table.mask + 1
	chunks := rs.chunks
	chunkSz := (tableLen + int(chunks) - 1) / int(chunks)
	isGrowth := (newTable.mask + 1) > tableLen
	for {
		process := atomic.AddInt32(&rs.process, 1)
		if process > chunks {
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSz
		end := min(start+chunkSz, tableLen)
		if isGrowth {
			m.copyBucket(&table, start, end, &newTable, false)
		} else {
			m.copyBucket(&table, start, end, &newTable, true)
		}
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			m.tableSeq.WriteLocked(&m.table, newTable)
			m.endRebuild(rs)
			return
		}
	}
}

func (m *FlatMap[K, V]) copyBucket(
	table *flatTable[K, V],
	start, end int,
	newTable *flatTable[K, V],
	lockBucket bool,
) {
	copied := 0
	var hash uintptr
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*flatBucket[K, V])(b.next) {
			meta := LoadIntFast(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).Ptr()
				if EmbeddedHash {
					hash = e.GetHash()
				} else {
					hash = m.keyHash(noescape(unsafe.Pointer(&e.Key)), m.seed)
				}
				idx := newTable.mask & h1(hash, m.intKey)
				destBucket := newTable.buckets.At(idx)
				h2v := h2(hash)
				if lockBucket {
					destBucket.Lock()
				}
				b := destBucket
			appendTo:
				for {
					meta := LoadIntFast(&b.meta)
					empty := (^meta) & metaMask
					if empty != 0 {
						emptyIdx := firstMarkedByteIndex(empty)
						StoreIntFast(&b.meta, setByte(meta, h2v, emptyIdx))
						entry := b.At(emptyIdx).Ptr()
						entry.Value = e.Value
						if EmbeddedHash {
							entry.SetHash(hash)
						}
						entry.Key = e.Key
						break appendTo
					}
					next := (*flatBucket[K, V])(b.next)
					if next == nil {
						newE := FlatEntry[K, V]{Key: e.Key, Value: e.Value}
						if EmbeddedHash {
							newE.SetHash(hash)
						}
						bucket := &flatBucket[K, V]{
							meta: setByte(emptyMeta, h2v, 0),
							entries: [entriesPerBucket]seqlockSlot[FlatEntry[K, V]]{
								{buf: newE},
							},
						}
						b.next = unsafe.Pointer(bucket)
						break appendTo
					}
					b = next
				}
				if lockBucket {
					destBucket.Unlock()
				}
				copied++
			}
		}
		srcBucket.Unlock()
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

func (t *flatTable[K, V]) makeTable(
	tableLen, cpus int,
) {
	sizeLen := calcSizeLen(tableLen, cpus)
	t.buckets = makeUnsafeSlice(make([]flatBucket[K, V], tableLen))
	t.mask = tableLen - 1
	// if sizeLen <= 1 {
	// 	t.size.ptr = unsafe.Pointer(&t.smallSz)
	// } else {
	t.size = makeUnsafeSlice(make([]CounterStripe, sizeLen))
	// }
	t.sizeMask = uint32(sizeLen - 1)
}

//go:nosplit
func (t *flatTable[K, V]) AddSize(idx, delta int) {
	atomic.AddUintptr(&t.size.At(int(t.sizeMask)&idx).C, uintptr(delta))
}

//go:nosplit
func (t *flatTable[K, V]) SumSize() int {
	var sum uintptr
	for i := 0; i <= int(t.sizeMask); i++ {
		sum += atomic.LoadUintptr(&t.size.At(i).C)
	}
	return int(sum)
}

//go:nosplit
func (t *flatTable[K, V]) SumSizeExceeds(limit int) bool {
	var sum uintptr
	for i := 0; i <= int(t.sizeMask); i++ {
		sum += atomic.LoadUintptr(&t.size.At(i).C)
		if int(sum) > limit {
			return true
		}
	}
	return false
}

//go:nosplit
func (b *flatBucket[K, V]) At(i int) *seqlockSlot[FlatEntry[K, V]] {
	return (*seqlockSlot[FlatEntry[K, V]])(unsafe.Add(
		unsafe.Pointer(&b.entries),
		uintptr(i)*unsafe.Sizeof(seqlockSlot[FlatEntry[K, V]]{}),
	))
}

//go:nosplit
func (b *flatBucket[K, V]) Lock() {
	cur := atomic.LoadUint64(&b.meta)
	if atomic.CompareAndSwapUint64(&b.meta, cur&(^opLockMask), cur|opLockMask) {
		return
	}
	b.slowLock()
}

//go:nosplit
func (b *flatBucket[K, V]) slowLock() {
	var spins int
	for !b.tryLock() {
		delay(&spins)
	}
}

//go:nosplit
func (b *flatBucket[K, V]) tryLock() bool {
	for {
		cur := atomic.LoadUint64(&b.meta)
		if cur&opLockMask != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64(&b.meta, cur, cur|opLockMask) {
			return true
		}
	}
}

//go:nosplit
func (b *flatBucket[K, V]) Unlock() {
	atomic.StoreUint64(&b.meta, LoadIntFast(&b.meta)&^opLockMask)
}

// FlatMapIter represents an entry being processed in Compute/ComputeRange operations.
// It provides methods to update or delete the entry during iteration.
type FlatMapIter[K comparable, V any] struct {
	entry  FlatEntry[K, V]
	loaded bool
	op     computeOp
}

// Key returns the key of the current entry.
//
//go:nosplit
func (e *FlatMapIter[K, V]) Key() K {
	return e.entry.Key
}

// Value returns the value of the current entry.
//
//go:nosplit
func (e *FlatMapIter[K, V]) Value() V {
	return e.entry.Value
}

// Loaded returns true if the entry is loaded, false otherwise.
//
//go:nosplit
func (e *FlatMapIter[K, V]) Loaded() bool {
	return e.loaded
}

// Update performs an upsert on the current entry.
// If Loaded()==true, replaces the value;
// if false, inserts the key with newValue.
//
//go:nosplit
func (e *FlatMapIter[K, V]) Update(newValue V) {
	e.entry.Value = newValue
	e.op = updateOp
}

// Delete marks the current entry for deletion.
// The entry will be removed from the map.
//
//go:nosplit
func (e *FlatMapIter[K, V]) Delete() {
	e.entry.Value = *new(V)
	e.op = deleteOp
}
