//go:build go1.23

package synx

import (
	"testing"
)

func TestMap_Entries_Basic(t *testing.T) {
	m := NewMap[string, int]()
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)

	got := make(map[string]int)
	count := 0
	for e := range m.entries() {
		got[e.Key] = e.Value
		count++
	}
	if count != 3 {
		t.Fatalf("count=%d", count)
	}
	if got["a"] != 1 || got["b"] != 2 || got["c"] != 3 {
		t.Fatalf("entries=%v", got)
	}
}

func TestMap_Entries_EarlyStop(t *testing.T) {
	m := NewMap[int, int]()
	for i := range 10 {
		m.Store(i, i)
	}

	count := 0
	for range m.entries() {
		count++
		if count >= 2 {
			break
		}
	}
	if count != 2 {
		t.Fatalf("stopped_count=%d", count)
	}
}

func TestMap_Entries_Empty(t *testing.T) {
	var m Map[int, int]
	count := 0
	for range m.entries() {
		count++
	}
	if count != 0 {
		t.Fatalf("empty_count=%d", count)
	}
}

// TestMap_ComputeAll_UpdateDelete verifies ComputeIter iteration can update and delete entries.
func TestMap_ComputeAll_UpdateDelete(t *testing.T) {
	m := NewMap[int, int]()
	const N = 128

	for i := range N {
		m.Store(i, i)
	}

	for it := range m.ComputeIter() {
		if it.Key()%2 == 0 {
			it.Update(it.Value() + 1)
		} else {
			it.Delete()
		}
	}

	for i := range N {
		if i%2 == 0 {
			v, ok := m.Load(i)
			if !ok || v != i+1 {
				t.Fatalf("even key %d: want %d, ok=true; got %v, ok=%v", i, i+1, v, ok)
			}
		} else {
			if _, ok := m.Load(i); ok {
				t.Fatalf("odd key %d: expected deleted", i)
			}
		}
	}
}

// TestMap_ComputeAll_EarlyStop verifies breaking the range stops iteration and only a subset is processed.
func TestMap_ComputeAll_EarlyStop(t *testing.T) {
	m := NewMap[int, int]()
	const N = 100
	for i := range N {
		m.Store(i, i)
	}

	processed := 0
	for it := range m.ComputeIter() {
		it.Update(it.Value() + 1000)
		processed++
		if processed == 10 {
			break
		}
	}

	if processed != 10 {
		t.Fatalf("processed=%d, want 10", processed)
	}

	updated := 0
	for k, v := range m.All() {
		_ = k
		if v >= 1000 {
			updated++
		}
	}
	// 早停时，最后一个回调的修改也会被应用
	if updated != 10 {
		t.Fatalf("updated=%d, want 10", updated)
	}
}
