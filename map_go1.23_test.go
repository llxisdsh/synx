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

    it := m.Entries()
    got := make(map[string]int)
    count := 0
    it(func(e *Entry[string, int]) bool {
        got[e.Key] = e.Value
        count++
        return true
    })
    if count != 3 {
        t.Fatalf("count=%d", count)
    }
    if got["a"] != 1 || got["b"] != 2 || got["c"] != 3 {
        t.Fatalf("entries=%v", got)
    }
}

func TestMap_Entries_EarlyStop(t *testing.T) {
    m := NewMap[int, int]()
    for i := 0; i < 10; i++ {
        m.Store(i, i)
    }
    it := m.Entries()
    count := 0
    it(func(e *Entry[int, int]) bool {
        count++
        return count < 2
    })
    if count != 2 {
        t.Fatalf("stopped_count=%d", count)
    }
}

func TestMap_Entries_Empty(t *testing.T) {
    var m Map[int, int]
    it := m.Entries()
    count := 0
    it(func(e *Entry[int, int]) bool {
        count++
        return true
    })
    if count != 0 {
        t.Fatalf("empty_count=%d", count)
    }
}
