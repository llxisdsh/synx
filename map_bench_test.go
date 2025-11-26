package synx

import (
	"testing"
)

func BenchmarkMapLoadSmall(b *testing.B) {
	benchmarkMapLoad(b, testDataSmall[:])
}

func BenchmarkMapLoad(b *testing.B) {
	benchmarkMapLoad(b, testData[:])
}

func BenchmarkMapLoadLarge(b *testing.B) {
	benchmarkMapLoad(b, testDataLarge[:])
}

func benchmarkMapLoad(b *testing.B, data []string) {
	b.ReportAllocs()
	var m Map[string, int]
	for i := range data {
		m.LoadOrStore(data[i], i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(data[i])
			i++
			if i >= len(data) {
				i = 0
			}
		}
	})
}

func BenchmarkMapLoadOrStore(b *testing.B) {
	benchmarkMapLoadOrStore(b, testData[:])
}

func BenchmarkMapLoadOrStoreLarge(b *testing.B) {
	benchmarkMapLoadOrStore(b, testDataLarge[:])
}

func benchmarkMapLoadOrStore(b *testing.B, data []string) {
	b.ReportAllocs()
	var m Map[string, int]
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(data[i], i)
			i++
			if i >= len(data) {
				i = 0
			}
		}
	})
}

//	func BenchmarkMapStore(b *testing.B) {
//		benchmarkMapStore(b, testData[:])
//	}
//
//	func BenchmarkMapStoreLarge(b *testing.B) {
//		benchmarkMapStore(b, testDataLarge[:])
//	}
//
//	func benchmarkMapStore(b *testing.B, data []string) {
//		b.ReportAllocs()
//		var m Map[string, int]
//		b.ResetTimer()
//		b.RunParallel(func(pb *testing.PB) {
//			i := 0
//			for pb.Next() {
//				m.Store(data[i], i)
//				i++
//				if i >= len(data) {
//					i = 0
//				}
//			}
//		})
//	}
func BenchmarkMapLoadOrStoreInt(b *testing.B) {
	benchmarkMapLoadOrStoreInt(b, testDataInt[:])
}

func BenchmarkMapLoadOrStoreIntLarge(b *testing.B) {
	benchmarkMapLoadOrStoreInt(b, testDataIntLarge[:])
}

func benchmarkMapLoadOrStoreInt(b *testing.B, data []int) {
	b.ReportAllocs()
	var m Map[int, int]
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(data[i], i)
			i++
			if i >= len(data) {
				i = 0
			}
		}
	})
}
