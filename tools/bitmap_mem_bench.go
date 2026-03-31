// Measures per-entry memory cost of map[[16]byte]*roaring.Bitmap at various scales.
//
// Usage:
//   go run tools/bitmap_mem_bench.go
//
// Tests:
//   1. Struct layout and internal breakdown
//   2. Per-component memory breakdown
//   3. Scale test
//   4. Varying cardinalities
//   5. RunOptimize effect

package main

import (
	"fmt"
	"reflect"
	"runtime"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
)

func main() {
	// =========================================================================
	// 1. Struct layout
	// =========================================================================
	fmt.Println("=== Struct Layout ===")
	bm := roaring.New()
	fmt.Printf("roaring.Bitmap struct: %d bytes\n", unsafe.Sizeof(*bm))

	bmType := reflect.TypeOf(*bm)
	for i := 0; i < bmType.NumField(); i++ {
		f := bmType.Field(i)
		fmt.Printf("  └─ %-20s %s (%d bytes)\n", f.Name, f.Type, f.Type.Size())
		if f.Type.Kind() == reflect.Struct {
			for j := 0; j < f.Type.NumField(); j++ {
				inner := f.Type.Field(j)
				fmt.Printf("       ├─ %-16s %s (%d bytes)\n", inner.Name, inner.Type, inner.Type.Size())
			}
		}
	}

	bm.Add(1)
	fmt.Printf("\nAfter Add(1): %+v\n", bm.Stats())
	fmt.Println()

	// =========================================================================
	// 2. Per-component memory breakdown
	// =========================================================================
	fmt.Println("=== Per-Component Breakdown (N=500,000, cardinality=1) ===")
	const N = 500_000

	// a) Empty bitmaps in a slice
	runtime.GC()
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	emptySlice := make([]*roaring.Bitmap, N)
	for i := range emptySlice {
		emptySlice[i] = roaring.New()
	}

	runtime.GC()
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	emptyBytes := m2.HeapAlloc - m1.HeapAlloc
	fmt.Printf("  Empty bitmap (New only):            %6.1f bytes/entry\n", float64(emptyBytes)/N)

	// b) Bitmaps with 1 element in a slice (keep emptySlice alive)
	runtime.GC()
	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)

	oneSlice := make([]*roaring.Bitmap, N)
	for i := range oneSlice {
		b := roaring.New()
		b.Add(uint32(i))
		oneSlice[i] = b
	}

	runtime.GC()
	runtime.GC()
	var m4 runtime.MemStats
	runtime.ReadMemStats(&m4)
	oneBytes := m4.HeapAlloc - m3.HeapAlloc
	containerCost := float64(oneBytes-emptyBytes) / N
	fmt.Printf("  Bitmap with 1 element:              %6.1f bytes/entry\n", float64(oneBytes)/N)
	fmt.Printf("  Container cost (diff):              %6.1f bytes/entry\n", containerCost)

	// c) Map overhead only (reuse existing bitmaps)
	runtime.GC()
	runtime.GC()
	var m5 runtime.MemStats
	runtime.ReadMemStats(&m5)

	mp := make(map[[16]byte]*roaring.Bitmap)
	for i := 0; i < N; i++ {
		var key [16]byte
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = byte(i >> 16)
		mp[key] = oneSlice[i] // reuse, no new bitmap alloc
	}

	runtime.GC()
	runtime.GC()
	var m6 runtime.MemStats
	runtime.ReadMemStats(&m6)
	mapOnlyBytes := m6.HeapAlloc - m5.HeapAlloc
	fmt.Printf("  Map bucket overhead (reuse bm):     %6.1f bytes/entry\n", float64(mapOnlyBytes)/N)

	// d) Full cost: map + new bitmaps
	runtime.GC()
	runtime.GC()
	var m7 runtime.MemStats
	runtime.ReadMemStats(&m7)

	mp2 := make(map[[16]byte]*roaring.Bitmap)
	for i := 0; i < N; i++ {
		var key [16]byte
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = byte(i >> 16)
		b := roaring.New()
		b.Add(uint32(i))
		mp2[key] = b
	}

	runtime.GC()
	runtime.GC()
	var m8 runtime.MemStats
	runtime.ReadMemStats(&m8)
	totalBytes := m8.HeapAlloc - m7.HeapAlloc
	fmt.Printf("  Total (map + bitmap + container):   %6.1f bytes/entry\n", float64(totalBytes)/N)

	fmt.Println()
	fmt.Println("  Breakdown:")
	emptyPer := float64(emptyBytes) / N
	fmt.Printf("    Bitmap struct (New):               %6.1f bytes\n", emptyPer)
	fmt.Printf("    + Container (Add 1 element):       %6.1f bytes\n", containerCost)
	fmt.Printf("    + Map bucket ([16]byte key + ptr): %6.1f bytes\n", float64(mapOnlyBytes)/N)
	fmt.Printf("    ────────────────────────────────────────\n")
	fmt.Printf("    = Total per entry:                 %6.1f bytes\n", float64(totalBytes)/N)

	// Keep everything alive
	runtime.KeepAlive(emptySlice)
	runtime.KeepAlive(oneSlice)
	runtime.KeepAlive(mp)
	runtime.KeepAlive(mp2)

	fmt.Println()

	// =========================================================================
	// 3. Scale test
	// =========================================================================
	fmt.Println("=== Scale Test: map[[16]byte]*roaring.Bitmap (1 element) ===")
	for _, n := range []int{100_000, 500_000, 1_000_000, 2_000_000} {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		m := make(map[[16]byte]*roaring.Bitmap)
		for i := 0; i < n; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			b := roaring.New()
			b.Add(uint32(i))
			m[key] = b
		}

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		total := after.HeapAlloc - before.HeapAlloc
		fmt.Printf("  N=%9d: %12d bytes total, %.1f bytes/entry\n", n, total, float64(total)/float64(n))
		runtime.KeepAlive(m)
	}
	fmt.Println()

	// =========================================================================
	// 4. Varying cardinalities
	// =========================================================================
	fmt.Println("=== Varying Cardinalities (N=500,000) ===")
	for _, card := range []int{1, 2, 5, 10, 50, 100, 500, 1000} {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		m := make(map[[16]byte]*roaring.Bitmap)
		for i := 0; i < N; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			b := roaring.New()
			for j := 0; j < card; j++ {
				b.Add(uint32(i*card + j))
			}
			m[key] = b
		}

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		total := after.HeapAlloc - before.HeapAlloc

		// Get bitmap-only data size for reference
		ref := roaring.New()
		for j := 0; j < card; j++ {
			ref.Add(uint32(j))
		}

		fmt.Printf("  cardinality=%5d: %6.1f bytes/entry  (bitmap data alone: %d bytes)\n",
			card, float64(total)/float64(N), ref.GetSizeInBytes())
		runtime.KeepAlive(m)
	}
	fmt.Println()

	// =========================================================================
	// 5. RunOptimize effect
	// =========================================================================
	fmt.Println("=== RunOptimize Effect (N=500,000, consecutive IDs) ===")
	for _, card := range []int{1, 10, 100, 1000} {
		// Without
		runtime.GC()
		runtime.GC()
		var b1 runtime.MemStats
		runtime.ReadMemStats(&b1)

		m1 := make(map[[16]byte]*roaring.Bitmap)
		for i := 0; i < N; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			b := roaring.New()
			for j := 0; j < card; j++ {
				b.Add(uint32(i*card + j))
			}
			m1[key] = b
		}

		runtime.GC()
		runtime.GC()
		var a1 runtime.MemStats
		runtime.ReadMemStats(&a1)
		without := a1.HeapAlloc - b1.HeapAlloc

		// With RunOptimize
		runtime.GC()
		runtime.GC()
		var b2 runtime.MemStats
		runtime.ReadMemStats(&b2)

		m2 := make(map[[16]byte]*roaring.Bitmap)
		for i := 0; i < N; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			b := roaring.New()
			for j := 0; j < card; j++ {
				b.Add(uint32(i*card + j))
			}
			b.RunOptimize()
			m2[key] = b
		}

		runtime.GC()
		runtime.GC()
		var a2 runtime.MemStats
		runtime.ReadMemStats(&a2)
		with := a2.HeapAlloc - b2.HeapAlloc

		saved := int64(without) - int64(with)
		pct := float64(saved) / float64(without) * 100
		fmt.Printf("  cardinality=%4d: without=%6.1f, with=%6.1f bytes/entry  (saved %.1f%%)\n",
			card, float64(without)/N, float64(with)/N, pct)
		runtime.KeepAlive(m1)
		runtime.KeepAlive(m2)
	}
	fmt.Println()

	// =========================================================================
	// 6. Tiered design: memory cost of map[[16]byte]uint32 for singles
	// =========================================================================
	fmt.Println("=== Tiered Design: Singles Map Memory ===")
	fmt.Println("map[[16]byte]uint32 (single eventID per term):")
	for _, n := range []int{100_000, 500_000, 1_000_000, 2_000_000} {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		singles := make(map[[16]byte]uint32)
		for i := 0; i < n; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			singles[key] = uint32(i)
		}

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		total := after.HeapAlloc - before.HeapAlloc
		fmt.Printf("  N=%9d: %12d bytes total, %.1f bytes/entry\n", n, total, float64(total)/float64(n))
		runtime.KeepAlive(singles)
	}

	fmt.Println()
	fmt.Println("Comparison at N=2,000,000:")
	// Roaring map
	runtime.GC()
	runtime.GC()
	var rm1 runtime.MemStats
	runtime.ReadMemStats(&rm1)

	roaringMap := make(map[[16]byte]*roaring.Bitmap)
	for i := 0; i < 2_000_000; i++ {
		var key [16]byte
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = byte(i >> 16)
		b := roaring.New()
		b.Add(uint32(i))
		roaringMap[key] = b
	}

	runtime.GC()
	runtime.GC()
	var rm2 runtime.MemStats
	runtime.ReadMemStats(&rm2)
	roaringBytes := rm2.HeapAlloc - rm1.HeapAlloc

	// Singles map
	runtime.GC()
	runtime.GC()
	var sm1 runtime.MemStats
	runtime.ReadMemStats(&sm1)

	singlesMap := make(map[[16]byte]uint32)
	for i := 0; i < 2_000_000; i++ {
		var key [16]byte
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = byte(i >> 16)
		singlesMap[key] = uint32(i)
	}

	runtime.GC()
	runtime.GC()
	var sm2 runtime.MemStats
	runtime.ReadMemStats(&sm2)
	singlesBytes := sm2.HeapAlloc - sm1.HeapAlloc

	saved := int64(roaringBytes) - int64(singlesBytes)
	fmt.Printf("  map[[16]byte]*roaring.Bitmap: %.1f bytes/entry (%d MB)\n",
		float64(roaringBytes)/2_000_000, roaringBytes/(1024*1024))
	fmt.Printf("  map[[16]byte]uint32:          %.1f bytes/entry (%d MB)\n",
		float64(singlesBytes)/2_000_000, singlesBytes/(1024*1024))
	fmt.Printf("  Savings: %d MB (%.1f%%)\n", saved/(1024*1024), float64(saved)/float64(roaringBytes)*100)

	runtime.KeepAlive(roaringMap)
	runtime.KeepAlive(singlesMap)
	fmt.Println()

	// =========================================================================
	// 7. Tiered flush cost: serializing singles to roaring bytes
	// =========================================================================
	fmt.Println("=== Tiered Flush: Serializing Singles to Roaring Bytes ===")
	for _, n := range []int{100_000, 500_000, 1_000_000, 2_000_000} {
		singles2 := make(map[[16]byte]uint32, n)
		for i := 0; i < n; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			singles2[key] = uint32(i)
		}

		start := time.Now()
		totalBytes := 0
		for _, eventID := range singles2 {
			b := roaring.New()
			b.Add(eventID)
			data, _ := b.ToBytes()
			totalBytes += len(data)
		}
		elapsed := time.Since(start)

		fmt.Printf("  N=%9d: %v (%.0f ns/entry, %d bytes serialized)\n",
			n, elapsed, float64(elapsed.Nanoseconds())/float64(n), totalBytes)
	}
	fmt.Println()

	// =========================================================================
	// 8. Tiered query cost: creating temp bitmaps from singles
	// =========================================================================
	fmt.Println("=== Tiered Query: Temp Bitmap Creation from Singles ===")
	for _, n := range []int{1, 10, 100, 1000, 10000} {
		start := time.Now()
		const iters = 100
		for iter := 0; iter < iters; iter++ {
			for i := 0; i < n; i++ {
				b := roaring.New()
				b.Add(uint32(i * 1000))
				runtime.KeepAlive(b)
			}
		}
		elapsed := time.Since(start)
		perCall := elapsed / time.Duration(iters*n)

		fmt.Printf("  %5d matching singles: %v per query (%v per bitmap)\n",
			n, elapsed/iters, perCall)
	}
}
