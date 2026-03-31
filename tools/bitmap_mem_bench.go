// Measures per-entry memory cost of map[[16]byte]*roaring.Bitmap at various scales.
//
// Usage:
//   go run tools/bitmap_mem_bench.go
//
// Tests three configurations:
//   1. Bitmap struct size (unsafe.Sizeof)
//   2. Slice of bitmaps (bitmap object + internal allocations, no map)
//   3. Map of bitmaps (bitmap + map bucket overhead)

package main

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
)

func main() {
	// 1. Struct size (no allocations, just the top-level struct)
	bm := roaring.New()
	fmt.Printf("roaring.Bitmap struct size: %d bytes\n", unsafe.Sizeof(*bm))
	fmt.Println()

	// 2. Slice of bitmaps (bitmap cost without map overhead)
	fmt.Println("Slice of bitmaps (no map):")
	for _, N := range []int{100_000, 500_000, 1_000_000, 2_000_000} {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		bitmaps := make([]*roaring.Bitmap, N)
		for i := 0; i < N; i++ {
			b := roaring.New()
			b.Add(uint32(i))
			bitmaps[i] = b
		}

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		total := after.HeapAlloc - before.HeapAlloc
		fmt.Printf("  N=%9d: %12d bytes total, %.1f bytes/entry\n", N, total, float64(total)/float64(N))
		runtime.KeepAlive(bitmaps)
	}
	fmt.Println()

	// 3. Map of bitmaps (bitmap + map bucket overhead)
	fmt.Println("map[[16]byte]*roaring.Bitmap (1 element per bitmap):")
	for _, N := range []int{100_000, 500_000, 1_000_000, 2_000_000} {
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		mp := make(map[[16]byte]*roaring.Bitmap)
		for i := 0; i < N; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			b := roaring.New()
			b.Add(uint32(i))
			mp[key] = b
		}

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		total := after.HeapAlloc - before.HeapAlloc
		fmt.Printf("  N=%9d: %12d bytes total, %.1f bytes/entry\n", N, total, float64(total)/float64(N))
		runtime.KeepAlive(mp)
	}
	fmt.Println()

	// 4. Map with varying bitmap cardinalities
	fmt.Println("map[[16]byte]*roaring.Bitmap with varying cardinalities (N=500,000):")
	for _, card := range []int{1, 10, 100, 1000} {
		const N = 500_000
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		mp := make(map[[16]byte]*roaring.Bitmap)
		for i := 0; i < N; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			b := roaring.New()
			for j := 0; j < card; j++ {
				b.Add(uint32(i*card + j))
			}
			mp[key] = b
		}

		runtime.GC()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		total := after.HeapAlloc - before.HeapAlloc
		fmt.Printf("  cardinality=%4d: %12d bytes total, %.1f bytes/entry\n", card, total, float64(total)/float64(N))
		runtime.KeepAlive(mp)
	}
	fmt.Println()

	// 5. Effect of RunOptimize on bitmaps with consecutive IDs
	fmt.Println("RunOptimize effect (N=500,000, consecutive IDs):")
	for _, card := range []int{1, 10, 100, 1000} {
		const N = 500_000

		// Without RunOptimize
		runtime.GC()
		runtime.GC()
		var b1 runtime.MemStats
		runtime.ReadMemStats(&b1)

		mp1 := make(map[[16]byte]*roaring.Bitmap)
		for i := 0; i < N; i++ {
			var key [16]byte
			key[0] = byte(i)
			key[1] = byte(i >> 8)
			key[2] = byte(i >> 16)
			b := roaring.New()
			for j := 0; j < card; j++ {
				b.Add(uint32(i*card + j))
			}
			mp1[key] = b
		}

		runtime.GC()
		runtime.GC()
		var a1 runtime.MemStats
		runtime.ReadMemStats(&a1)
		before := a1.HeapAlloc - b1.HeapAlloc

		// With RunOptimize
		runtime.GC()
		runtime.GC()
		var b2 runtime.MemStats
		runtime.ReadMemStats(&b2)

		mp2 := make(map[[16]byte]*roaring.Bitmap)
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
			mp2[key] = b
		}

		runtime.GC()
		runtime.GC()
		var a2 runtime.MemStats
		runtime.ReadMemStats(&a2)
		after := a2.HeapAlloc - b2.HeapAlloc

		saved := int64(before) - int64(after)
		pct := float64(saved) / float64(before) * 100
		fmt.Printf("  cardinality=%4d: without=%.1f bytes/entry, with=%.1f bytes/entry, saved=%.1f%% \n",
			card, float64(before)/N, float64(after)/N, pct)
		runtime.KeepAlive(mp1)
		runtime.KeepAlive(mp2)
	}
}
