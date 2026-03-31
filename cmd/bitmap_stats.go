package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/store"
)

func runBitmapStats(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("bitmap-stats", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: bitmap-stats [options]\n\n")
		fmt.Fprintf(os.Stderr, "Loads hot segment bitmaps from index_deltas.dat one segment at a time\n")
		fmt.Fprintf(os.Stderr, "and reports container/cardinality distribution per field.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	segmentPath := cfg.Storage.SegmentPath
	if segmentPath == "" {
		fmt.Fprintf(os.Stderr, "Error: storage.segment_path is required\n")
		os.Exit(2)
	}

	hotDir := filepath.Join(segmentPath, "hot")
	entries, err := os.ReadDir(hotDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading hot dir %s: %v\n", hotDir, err)
		os.Exit(1)
	}

	// Discover hot segments with index_deltas.dat
	var segIDs []uint32
	segDirs := make(map[uint32]string)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		segID64, err := strconv.ParseUint(entry.Name(), 10, 32)
		if err != nil {
			continue
		}
		segDir := filepath.Join(hotDir, entry.Name())
		deltasPath := filepath.Join(segDir, "index_deltas.dat")
		if _, err := os.Stat(deltasPath); err != nil {
			continue
		}
		segID := uint32(segID64)
		segIDs = append(segIDs, segID)
		segDirs[segID] = segDir
	}

	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })
	fmt.Fprintf(os.Stderr, "Found %d hot segment(s) with index_deltas.dat\n\n", len(segIDs))

	fieldNames := [5]string{"contracts", "topic0", "topic1", "topic2", "topic3"}

	for _, segID := range segIDs {
		segDir := segDirs[segID]
		fmt.Fprintf(os.Stderr, "=== Segment %06d ===\n", segID)

		// Load index_deltas.dat
		deltasPath := filepath.Join(segDir, "index_deltas.dat")
		deltasData, err := os.ReadFile(deltasPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Error reading %s: %v\n\n", deltasPath, err)
			continue
		}

		rebuildStart := time.Now()

		contracts := make(map[[16]byte]*roaring.Bitmap)
		var topics [4]map[[16]byte]*roaring.Bitmap
		for i := range topics {
			topics[i] = make(map[[16]byte]*roaring.Bitmap)
		}

		numDeltas := len(deltasData) / store.IndexDeltaSize
		var deltaCountPerField [5]int

		for i := 0; i < numDeltas; i++ {
			off := i * store.IndexDeltaSize
			fieldIndex := deltasData[off]
			var termHash [16]byte
			copy(termHash[:], deltasData[off+1:off+17])
			eventID := binary.LittleEndian.Uint32(deltasData[off+17 : off+21])

			if fieldIndex == 0 {
				deltaCountPerField[0]++
				bm, ok := contracts[termHash]
				if !ok {
					bm = roaring.New()
					contracts[termHash] = bm
				}
				bm.Add(eventID)
			} else if fieldIndex >= 1 && fieldIndex <= 4 {
				idx := fieldIndex - 1
				deltaCountPerField[fieldIndex]++
				bm, ok := topics[idx][termHash]
				if !ok {
					bm = roaring.New()
					topics[idx][termHash] = bm
				}
				bm.Add(eventID)
			}
		}

		rebuildTime := time.Since(rebuildStart)

		fieldMaps := [5]map[[16]byte]*roaring.Bitmap{contracts, topics[0], topics[1], topics[2], topics[3]}

		fmt.Fprintf(os.Stderr, "  index_deltas: %d entries, rebuild: %v\n", numDeltas, rebuildTime)
		fmt.Fprintf(os.Stderr, "  deltas: contracts=%d, topic0=%d, topic1=%d, topic2=%d, topic3=%d\n",
			deltaCountPerField[0], deltaCountPerField[1], deltaCountPerField[2], deltaCountPerField[3], deltaCountPerField[4])
		fmt.Fprintf(os.Stderr, "\n")

		for f := 0; f < 5; f++ {
			fm := fieldMaps[f]
			if len(fm) == 0 {
				continue
			}

			var (
				singleContainer int
				multiContainer  int
				totalContainers int
				cardBuckets     [6]int // 1, 2-5, 6-50, 51-1K, 1K-10K, 10K+
				totalCard       uint64
				totalBitmapBytes uint64
				maxCard         uint64
			)

			for _, bm := range fm {
				stats := bm.Stats()
				card := uint64(bm.GetCardinality())
				totalCard += card
				totalBitmapBytes += bm.GetSizeInBytes()
				totalContainers += int(stats.Containers)
				if card > maxCard {
					maxCard = card
				}

				if stats.Containers <= 1 {
					singleContainer++
				} else {
					multiContainer++
				}

				switch {
				case card <= 1:
					cardBuckets[0]++
				case card <= 5:
					cardBuckets[1]++
				case card <= 50:
					cardBuckets[2]++
				case card <= 1000:
					cardBuckets[3]++
				case card <= 10000:
					cardBuckets[4]++
				default:
					cardBuckets[5]++
				}
			}

			avgCard := float64(totalCard) / float64(len(fm))

			fmt.Fprintf(os.Stderr, "  %s: %d terms, %d total events, avg cardinality %.1f, max %d\n",
				fieldNames[f], len(fm), totalCard, avgCard, maxCard)
			fmt.Fprintf(os.Stderr, "    containers: single=%d (%d%%), multi=%d (%d%%), total=%d\n",
				singleContainer, singleContainer*100/len(fm),
				multiContainer, multiContainer*100/len(fm),
				totalContainers)
			fmt.Fprintf(os.Stderr, "    cardinality distribution: 1=%d, 2-5=%d, 6-50=%d, 51-1K=%d, 1K-10K=%d, 10K+=%d\n",
				cardBuckets[0], cardBuckets[1], cardBuckets[2], cardBuckets[3], cardBuckets[4], cardBuckets[5])
			fmt.Fprintf(os.Stderr, "    bitmap data: %s, overhead estimate: %s\n",
				formatBytesStore(int64(totalBitmapBytes)),
				formatBytesStore(int64(len(fm))*store.PerEntryOverhead))
			fmt.Fprintf(os.Stderr, "\n")
		}

		// Free memory before loading next segment
		contracts = nil
		for i := range topics {
			topics[i] = nil
		}
		runtime.GC()
		runtime.GC()
	}
}

// formatBytesStore formats bytes in human-readable form (duplicated from store package for cmd use).
func formatBytesStore(b int64) string {
	switch {
	case b >= 1024*1024*1024:
		return fmt.Sprintf("%.2f GB", float64(b)/(1024*1024*1024))
	case b >= 1024*1024:
		return fmt.Sprintf("%.2f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}