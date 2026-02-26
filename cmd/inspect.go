package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/tamirms/streamhash"
	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/store"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// =============================================================================
// Inspect Command
// =============================================================================

func runInspect(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	segment := fs.Int("segment", -1, "Inspect a single segment ID (default: all segments)")
	verbose := fs.Bool("verbose", false, "Report per-term bitmap cardinality and byte sizes")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: inspect [options]\n\n")
		fmt.Fprintf(os.Stderr, "Report term counts per index type per segment directory.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	cmdInspect(cfg, *segment, *verbose)
}

var indexNames = [5]string{"contracts", "topic0", "topic1", "topic2", "topic3"}

type segmentInfo struct {
	id     string
	counts [5]uint64 // contracts, topic0..topic3
}

func cmdInspect(cfg *config.Config, segmentFilter int, verbose bool) {
	p := message.NewPrinter(language.English)

	basePath := cfg.Storage.SegmentPath
	if basePath == "" {
		fmt.Fprintf(os.Stderr, "Error: storage.segment_path not configured\n")
		os.Exit(1)
	}

	entries, err := os.ReadDir(basePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading segment directory %s: %v\n", basePath, err)
		os.Exit(1)
	}

	// Collect segment directories
	var segDirs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		// Segment dirs are zero-padded numeric names like "000000", "000001"
		if len(name) == 0 {
			continue
		}
		isNumeric := true
		for _, c := range name {
			if c < '0' || c > '9' {
				isNumeric = false
				break
			}
		}
		if !isNumeric {
			continue
		}
		if segmentFilter >= 0 {
			want := fmt.Sprintf("%06d", segmentFilter)
			if name != want {
				continue
			}
		}
		segDirs = append(segDirs, name)
	}
	sort.Strings(segDirs)

	if len(segDirs) == 0 {
		if segmentFilter >= 0 {
			fmt.Fprintf(os.Stderr, "No segment directory found for segment %d\n", segmentFilter)
		} else {
			fmt.Fprintf(os.Stderr, "No segment directories found in %s\n", basePath)
		}
		os.Exit(1)
	}

	var segments []segmentInfo
	var totals [5]uint64

	for _, dir := range segDirs {
		dirPath := filepath.Join(basePath, dir)
		var info segmentInfo
		info.id = dir

		for i, name := range indexNames {
			hashPath := filepath.Join(dirPath, name+".hash")
			if _, err := os.Stat(hashPath); err != nil {
				continue
			}
			idx, err := streamhash.Open(hashPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to open %s: %v\n", hashPath, err)
				continue
			}
			info.counts[i] = idx.NumKeys()
			idx.Close()
		}

		for i := range totals {
			totals[i] += info.counts[i]
		}
		segments = append(segments, info)
	}

	// Print table
	p.Printf("\n%-8s  %10s  %10s  %10s  %10s  %10s  %10s\n",
		"segment", "contracts", "topic0", "topic1", "topic2", "topic3", "total")
	p.Printf("%s\n", strings.Repeat("─", 78))

	for _, seg := range segments {
		var rowTotal uint64
		for _, c := range seg.counts {
			rowTotal += c
		}
		p.Printf("%-8s  %10d  %10d  %10d  %10d  %10d  %10d\n",
			seg.id, seg.counts[0], seg.counts[1], seg.counts[2], seg.counts[3], seg.counts[4], rowTotal)
	}

	var grandTotal uint64
	for _, c := range totals {
		grandTotal += c
	}
	p.Printf("%s\n", strings.Repeat("─", 78))
	p.Printf("%-8s  %10d  %10d  %10d  %10d  %10d  %10d\n",
		"TOTAL", totals[0], totals[1], totals[2], totals[3], totals[4], grandTotal)

	// Verbose: per-term bitmap stats
	if verbose {
		p.Printf("\n=== Per-Index Bitmap Stats ===\n")
		for _, seg := range segments {
			dirPath := filepath.Join(basePath, seg.id)
			for i, name := range indexNames {
				if seg.counts[i] == 0 {
					continue
				}
				printBitmapStats(p, dirPath, name, seg.id, seg.counts[i])
			}
		}
	}
}

func printBitmapStats(p *message.Printer, dirPath, name, segID string, numKeys uint64) {
	hashPath := filepath.Join(dirPath, name+".hash")
	packPath := filepath.Join(dirPath, name+".pack")

	packMmap, err := store.OpenMmap(packPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to mmap %s: %v\n", packPath, err)
		return
	}
	defer packMmap.Close()

	fileSize := uint64(packMmap.Len())
	trailerSize := (numKeys + 1) * 8
	if fileSize < trailerSize {
		fmt.Fprintf(os.Stderr, "Warning: pack file too small %s\n", packPath)
		return
	}
	trailerStart := fileSize - trailerSize

	// Re-open hash to verify numKeys (already known, but keep consistent)
	_ = hashPath

	var minCard, maxCard, totalCard uint64
	var minBytes, maxBytes, totalBytes uint64
	first := true

	for slot := uint64(0); slot < numKeys; slot++ {
		offStart := trailerStart + slot*8
		offEnd := trailerStart + (slot+1)*8

		bitmapStart := binary.LittleEndian.Uint64(packMmap.Data()[offStart : offStart+8])
		bitmapEnd := binary.LittleEndian.Uint64(packMmap.Data()[offEnd : offEnd+8])

		if bitmapEnd < bitmapStart || bitmapEnd > trailerStart {
			continue
		}

		bitmapBytes := packMmap.Data()[bitmapStart:bitmapEnd]
		byteLen := bitmapEnd - bitmapStart

		bm := roaring.New()
		if err := bm.UnmarshalBinary(bitmapBytes); err != nil {
			continue
		}
		card := bm.GetCardinality()

		totalCard += card
		totalBytes += byteLen

		if first {
			minCard = card
			maxCard = card
			minBytes = byteLen
			maxBytes = byteLen
			first = false
		} else {
			if card < minCard {
				minCard = card
			}
			if card > maxCard {
				maxCard = card
			}
			if byteLen < minBytes {
				minBytes = byteLen
			}
			if byteLen > maxBytes {
				maxBytes = byteLen
			}
		}
	}

	meanCard := float64(0)
	meanBytes := float64(0)
	if numKeys > 0 {
		meanCard = float64(totalCard) / float64(numKeys)
		meanBytes = float64(totalBytes) / float64(numKeys)
	}

	p.Printf("\n  [%s/%s] %d terms\n", segID, name, numKeys)
	p.Printf("    Cardinality:  min=%d  max=%d  mean=%.1f  total=%d\n", minCard, maxCard, meanCard, totalCard)
	p.Printf("    Byte size:    min=%d  max=%d  mean=%.1f  total=%d\n", minBytes, maxBytes, meanBytes, totalBytes)
}
