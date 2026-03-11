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
	"github.com/tamir/events-analysis/packfile"
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

type segmentInfo struct {
	id         string
	termCount  uint64 // combined term count from index.hash
	eventCount uint32 // total events from events.pack appData
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
	var totalTerms uint64

	for _, dir := range segDirs {
		dirPath := filepath.Join(basePath, dir)
		var info segmentInfo
		info.id = dir

		hashPath := filepath.Join(dirPath, "index.hash")
		if _, err := os.Stat(hashPath); err == nil {
			idx, err := streamhash.Open(hashPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to open %s: %v\n", hashPath, err)
			} else {
				info.termCount = idx.NumKeys()
				idx.Close()
			}
		}

		// Read event count from events.pack appData (embedded ledger offsets)
		eventsPath := filepath.Join(dirPath, store.EventsFileName)
		if _, err := os.Stat(eventsPath); err == nil {
			pr := packfile.Open(eventsPath)
			if appData, err := pr.AppData(); err == nil && len(appData) == store.SegmentLedgerOffsetsSize {
				lm := &store.SegmentLedgerOffsets{Data: appData}
				info.eventCount = lm.TotalEvents()
			}
			pr.Close()
		}

		totalTerms += info.termCount
		segments = append(segments, info)
	}

	// Print table
	p.Printf("\n%-8s  %10s  %10s\n",
		"segment", "events", "terms")
	p.Printf("%s\n", strings.Repeat("─", 32))

	var totalEvents uint64
	for _, seg := range segments {
		totalEvents += uint64(seg.eventCount)
		p.Printf("%-8s  %10d  %10d\n",
			seg.id, seg.eventCount, seg.termCount)
	}

	p.Printf("%s\n", strings.Repeat("─", 32))
	p.Printf("%-8s  %10d  %10d\n",
		"TOTAL", totalEvents, totalTerms)

	// Verbose: per-segment event pack + bitmap stats
	if verbose {
		p.Printf("\n=== Per-Segment Stats ===\n")
		for _, seg := range segments {
			dirPath := filepath.Join(basePath, seg.id)
			printEventPackStats(p, dirPath, seg.id)
			if seg.termCount > 0 {
				printBitmapStats(p, dirPath, "index", seg.id, seg.termCount)
			}
		}
	}
}

func recordFormatString(f packfile.RecordFormat) string {
	switch f {
	case packfile.Compressed:
		return "compressed"
	case packfile.Uncompressed:
		return "uncompressed"
	case packfile.Raw:
		return "raw"
	default:
		return fmt.Sprintf("unknown(%d)", f)
	}
}

func printEventPackStats(p *message.Printer, dirPath, segID string) {
	eventsPath := filepath.Join(dirPath, store.EventsFileName)
	fi, err := os.Stat(eventsPath)
	if err != nil {
		return // no events.pack in this segment
	}
	fileSize := fi.Size()

	pr := packfile.Open(eventsPath)
	defer pr.Close()

	trailer, err := pr.Trailer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to read trailer for %s: %v\n", eventsPath, err)
		return
	}

	avgBytes := float64(0)
	if trailer.TotalItems > 0 {
		avgBytes = float64(fileSize) / float64(trailer.TotalItems)
	}

	p.Printf("\n  [%s/events.pack] %d events, %d records × %d\n",
		segID, trailer.TotalItems, trailer.RecordCount, trailer.RecordSize)
	p.Printf("    File size: %s   Avg: %s/event   Format: %s\n",
		formatBytes(fileSize), formatBytes(int64(avgBytes)), recordFormatString(trailer.Format))
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

		recordBytes := packMmap.Data()[bitmapStart:bitmapEnd]
		byteLen := bitmapEnd - bitmapStart

		// Skip 5-byte prefix (4-byte fingerprint + 1-byte fieldIndex)
		if len(recordBytes) < 5 {
			continue
		}
		bitmapBytes := recordBytes[5:]

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
