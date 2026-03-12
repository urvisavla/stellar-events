package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/progress"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Store Initialization
// =============================================================================

// openStore opens the event store with config options.
func openStore(cfg *config.Config) (*store.Store, error) {
	opts := store.Config{
		SegmentPath:       cfg.Storage.SegmentPath,
		WriteSegmentFiles: cfg.Storage.SegmentFiles,
		CompressData:      cfg.Storage.CompressData,
		BlockSize:         cfg.Storage.BlockSize,
	}

	if cfg.Storage.RocksDB {
		opts.DBPath = cfg.Storage.DBPath
		opts.RocksOpts = configToRocksDBOptions(&cfg.Storage)
	}

	return store.New(opts)
}

// configToRocksDBOptions converts config.StorageConfig to store.RocksDBOptions
func configToRocksDBOptions(cfg *config.StorageConfig) *store.RocksDBOptions {
	return &store.RocksDBOptions{
		WriteBufferSizeMB:           cfg.WriteBufferSizeMB,
		MaxWriteBufferNumber:        cfg.MaxWriteBufferNumber,
		MinWriteBufferNumberToMerge: cfg.MinWriteBufferNumberToMerge,
		BloomFilterBitsPerKey:       cfg.BloomFilterBitsPerKey,
		MaxBackgroundJobs:           cfg.MaxBackgroundJobs,
		Compression:                 cfg.Compression,
		BottommostCompression:       cfg.BottommostCompression,
		DisableWAL:                  cfg.DisableWAL,
		DisableAutoCompaction:       cfg.DisableAutoCompaction,
		TargetFileSizeMB:            cfg.TargetFileSizeMB,
		MaxBytesForLevelBaseMB:      cfg.MaxBytesForLevelBaseMB,
	}
}

// =============================================================================
// Encoding Helpers
// =============================================================================

// decodeBase64 decodes a base64 string
func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

// decodeContractID decodes a strkey-encoded contract ID (C...)
func decodeContractID(s string) ([]byte, error) {
	return strkey.Decode(strkey.VersionByteContract, s)
}

// =============================================================================
// Formatting Helpers
// =============================================================================

// formatElapsed formats a duration for display (human readable)
func formatElapsed(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", mins, secs)
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) % 60
	if hours >= 24 {
		days := hours / 24
		hours = hours % 24
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	}
	return fmt.Sprintf("%dh %dm", hours, mins)
}

// formatDuration formats a duration with appropriate precision for benchmarking
func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fus", float64(d.Nanoseconds())/1000)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1000000)
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}

// formatBytes formats byte count with appropriate unit
func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	if bytes < KB {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < MB {
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	}
	if bytes < GB {
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	}
	return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
}

// =============================================================================
// Storage Summary Helpers
// =============================================================================

// printStorageSnapshot prints a storage snapshot in a formatted table
func printStorageSnapshot(sb *strings.Builder, snapshot *store.StorageSnapshot) {
	p := message.NewPrinter(language.English)

	sb.WriteString("  Column Family      Keys          SST Files    Memtable     Files\n")
	sb.WriteString("  ─────────────────────────────────────────────────────────────────\n")

	// Print in a consistent order
	cfOrder := []string{
		"events", "unique", "default",
		"contracts_bm32", "topics_bm32",
	}
	for _, name := range cfOrder {
		cf, ok := snapshot.ColumnFamilies[name]
		if !ok {
			continue
		}
		sstMB := float64(cf.SSTFilesBytes) / (1024 * 1024)
		memMB := float64(cf.MemtableBytes) / (1024 * 1024)
		sb.WriteString(p.Sprintf("  %-16s %12d    %8.1f MB  %8.1f MB  %5d\n",
			cf.Name, cf.EstimatedKeys, sstMB, memMB, cf.NumFiles))
	}

	sb.WriteString("  ─────────────────────────────────────────────────────────────────\n")
	totalSSTMB := float64(snapshot.TotalSST) / (1024 * 1024)
	totalMemMB := float64(snapshot.TotalMemtable) / (1024 * 1024)
	sb.WriteString(p.Sprintf("  %-16s %12s    %8.1f MB  %8.1f MB  %5d\n",
		"TOTAL", "", totalSSTMB, totalMemMB, snapshot.TotalFiles))
}

// printCompactionSummary prints the compaction results in a formatted table
func printCompactionSummary(sb *strings.Builder, cs *store.CompactionSummary) {
	p := message.NewPrinter(language.English)

	sb.WriteString("\n")
	sb.WriteString(p.Sprintf("=== Compaction Summary (%s) ===\n", formatElapsed(cs.Duration)))
	sb.WriteString("\n")
	sb.WriteString("  Column Family      Before       After        Reclaimed    Savings\n")
	sb.WriteString("  ─────────────────────────────────────────────────────────────────\n")

	// Print in a consistent order
	cfOrder := []string{
		"events", "unique", "default",
		"contracts_bm32", "topics_bm32",
	}
	for _, name := range cfOrder {
		cf, ok := cs.PerCF[name]
		if !ok {
			continue
		}
		beforeMB := float64(cf.BeforeBytes) / (1024 * 1024)
		afterMB := float64(cf.AfterBytes) / (1024 * 1024)
		reclaimedMB := float64(cf.Reclaimed) / (1024 * 1024)
		sb.WriteString(p.Sprintf("  %-16s %8.1f MB  %8.1f MB  %8.1f MB  %6.1f%%\n",
			cf.Name, beforeMB, afterMB, reclaimedMB, cf.SavingsPercent))
	}

	sb.WriteString("  ─────────────────────────────────────────────────────────────────\n")
	beforeMB := float64(cs.Before.TotalSST) / (1024 * 1024)
	afterMB := float64(cs.After.TotalSST) / (1024 * 1024)
	reclaimedMB := float64(cs.TotalReclaimed) / (1024 * 1024)
	sb.WriteString(p.Sprintf("  %-16s %8.1f MB  %8.1f MB  %8.1f MB  %6.1f%%\n",
		"TOTAL", beforeMB, afterMB, reclaimedMB, cs.SavingsPercent))
}

// =============================================================================
// Stats Display Helpers
// =============================================================================

// setupStderrTee redirects os.Stderr so that all output is written to both
// the console and a log file. Returns a cleanup function that must be called
// before the program exits (typically via defer).
func setupStderrTee(logPath string) func() {
	logFile, err := os.Create(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to create log file %s: %v\n", logPath, err)
		return func() {}
	}

	origStderr := os.Stderr

	r, w, err := os.Pipe()
	if err != nil {
		logFile.Close()
		fmt.Fprintf(os.Stderr, "Warning: failed to create stderr tee: %v\n", err)
		return func() {}
	}

	os.Stderr = w

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				origStderr.Write(buf[:n])
				logFile.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
	}()

	return func() {
		w.Close()
		<-done
		r.Close()
		logFile.Close()
		os.Stderr = origStderr
	}
}

// writeSegmentMetricsSummary appends a per-segment metrics table to the summary.
func writeSegmentMetricsSummary(sb *strings.Builder, metrics []progress.SegmentStats) {
	if len(metrics) == 0 {
		return
	}

	p := message.NewPrinter(language.English)

	sb.WriteString("\n")
	sb.WriteString("Per-Segment Metrics:\n")
	sb.WriteString("  Segment   Ledgers   Events   Event Data     Terms   Index Data     Ingest     Freeze     Heap   GC Freed   Events/s\n")
	sb.WriteString("  ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────\n")

	var totalEvents int
	var totalEventBytes, totalIndexBytes int64
	var totalIngestMs, totalFreezeMs float64
	var totalFreedMB int64
	var totalTerms int
	for _, m := range metrics {
		freezeStr := ""
		if m.FreezeWallMs > 0 {
			freezeStr = formatElapsed(time.Duration(m.FreezeWallMs) * time.Millisecond)
		}
		freedStr := ""
		if m.HeapFreedMB != 0 {
			freedStr = p.Sprintf("%d MB", m.HeapFreedMB)
		}
		sb.WriteString(p.Sprintf("  %06d    %5d    %6d   %10s   %5d   %10s   %6s   %8s   %4d MB   %6s   %8.0f\n",
			m.SegmentID, m.Ledgers, m.Events,
			formatBytes(m.EventBytes), m.IndexTerms, formatBytes(m.IndexBytes),
			formatElapsed(time.Duration(m.IngestWallMs)*time.Millisecond),
			freezeStr, m.HeapInUseMB, freedStr, m.EventsPerSec))

		totalEvents += m.Events
		totalEventBytes += m.EventBytes
		totalIndexBytes += m.IndexBytes
		totalIngestMs += m.IngestWallMs
		totalFreezeMs += m.FreezeWallMs
		totalFreedMB += m.HeapFreedMB
		totalTerms += m.IndexTerms
	}

	sb.WriteString("  ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────\n")
	totalIngestSec := totalIngestMs / 1000.0
	var avgEventsPerSec float64
	if totalIngestSec > 0 {
		avgEventsPerSec = float64(totalEvents) / totalIngestSec
	}
	totalFreezeStr := ""
	if totalFreezeMs > 0 {
		totalFreezeStr = formatElapsed(time.Duration(totalFreezeMs) * time.Millisecond)
	}
	totalFreedStr := ""
	if totalFreedMB != 0 {
		totalFreedStr = p.Sprintf("%d MB", totalFreedMB)
	}
	sb.WriteString(p.Sprintf("  TOTAL              %6d   %10s   %5d   %10s   %6s   %8s            %6s   %8.0f\n",
		totalEvents, formatBytes(totalEventBytes), totalTerms, formatBytes(totalIndexBytes),
		formatElapsed(time.Duration(totalIngestMs)*time.Millisecond),
		totalFreezeStr, totalFreedStr, avgEventsPerSec))
	sb.WriteString(p.Sprintf("  Segments: %d\n", len(metrics)))
}

// printDistribution prints distribution stats for an index type
func printDistribution(name string, stats *store.DistributionStats) {
	p := message.NewPrinter(language.English)
	if stats == nil || stats.Count == 0 {
		p.Printf("\n%s Distribution: (no data)\n", name)
		return
	}

	p.Printf("\n%s Distribution (%d unique values, %d total events):\n", name, stats.Count, stats.Total)
	p.Printf("  Min:    %d events\n", stats.Min)
	p.Printf("  P50:    %d events (median)\n", stats.P50)
	p.Printf("  P75:    %d events\n", stats.P75)
	p.Printf("  P90:    %d events\n", stats.P90)
	p.Printf("  P99:    %d events\n", stats.P99)
	p.Printf("  Max:    %d events\n", stats.Max)
	p.Printf("  Mean:   %.2f events\n", stats.Mean)

	// Show count of values > 32 bytes (relevant for bitmap index truncation)
	if stats.Over32Bytes > 0 {
		pct := float64(stats.Over32Bytes) * 100.0 / float64(stats.Count)
		p.Printf("  >32 bytes: %d values (%.2f%% - may collide in bitmap index)\n", stats.Over32Bytes, pct)
	}

	if len(stats.TopN) > 0 {
		p.Printf("  Top %d by event count:\n", len(stats.TopN))
		for i, entry := range stats.TopN {
			p.Printf("    %2d. %s (%d events)\n", i+1, entry.Value, entry.EventCount)
		}
	}
	if len(stats.BottomN) > 0 {
		p.Printf("  Bottom %d by event count:\n", len(stats.BottomN))
		for i, entry := range stats.BottomN {
			p.Printf("    %2d. %s (%d events)\n", i+1, entry.Value, entry.EventCount)
		}
	}
}
