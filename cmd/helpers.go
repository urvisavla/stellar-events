package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/progress"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Store Initialization
// =============================================================================

// openEventStore opens the event store with config options
func openEventStore(cfg *config.Config) (store.Store, error) {
	return store.NewEventStoreWithOptions(
		cfg.Storage.DBPath,
		configToRocksDBOptions(&cfg.Storage),
		configToIndexOptions(&cfg.Indexes),
	)
}

// configToRocksDBOptions converts config.StorageConfig to store.RocksDBOptions
func configToRocksDBOptions(cfg *config.StorageConfig) *store.RocksDBOptions {
	return &store.RocksDBOptions{
		WriteBufferSizeMB:           cfg.WriteBufferSizeMB,
		MaxWriteBufferNumber:        cfg.MaxWriteBufferNumber,
		MinWriteBufferNumberToMerge: cfg.MinWriteBufferNumberToMerge,
		BlockCacheSizeMB:            cfg.BlockCacheSizeMB,
		BloomFilterBitsPerKey:       cfg.BloomFilterBitsPerKey,
		CacheIndexAndFilterBlocks:   cfg.CacheIndexAndFilterBlocks,
		MaxBackgroundJobs:           cfg.MaxBackgroundJobs,
		Compression:                 cfg.Compression,
		BottommostCompression:       cfg.BottommostCompression,
		DisableWAL:                  cfg.DisableWAL,
		DisableAutoCompaction:       cfg.DisableAutoCompaction,
		TargetFileSizeMB:            cfg.TargetFileSizeMB,
		MaxBytesForLevelBaseMB:      cfg.MaxBytesForLevelBaseMB,
	}
}

// configToIndexOptions converts config.IndexConfig to store.IndexConfig
func configToIndexOptions(cfg *config.IndexConfig) *store.IndexConfig {
	return &store.IndexConfig{
		ContractID: cfg.ContractID,
		Topics:     cfg.Topics,
	}
}

// =============================================================================
// Stats Provider Adapter
// =============================================================================

// statsProviderAdapter adapts store.Store to progress.StatsProvider
type statsProviderAdapter struct {
	store store.Store
}

func (a *statsProviderAdapter) GetSnapshotStats() *progress.SnapshotStats {
	s := a.store.GetSnapshotStats()
	return &progress.SnapshotStats{
		SSTFilesSizeBytes:   s.SSTFilesSizeBytes,
		MemtableSizeBytes:   s.MemtableSizeBytes,
		EstimatedNumKeys:    s.EstimatedNumKeys,
		PendingCompactBytes: s.PendingCompactBytes,
		L0Files:             s.L0Files,
		L1Files:             s.L1Files,
		L2Files:             s.L2Files,
		L3Files:             s.L3Files,
		L4Files:             s.L4Files,
		L5Files:             s.L5Files,
		L6Files:             s.L6Files,
		RunningCompactions:  s.RunningCompactions,
		CompactionPending:   s.CompactionPending,
	}
}

// =============================================================================
// Encoding Helpers
// =============================================================================

// decodeBase64 decodes a base64 string
func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

// =============================================================================
// Formatting Helpers
// =============================================================================

// bytesToMB converts a byte count string to megabytes
func bytesToMB(bytesStr string) string {
	var bytes int64
	if _, err := fmt.Sscan(bytesStr, &bytes); err != nil {
		return bytesStr
	}
	mb := float64(bytes) / (1024 * 1024)
	return fmt.Sprintf("%.2f", mb)
}

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

// =============================================================================
// Progress Tracking Helpers
// =============================================================================

// printPerformanceTrend reads the history file and prints min/max/avg rates
func printPerformanceTrend(historyFile string) {
	data, err := os.ReadFile(historyFile)
	if err != nil {
		return
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		return
	}

	var minRate, maxRate, sumRate float64
	var count int
	minRate = -1

	for _, line := range lines {
		if line == "" {
			continue
		}
		var snapshot progress.ProgressSnapshot
		if err := json.Unmarshal([]byte(line), &snapshot); err != nil {
			continue
		}

		rate := snapshot.LedgersPerSec
		if rate <= 0 {
			continue
		}

		if minRate < 0 || rate < minRate {
			minRate = rate
		}
		if rate > maxRate {
			maxRate = rate
		}
		sumRate += rate
		count++
	}

	if count < 2 {
		return
	}

	avgRate := sumRate / float64(count)

	fmt.Fprintf(os.Stderr, "\nPerformance Trend (%d intervals):\n", count)
	fmt.Fprintf(os.Stderr, "  Min ledgers/sec:   %.2f\n", minRate)
	fmt.Fprintf(os.Stderr, "  Max ledgers/sec:   %.2f\n", maxRate)
	fmt.Fprintf(os.Stderr, "  Avg ledgers/sec:   %.2f\n", avgRate)
}

// =============================================================================
// Stats Display Helpers
// =============================================================================

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

	if len(stats.TopN) > 0 {
		p.Printf("  Top %d by event count:\n", len(stats.TopN))
		for i, entry := range stats.TopN {
			displayVal := entry.Value
			//if len(displayVal) > 44 {
			//	//	displayVal = displayVal[:20] + "..." + displayVal[len(displayVal)-20:]
			//}
			p.Printf("    %2d. %s (%d events)\n", i+1, displayVal, entry.EventCount)
		}
	}
}
