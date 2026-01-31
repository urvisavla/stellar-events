package main

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Store Initialization
// =============================================================================

// openEventStore opens the event store with config options
func openEventStore(cfg *config.Config) (*store.RocksDBEventStore, error) {
	es, err := store.NewEventStoreWithOptions(
		cfg.Storage.DBPath,
		configToRocksDBOptions(&cfg.Storage),
		configToIndexOptions(&cfg.Indexes),
	)
	if err != nil {
		return nil, err
	}

	// Set event format if configured
	if cfg.Storage.EventFormat != "" {
		es.SetEventFormat(cfg.Storage.EventFormat)
	}

	return es, nil
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
}
