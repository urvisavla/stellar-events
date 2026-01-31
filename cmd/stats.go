package main

import (
	"flag"
	"fmt"
	"os"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Stats Command
// =============================================================================

func runStats(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	top := fs.Int("top", 10, "Number of top entries to show in distribution stats")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: stats [options]\n\n")
		fmt.Fprintf(os.Stderr, "Shows comprehensive database statistics including:\n")
		fmt.Fprintf(os.Stderr, "  - Event counts and ledger ranges\n")
		fmt.Fprintf(os.Stderr, "  - RocksDB storage metrics per column family\n")
		fmt.Fprintf(os.Stderr, "  - Bitmap index statistics\n")
		fmt.Fprintf(os.Stderr, "  - Unique index entry counts\n")
		fmt.Fprintf(os.Stderr, "  - Event distribution by contract/topic\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	cmdStats(cfg, *top)
}

func cmdStats(cfg *config.Config, top int) {
	p := message.NewPrinter(language.English)

	eventStore, err := openEventStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	fmt.Fprintf(os.Stderr, "Calculating statistics...\n")

	dbStats, err := eventStore.GetStats()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get stats: %v\n", err)
		os.Exit(1)
	}

	p.Printf("\n=== Database Stats ===\n")
	p.Printf("  Total events:          %d\n", dbStats.TotalEvents)
	if dbStats.TotalEvents > 0 {
		p.Printf("  Ledger range:          %d - %d\n", dbStats.MinLedger, dbStats.MaxLedger)
		p.Printf("  Ledger span:           %d ledgers\n", dbStats.MaxLedger-dbStats.MinLedger+1)
	}
	p.Printf("  Last processed ledger: %d\n", dbStats.LastProcessed)
	p.Printf("  Unique contracts:      %d\n", dbStats.UniqueContracts)

	// Per-column-family storage stats
	snapshot, err := eventStore.GetStorageSnapshot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get storage stats: %v\n", err)
	} else {
		printStorageStats(snapshot)
	}

	// Bitmap index stats
	bitmapStats := eventStore.GetBitmapStats()
	if bitmapStats != nil {
		p.Printf("\n=== Bitmap Index Stats ===\n")
		p.Printf("  Current segment ID:    %d (each segment = 1M ledgers)\n", bitmapStats.CurrentSegmentID)
		p.Printf("  Hot segments (memory): %d segments, %d entries, %.2f MB\n",
			bitmapStats.HotSegmentCount,
			bitmapStats.HotSegmentCards,
			float64(bitmapStats.HotSegmentMemBytes)/(1024*1024))
		p.Printf("  Stored bitmap segments:\n")
		p.Printf("    Contracts: %d\n", bitmapStats.ContractIndexCount)
		p.Printf("    Topic0:    %d\n", bitmapStats.Topic0IndexCount)
		p.Printf("    Topic1:    %d\n", bitmapStats.Topic1IndexCount)
		p.Printf("    Topic2:    %d\n", bitmapStats.Topic2IndexCount)
		p.Printf("    Topic3:    %d\n", bitmapStats.Topic3IndexCount)
	} else {
		p.Printf("\n=== Bitmap Index Stats ===\n")
		p.Printf("  (not available)\n")
	}

	// Unique index counts
	fmt.Fprintf(os.Stderr, "Counting unique index entries...\n")
	uniqueCounts, err := eventStore.CountUniqueIndexes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to count unique indexes: %v\n", err)
	} else if uniqueCounts != nil {
		p.Printf("\n=== Unique Index Counts ===\n")
		p.Printf("  Contracts: %d unique (%d total events)\n", uniqueCounts.UniqueContracts, uniqueCounts.TotalContractEvents)
		p.Printf("  Topic0:    %d unique (%d total events)\n", uniqueCounts.UniqueTopic0, uniqueCounts.TotalTopic0Events)
		p.Printf("  Topic1:    %d unique (%d total events)\n", uniqueCounts.UniqueTopic1, uniqueCounts.TotalTopic1Events)
		p.Printf("  Topic2:    %d unique (%d total events)\n", uniqueCounts.UniqueTopic2, uniqueCounts.TotalTopic2Events)
		p.Printf("  Topic3:    %d unique (%d total events)\n", uniqueCounts.UniqueTopic3, uniqueCounts.TotalTopic3Events)
	}

	// Distribution stats
	fmt.Fprintf(os.Stderr, "Computing distribution statistics (top %d)...\n", top)
	dist, err := eventStore.GetIndexDistribution(top)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to get distribution: %v\n", err)
	} else {
		printDistribution("Contracts", dist.Contracts)
		printDistribution("Topic0", dist.Topic0)
		printDistribution("Topic1", dist.Topic1)
		printDistribution("Topic2", dist.Topic2)
		printDistribution("Topic3", dist.Topic3)
	}
}

// printStorageStats prints per-column-family storage statistics
func printStorageStats(snapshot *store.StorageSnapshot) {
	p := message.NewPrinter(language.English)

	p.Printf("\n=== Storage by Column Family ===\n")
	p.Printf("  Name             Keys          SST Files    Memtable     Pending      Files\n")
	p.Printf("  ─────────────────────────────────────────────────────────────────────────────\n")

	// Print in a consistent order
	cfOrder := []string{"events", "bitmap", "unique", "default"}
	for _, name := range cfOrder {
		cf, ok := snapshot.ColumnFamilies[name]
		if !ok {
			continue
		}
		sstMB := float64(cf.SSTFilesBytes) / (1024 * 1024)
		memMB := float64(cf.MemtableBytes) / (1024 * 1024)
		pendingMB := float64(cf.PendingCompact) / (1024 * 1024)
		p.Printf("  %-14s %12d  %8.1f MB  %8.1f MB  %8.1f MB  %5d\n",
			cf.Name, cf.EstimatedKeys, sstMB, memMB, pendingMB, cf.NumFiles)
	}

	p.Printf("  ─────────────────────────────────────────────────────────────────────────────\n")
	totalSSTMB := float64(snapshot.TotalSST) / (1024 * 1024)
	totalMemMB := float64(snapshot.TotalMemtable) / (1024 * 1024)
	p.Printf("  %-14s %12s  %8.1f MB  %8.1f MB  %8s     %5d\n",
		"TOTAL", "", totalSSTMB, totalMemMB, "", snapshot.TotalFiles)
}
