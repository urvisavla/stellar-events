package main

import (
	"fmt"
	"os"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
)

// =============================================================================
// Stats Command
// =============================================================================

func runStats(cfg *config.Config, args []string) {
	// Check for help flag
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			fmt.Fprintf(os.Stderr, "Usage: stats\n\n")
			fmt.Fprintf(os.Stderr, "Shows comprehensive database statistics including:\n")
			fmt.Fprintf(os.Stderr, "  - Event counts and ledger ranges\n")
			fmt.Fprintf(os.Stderr, "  - RocksDB storage metrics\n")
			fmt.Fprintf(os.Stderr, "  - Bitmap index statistics\n")
			fmt.Fprintf(os.Stderr, "  - Unique index entry counts\n")
			fmt.Fprintf(os.Stderr, "  - Event distribution by contract/topic\n")
			os.Exit(0)
		}
	}

	cmdStats(cfg)
}

func cmdStats(cfg *config.Config) {
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

	p.Printf("\nEvents Database Statistics (%s):\n", cfg.Storage.DBPath)
	p.Printf("  Total events:          %d\n", dbStats.TotalEvents)
	if dbStats.TotalEvents > 0 {
		p.Printf("  Ledger range:          %d - %d\n", dbStats.MinLedger, dbStats.MaxLedger)
		p.Printf("  Ledger span:           %d ledgers\n", dbStats.MaxLedger-dbStats.MinLedger+1)
	}
	p.Printf("  Last processed ledger: %d\n", dbStats.LastProcessed)
	p.Printf("  Unique contracts:      %d\n", dbStats.UniqueContracts)

	storageStats := eventStore.GetStorageStats()
	p.Printf("\nRocksDB Storage Metrics:\n")
	p.Printf("  Estimated keys:        %s\n", storageStats.EstimatedNumKeys)
	p.Printf("  Live data size:        %s MB\n", bytesToMB(storageStats.EstimateLiveDataSize))
	p.Printf("  Total SST size:        %s MB\n", bytesToMB(storageStats.TotalSstFilesSize))
	p.Printf("  Memtable size:         %s MB\n", bytesToMB(storageStats.CurSizeAllMemTables))
	p.Printf("  Pending compaction:    %s MB\n", bytesToMB(storageStats.EstimatePendingCompactionBytes))
	p.Printf("  Files at level 0:      %s\n", storageStats.NumFilesAtLevel0)
	p.Printf("  Files at level 1:      %s\n", storageStats.NumFilesAtLevel1)
	p.Printf("  Block cache usage:     %s MB\n", bytesToMB(storageStats.BlockCacheUsage))

	// Always show bitmap index stats if available
	bitmapStats := eventStore.GetBitmapStats()
	if bitmapStats != nil {
		p.Printf("\nBitmap Index Statistics:\n")
		p.Printf("  Current segment ID:    %d (each segment = 1M ledgers)\n", bitmapStats.CurrentSegmentID)
		p.Printf("  Hot segments (memory): %d segments, %d entries, %.2f MB\n",
			bitmapStats.HotSegmentCount,
			bitmapStats.HotSegmentCards,
			float64(bitmapStats.HotSegmentMemBytes)/(1024*1024))
		p.Printf("  Stored bitmap segments:\n")
		p.Printf("    Contract index:      %d segments\n", bitmapStats.ContractIndexCount)
		p.Printf("    Topic0 index:        %d segments\n", bitmapStats.Topic0IndexCount)
		p.Printf("    Topic1 index:        %d segments\n", bitmapStats.Topic1IndexCount)
		p.Printf("    Topic2 index:        %d segments\n", bitmapStats.Topic2IndexCount)
		p.Printf("    Topic3 index:        %d segments\n", bitmapStats.Topic3IndexCount)
	} else {
		p.Printf("\nBitmap Index: not available\n")
	}

	// Always show index entry counts
	fmt.Fprintf(os.Stderr, "Counting index entries...\n")
	indexStats := eventStore.GetIndexStats()
	p.Printf("\nUnique Index Entry Counts:\n")
	p.Printf("  Events (primary):      %d\n", indexStats.EventsCount)
	p.Printf("  Contract index:        %d\n", indexStats.ContractCount)
	p.Printf("  TxHash index:          %d\n", indexStats.TxHashCount)
	p.Printf("  Type index:            %d\n", indexStats.TypeCount)
	p.Printf("  Topic0 index:          %d\n", indexStats.Topic0Count)
	p.Printf("  Topic1 index:          %d\n", indexStats.Topic1Count)
	p.Printf("  Topic2 index:          %d\n", indexStats.Topic2Count)
	p.Printf("  Topic3 index:          %d\n", indexStats.Topic3Count)

	// Always show distribution (top 10)
	fmt.Fprintf(os.Stderr, "Computing distribution statistics...\n")
	dist, err := eventStore.GetIndexDistribution(10)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get distribution: %v\n", err)
	} else {
		printDistribution("Contracts", dist.Contracts)
		printDistribution("Topic0", dist.Topic0)
		printDistribution("Topic1", dist.Topic1)
		printDistribution("Topic2", dist.Topic2)
		printDistribution("Topic3", dist.Topic3)
	}
}
