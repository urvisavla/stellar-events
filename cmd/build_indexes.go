package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Build Indexes Command
// =============================================================================

func runBuildIndexes(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("build-indexes", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	l2Flag := fs.Bool("l2", false, "Also build L2 bitmap indexes (event-level granularity)")
	uniqueFlag := fs.Bool("unique", false, "Also build unique indexes (counts for stats)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: build-indexes [options]\n\n")
		fmt.Fprintf(os.Stderr, "Rebuilds bitmap indexes from existing events for query support.\n\n")
		fmt.Fprintf(os.Stderr, "By default, builds L1 bitmap indexes (ledger-level) which enable\n")
		fmt.Fprintf(os.Stderr, "filtered queries by contract ID and topics.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --l2       Also build L2 bitmap indexes (event-level granularity)\n")
		fmt.Fprintf(os.Stderr, "             Enables faster hierarchical queries but increases index size\n")
		fmt.Fprintf(os.Stderr, "  --unique   Also build unique indexes (counts for stats command)\n\n")
		fmt.Fprintf(os.Stderr, "Uses all available CPUs (%d) for parallel processing.\n", runtime.NumCPU())
	}

	// Check for help flag first
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			fs.Usage()
			os.Exit(0)
		}
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	cmdBuildIndexes(cfg, *l2Flag, *uniqueFlag)
}

func cmdBuildIndexes(cfg *config.Config, buildL2, buildUnique bool) {
	p := message.NewPrinter(language.English)
	workers := runtime.NumCPU()

	fmt.Fprintf(os.Stderr, "Building indexes for database: %s\n", cfg.Storage.DBPath)
	fmt.Fprintf(os.Stderr, "Index types:\n")
	fmt.Fprintf(os.Stderr, "  - L1 Bitmap indexes (ledger-level) [always]\n")
	if buildL2 {
		fmt.Fprintf(os.Stderr, "  - L2 Bitmap indexes (event-level)\n")
	}
	if buildUnique {
		fmt.Fprintf(os.Stderr, "  - Unique indexes (counts)\n")
	}
	fmt.Fprintf(os.Stderr, "Using %d workers\n\n", workers)

	// Open event store
	eventStore, err := openEventStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	// Get event count first
	stats, err := eventStore.GetStats()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get stats: %v\n", err)
		os.Exit(1)
	}

	if stats.TotalEvents == 0 {
		fmt.Fprintf(os.Stderr, "No events in database. Nothing to index.\n")
		return
	}

	fmt.Fprintf(os.Stderr, "Scanning %d events across ledgers %d-%d...\n",
		stats.TotalEvents, stats.MinLedger, stats.MaxLedger)

	startTime := time.Now()
	var lastPrint time.Time
	var lastProcessed int64

	progressFn := func(processed int64) {
		now := time.Now()
		if now.Sub(lastPrint) >= time.Second {
			rate := float64(processed-lastProcessed) / now.Sub(lastPrint).Seconds()
			pct := float64(processed) / float64(stats.TotalEvents) * 100
			fmt.Fprintf(os.Stderr, "\rProcessed %d / %d events (%.1f%%) - %.0f events/sec",
				processed, stats.TotalEvents, pct, rate)
			lastPrint = now
			lastProcessed = processed
		}
	}

	// Build indexes with specified options
	opts := &store.BuildIndexOptions{
		BitmapIndexes: true, // Always build L1 bitmap
		L2Indexes:     buildL2,
		UniqueIndexes: buildUnique,
	}

	if err := eventStore.BuildIndexes(workers, opts, progressFn); err != nil {
		fmt.Fprintf(os.Stderr, "\nFailed to build indexes: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(startTime)
	fmt.Fprintf(os.Stderr, "\n\nIndex build complete in %s\n", formatElapsed(elapsed))

	// Show bitmap stats
	if bitmapStats := eventStore.GetBitmapStats(); bitmapStats != nil {
		p.Printf("\nBitmap Index Segments:\n")
		p.Printf("  Contracts: %d\n", bitmapStats.ContractIndexCount)
		p.Printf("  Topic0:    %d\n", bitmapStats.Topic0IndexCount)
		p.Printf("  Topic1:    %d\n", bitmapStats.Topic1IndexCount)
		p.Printf("  Topic2:    %d\n", bitmapStats.Topic2IndexCount)
		p.Printf("  Topic3:    %d\n", bitmapStats.Topic3IndexCount)
	}

	// Show unique counts if built
	if buildUnique {
		counts, err := eventStore.CountUniqueIndexes()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to count unique indexes: %v\n", err)
			return
		}

		p.Printf("\nUnique Index Counts:\n")
		p.Printf("  Contracts: %d unique (%d total events)\n", counts.UniqueContracts, counts.TotalContractEvents)
		p.Printf("  Topic0:    %d unique (%d total events)\n", counts.UniqueTopic0, counts.TotalTopic0Events)
		p.Printf("  Topic1:    %d unique (%d total events)\n", counts.UniqueTopic1, counts.TotalTopic1Events)
		p.Printf("  Topic2:    %d unique (%d total events)\n", counts.UniqueTopic2, counts.TotalTopic2Events)
		p.Printf("  Topic3:    %d unique (%d total events)\n", counts.UniqueTopic3, counts.TotalTopic3Events)
	}
}
