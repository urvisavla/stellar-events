package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
)

// =============================================================================
// Build Indexes Command
// =============================================================================

func runBuildIndexes(cfg *config.Config, args []string) {
	// Check for help flag
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			fmt.Fprintf(os.Stderr, "Usage: build-indexes\n\n")
			fmt.Fprintf(os.Stderr, "Scans all events and builds unique indexes with counts.\n")
			fmt.Fprintf(os.Stderr, "Use this after ingesting with maintain_unique_idx=false.\n\n")
			fmt.Fprintf(os.Stderr, "Uses all available CPUs (%d) for parallel processing.\n", runtime.NumCPU())
			os.Exit(0)
		}
	}

	cmdBuildIndexes(cfg)
}

func cmdBuildIndexes(cfg *config.Config) {
	p := message.NewPrinter(language.English)
	workers := runtime.NumCPU()

	fmt.Fprintf(os.Stderr, "Building unique indexes for database: %s\n", cfg.Storage.DBPath)
	fmt.Fprintf(os.Stderr, "Using %d workers\n", workers)

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

	if err := eventStore.BuildUniqueIndexes(workers, progressFn); err != nil {
		fmt.Fprintf(os.Stderr, "\nFailed to build indexes: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(startTime)
	fmt.Fprintf(os.Stderr, "\n\nIndex build complete in %s\n", formatElapsed(elapsed))

	// Show new counts
	counts, err := eventStore.CountUniqueIndexes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to count indexes: %v\n", err)
		return
	}

	p.Printf("\nUnique Index Counts:\n")
	p.Printf("  Contracts: %d unique (%d total events)\n", counts.UniqueContracts, counts.TotalContractEvents)
	p.Printf("  Topic0:    %d unique (%d total events)\n", counts.UniqueTopic0, counts.TotalTopic0Events)
	p.Printf("  Topic1:    %d unique (%d total events)\n", counts.UniqueTopic1, counts.TotalTopic1Events)
	p.Printf("  Topic2:    %d unique (%d total events)\n", counts.UniqueTopic2, counts.TotalTopic2Events)
	p.Printf("  Topic3:    %d unique (%d total events)\n", counts.UniqueTopic3, counts.TotalTopic3Events)
}
