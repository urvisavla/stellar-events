package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/ingest"
	"github.com/urvisavla/stellar-events/internal/progress"
	"github.com/urvisavla/stellar-events/internal/reader"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Ingest Command
// =============================================================================

func runIngest(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	start := fs.Uint("start", 0, "Start ledger (0 = default/source min)")
	end := fs.Uint("end", 0, "End ledger (0 = auto-detect max)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: ingest [options]\n\n")
		fmt.Fprintf(os.Stderr, "Ingests contract events from ledger files into RocksDB.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --start <ledger>   Start ledger (default: %d)\n", reader.FirstLedgerSequence)
		fmt.Fprintf(os.Stderr, "  --end <ledger>     End ledger (0 = auto-detect max)\n\n")
		fmt.Fprintf(os.Stderr, "Parallelism configured in config.toml [ingestion]\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	cmdIngest(cfg, uint32(*start), uint32(*end))
}

func cmdIngest(cfg *config.Config, startLedger, endLedger uint32) {
	fmt := message.NewPrinter(language.English)

	// Resolve ledger range
	if startLedger == 0 {
		startLedger = reader.FirstLedgerSequence
	}

	if endLedger == 0 {
		minLedger, maxLedger, _, err := reader.GetLedgerDataStats(cfg.Source.LedgerDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get source stats: %v\n", err)
			os.Exit(1)
		}
		endLedger = maxLedger
		fmt.Fprintf(os.Stderr, "Source ledger range: %d - %d\n", minLedger, maxLedger)
	}

	// Validate
	if startLedger < reader.FirstLedgerSequence {
		fmt.Fprintf(os.Stderr, "Error: start ledger must be >= %d\n", reader.FirstLedgerSequence)
		os.Exit(2)
	}
	if endLedger < startLedger {
		fmt.Fprintf(os.Stderr, "Error: end ledger must be >= start ledger\n")
		os.Exit(2)
	}

	// Open event store
	eventStore, err := openEventStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	networkPassphrase := cfg.GetNetworkPassphrase()

	// Config-only parallelism settings (with safe defaults)
	workers := cfg.Ingestion.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	batchSize := cfg.Ingestion.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	queueSize := cfg.Ingestion.QueueSize
	if queueSize <= 0 {
		queueSize = workers * 2
	}

	fmt.Fprintf(os.Stderr, "Ingesting events from ledgers %d to %d...\n", startLedger, endLedger)
	fmt.Fprintf(os.Stderr, "Parallel mode: %d workers, batch size %d, queue size %d\n", workers, batchSize, queueSize)

	// Create progress writer if configured
	var progressWriter *progress.Writer
	if cfg.Ingestion.ProgressFile != "" {
		progressWriter = progress.NewWriter(cfg.Ingestion.ProgressFile, startLedger, endLedger)
		fmt.Fprintf(os.Stderr, "Progress file: %s\n", cfg.Ingestion.ProgressFile)
	}

	pipelineConfig := ingest.PipelineConfig{
		Workers:             workers,
		BatchSize:           batchSize,
		QueueSize:           queueSize,
		DataDir:             cfg.Source.LedgerDir,
		NetworkPassphrase:   networkPassphrase,
		MaintainUniqueIdx:   cfg.Ingestion.UniqueIndexes,
		MaintainBitmapIdx:   cfg.Ingestion.BitmapIndexes,
		BitmapFlushInterval: cfg.Ingestion.BitmapFlushInterval,
	}

	pipeline := ingest.NewPipeline(pipelineConfig, eventStore)

	// Aggregated stats for tracking
	stats := ingest.NewLedgerStats()
	var ledgerCount, totalEvents int
	startTime := time.Now()

	pipeline.SetProgressCallback(func(ledger uint32, ledgersProcessed, eventsTotal int, pipeStats *ingest.LedgerStats) {
		ledgerCount = ledgersProcessed
		totalEvents = eventsTotal

		stats.TotalLedgers = pipeStats.TotalLedgers
		stats.TotalTransactions = pipeStats.TotalTransactions
		stats.TotalEvents = pipeStats.TotalEvents
		stats.OperationEvents = pipeStats.OperationEvents
		stats.TransactionEvents = pipeStats.TransactionEvents

		fmt.Fprintf(os.Stderr, "Processed %d ledgers, %d events (ledger %d)...\n", ledgersProcessed, eventsTotal, ledger)

		// Update progress file
		if progressWriter != nil {
			if err := progressWriter.Update(ledger, ledgersProcessed, eventsTotal); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to write progress: %v\n", err)
			}
		}
	})

	pipeline.SetErrorCallback(func(ledger uint32, err error) {
		fmt.Fprintf(os.Stderr, "Error processing ledger %d: %v\n", ledger, err)
	})

	if err := pipeline.Run(startLedger, endLedger); err != nil {
		// Write failure to progress file
		if progressWriter != nil {
			_ = progressWriter.Failed(endLedger, ledgerCount, totalEvents, err)
		}
		fmt.Fprintf(os.Stderr, "Pipeline failed: %v\n", err)
		os.Exit(1)
	}

	// Write completion to progress file
	if progressWriter != nil {
		pipeStats := pipeline.GetStats()
		if err := progressWriter.Complete(int(pipeStats.LedgersProcessed), int(pipeStats.EventsExtracted)); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to write progress: %v\n", err)
		}
	}

	pipeStats := pipeline.GetStats()
	ledgerCount = int(pipeStats.LedgersProcessed)
	totalEvents = int(pipeStats.EventsExtracted)

	diskReadTime := pipeline.GetDiskReadTime()
	decompressTime := pipeline.GetDecompressTime()
	unmarshalTime := pipeline.GetUnmarshalTime()
	writeTime := pipeline.GetWriteTime()
	rawBytesTotal := pipeline.GetRawBytesTotal()
	ingestionElapsed := time.Since(startTime)

	// Flush memtables to SST files before getting accurate storage stats
	fmt.Fprintf(os.Stderr, "\nFlushing memtables to disk...\n")
	if err := eventStore.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to flush memtables: %v\n", err)
	}

	// =========================================================================
	// Ingestion Summary
	// =========================================================================

	var summary strings.Builder
	rawDataMB := float64(rawBytesTotal) / (1024 * 1024)

	// Get pre-compaction storage snapshot
	preSnapshot, err := eventStore.GetStorageSnapshot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to get storage stats: %v\n", err)
	}

	summary.WriteString("\n")
	summary.WriteString("=== Ingestion Complete ===\n")
	summary.WriteString("\n")
	summary.WriteString(fmt.Sprintf("  Ledgers processed:       %d\n", ledgerCount))
	summary.WriteString(fmt.Sprintf("  Transactions processed:  %d\n", stats.TotalTransactions))
	summary.WriteString(fmt.Sprintf("  Events ingested:         %d\n", totalEvents))

	if ingestionElapsed.Seconds() > 0 {
		ledgersPerSec := float64(ledgerCount) / ingestionElapsed.Seconds()
		eventsPerSec := float64(totalEvents) / ingestionElapsed.Seconds()
		summary.WriteString(fmt.Sprintf("  Avg ledgers/sec:         %.0f\n", ledgersPerSec))
		summary.WriteString(fmt.Sprintf("  Avg events/sec:          %.0f\n", eventsPerSec))
	}

	totalWorkerTime := diskReadTime + decompressTime + unmarshalTime + writeTime
	summary.WriteString("\n")
	summary.WriteString("Ingestion Time Breakdown:\n")
	summary.WriteString(fmt.Sprintf("  Wall clock time:         %s\n", formatElapsed(ingestionElapsed)))
	if totalWorkerTime > 0 {
		summary.WriteString(fmt.Sprintf("  Disk read:               %s (%.1f%%)\n", formatElapsed(diskReadTime), float64(diskReadTime)/float64(totalWorkerTime)*100))
		summary.WriteString(fmt.Sprintf("  Decompress (zstd):       %s (%.1f%%)\n", formatElapsed(decompressTime), float64(decompressTime)/float64(totalWorkerTime)*100))
		summary.WriteString(fmt.Sprintf("  XDR unmarshal:           %s (%.1f%%)\n", formatElapsed(unmarshalTime), float64(unmarshalTime)/float64(totalWorkerTime)*100))
		summary.WriteString(fmt.Sprintf("  Write to RocksDB:        %s (%.1f%%)\n", formatElapsed(writeTime), float64(writeTime)/float64(totalWorkerTime)*100))
	}

	summary.WriteString("\n")
	summary.WriteString("Raw Event Data (XDR):\n")
	summary.WriteString(fmt.Sprintf("  Total size:              %.2f MB\n", rawDataMB))
	summary.WriteString(fmt.Sprintf("  Event count:             %d\n", totalEvents))
	if totalEvents > 0 && rawDataMB > 0 {
		rawBytesPerEvent := (rawDataMB * 1024 * 1024) / float64(totalEvents)
		summary.WriteString(fmt.Sprintf("  Avg bytes/event:         %.f\n", rawBytesPerEvent))
	}

	// Pre-compaction storage stats by column family
	if preSnapshot != nil {
		summary.WriteString("\n")
		summary.WriteString("RocksDB Storage (pre-compaction):\n")
		printStorageSnapshot(&summary, preSnapshot)
	}

	fmt.Fprint(os.Stderr, summary.String())

	// Write ingestion summary to file (timestamped)
	filetime := time.Now().Format("20060102T150405")
	summaryFile := fmt.Sprintf("summary_%s.txt", filetime)
	if err := os.WriteFile(summaryFile, []byte(summary.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write summary file: %v\n", err)
	}

	var postSummary strings.Builder

	// Run compaction if enabled
	if cfg.Ingestion.FinalCompaction {
		fmt.Fprintf(os.Stderr, "\nRunning final compaction...\n")
		compactionSummary, err := eventStore.CompactAllWithStats()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: compaction failed: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "Compaction completed in %s\n", formatElapsed(compactionSummary.Duration))
			printCompactionSummary(&postSummary, compactionSummary)
		}
	}

	// Compute event stats if enabled
	if cfg.Ingestion.ComputeStats {
		fmt.Fprintf(os.Stderr, "\nComputing event statistics using %d workers...\n", workers)

		statsStart := time.Now()
		eventStats, err := eventStore.ComputeEventStats(workers)
		statsTime := time.Since(statsStart)
		fmt.Fprintf(os.Stderr, "Stats computed in %s\n", formatElapsed(statsTime))

		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to compute event stats: %v\n", err)
		} else {
			postSummary.WriteString("\n")
			postSummary.WriteString("Event Statistics:\n")
			postSummary.WriteString(fmt.Sprintf("  Computation time:        %s\n", formatElapsed(statsTime)))
			postSummary.WriteString(fmt.Sprintf("  Contract events:         %d\n", eventStats.ContractEvents))
			postSummary.WriteString(fmt.Sprintf("  System events:           %d\n", eventStats.SystemEvents))
			postSummary.WriteString(fmt.Sprintf("  Diagnostic events:       %d\n", eventStats.DiagnosticEvents))
			postSummary.WriteString("\n")
			postSummary.WriteString(fmt.Sprintf("  Unique contracts:        %d\n", eventStats.UniqueContracts))
			postSummary.WriteString(fmt.Sprintf("  Unique topic0:           %d\n", eventStats.UniqueTopic0))
			postSummary.WriteString(fmt.Sprintf("  Unique topic1:           %d\n", eventStats.UniqueTopic1))
			postSummary.WriteString(fmt.Sprintf("  Unique topic2:           %d\n", eventStats.UniqueTopic2))
			postSummary.WriteString(fmt.Sprintf("  Unique topic3:           %d\n", eventStats.UniqueTopic3))
		}
	}

	fmt.Fprint(os.Stderr, postSummary.String())

	// Append to summary file
	f, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err == nil {
		_, _ = f.WriteString(postSummary.String())
		_ = f.Close()
	}
}

// printStorageSnapshot prints a storage snapshot in a formatted table
func printStorageSnapshot(sb *strings.Builder, snapshot *store.StorageSnapshot) {
	p := message.NewPrinter(language.English)

	sb.WriteString("  Column Family      Keys          SST Files    Memtable     Files\n")
	sb.WriteString("  ─────────────────────────────────────────────────────────────────\n")

	// Print in a consistent order
	cfOrder := []string{"events", "bitmap", "unique", "default"}
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
	cfOrder := []string{"events", "bitmap", "unique", "default"}
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
