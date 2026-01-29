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
		fmt.Fprintf(os.Stderr, "Progress/workers/batch/queue are configured in config.toml [ingestion]\n")
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

	// Config-only progress file (required)
	progressFile := cfg.Ingestion.ProgressFile
	if progressFile == "" {
		fmt.Fprintf(os.Stderr, "Error: ingestion.progress_file must be set in config\n")
		os.Exit(2)
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

	// Setup progress tracker (always enabled)
	progressTracker := progress.NewTracker(progressFile, startLedger, endLedger)
	progressTracker.SetStatsProvider(&statsProviderAdapter{store: eventStore})
	if cfg.Ingestion.SnapshotInterval > 0 {
		progressTracker.SetSnapshotInterval(cfg.Ingestion.SnapshotInterval)
	}
	fmt.Fprintf(os.Stderr, "Progress file: %s\n", progressFile)
	fmt.Fprintf(os.Stderr, "History file:  %s\n", progressTracker.GetHistoryFile())
	progressTracker.Start()
	defer func() {
		progressTracker.SetStatus("completed")
		progressTracker.Stop()
	}()

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

	pipelineConfig := ingest.PipelineConfig{
		Workers:           workers,
		BatchSize:         batchSize,
		QueueSize:         queueSize,
		DataDir:           cfg.Source.LedgerDir,
		NetworkPassphrase: networkPassphrase,
		MaintainUniqueIdx: cfg.Ingestion.UniqueIndexes,
		MaintainBitmapIdx: cfg.Ingestion.BitmapIndexes,
		MaintainL2Idx:     cfg.Ingestion.L2Indexes,
	}

	pipeline := ingest.NewPipeline(pipelineConfig, eventStore)

	// Aggregated stats for tracking
	stats := ingest.NewLedgerStats()
	var ledgerCount, totalEvents int
	var lastReportedLedger uint32

	pipeline.SetProgressCallback(func(ledger uint32, ledgersProcessed, eventsTotal int, pipeStats *ingest.LedgerStats) {
		ledgerCount = ledgersProcessed
		totalEvents = eventsTotal
		lastReportedLedger = ledger

		stats.TotalLedgers = pipeStats.TotalLedgers
		stats.TotalTransactions = pipeStats.TotalTransactions
		stats.TotalEvents = pipeStats.TotalEvents
		stats.OperationEvents = pipeStats.OperationEvents
		stats.TransactionEvents = pipeStats.TransactionEvents

		progressTracker.Update(ledger, ledgersProcessed, eventsTotal, stats)
		fmt.Fprintf(os.Stderr, "Processed %d ledgers, %d events (ledger %d)...\n", ledgersProcessed, eventsTotal, ledger)
	})

	pipeline.SetErrorCallback(func(ledger uint32, err error) {
		fmt.Fprintf(os.Stderr, "Error processing ledger %d: %v\n", ledger, err)
		progressTracker.AddError(err)
	})

	if err := pipeline.Run(startLedger, endLedger); err != nil {
		fmt.Fprintf(os.Stderr, "Pipeline failed: %v\n", err)
		progressTracker.SetStatus("failed")
		os.Exit(1)
	}

	pipeStats := pipeline.GetStats()
	ledgerCount = int(pipeStats.LedgersProcessed)
	totalEvents = int(pipeStats.EventsExtracted)

	diskReadTime := pipeline.GetDiskReadTime()
	decompressTime := pipeline.GetDecompressTime()
	unmarshalTime := pipeline.GetUnmarshalTime()
	writeTime := pipeline.GetWriteTime()
	rawBytesTotal := pipeline.GetRawBytesTotal()

	progressTracker.AddRawBytes(rawBytesTotal)
	progressTracker.Update(lastReportedLedger, ledgerCount, totalEvents, stats)

	finalProgress := progressTracker.GetProgress()
	ingestionElapsed := time.Since(finalProgress.StartedAt)

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
	preCompactStats := eventStore.GetSnapshotStats()
	sstSizeMB := float64(preCompactStats.SSTFilesSizeBytes) / (1024 * 1024)
	memtableMB := float64(preCompactStats.MemtableSizeBytes) / (1024 * 1024)
	storedSizeMB := sstSizeMB + memtableMB
	totalFiles := preCompactStats.L0Files + preCompactStats.L1Files + preCompactStats.L2Files +
		preCompactStats.L3Files + preCompactStats.L4Files + preCompactStats.L5Files + preCompactStats.L6Files

	summary.WriteString("\n")
	summary.WriteString("=== Ingestion Complete ===\n")
	summary.WriteString("\n")
	summary.WriteString(fmt.Sprintf("  Ledgers processed:       %d\n", ledgerCount))
	summary.WriteString(fmt.Sprintf("  Transactions processed:  %d\n", stats.TotalTransactions))
	summary.WriteString(fmt.Sprintf("  Events ingested:         %d\n", totalEvents))
	summary.WriteString(fmt.Sprintf("  Avg ledgers/sec:         %.0f\n", finalProgress.LedgersPerSec))
	summary.WriteString(fmt.Sprintf("  Avg events/sec:          %.0f\n", finalProgress.EventsPerSec))

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

	summary.WriteString("\n")
	summary.WriteString("RocksDB Storage (pre-compaction):\n")
	summary.WriteString(fmt.Sprintf("  SST files:               %.2f MB (%d files)\n", sstSizeMB, totalFiles))
	summary.WriteString(fmt.Sprintf("  Memtable:                %.2f MB\n", memtableMB))
	summary.WriteString(fmt.Sprintf("  Total stored:            %.2f MB\n", storedSizeMB))
	if totalEvents > 0 && storedSizeMB > 0 {
		bytesPerEvent := (storedSizeMB * 1024 * 1024) / float64(totalEvents)
		summary.WriteString(fmt.Sprintf("  Avg bytes/event:         %.f\n", bytesPerEvent))
	}
	if rawDataMB > 0 && storedSizeMB > 0 {
		compressionRatio := rawDataMB / storedSizeMB
		savings := (rawDataMB - storedSizeMB) / rawDataMB * 100
		summary.WriteString(fmt.Sprintf("  Compression:             %.1fx (%.1f%% savings)\n", compressionRatio, savings))
	}
	summary.WriteString(fmt.Sprintf("  Pending compaction:      %.2f MB\n", float64(preCompactStats.PendingCompactBytes)/(1024*1024)))
	summary.WriteString(fmt.Sprintf("  Files by level:          L0=%d L1=%d L2=%d L3=%d L4=%d L5=%d L6=%d\n",
		preCompactStats.L0Files, preCompactStats.L1Files, preCompactStats.L2Files,
		preCompactStats.L3Files, preCompactStats.L4Files, preCompactStats.L5Files, preCompactStats.L6Files))

	fmt.Fprint(os.Stderr, summary.String())
	printPerformanceTrend(progressTracker.GetHistoryFile())

	// Write ingestion summary to file (timestamped)
	filetime := time.Now().Format("20060102T150405")
	summaryFile := fmt.Sprintf("summary_%s.txt", filetime)
	if err := os.WriteFile(summaryFile, []byte(summary.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write summary file: %v\n", err)
	}

	var postSummary strings.Builder
	if cfg.Ingestion.FinalCompaction {
		var compactionResult *store.CompactionResult
		var compactionTime time.Duration
		fmt.Fprintf(os.Stderr, "\nRunning final compaction...\n")
		compactStart := time.Now()
		compactionResult = eventStore.CompactAll()
		compactionTime = time.Since(compactStart)
		fmt.Fprintf(os.Stderr, "Compaction completed in %s\n", formatElapsed(compactionTime))

		if compactionResult != nil {
			rocksStats := eventStore.GetSnapshotStats()
			postTotalFiles := rocksStats.L0Files + rocksStats.L1Files + rocksStats.L2Files +
				rocksStats.L3Files + rocksStats.L4Files + rocksStats.L5Files + rocksStats.L6Files
			postSstSizeMB := float64(rocksStats.SSTFilesSizeBytes) / (1024 * 1024)

			postSummary.WriteString("\n")
			postSummary.WriteString("RocksDB Storage (post-compaction):\n")
			postSummary.WriteString(fmt.Sprintf("  SST files:               %.2f MB (%d files)\n", postSstSizeMB, postTotalFiles))
			if totalEvents > 0 && postSstSizeMB > 0 {
				bytesPerEvent := (postSstSizeMB * 1024 * 1024) / float64(totalEvents)
				postSummary.WriteString(fmt.Sprintf("  Avg bytes/event:         %.0f\n", bytesPerEvent))
			}
			if rawDataMB > 0 && postSstSizeMB > 0 {
				compressionRatio := rawDataMB / postSstSizeMB
				savings := (rawDataMB - postSstSizeMB) / rawDataMB * 100
				postSummary.WriteString(fmt.Sprintf("  Compression:             %.1fx (%.1f%% savings)\n", compressionRatio, savings))
			}
			postSummary.WriteString(fmt.Sprintf("  Files by level:          L0=%d L1=%d L2=%d L3=%d L4=%d L5=%d L6=%d\n",
				rocksStats.L0Files, rocksStats.L1Files, rocksStats.L2Files,
				rocksStats.L3Files, rocksStats.L4Files, rocksStats.L5Files, rocksStats.L6Files))

			postSummary.WriteString("\n")
			postSummary.WriteString("Compaction Results:\n")
			postSummary.WriteString(fmt.Sprintf("  Duration:                %s\n", formatElapsed(compactionTime)))
			postSummary.WriteString(fmt.Sprintf("  Before:                  %.2f MB\n", float64(compactionResult.BeforeSSTBytes)/(1024*1024)))
			postSummary.WriteString(fmt.Sprintf("  After:                   %.2f MB\n", float64(compactionResult.AfterSSTBytes)/(1024*1024)))
			if compactionResult.BytesReclaimed > 0 {
				postSummary.WriteString(fmt.Sprintf("  Reclaimed:               %.2f MB (%.1f%%)\n",
					float64(compactionResult.BytesReclaimed)/(1024*1024),
					compactionResult.SpaceSavingsPercent))
			}
		}
	}

	if cfg.Ingestion.ComputeStats {
		fmt.Fprintf(os.Stderr, "\nComputing event statistics using %d workers...\n", workers)

		var eventStats *store.EventStats
		var statsTime time.Duration
		statsStart := time.Now()
		eventStats, err = eventStore.ComputeEventStats(workers)
		statsTime = time.Since(statsStart)
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
