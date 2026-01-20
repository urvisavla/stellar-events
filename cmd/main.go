package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/support/storage"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/ingest"
	"github.com/urvisavla/stellar-events/internal/progress"
	"github.com/urvisavla/stellar-events/internal/reader"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Types
// =============================================================================

type QueryParams struct {
	StartLedger uint32
	EndLedger   uint32
	ContractID  string
	TopicPos    int    // -1 means none
	TopicValue  string // used only if TopicPos >= 0
	Limit       int
}

type StatsParams struct {
	ShowIndexes      bool
	ShowDistribution bool
	TopN             int
}

// statsProviderAdapter adapts store.EventStore to progress.StatsProvider
type statsProviderAdapter struct {
	store *store.EventStore
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
// Usage / Main
// =============================================================================

func printUsage() {
	fmt.Fprintf(os.Stderr, "Stellar Events - Extract and query Soroban contract events\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  ingest   Ingest events from ledger files to RocksDB\n")
	fmt.Fprintf(os.Stderr, "  query    Query events from RocksDB\n")
	fmt.Fprintf(os.Stderr, "  stats    Show database statistics\n")
	fmt.Fprintf(os.Stderr, "  ledgers  Show available ledger ranges in source data\n")
	fmt.Fprintf(os.Stderr, "\nConfiguration:\n")
	fmt.Fprintf(os.Stderr, "  Requires stellar-events.toml or config.toml in current directory\n")
	fmt.Fprintf(os.Stderr, "  See configs/stellar-events.example.toml for reference\n")
	fmt.Fprintf(os.Stderr, "\nExamples:\n")
	fmt.Fprintf(os.Stderr, "  %s ingest --start 1000 --end 2000\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query --start 100 --end 200 --contract <base64_id>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query --start 100 --end 200 --topic0 <base64_value>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s stats --distribution --indexes\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s ledgers\n", os.Args[0])
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	command := os.Args[1]
	args := os.Args[2:]

	// Help without config
	if command == "help" || command == "--help" || command == "-h" {
		printUsage()
		return
	}

	// Load config
	configPath, err := config.FindConfigFile()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Using config: %s\n", configPath)

	switch command {
	case "ingest":
		runIngest(cfg, args)
	case "query":
		runQuery(cfg, args)
	case "stats":
		runStats(cfg, args)
	case "build-indexes":
		runBuildIndexes(cfg, args)
	case "ledgers":
		runLedgers(cfg, args)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(2)
	}
}

// =============================================================================
// Flag parsing wrappers
// =============================================================================

func runIngest(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	start := fs.Uint("start", 0, "Start ledger (0 = default/source min)")
	end := fs.Uint("end", 0, "End ledger (0 = auto-detect max)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: ingest [options]\n\n")
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

func runQuery(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	start := fs.Uint("start", 0, "Start ledger (required)")
	end := fs.Uint("end", 0, "End ledger (required)")

	contract := fs.String("contract", "", "Contract ID (base64)")
	topic0 := fs.String("topic0", "", "Topic0 (base64)")
	topic1 := fs.String("topic1", "", "Topic1 (base64)")
	topic2 := fs.String("topic2", "", "Topic2 (base64)")
	topic3 := fs.String("topic3", "", "Topic3 (base64)")
	limit := fs.Int("limit", 100, "Max results")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: query --start <seq> --end <seq> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --contract <id>   Filter by contract ID (base64)\n")
		fmt.Fprintf(os.Stderr, "  --topic0 <val>    Filter by topic0 (base64)\n")
		fmt.Fprintf(os.Stderr, "  --topic1 <val>    Filter by topic1 (base64)\n")
		fmt.Fprintf(os.Stderr, "  --topic2 <val>    Filter by topic2 (base64)\n")
		fmt.Fprintf(os.Stderr, "  --topic3 <val>    Filter by topic3 (base64)\n")
		fmt.Fprintf(os.Stderr, "  --limit <n>       Max results (default: 100)\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	if *start == 0 || *end == 0 {
		fmt.Fprintf(os.Stderr, "Error: --start and --end are required\n\n")
		fs.Usage()
		os.Exit(2)
	}
	if *end < *start {
		fmt.Fprintf(os.Stderr, "Error: --end must be >= --start\n")
		os.Exit(2)
	}
	if *limit <= 0 {
		*limit = 100
	}

	// Enforce at most one topic filter
	topicPos := -1
	topicVal := ""
	setTopic := func(pos int, val string) {
		if val == "" {
			return
		}
		if topicPos != -1 {
			fmt.Fprintf(os.Stderr, "Error: only one of --topic0..--topic3 may be specified\n")
			os.Exit(2)
		}
		topicPos = pos
		topicVal = val
	}
	setTopic(0, *topic0)
	setTopic(1, *topic1)
	setTopic(2, *topic2)
	setTopic(3, *topic3)

	cmdQuery(cfg, QueryParams{
		StartLedger: uint32(*start),
		EndLedger:   uint32(*end),
		ContractID:  *contract,
		TopicPos:    topicPos,
		TopicValue:  topicVal,
		Limit:       *limit,
	})
}

func runStats(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	indexes := fs.Bool("indexes", false, "Show per-index entry counts")
	distribution := fs.Bool("distribution", false, "Show event count distribution")
	top := fs.Int("top", 10, "Top N values in distribution")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: stats [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --indexes       Show per-index entry counts\n")
		fmt.Fprintf(os.Stderr, "  --distribution  Show event count distribution (default: on)\n")
		fmt.Fprintf(os.Stderr, "  --top <n>       Top N values in distribution (default: 10)\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *top <= 0 {
		*top = 10
	}

	cmdStats(cfg, StatsParams{
		ShowIndexes:      *indexes,
		ShowDistribution: *distribution,
		TopN:             *top,
	})
}

func runLedgers(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("ledgers", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	years := fs.Bool("years", false, "Show ledger ranges by year (queries history archive)")
	archiveURL := fs.String("archive", "", "History archive URL (default: uses network from config)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: ledgers [options]\n\n")
		fmt.Fprintf(os.Stderr, "Shows available ledger ranges per chunk directory in the source data.\n")
		fmt.Fprintf(os.Stderr, "Uses ledger_dir from config file: %s\n\n", cfg.Source.LedgerDir)
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --years          Show ledger ranges by year (queries history archive)\n")
		fmt.Fprintf(os.Stderr, "  --archive <url>  History archive URL (default: SDF archive for network)\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	if *years {
		cmdLedgersByYear(cfg, *archiveURL)
	} else {
		cmdLedgers(cfg)
	}
}

// =============================================================================
// Ledgers Command
// =============================================================================

func cmdLedgers(cfg *config.Config) {
	fmt := message.NewPrinter(language.English)

	fmt.Printf("Source ledger directory: %s\n\n", cfg.Source.LedgerDir)

	// Get overall stats first
	minLedger, maxLedger, totalChunks, err := reader.GetLedgerDataStats(cfg.Source.LedgerDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get ledger stats: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Overall Range: %d - %d (%d ledgers)\n", minLedger, maxLedger, maxLedger-minLedger+1)
	fmt.Printf("Total Chunks:  %d (each chunk = %d ledgers)\n\n", totalChunks, reader.ChunkSize)

	// Get per-directory info
	dirInfo, err := reader.GetChunkDirectoryInfo(cfg.Source.LedgerDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get chunk directory info: %v\n", err)
		os.Exit(1)
	}

	// Print header
	fmt.Printf("%-8s  %12s  %12s  %12s  %8s  %10s\n",
		"Dir", "Min Ledger", "Max Ledger", "Ledgers", "Chunks", "Data Size")
	fmt.Printf("%-8s  %12s  %12s  %12s  %8s  %10s\n",
		"--------", "------------", "------------", "------------", "--------", "----------")

	var totalDataMB float64
	for _, info := range dirInfo {
		ledgerCount := info.MaxLedger - info.MinLedger + 1
		fmt.Printf("%-8s  %12d  %12d  %12d  %8d  %9.2f MB\n",
			info.DirName,
			info.MinLedger,
			info.MaxLedger,
			ledgerCount,
			info.ChunkCount,
			info.DataSizeMB)
		totalDataMB += info.DataSizeMB
	}

	fmt.Printf("%-8s  %12s  %12s  %12s  %8s  %10s\n",
		"--------", "------------", "------------", "------------", "--------", "----------")
	fmt.Printf("%-8s  %12s  %12s  %12d  %8d  %9.2f MB\n",
		"Total", "", "", maxLedger-minLedger+1, totalChunks, totalDataMB)
}

// =============================================================================
// Ledgers By Year Command
// =============================================================================

func cmdLedgersByYear(cfg *config.Config, archiveURL string) {
	fmt := message.NewPrinter(language.English)

	// Determine archive URL based on network
	if archiveURL == "" {
		switch cfg.Source.Network {
		case "mainnet":
			archiveURL = "https://history.stellar.org/prd/core-live/core_live_001"
		case "testnet":
			archiveURL = "https://history.stellar.org/prd/core-testnet/core_testnet_001"
		default:
			fmt.Fprintf(os.Stderr, "Error: --archive URL required for custom network\n")
			os.Exit(1)
		}
	}

	fmt.Printf("Querying history archive: %s\n", archiveURL)
	fmt.Printf("Network: %s\n\n", cfg.Source.Network)

	// Connect to archive
	archive, err := historyarchive.Connect(
		archiveURL,
		historyarchive.ArchiveOptions{
			ConnectOptions: storage.ConnectOptions{
				Context: context.Background(),
			},
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to archive: %v\n", err)
		os.Exit(1)
	}

	// Get current ledger info
	root, err := archive.GetRootHAS()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get root HAS: %v\n", err)
		os.Exit(1)
	}

	currentLedger := root.CurrentLedger
	fmt.Printf("Current ledger: %d\n\n", currentLedger)

	// Stellar mainnet genesis: July 2015
	// We'll start from 2015 and go to current year
	startYear := 2015
	currentYear := time.Now().Year()

	fmt.Printf("%-6s  %12s  %12s  %12s\n", "Year", "Start Ledger", "End Ledger", "Ledgers")
	fmt.Printf("%-6s  %12s  %12s  %12s\n", "------", "------------", "------------", "------------")

	var totalLedgers uint32

	for year := startYear; year <= currentYear; year++ {
		// Start of year (Jan 1 00:00:00 UTC)
		yearStart := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		// End of year (Dec 31 23:59:59 UTC)
		yearEnd := time.Date(year, 12, 31, 23, 59, 59, 0, time.UTC)

		// For current year, use current time
		if year == currentYear {
			yearEnd = time.Now().UTC()
		}

		startLedger, err := historyarchive.LedgerForTime(archive, yearStart)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not get start ledger for %d: %v\n", year, err)
			continue
		}

		endLedger, err := historyarchive.LedgerForTime(archive, yearEnd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not get end ledger for %d: %v\n", year, err)
			continue
		}

		ledgerCount := endLedger - startLedger + 1
		totalLedgers += ledgerCount

		fmt.Printf("%-6d  %12d  %12d  %12d\n", year, startLedger, endLedger, ledgerCount)
	}

	fmt.Printf("%-6s  %12s  %12s  %12s\n", "------", "------------", "------------", "------------")
	fmt.Printf("%-6s  %12s  %12s  %12d\n", "Total", "", "", totalLedgers)
}

// =============================================================================
// Ingest Command (typed)
// =============================================================================

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
	eventStore, err := store.NewEventStoreWithOptions(
		cfg.Storage.DBPath,
		configToRocksDBOptions(&cfg.Storage.RocksDB),
		configToIndexOptions(&cfg.Indexes),
	)
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
		MaintainUniqueIdx: cfg.Ingestion.MaintainUniqueIdx,
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
		eventStats, err = eventStore.ComputeEventStatsParallel(workers)
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

	//totalElapsed := ingestionElapsed + statsTime + compactionTime

	//postSummary.WriteString(fmt.Sprintf("\nTotal time:          %s\n", formatElapsed(totalElapsed)))
	fmt.Fprint(os.Stderr, postSummary.String())

	// Append to summary file
	f, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err == nil {
		_, _ = f.WriteString(postSummary.String())
		_ = f.Close()
	}
}

// =============================================================================
// Query Command (typed)
// =============================================================================

func cmdQuery(cfg *config.Config, p QueryParams) {
	eventStore, err := store.NewEventStoreWithOptions(
		cfg.Storage.DBPath,
		configToRocksDBOptions(&cfg.Storage.RocksDB),
		configToIndexOptions(&cfg.Indexes),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	var (
		events []*store.ContractEvent
	)

	// Range-only query
	switch {
	case p.ContractID != "" && p.TopicPos >= 0:
		fmt.Fprintf(os.Stderr, "Error: combining --contract and --topic filters is not supported\n")
		os.Exit(2)

	case p.ContractID != "":
		fmt.Fprintf(os.Stderr, "Querying events for contract %s in ledgers %d-%d...\n", p.ContractID, p.StartLedger, p.EndLedger)
		events, err = eventStore.GetEventsByContractIDInRange(p.ContractID, p.StartLedger, p.EndLedger)

	case p.TopicPos >= 0:
		fmt.Fprintf(os.Stderr, "Querying events with topic%d in ledgers %d-%d...\n", p.TopicPos, p.StartLedger, p.EndLedger)
		events, err = eventStore.GetEventsByTopicInRange(p.TopicPos, p.TopicValue, p.StartLedger, p.EndLedger)

	default:
		fmt.Fprintf(os.Stderr, "Querying events in ledgers %d-%d...\n", p.StartLedger, p.EndLedger)
		events, err = eventStore.GetEventsByLedgerRange(p.StartLedger, p.EndLedger)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	// Defensive limit (in case store range methods can return huge lists)
	if p.Limit > 0 && len(events) > p.Limit {
		events = events[:p.Limit]
	}

	fmt.Fprintf(os.Stderr, "Found %d events\n\n", len(events))

	output, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// =============================================================================
// Stats Command (typed)
// =============================================================================

func cmdStats(cfg *config.Config, p StatsParams) {
	fmt := message.NewPrinter(language.English)

	if p.TopN <= 0 {
		p.TopN = 10
	}

	eventStore, err := store.NewEventStoreWithOptions(
		cfg.Storage.DBPath,
		configToRocksDBOptions(&cfg.Storage.RocksDB),
		configToIndexOptions(&cfg.Indexes),
	)
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

	fmt.Printf("\nEvents Database Statistics (%s):\n", cfg.Storage.DBPath)
	fmt.Printf("  Total events:          %d\n", dbStats.TotalEvents)
	if dbStats.TotalEvents > 0 {
		fmt.Printf("  Ledger range:          %d - %d\n", dbStats.MinLedger, dbStats.MaxLedger)
		fmt.Printf("  Ledger span:           %d ledgers\n", dbStats.MaxLedger-dbStats.MinLedger+1)
	}
	fmt.Printf("  Last processed ledger: %d\n", dbStats.LastProcessed)
	fmt.Printf("  Unique contracts:      %d\n", dbStats.UniqueContracts)

	storageStats := eventStore.GetStorageStats()
	fmt.Printf("\nRocksDB Storage Metrics:\n")
	fmt.Printf("  Estimated keys:        %s\n", storageStats.EstimatedNumKeys)
	fmt.Printf("  Live data size:        %s MB\n", bytesToMB(storageStats.EstimateLiveDataSize))
	fmt.Printf("  Total SST size:        %s MB\n", bytesToMB(storageStats.TotalSstFilesSize))
	fmt.Printf("  Memtable size:         %s MB\n", bytesToMB(storageStats.CurSizeAllMemTables))
	fmt.Printf("  Pending compaction:    %s MB\n", bytesToMB(storageStats.EstimatePendingCompactionBytes))
	fmt.Printf("  Files at level 0:      %s\n", storageStats.NumFilesAtLevel0)
	fmt.Printf("  Files at level 1:      %s\n", storageStats.NumFilesAtLevel1)
	fmt.Printf("  Block cache usage:     %s MB\n", bytesToMB(storageStats.BlockCacheUsage))

	if p.ShowIndexes {
		fmt.Fprintf(os.Stderr, "Counting index entries...\n")
		indexStats := eventStore.GetIndexStats()
		fmt.Printf("\nIndex Entry Counts:\n")
		fmt.Printf("  Events (primary):      %d\n", indexStats.EventsCount)
		fmt.Printf("  Contract index:        %d\n", indexStats.ContractCount)
		fmt.Printf("  TxHash index:          %d\n", indexStats.TxHashCount)
		fmt.Printf("  Type index:            %d\n", indexStats.TypeCount)
		fmt.Printf("  Topic0 index:          %d\n", indexStats.Topic0Count)
		fmt.Printf("  Topic1 index:          %d\n", indexStats.Topic1Count)
		fmt.Printf("  Topic2 index:          %d\n", indexStats.Topic2Count)
		fmt.Printf("  Topic3 index:          %d\n", indexStats.Topic3Count)
	}

	if p.ShowDistribution {
		fmt.Fprintf(os.Stderr, "Computing distribution statistics...\n")
		dist, err := eventStore.GetIndexDistribution(p.TopN)
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
}

// =============================================================================
// Helpers (unchanged from your file)
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

// formatElapsed formats a duration for display
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

// configToRocksDBOptions converts config.RocksDBConfig to store.RocksDBOptions
func configToRocksDBOptions(cfg *config.RocksDBConfig) *store.RocksDBOptions {
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
	}
}

// configToIndexOptions converts config.IndexConfig to store.IndexConfig
func configToIndexOptions(cfg *config.IndexConfig) *store.IndexConfig {
	return &store.IndexConfig{
		ContractID: cfg.ContractID,
		Topics:     cfg.Topics,
	}
}

// printDistribution prints distribution stats for an index type
func printDistribution(name string, stats *store.DistributionStats) {
	fmt := message.NewPrinter(language.English)
	if stats == nil || stats.Count == 0 {
		fmt.Printf("\n%s Distribution: (no data)\n", name)
		return
	}

	fmt.Printf("\n%s Distribution (%d unique values, %d total events):\n", name, stats.Count, stats.Total)
	fmt.Printf("  Min:    %d events\n", stats.Min)
	fmt.Printf("  P50:    %d events (median)\n", stats.P50)
	fmt.Printf("  P75:    %d events\n", stats.P75)
	fmt.Printf("  P90:    %d events\n", stats.P90)
	fmt.Printf("  P99:    %d events\n", stats.P99)
	fmt.Printf("  Max:    %d events\n", stats.Max)
	fmt.Printf("  Mean:   %.2f events\n", stats.Mean)

	if len(stats.TopN) > 0 {
		fmt.Printf("  Top %d by event count:\n", len(stats.TopN))
		for i, entry := range stats.TopN {
			displayVal := entry.Value
			if len(displayVal) > 44 {
				displayVal = displayVal[:20] + "..." + displayVal[len(displayVal)-20:]
			}
			fmt.Printf("    %2d. %s (%d events)\n", i+1, displayVal, entry.EventCount)
		}
	}
}

// =============================================================================
// Build Indexes Command
// =============================================================================

func runBuildIndexes(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("build-indexes", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workers := fs.Int("workers", 0, "Number of parallel workers (0 = use all CPUs)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: build-indexes [options]\n\n")
		fmt.Fprintf(os.Stderr, "Scans all events and builds unique indexes with counts.\n")
		fmt.Fprintf(os.Stderr, "Use this after ingesting with maintain_unique_idx=false.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		os.Exit(2)
	}

	cmdBuildIndexes(cfg, *workers)
}

func cmdBuildIndexes(cfg *config.Config, workers int) {
	fmt.Fprintf(os.Stderr, "Building unique indexes for database: %s\n", cfg.Storage.DBPath)

	// Open event store
	eventStore, err := store.NewEventStoreWithOptions(
		cfg.Storage.DBPath,
		configToRocksDBOptions(&cfg.Storage.RocksDB),
		configToIndexOptions(&cfg.Indexes),
	)
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
	fmt := message.NewPrinter(language.English)

	fmt.Fprintf(os.Stderr, "\nUnique Index Counts:\n")
	fmt.Fprintf(os.Stderr, "  Contracts: %d unique (%d total events)\n", counts.UniqueContracts, counts.TotalContractEvents)
	fmt.Fprintf(os.Stderr, "  Topic0:    %d unique (%d total events)\n", counts.UniqueTopic0, counts.TotalTopic0Events)
	fmt.Fprintf(os.Stderr, "  Topic1:    %d unique (%d total events)\n", counts.UniqueTopic1, counts.TotalTopic1Events)
	fmt.Fprintf(os.Stderr, "  Topic2:    %d unique (%d total events)\n", counts.UniqueTopic2, counts.TotalTopic2Events)
	fmt.Fprintf(os.Stderr, "  Topic3:    %d unique (%d total events)\n", counts.UniqueTopic3, counts.TotalTopic3Events)
}
