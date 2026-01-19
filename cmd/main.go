package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/ingest"
	"github.com/urvisavla/stellar-events/internal/progress"
	"github.com/urvisavla/stellar-events/internal/reader"
	"github.com/urvisavla/stellar-events/internal/store"
)

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

func printUsage() {
	fmt.Fprintf(os.Stderr, "Stellar Events - Extract and query Soroban contract events\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  ingest    Ingest events from ledger files to RocksDB\n")
	fmt.Fprintf(os.Stderr, "  query     Query events from RocksDB\n")
	fmt.Fprintf(os.Stderr, "  stats     Show database statistics\n")
	fmt.Fprintf(os.Stderr, "\nConfiguration:\n")
	fmt.Fprintf(os.Stderr, "  Requires stellar-events.toml or config.toml in current directory\n")
	fmt.Fprintf(os.Stderr, "  See configs/stellar-events.example.toml for reference\n")
	fmt.Fprintf(os.Stderr, "\nExamples:\n")
	fmt.Fprintf(os.Stderr, "  %s ingest                              # Ingest using config settings\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s ingest --start 1000 --end 2000      # Override ledger range\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query --ledger 12345                # Query events for a ledger\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query --start 100 --end 200         # Query ledger range\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query --contract <base64_id>        # Query by contract\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query --txhash <hex_hash>           # Query by transaction\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query --type contract               # Query by event type\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query --topic0 <base64_value>       # Query by topic\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s stats                               # Show database stats\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s stats --storage --indexes           # Include storage metrics\n", os.Args[0])
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	// Handle help before loading config
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
		cmdIngest(cfg, args)
	case "query":
		cmdQuery(cfg, args)
	case "stats":
		cmdStats(cfg, args)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

// =============================================================================
// Ingest Command
// =============================================================================

func cmdIngest(cfg *config.Config, args []string) {
	startLedger := uint32(2)
	endLedger := uint32(0)
	var resumeFile string

	// Parse args
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--start":
			if i+1 < len(args) {
				fmt.Sscan(args[i+1], &startLedger)
				i++
			}
		case "--end":
			if i+1 < len(args) {
				fmt.Sscan(args[i+1], &endLedger)
				i++
			}
		case "--resume":
			if i+1 < len(args) {
				resumeFile = args[i+1]
				i++
			}
		case "--help", "-h":
			fmt.Fprintf(os.Stderr, "Usage: ingest [options]\n\n")
			fmt.Fprintf(os.Stderr, "Options:\n")
			fmt.Fprintf(os.Stderr, "  --start <ledger>      Start ledger (default: from config)\n")
			fmt.Fprintf(os.Stderr, "  --end <ledger>        End ledger (default: from config, 0=all)\n")
			fmt.Fprintf(os.Stderr, "  --resume <file>       Resume from progress file\n")
			return
		}
	}

	// If end is 0, get max available from source
	if endLedger == 0 {
		minLedger, maxLedger, _, err := reader.GetLedgerDataStats(cfg.Source.LedgerDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get source stats: %v\n", err)
			os.Exit(1)
		}
		endLedger = maxLedger
		fmt.Fprintf(os.Stderr, "Source ledger range: %d - %d\n", minLedger, maxLedger)
	}

	// Resume from progress file if requested
	if resumeFile != "" {
		prog, err := progress.LoadProgress(resumeFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load progress: %v\n", err)
			os.Exit(1)
		}
		startLedger = prog.CurrentLedger + 1
		fmt.Fprintf(os.Stderr, "Resuming from ledger %d\n", startLedger)
	}

	// Generate progress file name with timestamp
	progressFile := fmt.Sprintf("progress_%s.json", time.Now().Format("20060102T150405"))

	// Validate
	if startLedger < reader.FirstLedgerSequence {
		fmt.Fprintf(os.Stderr, "Error: start ledger must be >= %d\n", reader.FirstLedgerSequence)
		os.Exit(1)
	}

	cfg.Print()

	// Open event store
	eventStore, err := store.NewEventStoreWithOptions(cfg.Storage.DBPath, configToRocksDBOptions(&cfg.Storage.RocksDB), configToIndexOptions(&cfg.Indexes))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	// Create iterator
	iterator, err := reader.NewLedgerIterator(cfg.Source.LedgerDir, startLedger, endLedger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create iterator: %v\n", err)
		os.Exit(1)
	}
	defer iterator.Close()

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
	ledgerCount := 0
	totalEvents := 0
	stats := ingest.NewLedgerStats()

	fmt.Fprintf(os.Stderr, "Ingesting events from ledgers %d to %d...\n", startLedger, endLedger)

	for {
		// Time the read operation
		readStart := time.Now()
		xdrBytes, seq, ok, err := iterator.Next()
		progressTracker.AddReadTime(time.Since(readStart))

		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read ledger %d: %v\n", seq, err)
			progressTracker.AddError(err)
			progressTracker.SetStatus("failed")
			os.Exit(1)
		}
		if !ok {
			break
		}

		// Extract events as minimal/raw XDR (count as read time since it's parsing)
		readStart = time.Now()
		events, err := ingest.ExtractEvents(xdrBytes, networkPassphrase, stats)
		progressTracker.AddReadTime(time.Since(readStart))

		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to extract events from ledger %d: %v\n", seq, err)
			progressTracker.AddError(err)
			progressTracker.SetStatus("failed")
			os.Exit(1)
		}

		// Store events - time the write operation and track raw bytes
		if len(events) > 0 {
			writeStart := time.Now()
			rawBytes, err := eventStore.StoreMinimalEventsMultiLedger(events)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to store events for ledger %d: %v\n", seq, err)
				os.Exit(1)
			}
			progressTracker.AddWriteTime(time.Since(writeStart))
			progressTracker.AddRawBytes(rawBytes)
			totalEvents += len(events)
		}

		// Update metadata
		writeStart := time.Now()
		if err := eventStore.SetLastProcessedLedger(seq); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to update last processed ledger: %v\n", err)
			os.Exit(1)
		}
		progressTracker.AddWriteTime(time.Since(writeStart))

		ledgerCount++

		// Update progress
		progressTracker.Update(seq, ledgerCount, totalEvents, stats)

		if ledgerCount%1000 == 0 {
			fmt.Fprintf(os.Stderr, "Processed %d ledgers, %d events...\n", ledgerCount, totalEvents)
		}
	}

	// Get final progress for summary
	finalProgress := progressTracker.GetProgress()
	ingestionElapsed := time.Since(finalProgress.StartedAt)

	// Get pre-compaction RocksDB stats
	preCompactStats := eventStore.GetSnapshotStats()

	// Run final compaction if enabled
	var compactionResult *store.CompactionResult
	var compactionTime time.Duration
	if cfg.Ingestion.FinalCompaction {
		fmt.Fprintf(os.Stderr, "\nRunning final compaction...\n")
		compactStart := time.Now()
		compactionResult = eventStore.CompactAll()
		compactionTime = time.Since(compactStart)
		fmt.Fprintf(os.Stderr, "Compaction completed in %s\n", formatElapsed(compactionTime))
	}

	// Get final RocksDB stats (after compaction if run)
	rocksStats := eventStore.GetSnapshotStats()
	totalElapsed := ingestionElapsed + compactionTime

	// Build summary
	var summary strings.Builder
	summary.WriteString("\n")
	summary.WriteString("=== Ingestion Complete ===\n")
	summary.WriteString("\nResults:\n")
	summary.WriteString(fmt.Sprintf("  Ledgers processed: %d\n", ledgerCount))
	summary.WriteString(fmt.Sprintf("  Events ingested:   %d\n", totalEvents))
	summary.WriteString(fmt.Sprintf("  Transactions:      %d\n", stats.TotalTransactions))
	summary.WriteString(fmt.Sprintf("  Ingestion time:    %s\n", formatElapsed(ingestionElapsed)))
	if compactionResult != nil {
		summary.WriteString(fmt.Sprintf("  Compaction time:   %s\n", formatElapsed(compactionTime)))
	}
	summary.WriteString(fmt.Sprintf("  Total time:        %s\n", formatElapsed(totalElapsed)))

	summary.WriteString("\nEvent Breakdown:\n")
	summary.WriteString(fmt.Sprintf("  Operation events:   %d\n", stats.OperationEvents))
	summary.WriteString(fmt.Sprintf("  Transaction events: %d\n", stats.TransactionEvents))

	summary.WriteString("\nPerformance:\n")
	summary.WriteString(fmt.Sprintf("  Avg ledgers/sec:   %.2f\n", finalProgress.LedgersPerSec))
	summary.WriteString(fmt.Sprintf("  Avg events/sec:    %.2f\n", finalProgress.EventsPerSec))

	// Storage efficiency
	rawDataMB := float64(finalProgress.RawDataBytes) / (1024 * 1024)
	sstSizeMB := float64(rocksStats.SSTFilesSizeBytes) / (1024 * 1024)
	memtableMB := float64(rocksStats.MemtableSizeBytes) / (1024 * 1024)

	summary.WriteString("\nStorage:\n")
	summary.WriteString(fmt.Sprintf("  Raw event data:    %.2f MB\n", rawDataMB))
	summary.WriteString(fmt.Sprintf("  RocksDB SST files: %.2f MB\n", sstSizeMB))
	summary.WriteString(fmt.Sprintf("  RocksDB memtable:  %.2f MB\n", memtableMB))
	if rawDataMB > 0 {
		if sstSizeMB < rawDataMB {
			savings := (rawDataMB - sstSizeMB) / rawDataMB * 100
			summary.WriteString(fmt.Sprintf("  Compression:       %.1f%% savings\n", savings))
		} else {
			overhead := (sstSizeMB - rawDataMB) / rawDataMB * 100
			summary.WriteString(fmt.Sprintf("  Overhead:          %.1f%%\n", overhead))
		}
	}
	if totalEvents > 0 {
		bytesPerEvent := float64(rocksStats.SSTFilesSizeBytes) / float64(totalEvents)
		summary.WriteString(fmt.Sprintf("  Avg bytes/event:   %.1f\n", bytesPerEvent))
	}

	// Compaction results
	if compactionResult != nil {
		summary.WriteString("\nCompaction:\n")
		summary.WriteString(fmt.Sprintf("  Duration:          %s\n", formatElapsed(compactionTime)))
		summary.WriteString(fmt.Sprintf("  Before:            %.2f MB (%d files, %d L0)\n",
			float64(compactionResult.BeforeSSTBytes)/(1024*1024),
			compactionResult.BeforeTotalFiles,
			compactionResult.BeforeL0Files))
		summary.WriteString(fmt.Sprintf("  After:             %.2f MB (%d files, %d L0)\n",
			float64(compactionResult.AfterSSTBytes)/(1024*1024),
			compactionResult.AfterTotalFiles,
			compactionResult.AfterL0Files))
		if compactionResult.BytesReclaimed > 0 {
			summary.WriteString(fmt.Sprintf("  Reclaimed:         %.2f MB (%.1f%%)\n",
				float64(compactionResult.BytesReclaimed)/(1024*1024),
				compactionResult.SpaceSavingsPercent))
		}
	} else {
		// Show pre-compaction state if compaction was disabled
		summary.WriteString("\nRocksDB State (no final compaction):\n")
		summary.WriteString(fmt.Sprintf("  Estimated keys:    %d\n", preCompactStats.EstimatedNumKeys))
		summary.WriteString(fmt.Sprintf("  L0 files:          %d\n", preCompactStats.L0Files))
		summary.WriteString(fmt.Sprintf("  Pending compact:   %.2f MB\n", float64(preCompactStats.PendingCompactBytes)/(1024*1024)))
	}

	// RocksDB final state
	summary.WriteString("\nRocksDB Final State:\n")
	summary.WriteString(fmt.Sprintf("  Estimated keys:    %d\n", rocksStats.EstimatedNumKeys))
	totalFiles := rocksStats.L0Files + rocksStats.L1Files + rocksStats.L2Files +
		rocksStats.L3Files + rocksStats.L4Files + rocksStats.L5Files + rocksStats.L6Files
	summary.WriteString(fmt.Sprintf("  Total SST files:   %d\n", totalFiles))
	summary.WriteString(fmt.Sprintf("  Files by level:    L0=%d L1=%d L2=%d L3=%d L4=%d L5=%d L6=%d\n",
		rocksStats.L0Files, rocksStats.L1Files, rocksStats.L2Files,
		rocksStats.L3Files, rocksStats.L4Files, rocksStats.L5Files, rocksStats.L6Files))

	// Print performance trend from history
	fmt.Fprint(os.Stderr, summary.String())
	printPerformanceTrend(progressTracker.GetHistoryFile())

	summary.WriteString("\nFiles:\n")
	summary.WriteString(fmt.Sprintf("  Progress: %s\n", progressFile))
	summary.WriteString(fmt.Sprintf("  History:  %s\n", progressTracker.GetHistoryFile()))

	// Write summary to file
	summaryFile := strings.TrimSuffix(progressFile, ".json") + "_summary.txt"
	if err := os.WriteFile(summaryFile, []byte(summary.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write summary file: %v\n", err)
	} else {
		summary.WriteString(fmt.Sprintf("  Summary:  %s\n", summaryFile))
	}

	fmt.Fprintf(os.Stderr, "\nFiles:\n")
	fmt.Fprintf(os.Stderr, "  Progress: %s\n", progressFile)
	fmt.Fprintf(os.Stderr, "  History:  %s\n", progressTracker.GetHistoryFile())
	fmt.Fprintf(os.Stderr, "  Summary:  %s\n", summaryFile)
}

// =============================================================================
// Query Command
// =============================================================================

func cmdQuery(cfg *config.Config, args []string) {
	var ledger, startLedger, endLedger uint32
	var contractID, txHash, eventType string
	var topicPos = -1
	var topicValue string
	var limit = 100

	// Parse args
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--ledger":
			if i+1 < len(args) {
				fmt.Sscan(args[i+1], &ledger)
				i++
			}
		case "--start":
			if i+1 < len(args) {
				fmt.Sscan(args[i+1], &startLedger)
				i++
			}
		case "--end":
			if i+1 < len(args) {
				fmt.Sscan(args[i+1], &endLedger)
				i++
			}
		case "--contract":
			if i+1 < len(args) {
				contractID = args[i+1]
				i++
			}
		case "--txhash":
			if i+1 < len(args) {
				txHash = args[i+1]
				i++
			}
		case "--type":
			if i+1 < len(args) {
				eventType = args[i+1]
				i++
			}
		case "--topic0":
			if i+1 < len(args) {
				topicPos = 0
				topicValue = args[i+1]
				i++
			}
		case "--topic1":
			if i+1 < len(args) {
				topicPos = 1
				topicValue = args[i+1]
				i++
			}
		case "--topic2":
			if i+1 < len(args) {
				topicPos = 2
				topicValue = args[i+1]
				i++
			}
		case "--topic3":
			if i+1 < len(args) {
				topicPos = 3
				topicValue = args[i+1]
				i++
			}
		case "--limit":
			if i+1 < len(args) {
				fmt.Sscan(args[i+1], &limit)
				i++
			}
		case "--help", "-h":
			fmt.Fprintf(os.Stderr, "Usage: query [options]\n\n")
			fmt.Fprintf(os.Stderr, "Options:\n")
			fmt.Fprintf(os.Stderr, "  --ledger <seq>      Query single ledger\n")
			fmt.Fprintf(os.Stderr, "  --start <seq>       Start of ledger range\n")
			fmt.Fprintf(os.Stderr, "  --end <seq>         End of ledger range\n")
			fmt.Fprintf(os.Stderr, "  --contract <id>     Filter by contract ID (base64)\n")
			fmt.Fprintf(os.Stderr, "  --txhash <hash>     Filter by transaction hash (hex)\n")
			fmt.Fprintf(os.Stderr, "  --type <type>       Filter by event type (contract|system|diagnostic)\n")
			fmt.Fprintf(os.Stderr, "  --topic0 <value>    Filter by topic0 (base64)\n")
			fmt.Fprintf(os.Stderr, "  --topic1 <value>    Filter by topic1 (base64)\n")
			fmt.Fprintf(os.Stderr, "  --topic2 <value>    Filter by topic2 (base64)\n")
			fmt.Fprintf(os.Stderr, "  --topic3 <value>    Filter by topic3 (base64)\n")
			fmt.Fprintf(os.Stderr, "  --limit <n>         Max results (default: 100)\n")
			return
		}
	}

	// Open event store
	eventStore, err := store.NewEventStoreWithOptions(cfg.Storage.DBPath, configToRocksDBOptions(&cfg.Storage.RocksDB), configToIndexOptions(&cfg.Indexes))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	var events []*store.ContractEvent

	// Single ledger query
	if ledger > 0 {
		startLedger = ledger
		endLedger = ledger
	}

	// Execute query based on filters
	switch {
	case txHash != "":
		fmt.Fprintf(os.Stderr, "Querying events for transaction %s...\n", txHash)
		events, err = eventStore.GetEventsByTxHash(txHash)

	case contractID != "" && startLedger > 0:
		fmt.Fprintf(os.Stderr, "Querying events for contract %s in ledgers %d-%d...\n", contractID, startLedger, endLedger)
		events, err = eventStore.GetEventsByContractIDInRange(contractID, startLedger, endLedger)

	case contractID != "":
		fmt.Fprintf(os.Stderr, "Querying events for contract %s...\n", contractID)
		events, err = eventStore.GetEventsByContractID(contractID, limit)

	case eventType != "" && startLedger > 0:
		fmt.Fprintf(os.Stderr, "Querying %s events in ledgers %d-%d...\n", eventType, startLedger, endLedger)
		events, err = eventStore.GetEventsByTypeInRange(eventType, startLedger, endLedger)

	case eventType != "":
		fmt.Fprintf(os.Stderr, "Querying %s events...\n", eventType)
		events, err = eventStore.GetEventsByType(eventType, limit)

	case topicPos >= 0 && startLedger > 0:
		fmt.Fprintf(os.Stderr, "Querying events with topic%d in ledgers %d-%d...\n", topicPos, startLedger, endLedger)
		events, err = eventStore.GetEventsByTopicInRange(topicPos, topicValue, startLedger, endLedger)

	case topicPos >= 0:
		fmt.Fprintf(os.Stderr, "Querying events with topic%d...\n", topicPos)
		events, err = eventStore.GetEventsByTopic(topicPos, topicValue, limit)

	case startLedger > 0:
		fmt.Fprintf(os.Stderr, "Querying events in ledgers %d-%d...\n", startLedger, endLedger)
		events, err = eventStore.GetEventsByLedgerRange(startLedger, endLedger)

	default:
		fmt.Fprintf(os.Stderr, "Error: specify --ledger, --start/--end, --contract, --txhash, --type, or --topic\n")
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Found %d events\n\n", len(events))

	// Output as JSON
	output, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// =============================================================================
// Stats Command
// =============================================================================

func cmdStats(cfg *config.Config, args []string) {
	showStorage := false
	showIndexes := false
	var dumpFile string
	showSource := false

	// Parse args
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--storage":
			showStorage = true
		case "--indexes":
			showIndexes = true
		case "--source":
			showSource = true
		case "--dump":
			if i+1 < len(args) {
				dumpFile = args[i+1]
				i++
			}
		case "--help", "-h":
			fmt.Fprintf(os.Stderr, "Usage: stats [options]\n\n")
			fmt.Fprintf(os.Stderr, "Options:\n")
			fmt.Fprintf(os.Stderr, "  --storage       Show RocksDB storage metrics\n")
			fmt.Fprintf(os.Stderr, "  --indexes       Show per-index entry counts\n")
			fmt.Fprintf(os.Stderr, "  --source        Show source ledger data stats\n")
			fmt.Fprintf(os.Stderr, "  --dump <file>   Dump all stats to JSON file\n")
			return
		}
	}

	// Source data stats
	if showSource {
		fmt.Printf("\nSource Data Statistics (%s):\n", cfg.Source.LedgerDir)
		minLedger, maxLedger, chunkCount, err := reader.GetLedgerDataStats(cfg.Source.LedgerDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Error: %v\n", err)
		} else {
			fmt.Printf("  Ledger range:    %d - %d\n", minLedger, maxLedger)
			fmt.Printf("  Total ledgers:   %d\n", maxLedger-minLedger+1)
			fmt.Printf("  Chunk files:     %d\n", chunkCount)
		}
	}

	// Open event store
	eventStore, err := store.NewEventStoreWithOptions(cfg.Storage.DBPath, configToRocksDBOptions(&cfg.Storage.RocksDB), configToIndexOptions(&cfg.Indexes))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	fmt.Fprintf(os.Stderr, "Calculating statistics...\n")

	// Database stats
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

	// Storage stats
	var storageStats *store.StorageStats
	if showStorage || dumpFile != "" {
		storageStats = eventStore.GetStorageStats()
		if showStorage {
			fmt.Printf("\nRocksDB Storage Metrics:\n")
			fmt.Printf("  Estimated keys:        %s\n", storageStats.EstimatedNumKeys)
			fmt.Printf("  Live data size:        %s MB\n", bytesToMB(storageStats.EstimateLiveDataSize))
			fmt.Printf("  Total SST size:        %s MB\n", bytesToMB(storageStats.TotalSstFilesSize))
			fmt.Printf("  Memtable size:         %s MB\n", bytesToMB(storageStats.CurSizeAllMemTables))
			fmt.Printf("  Pending compaction:    %s MB\n", bytesToMB(storageStats.EstimatePendingCompactionBytes))
			fmt.Printf("  Files at level 0:      %s\n", storageStats.NumFilesAtLevel0)
			fmt.Printf("  Files at level 1:      %s\n", storageStats.NumFilesAtLevel1)
			fmt.Printf("  Block cache usage:     %s MB\n", bytesToMB(storageStats.BlockCacheUsage))
		}
	}

	// Index stats
	var indexStats *store.IndexStats
	if showIndexes || dumpFile != "" {
		fmt.Fprintf(os.Stderr, "Counting index entries...\n")
		indexStats = eventStore.GetIndexStats()
		if showIndexes {
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
	}

	// Dump to file
	if dumpFile != "" {
		allStats := &store.AllStats{
			Database: dbStats,
			Storage:  storageStats,
			Indexes:  indexStats,
		}

		data, err := json.MarshalIndent(allStats, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to marshal stats: %v\n", err)
			os.Exit(1)
		}

		if err := os.WriteFile(dumpFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write stats file: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("\nStats dumped to: %s\n", dumpFile)
	}
}

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
		return // No history file yet
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		return // Need at least 2 snapshots for trend
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
		ContractID:      cfg.ContractID,
		TransactionHash: cfg.TransactionHash,
		EventType:       cfg.EventType,
		Topics:          cfg.Topics,
	}
}
