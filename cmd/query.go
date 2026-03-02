package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/query"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Query Command
// =============================================================================

func runQuery(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	contract := fs.String("contract", "", "Contract ID (C... strkey format)")
	topics := fs.String("topics", "", "Comma-separated topics (base64), position-independent AND logic")
	indexFlag := fs.String("index", "rocksdb", "Index/data path: rocksdb (default) or flat-all (segment files)")
	topic0 := fs.String("topic0", "", "Topic0 (base64) - positional filter at position 0")
	topic1 := fs.String("topic1", "", "Topic1 (base64) - positional filter at position 1")
	topic2 := fs.String("topic2", "", "Topic2 (base64) - positional filter at position 2")
	topic3 := fs.String("topic3", "", "Topic3 (base64) - positional filter at position 3")
	limit := fs.Int("limit", 0, "Max results (0 = use config default)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: query <start> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Queries contract events from RocksDB.\n\n")
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  <start>           Start ledger (required)\n")
		fmt.Fprintf(os.Stderr, "  Range is start + %d (max_ledger_range from config)\n\n", cfg.Query.MaxLedgerRange)
		fmt.Fprintf(os.Stderr, "Index options:\n")
		fmt.Fprintf(os.Stderr, "  --index <type>    Index/data path: rocksdb (default) or flat-all\n")
		fmt.Fprintf(os.Stderr, "\nFilter options:\n")
		fmt.Fprintf(os.Stderr, "  --contract <id>   Filter by contract ID (C... strkey format)\n")
		fmt.Fprintf(os.Stderr, "  --topics <list>   Comma-separated topics (base64), position-independent AND\n")
		fmt.Fprintf(os.Stderr, "  --topic0 <val>    Filter by topic0 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic1 <val>    Filter by topic1 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic2 <val>    Filter by topic2 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic3 <val>    Filter by topic3 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --limit <n>       Max results (default: %d from config)\n", cfg.Query.DefaultLimit)
		fmt.Fprintf(os.Stderr, "\nTiming stats are always shown.\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  query 55000000                                  # Query all events in range\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 --contract C...                  # rocksdb (default)\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 --index flat-all --contract C... # flat-all segment files\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 --topic0 <b64>                  # rocksdb with positional topic\n")
	}

	// Custom parsing to handle positional args before flags
	var positionalArgs []string
	var flagArgs []string
	inFlags := false

	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			fs.Usage()
			os.Exit(0)
		}
		if len(arg) > 0 && arg[0] == '-' {
			inFlags = true
		}
		if inFlags {
			flagArgs = append(flagArgs, arg)
		} else {
			positionalArgs = append(positionalArgs, arg)
		}
	}

	if err := fs.Parse(flagArgs); err != nil {
		os.Exit(2)
	}

	// Parse positional arguments
	if len(positionalArgs) < 1 {
		fmt.Fprintf(os.Stderr, "Error: start ledger is required\n\n")
		fs.Usage()
		os.Exit(2)
	}

	startLedger, err := strconv.ParseUint(positionalArgs[0], 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid start ledger: %v\n", err)
		os.Exit(2)
	}

	endLedger := startLedger + uint64(cfg.Query.MaxLedgerRange)

	// Apply config defaults for limit
	queryLimit := *limit
	if queryLimit <= 0 {
		queryLimit = cfg.Query.DefaultLimit
	}

	// Validate --index flag
	indexType := *indexFlag
	if indexType != "rocksdb" && indexType != "flat-all" {
		fmt.Fprintf(os.Stderr, "Error: invalid --index value %q (must be rocksdb or flat-all)\n", indexType)
		os.Exit(2)
	}

	cmdQuery(cfg, uint32(startLedger), uint32(endLedger), *contract, *topics, *topic0, *topic1, *topic2, *topic3, queryLimit, indexType)
}

func cmdQuery(cfg *config.Config, startLedger, endLedger uint32, contractID, topicsCSV, topic0, topic1, topic2, topic3 string, limit int, indexType string) {
	eventStore, err := openEventStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	// Check if any filter is specified
	hasContract := contractID != ""
	hasTopics := topicsCSV != ""
	hasTopic0 := topic0 != ""
	hasTopic1 := topic1 != ""
	hasTopic2 := topic2 != ""
	hasTopic3 := topic3 != ""
	hasAnyFilter := hasContract || hasTopics || hasTopic0 || hasTopic1 || hasTopic2 || hasTopic3

	// No filter - scan all events in range (use direct store method)
	if !hasAnyFilter {
		if indexType == "flat-all" {
			runSegmentDataUnfilteredQuery(eventStore, startLedger, endLedger, limit)
			return
		}

		fmt.Fprintf(os.Stderr, "Querying all events in ledgers %d-%d...\n", startLedger, endLedger)
		startTime := time.Now()
		rangeResult, err := eventStore.GetEventsInRangeWithTiming(startLedger, endLedger, limit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
			os.Exit(1)
		}
		totalTime := time.Since(startTime)

		// Build a result struct for consistent display
		result := &query.Result{
			Events:         rangeResult.Events,
			EventsScanned:  rangeResult.EventsScanned,
			EventsReturned: len(rangeResult.Events),
			LedgerRange:    endLedger - startLedger + 1,
			DiskReadTime:   rangeResult.Timing.DiskReadTime,
			UnmarshalTime:  rangeResult.Timing.UnmarshalTime,
			BytesRead:      rangeResult.Timing.BytesRead,
			EventFetchTime: totalTime, // Total fetch time (no index lookup for unfiltered)
			TotalTime:      totalTime,
		}

		printRangeQueryResult(result)

		output, err := json.MarshalIndent(result.Events, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(output))
		return
	}

	// If positional --topic0/1/2/3 flags are used, build a filter from them
	hasPositionalTopics := hasTopic0 || hasTopic1 || hasTopic2 || hasTopic3

	if hasPositionalTopics || !hasTopics {
		// Use positional topic flags (or contract-only)
		filter := buildFilter(contractID, topic0, topic1, topic2, topic3)
		if filter == nil {
			os.Exit(2)
		}

		switch indexType {
		case "rocksdb":
			runBitmap32PositionalQuery(eventStore, filter, startLedger, endLedger, limit)
		case "flat-all":
			runSegmentIndexPositionalQuery(eventStore, filter, startLedger, endLedger, limit)
		}
		return
	}

	// --topics CSV flag: parse as positional (topic0,topic1,topic2,topic3)
	switch indexType {
	case "rocksdb":
		runBitmap32NonPositionalQuery(eventStore, contractID, topicsCSV, startLedger, endLedger, limit)
	case "flat-all":
		runSegmentIndexNonPositionalQuery(eventStore, contractID, topicsCSV, startLedger, endLedger, limit)
	}
}

// buildFilter creates a query filter from string parameters
func buildFilter(contractID, topic0, topic1, topic2, topic3 string) *query.Filter {
	filter := &query.Filter{}

	if contractID != "" {
		contractBytes, decErr := decodeContractID(contractID)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid contract ID (expected C... format): %v\n", decErr)
			return nil
		}
		filter.ContractID = contractBytes
	}

	if topic0 != "" {
		topicBytes, decErr := decodeBase64(topic0)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid topic0: %v\n", decErr)
			return nil
		}
		filter.Topic0 = topicBytes
	}

	if topic1 != "" {
		topicBytes, decErr := decodeBase64(topic1)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid topic1: %v\n", decErr)
			return nil
		}
		filter.Topic1 = topicBytes
	}

	if topic2 != "" {
		topicBytes, decErr := decodeBase64(topic2)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid topic2: %v\n", decErr)
			return nil
		}
		filter.Topic2 = topicBytes
	}

	if topic3 != "" {
		topicBytes, decErr := decodeBase64(topic3)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid topic3: %v\n", decErr)
			return nil
		}
		filter.Topic3 = topicBytes
	}

	return filter
}

// runBitmap32PositionalQuery runs a query using V2 bitmap32 index with positional topic filters.
func runBitmap32PositionalQuery(eventStore *store.EventStore, filter *query.Filter, startLedger, endLedger uint32, limit int) {
	fmt.Fprintf(os.Stderr, "Querying with V2 bitmap32 index in ledgers %d-%d...\n", startLedger, endLedger)

	topicGroups := filter.TopicGroups()
	stats, events, err := eventStore.QueryEventsWithBitmap32EventIndex(filter.ContractID, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	printUnifiedResult(stats.ToUnified())

	if events == nil {
		fmt.Println("[]")
		return
	}

	output, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// runBitmap32NonPositionalQuery runs a query using V2 bitmap32 index with non-positional topics
func runBitmap32NonPositionalQuery(eventStore *store.EventStore, contractID, topicsCSV string, startLedger, endLedger uint32, limit int) {
	fmt.Fprintf(os.Stderr, "Querying with V2 bitmap32 index in ledgers %d-%d...\n", startLedger, endLedger)

	// Parse contract ID (strkey format)
	var contractBytes []byte
	if contractID != "" {
		var err error
		contractBytes, err = decodeContractID(contractID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid contract ID (expected C... format): %v\n", err)
			os.Exit(2)
		}
	}

	// Parse topics (base64 encoded, positional: topic0,topic1,topic2,topic3)
	var topicGroups [4][][]byte
	hasTopics := false
	if topicsCSV != "" {
		for i, t := range strings.Split(topicsCSV, ",") {
			if i >= 4 {
				break
			}
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			topicBytes, err := decodeBase64(t)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid topic (expected base64): %v\n", err)
				os.Exit(2)
			}
			topicGroups[i] = [][]byte{topicBytes}
			hasTopics = true
		}
	}

	if len(contractBytes) == 0 && !hasTopics {
		fmt.Fprintf(os.Stderr, "Error: at least one filter (--contract or --topics) must be specified\n")
		os.Exit(2)
	}

	// Query using V2 bitmap32 index
	stats, events, err := eventStore.QueryEventsWithBitmap32EventIndex(contractBytes, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	// Print detailed results using unified format
	printUnifiedResult(stats.ToUnified())

	if events == nil {
		fmt.Println("[]")
		return
	}

	output, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// runSegmentIndexPositionalQuery runs a query using segment flat file indexes with positional topic filters.
func runSegmentIndexPositionalQuery(eventStore *store.EventStore, filter *query.Filter, startLedger, endLedger uint32, limit int) {
	fmt.Fprintf(os.Stderr, "Querying with segment index in ledgers %d-%d...\n", startLedger, endLedger)

	topicGroups := filter.TopicGroups()
	stats, events, err := eventStore.QueryEventsWithSegmentData(filter.ContractID, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	printUnifiedResult(store.SegmentDataToUnified(stats))

	if events == nil {
		fmt.Println("[]")
		return
	}

	output, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// runSegmentDataUnfilteredQuery runs an unfiltered range query using segment data (no index, no RocksDB).
func runSegmentDataUnfilteredQuery(eventStore *store.EventStore, startLedger, endLedger uint32, limit int) {
	fmt.Fprintf(os.Stderr, "Querying all events from segment data in ledgers %d-%d...\n", startLedger, endLedger)

	stats, events, err := eventStore.GetEventsInRangeFromSegmentData(startLedger, endLedger, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	printUnifiedResult(store.SegmentDataToUnified(stats))

	if events == nil {
		fmt.Println("[]")
		return
	}

	output, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// runSegmentIndexNonPositionalQuery runs a query using segment flat file indexes with non-positional topics
func runSegmentIndexNonPositionalQuery(eventStore *store.EventStore, contractID, topicsCSV string, startLedger, endLedger uint32, limit int) {
	fmt.Fprintf(os.Stderr, "Querying with segment index in ledgers %d-%d...\n", startLedger, endLedger)

	var contractBytes []byte
	if contractID != "" {
		var err error
		contractBytes, err = decodeContractID(contractID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid contract ID (expected C... format): %v\n", err)
			os.Exit(2)
		}
	}

	var topicGroups [4][][]byte
	hasTopics := false
	if topicsCSV != "" {
		for i, t := range strings.Split(topicsCSV, ",") {
			if i >= 4 {
				break
			}
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			topicBytes, err := decodeBase64(t)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid topic (expected base64): %v\n", err)
				os.Exit(2)
			}
			topicGroups[i] = [][]byte{topicBytes}
			hasTopics = true
		}
	}

	if len(contractBytes) == 0 && !hasTopics {
		fmt.Fprintf(os.Stderr, "Error: at least one filter (--contract or --topics) must be specified\n")
		os.Exit(2)
	}

	stats, events, err := eventStore.QueryEventsWithSegmentData(contractBytes, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	printUnifiedResult(store.SegmentDataToUnified(stats))

	if events == nil {
		fmt.Println("[]")
		return
	}

	output, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// printUnifiedResult prints query stats in a unified format for all index types
func printUnifiedResult(r *store.UnifiedQueryResult) {
	// Header with index type
	var header string
	switch r.IndexType {
	case "rocksdb":
		header = "RocksDB"
	case "flat-all":
		header = "Flat Files"
	default:
		header = r.IndexType
	}

	fmt.Fprintf(os.Stderr, "\n=== Query Results (%s) ===\n", header)
	fmt.Fprintf(os.Stderr, "  Ledger range:      %d ledgers\n", r.LedgerRange)
	fmt.Fprintf(os.Stderr, "  Segments touched:  %d\n", r.SegmentsTouched)
	fmt.Fprintf(os.Stderr, "  Index matches:     %d %s\n", r.IndexMatches, r.MatchUnitName)
	fmt.Fprintf(os.Stderr, "  Events scanned:    %d\n", r.EventsScanned)
	fmt.Fprintf(os.Stderr, "  Events returned:   %d\n", r.EventsReturned)

	if r.GroupsDecompressed > 0 {
		fmt.Fprintf(os.Stderr, "  Groups decompressed: %d\n", r.GroupsDecompressed)
	}

	fmt.Fprintf(os.Stderr, "\n=== I/O Stats ===\n")
	fmt.Fprintf(os.Stderr, "  Index bytes read:  %s\n", formatBytes(r.IndexBytesRead))
	fmt.Fprintf(os.Stderr, "  Event bytes read:  %s\n", formatBytes(r.EventBytesRead))
	fmt.Fprintf(os.Stderr, "  Total bytes read:  %s\n", formatBytes(r.IndexBytesRead+r.EventBytesRead))

	fmt.Fprintf(os.Stderr, "\n=== Timing Breakdown ===\n")
	fmt.Fprintf(os.Stderr, "  Index lookup:      %s\n", formatDuration(r.IndexLookupTime))
	fmt.Fprintf(os.Stderr, "  Event fetch:       %s\n", formatDuration(r.EventFetchTime))
	if r.EventDiskReadTime > 0 {
		fmt.Fprintf(os.Stderr, "  Event disk read:   %s\n", formatDuration(r.EventDiskReadTime))
	}
	if r.DecompressTime > 0 {
		fmt.Fprintf(os.Stderr, "  Decompress:        %s\n", formatDuration(r.DecompressTime))
	}
	fmt.Fprintf(os.Stderr, "  Event decode:      %s\n", formatDuration(r.DecodeTime))
	if r.FilterTime > 0 {
		fmt.Fprintf(os.Stderr, "  Event filter:      %s\n", formatDuration(r.FilterTime))
	}
	fmt.Fprintf(os.Stderr, "  Total time:        %s\n", formatDuration(r.TotalTime))

	// Time distribution
	if r.TotalTime > 0 {
		idxPct := float64(r.IndexLookupTime) / float64(r.TotalTime) * 100
		fetchPct := float64(r.EventFetchTime) / float64(r.TotalTime) * 100
		decodePct := float64(r.DecodeTime) / float64(r.TotalTime) * 100
		fmt.Fprintf(os.Stderr, "  Time distribution: idx=%.1f%%, fetch=%.1f%%, decode=%.1f%%\n", idxPct, fetchPct, decodePct)
	}
	fmt.Fprintf(os.Stderr, "\n")
}

// printRangeQueryResult displays query statistics for unfiltered range queries


// printRangeQueryResult displays query statistics for unfiltered range queries
func printRangeQueryResult(result *query.Result) {
	fmt.Fprintf(os.Stderr, "\n=== Range Query Results ===\n")
	fmt.Fprintf(os.Stderr, "  Ledger range:      %d ledgers\n", result.LedgerRange)
	fmt.Fprintf(os.Stderr, "  Events scanned:    %d\n", result.EventsScanned)
	fmt.Fprintf(os.Stderr, "  Events returned:   %d\n", result.EventsReturned)
	fmt.Fprintf(os.Stderr, "  Bytes read:        %s\n", formatBytes(result.BytesRead))

	fmt.Fprintf(os.Stderr, "\n=== Timing Breakdown ===\n")
	fmt.Fprintf(os.Stderr, "  Event fetch:       %s (total)\n", formatDuration(result.EventFetchTime))
	fmt.Fprintf(os.Stderr, "    - Disk read:     %s\n", formatDuration(result.DiskReadTime))
	fmt.Fprintf(os.Stderr, "    - Unmarshal:     %s\n", formatDuration(result.UnmarshalTime))
	fmt.Fprintf(os.Stderr, "  Total time:        %s\n", formatDuration(result.TotalTime))

	// Detailed fetch breakdown as percentages
	if result.EventFetchTime > 0 {
		diskPct := float64(result.DiskReadTime) / float64(result.EventFetchTime) * 100
		unmarshalPct := float64(result.UnmarshalTime) / float64(result.EventFetchTime) * 100
		otherPct := 100 - diskPct - unmarshalPct
		fmt.Fprintf(os.Stderr, "  Fetch breakdown:   disk=%.1f%%, unmarshal=%.1f%%, other=%.1f%%\n",
			diskPct, unmarshalPct, otherPct)
	}

	// Throughput stats
	if result.TotalTime > 0 && result.EventsScanned > 0 {
		eventsPerSec := float64(result.EventsScanned) / result.TotalTime.Seconds()
		fmt.Fprintf(os.Stderr, "  Throughput:        %.0f events/sec\n", eventsPerSec)
	}
	if result.DiskReadTime > 0 && result.BytesRead > 0 {
		mbPerSec := float64(result.BytesRead) / (1024 * 1024) / result.DiskReadTime.Seconds()
		fmt.Fprintf(os.Stderr, "  Disk throughput:   %.1f MB/sec\n", mbPerSec)
	}

	fmt.Fprintf(os.Stderr, "\n")
}

