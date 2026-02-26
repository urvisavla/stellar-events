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
	useSegment := fs.Bool("segment", false, "Use segment flat file index (positional topics)")
	topic0 := fs.String("topic0", "", "Topic0 (base64) - positional filter at position 0")
	topic1 := fs.String("topic1", "", "Topic1 (base64) - positional filter at position 1")
	topic2 := fs.String("topic2", "", "Topic2 (base64) - positional filter at position 2")
	topic3 := fs.String("topic3", "", "Topic3 (base64) - positional filter at position 3")
	limit := fs.Int("limit", 0, "Max results (0 = use config default)")
	compareDB := fs.String("compare-db", "", "Compare with another database (path)")
	compareFormat := fs.String("compare-format", "", "Format of compare database: xdr or binary (auto-detect if not set)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: query <start> [end] [options]\n\n")
		fmt.Fprintf(os.Stderr, "Queries contract events from RocksDB.\n\n")
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  <start>           Start ledger (required)\n")
		fmt.Fprintf(os.Stderr, "  [end]             End ledger (default: start + %d from config)\n\n", cfg.Query.MaxLedgerRange)
		fmt.Fprintf(os.Stderr, "Index options (default: bitmap32):\n")
		fmt.Fprintf(os.Stderr, "  --segment    Use segment flat file index (positional topics)\n")
		fmt.Fprintf(os.Stderr, "\nFilter options:\n")
		fmt.Fprintf(os.Stderr, "  --contract <id>   Filter by contract ID (C... strkey format)\n")
		fmt.Fprintf(os.Stderr, "  --topics <list>   Comma-separated topics (base64), position-independent AND\n")
		fmt.Fprintf(os.Stderr, "  --topic0 <val>    Filter by topic0 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic1 <val>    Filter by topic1 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic2 <val>    Filter by topic2 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic3 <val>    Filter by topic3 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --limit <n>       Max results (default: %d from config)\n", cfg.Query.DefaultLimit)
		fmt.Fprintf(os.Stderr, "\nComparison options:\n")
		fmt.Fprintf(os.Stderr, "  --compare-db <path>     Compare with another database\n")
		fmt.Fprintf(os.Stderr, "  --compare-format <fmt>  Format of compare DB (xdr/binary)\n")
		fmt.Fprintf(os.Stderr, "\nTiming stats are always shown.\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  query 55000000                                  # Query all events in range\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 --contract C...                  # bitmap32 (default)\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 --topic0 <b64>                  # bitmap32 with positional topic\n")
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

	var endLedger uint64
	if len(positionalArgs) >= 2 {
		endLedger, err = strconv.ParseUint(positionalArgs[1], 10, 32)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid end ledger: %v\n", err)
			os.Exit(2)
		}
	} else {
		// Use config default range
		endLedger = startLedger + uint64(cfg.Query.MaxLedgerRange)
	}

	if endLedger < startLedger {
		fmt.Fprintf(os.Stderr, "Error: end ledger must be >= start ledger\n")
		os.Exit(2)
	}

	// Enforce max ledger range from config
	maxEnd := startLedger + uint64(cfg.Query.MaxLedgerRange)
	if endLedger > maxEnd {
		fmt.Fprintf(os.Stderr, "Warning: range exceeds max_ledger_range (%d), capping end to %d\n", cfg.Query.MaxLedgerRange, maxEnd)
		endLedger = maxEnd
	}

	// Apply config defaults for limit
	queryLimit := *limit
	if queryLimit <= 0 {
		queryLimit = cfg.Query.DefaultLimit
	}

	// Determine index type: bitmap32 (default) or segment-index
	indexType := "bitmap32"
	if *useSegment {
		indexType = "segment-index"
	}

	cmdQuery(cfg, uint32(startLedger), uint32(endLedger), *contract, *topics, *topic0, *topic1, *topic2, *topic3, queryLimit, indexType, *compareDB, *compareFormat)
}

func cmdQuery(cfg *config.Config, startLedger, endLedger uint32, contractID, topicsCSV, topic0, topic1, topic2, topic3 string, limit int, indexType string, compareDB, compareFormat string) {
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
		// If comparison mode, run both unfiltered queries and compare
		if compareDB != "" {
			runUnfilteredComparisonQuery(cfg, eventStore, compareDB, compareFormat, startLedger, endLedger, limit)
			return
		}

		if indexType == "segment-index" {
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

		// If comparison mode, run both queries and compare
		if compareDB != "" {
			runComparisonQuery(cfg, eventStore, compareDB, compareFormat, filter, startLedger, endLedger, limit)
			return
		}

		switch indexType {
		case "bitmap32":
			runBitmap32PositionalQuery(eventStore, filter, startLedger, endLedger, limit)
		case "segment-index":
			runSegmentIndexPositionalQuery(eventStore, filter, startLedger, endLedger, limit)
		}
		return
	}

	// --topics CSV flag: parse as positional (topic0,topic1,topic2,topic3)
	switch indexType {
	case "bitmap32":
		runBitmap32NonPositionalQuery(eventStore, contractID, topicsCSV, startLedger, endLedger, limit)
	case "segment-index":
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
	case "bitmap32":
		header = "32-bit Bitmap"
	case "segment-index":
		header = "Segment Index"
	case "segment-data":
		header = "Segment Data"
	default:
		header = r.IndexType
	}

	fmt.Fprintf(os.Stderr, "\n=== %s Query Results ===\n", header)
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

// runComparisonQuery runs the same query against two databases and compares
func runComparisonQuery(cfg *config.Config, primaryStore *store.EventStore, compareDBPath, compareFormat string, filter *query.Filter, startLedger, endLedger uint32, limit int) {
	// Run query on primary database
	fmt.Fprintf(os.Stderr, "=== Primary Database ===\n")
	primaryResult := executeQuery(primaryStore, filter, startLedger, endLedger, limit)
	if primaryResult == nil {
		return
	}
	printCompactResult("Primary", primaryResult)

	// Open comparison database
	compareStore, err := store.NewEventStoreWithOptions(compareDBPath, nil, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open comparison database: %v\n", err)
		os.Exit(1)
	}
	defer compareStore.Close()

	// Run query on comparison database
	fmt.Fprintf(os.Stderr, "\n=== Comparison Database ===\n")
	compareResult := executeQuery(compareStore, filter, startLedger, endLedger, limit)
	if compareResult == nil {
		return
	}
	printCompactResult("Compare", compareResult)

	// Print comparison
	fmt.Fprintf(os.Stderr, "\n=== Performance Comparison ===\n")
	printPerformanceComparison(primaryResult, compareResult, "xdr", "xdr")
}

// runUnfilteredComparisonQuery runs unfiltered queries against two databases and compares
func runUnfilteredComparisonQuery(cfg *config.Config, primaryStore *store.EventStore, compareDBPath, compareFormat string, startLedger, endLedger uint32, limit int) {
	// Run query on primary database
	fmt.Fprintf(os.Stderr, "=== Primary Database ===\n")
	fmt.Fprintf(os.Stderr, "Querying all events in ledgers %d-%d...\n", startLedger, endLedger)
	primaryResult := executeUnfilteredQuery(primaryStore, startLedger, endLedger, limit)
	if primaryResult == nil {
		return
	}
	printCompactRangeResult("Primary", primaryResult)

	// Open comparison database
	compareStore, err := store.NewEventStoreWithOptions(compareDBPath, nil, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open comparison database: %v\n", err)
		os.Exit(1)
	}
	defer compareStore.Close()

	// Run query on comparison database
	fmt.Fprintf(os.Stderr, "\n=== Comparison Database ===\n")
	fmt.Fprintf(os.Stderr, "Querying all events in ledgers %d-%d...\n", startLedger, endLedger)
	compareResult := executeUnfilteredQuery(compareStore, startLedger, endLedger, limit)
	if compareResult == nil {
		return
	}
	printCompactRangeResult("Compare", compareResult)

	// Print comparison
	fmt.Fprintf(os.Stderr, "\n=== Performance Comparison ===\n")
	printUnfilteredPerformanceComparison(primaryResult, compareResult, "xdr", "xdr")
}

// executeUnfilteredQuery runs an unfiltered range query and returns the result
func executeUnfilteredQuery(eventStore *store.EventStore, startLedger, endLedger uint32, limit int) *query.Result {
	startTime := time.Now()
	rangeResult, err := eventStore.GetEventsInRangeWithTiming(startLedger, endLedger, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		return nil
	}
	totalTime := time.Since(startTime)

	return &query.Result{
		Events:         rangeResult.Events,
		EventsScanned:  rangeResult.EventsScanned,
		EventsReturned: len(rangeResult.Events),
		LedgerRange:    endLedger - startLedger + 1,
		DiskReadTime:   rangeResult.Timing.DiskReadTime,
		UnmarshalTime:  rangeResult.Timing.UnmarshalTime,
		BytesRead:      rangeResult.Timing.BytesRead,
		EventFetchTime: totalTime,
		TotalTime:      totalTime,
	}
}

// printCompactRangeResult prints a compact version of unfiltered range query results
func printCompactRangeResult(label string, result *query.Result) {
	fmt.Fprintf(os.Stderr, "  Ledger range:      %d ledgers\n", result.LedgerRange)
	fmt.Fprintf(os.Stderr, "  Events scanned:    %d\n", result.EventsScanned)
	fmt.Fprintf(os.Stderr, "  Events returned:   %d\n", result.EventsReturned)
	fmt.Fprintf(os.Stderr, "  Bytes read:        %s\n", formatBytes(result.BytesRead))
	fmt.Fprintf(os.Stderr, "  Timing:\n")
	fmt.Fprintf(os.Stderr, "    Disk read:       %s\n", formatDuration(result.DiskReadTime))
	fmt.Fprintf(os.Stderr, "    Unmarshal:       %s\n", formatDuration(result.UnmarshalTime))
	fmt.Fprintf(os.Stderr, "    Total:           %s\n", formatDuration(result.TotalTime))
}

// printUnfilteredPerformanceComparison prints a side-by-side comparison for unfiltered queries
func printUnfilteredPerformanceComparison(primary, compare *query.Result, primaryFormat, compareFormat string) {
	// Verify results match
	if primary.EventsReturned != compare.EventsReturned {
		fmt.Fprintf(os.Stderr, "  WARNING: Different events returned (%d vs %d)\n",
			primary.EventsReturned, compare.EventsReturned)
	}

	// Calculate per-event metrics for fair comparison
	var primaryPerEvent, comparePerEvent float64
	if primary.EventsScanned > 0 {
		primaryPerEvent = float64(primary.UnmarshalTime.Nanoseconds()) / float64(primary.EventsScanned)
	}
	if compare.EventsScanned > 0 {
		comparePerEvent = float64(compare.UnmarshalTime.Nanoseconds()) / float64(compare.EventsScanned)
	}

	fmt.Fprintf(os.Stderr, "\n  %-20s %15s %15s\n", "Metric", primaryFormat, compareFormat)
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "------", "------", "------")
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Total time", formatDuration(primary.TotalTime), formatDuration(compare.TotalTime))
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Disk read", formatDuration(primary.DiskReadTime), formatDuration(compare.DiskReadTime))
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Unmarshal", formatDuration(primary.UnmarshalTime), formatDuration(compare.UnmarshalTime))
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Bytes read", formatBytes(primary.BytesRead), formatBytes(compare.BytesRead))
	fmt.Fprintf(os.Stderr, "  %-20s %15.2f ns %13.2f ns\n", "Unmarshal/event", primaryPerEvent, comparePerEvent)

	// Disk throughput
	var primaryMBps, compareMBps float64
	if primary.DiskReadTime > 0 && primary.BytesRead > 0 {
		primaryMBps = float64(primary.BytesRead) / (1024 * 1024) / primary.DiskReadTime.Seconds()
	}
	if compare.DiskReadTime > 0 && compare.BytesRead > 0 {
		compareMBps = float64(compare.BytesRead) / (1024 * 1024) / compare.DiskReadTime.Seconds()
	}
	fmt.Fprintf(os.Stderr, "  %-20s %12.1f MB/s %10.1f MB/s\n", "Disk throughput", primaryMBps, compareMBps)

	// Speedup calculations
	if compare.TotalTime > 0 && primary.TotalTime > 0 {
		totalSpeedup := float64(primary.TotalTime) / float64(compare.TotalTime)
		fmt.Fprintf(os.Stderr, "\n  Total speedup: %.2fx", totalSpeedup)
		if totalSpeedup > 1 {
			fmt.Fprintf(os.Stderr, " (%s is faster)\n", compareFormat)
		} else {
			fmt.Fprintf(os.Stderr, " (%s is faster)\n", primaryFormat)
		}
	}

	if comparePerEvent > 0 && primaryPerEvent > 0 {
		unmarshalSpeedup := primaryPerEvent / comparePerEvent
		fmt.Fprintf(os.Stderr, "  Unmarshal speedup: %.2fx per event\n", unmarshalSpeedup)
	}

	// Time saved
	if primary.TotalTime > compare.TotalTime {
		saved := primary.TotalTime - compare.TotalTime
		pctSaved := float64(saved) / float64(primary.TotalTime) * 100
		fmt.Fprintf(os.Stderr, "  Time saved: %s (%.1f%%)\n", formatDuration(saved), pctSaved)
	} else if compare.TotalTime > primary.TotalTime {
		saved := compare.TotalTime - primary.TotalTime
		pctSaved := float64(saved) / float64(compare.TotalTime) * 100
		fmt.Fprintf(os.Stderr, "  Time saved: %s (%.1f%%) with %s\n", formatDuration(saved), pctSaved, primaryFormat)
	}

	// Size comparison
	if primary.BytesRead > 0 && compare.BytesRead > 0 {
		sizeRatio := float64(compare.BytesRead) / float64(primary.BytesRead)
		if sizeRatio > 1 {
			fmt.Fprintf(os.Stderr, "  Size overhead: %.1f%% larger with %s\n", (sizeRatio-1)*100, compareFormat)
		} else if sizeRatio < 1 {
			fmt.Fprintf(os.Stderr, "  Size savings: %.1f%% smaller with %s\n", (1-sizeRatio)*100, compareFormat)
		}
	}
}

// executeQuery runs a query using bitmap32 and returns the result
func executeQuery(eventStore *store.EventStore, filter *query.Filter, startLedger, endLedger uint32, limit int) *query.Result {
	topicGroups := filter.TopicGroups()
	stats, events, err := eventStore.QueryEventsWithBitmap32EventIndex(filter.ContractID, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		return nil
	}

	unified := stats.ToUnified()
	return &query.Result{
		LedgerRange:    unified.LedgerRange,
		EventsScanned:  int64(unified.EventsScanned),
		EventsReturned: unified.EventsReturned,
		Events:         events,
		TotalTime:      unified.TotalTime,
		EventFetchTime: unified.EventFetchTime,
		BytesRead:      unified.EventBytesRead,
	}
}

// printCompactResult prints a compact version of query results
func printCompactResult(label string, result *query.Result) {
	fmt.Fprintf(os.Stderr, "  Matching ledgers:  %d\n", result.MatchingLedgers)
	fmt.Fprintf(os.Stderr, "  Events scanned:    %d\n", result.EventsScanned)
	fmt.Fprintf(os.Stderr, "  Events returned:   %d\n", result.EventsReturned)
	fmt.Fprintf(os.Stderr, "  Bytes read:        %s\n", formatBytes(result.BytesRead))
	fmt.Fprintf(os.Stderr, "  Timing:\n")
	fmt.Fprintf(os.Stderr, "    Index lookup:    %s\n", formatDuration(result.IndexLookupTime))
	fmt.Fprintf(os.Stderr, "    Disk read:       %s\n", formatDuration(result.DiskReadTime))
	fmt.Fprintf(os.Stderr, "    Unmarshal:       %s\n", formatDuration(result.UnmarshalTime))
	fmt.Fprintf(os.Stderr, "    Filter:          %s\n", formatDuration(result.FilterTime))
	fmt.Fprintf(os.Stderr, "    Total:           %s\n", formatDuration(result.TotalTime))
}

// printPerformanceComparison prints a side-by-side comparison
func printPerformanceComparison(primary, compare *query.Result, primaryFormat, compareFormat string) {
	// Verify results match
	if primary.EventsReturned != compare.EventsReturned {
		fmt.Fprintf(os.Stderr, "  WARNING: Different events returned (%d vs %d)\n",
			primary.EventsReturned, compare.EventsReturned)
	}

	// Calculate per-event metrics for fair comparison
	var primaryPerEvent, comparePerEvent float64
	if primary.EventsScanned > 0 {
		primaryPerEvent = float64(primary.UnmarshalTime.Nanoseconds()) / float64(primary.EventsScanned)
	}
	if compare.EventsScanned > 0 {
		comparePerEvent = float64(compare.UnmarshalTime.Nanoseconds()) / float64(compare.EventsScanned)
	}

	fmt.Fprintf(os.Stderr, "\n  %-20s %15s %15s\n", "Metric", primaryFormat, compareFormat)
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "------", "------", "------")
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Total time", formatDuration(primary.TotalTime), formatDuration(compare.TotalTime))
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Disk read", formatDuration(primary.DiskReadTime), formatDuration(compare.DiskReadTime))
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Unmarshal", formatDuration(primary.UnmarshalTime), formatDuration(compare.UnmarshalTime))
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Filter", formatDuration(primary.FilterTime), formatDuration(compare.FilterTime))
	fmt.Fprintf(os.Stderr, "  %-20s %15s %15s\n", "Bytes read", formatBytes(primary.BytesRead), formatBytes(compare.BytesRead))
	fmt.Fprintf(os.Stderr, "  %-20s %15.2f ns %13.2f ns\n", "Unmarshal/event", primaryPerEvent, comparePerEvent)

	// Speedup calculations
	if compare.TotalTime > 0 && primary.TotalTime > 0 {
		totalSpeedup := float64(primary.TotalTime) / float64(compare.TotalTime)
		fmt.Fprintf(os.Stderr, "\n  Total speedup: %.2fx", totalSpeedup)
		if totalSpeedup > 1 {
			fmt.Fprintf(os.Stderr, " (%s is faster)\n", compareFormat)
		} else {
			fmt.Fprintf(os.Stderr, " (%s is faster)\n", primaryFormat)
		}
	}

	if comparePerEvent > 0 && primaryPerEvent > 0 {
		unmarshalSpeedup := primaryPerEvent / comparePerEvent
		fmt.Fprintf(os.Stderr, "  Unmarshal speedup: %.2fx per event\n", unmarshalSpeedup)
	}

	// Time saved
	if primary.TotalTime > compare.TotalTime {
		saved := primary.TotalTime - compare.TotalTime
		pctSaved := float64(saved) / float64(primary.TotalTime) * 100
		fmt.Fprintf(os.Stderr, "  Time saved: %s (%.1f%%)\n", formatDuration(saved), pctSaved)
	} else if compare.TotalTime > primary.TotalTime {
		saved := compare.TotalTime - primary.TotalTime
		pctSaved := float64(saved) / float64(compare.TotalTime) * 100
		fmt.Fprintf(os.Stderr, "  Time saved: %s (%.1f%%) with %s\n", formatDuration(saved), pctSaved, primaryFormat)
	}
}

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

