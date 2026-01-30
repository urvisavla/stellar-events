package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/query"
)

// =============================================================================
// Query Command
// =============================================================================

func runQuery(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	contract := fs.String("contract", "", "Contract ID (base64)")
	topic0 := fs.String("topic0", "", "Topic0 (base64)")
	topic1 := fs.String("topic1", "", "Topic1 (base64)")
	topic2 := fs.String("topic2", "", "Topic2 (base64)")
	topic3 := fs.String("topic3", "", "Topic3 (base64)")
	limit := fs.Int("limit", 0, "Max results (0 = use config default)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: query <start> [end] [options]\n\n")
		fmt.Fprintf(os.Stderr, "Queries contract events from RocksDB.\n\n")
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  <start>           Start ledger (required)\n")
		fmt.Fprintf(os.Stderr, "  [end]             End ledger (default: start + %d from config)\n\n", cfg.Query.MaxLedgerRange)
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --contract <id>   Filter by contract ID (base64)\n")
		fmt.Fprintf(os.Stderr, "  --topic0 <val>    Filter by topic0 (base64)\n")
		fmt.Fprintf(os.Stderr, "  --topic1 <val>    Filter by topic1 (base64)\n")
		fmt.Fprintf(os.Stderr, "  --topic2 <val>    Filter by topic2 (base64)\n")
		fmt.Fprintf(os.Stderr, "  --topic3 <val>    Filter by topic3 (base64)\n")
		fmt.Fprintf(os.Stderr, "  --limit <n>       Max results (default: %d from config)\n", cfg.Query.DefaultLimit)
		fmt.Fprintf(os.Stderr, "\nTiming stats are always shown.\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  query 55000000                              # Query single ledger\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 56000000                     # Query range\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 --contract <base64_id>       # Filter by contract\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 56000000 --topic0 <base64>   # Filter by topic\n")
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

	// Apply config defaults for limit
	queryLimit := *limit
	if queryLimit <= 0 {
		queryLimit = cfg.Query.DefaultLimit
	}

	cmdQuery(cfg, uint32(startLedger), uint32(endLedger), *contract, *topic0, *topic1, *topic2, *topic3, queryLimit)
}

func cmdQuery(cfg *config.Config, startLedger, endLedger uint32, contractID, topic0, topic1, topic2, topic3 string, limit int) {
	eventStore, err := openEventStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	// Check if any filter is specified
	hasContract := contractID != ""
	hasTopic0 := topic0 != ""
	hasTopic1 := topic1 != ""
	hasTopic2 := topic2 != ""
	hasTopic3 := topic3 != ""
	hasAnyFilter := hasContract || hasTopic0 || hasTopic1 || hasTopic2 || hasTopic3

	// No filter - scan all events in range (use direct store method)
	if !hasAnyFilter {
		startTime := time.Now()
		fmt.Fprintf(os.Stderr, "Querying all events in ledgers %d-%d...\n", startLedger, endLedger)
		events, err := eventStore.GetEventsByLedgerRange(startLedger, endLedger)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
			os.Exit(1)
		}
		elapsed := time.Since(startTime)

		// Defensive limit
		if limit > 0 && len(events) > limit {
			events = events[:limit]
		}

		fmt.Fprintf(os.Stderr, "Found %d events in %s\n\n", len(events), formatElapsed(elapsed))

		output, err := json.MarshalIndent(events, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(output))
		return
	}

	// Build query filter
	filter := &query.Filter{}

	if hasContract {
		contractBytes, decErr := decodeBase64(contractID)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid contract ID: %v\n", decErr)
			os.Exit(2)
		}
		filter.ContractID = contractBytes
	}

	if hasTopic0 {
		topicBytes, decErr := decodeBase64(topic0)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid topic0: %v\n", decErr)
			os.Exit(2)
		}
		filter.Topic0 = topicBytes
	}

	if hasTopic1 {
		topicBytes, decErr := decodeBase64(topic1)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid topic1: %v\n", decErr)
			os.Exit(2)
		}
		filter.Topic1 = topicBytes
	}

	if hasTopic2 {
		topicBytes, decErr := decodeBase64(topic2)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid topic2: %v\n", decErr)
			os.Exit(2)
		}
		filter.Topic2 = topicBytes
	}

	if hasTopic3 {
		topicBytes, decErr := decodeBase64(topic3)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid topic3: %v\n", decErr)
			os.Exit(2)
		}
		filter.Topic3 = topicBytes
	}

	// Get the index reader
	indexReader := eventStore.GetIndexReader()
	if indexReader == nil {
		fmt.Fprintf(os.Stderr, "Error: index store not available\n")
		os.Exit(1)
	}

	// Create query engine
	engine := query.NewEngine(indexReader, eventStore)

	// Configure query options
	opts := &query.Options{
		Limit: limit,
	}

	// Execute query
	fmt.Fprintf(os.Stderr, "Querying with bitmap index in ledgers %d-%d...\n", startLedger, endLedger)

	result, err := engine.Query(filter, startLedger, endLedger, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	// Display results
	printQueryResult(result)

	output, err := json.MarshalIndent(result.Events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// printQueryResult displays query statistics to stderr
func printQueryResult(result *query.Result) {
	fmt.Fprintf(os.Stderr, "\n=== Bitmap Query Results ===\n")
	fmt.Fprintf(os.Stderr, "  Ledger range:      %d ledgers\n", result.LedgerRange)
	fmt.Fprintf(os.Stderr, "  Matching ledgers:  %d\n", result.MatchingLedgers)
	fmt.Fprintf(os.Stderr, "  Events scanned:    %d\n", result.EventsScanned)
	fmt.Fprintf(os.Stderr, "  Events returned:   %d\n", result.EventsReturned)

	if result.LedgerRange > 0 {
		ledgerSelectivity := float64(result.MatchingLedgers) / float64(result.LedgerRange) * 100
		fmt.Fprintf(os.Stderr, "  Ledger selectivity: %.4f%% (%.0fx reduction)\n",
			ledgerSelectivity,
			float64(result.LedgerRange)/float64(max(result.MatchingLedgers, 1)))
	}

	fmt.Fprintf(os.Stderr, "\n=== Timing Breakdown ===\n")
	fmt.Fprintf(os.Stderr, "  Index lookup:      %s\n", formatDuration(result.IndexLookupTime))
	fmt.Fprintf(os.Stderr, "  Event fetch:       %s (iterate + filter)\n", formatDuration(result.EventFetchTime))
	fmt.Fprintf(os.Stderr, "  Total time:        %s\n", formatDuration(result.TotalTime))

	if result.TotalTime > 0 {
		indexPct := float64(result.IndexLookupTime) / float64(result.TotalTime) * 100
		fetchPct := float64(result.EventFetchTime) / float64(result.TotalTime) * 100
		fmt.Fprintf(os.Stderr, "  Time distribution: index=%.1f%%, fetch=%.1f%%\n", indexPct, fetchPct)
	}

	fmt.Fprintf(os.Stderr, "\n")
}
