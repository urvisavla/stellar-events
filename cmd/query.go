package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/store"
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
		fmt.Fprintf(os.Stderr, "  --limit <n>       Max results (default: %d from config)\n\n", cfg.Query.DefaultLimit)
		fmt.Fprintf(os.Stderr, "Query uses hierarchical L1+L2 bitmap with auto-fallback.\n")
		fmt.Fprintf(os.Stderr, "Timing stats are always shown.\n\n")
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

	var events []*store.ContractEvent
	var hierarchicalResult *store.HierarchicalQueryResult
	var queryResult *store.QueryResult
	startTime := time.Now()

	// Check if any filter is specified
	hasContract := contractID != ""
	hasTopic0 := topic0 != ""
	hasTopic1 := topic1 != ""
	hasTopic2 := topic2 != ""
	hasTopic3 := topic3 != ""
	hasAnyFilter := hasContract || hasTopic0 || hasTopic1 || hasTopic2 || hasTopic3

	// Build filter if any filter is specified
	var filter *store.QueryFilter
	if hasAnyFilter {
		filter = &store.QueryFilter{}

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
	}

	// Always try hierarchical query first (if filter specified), with auto-fallback
	if hasAnyFilter {
		fmt.Fprintf(os.Stderr, "Querying with HIERARCHICAL L1+L2 bitmap in ledgers %d-%d...\n", startLedger, endLedger)
		hierarchicalResult, err = eventStore.GetEventsWithFilterHierarchical(filter, startLedger, endLedger, limit)
		if err != nil {
			// Fallback to L1-only bitmap query
			fmt.Fprintf(os.Stderr, "Hierarchical query unavailable (%v), falling back to L1 bitmap...\n", err)
			hierarchicalResult = nil

			fmt.Fprintf(os.Stderr, "Querying with L1 bitmap index in ledgers %d-%d...\n", startLedger, endLedger)
			queryResult, err = eventStore.GetEventsWithFilter(filter, startLedger, endLedger, limit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
				os.Exit(1)
			}
			events = queryResult.Events
		} else {
			events = hierarchicalResult.Events
		}
	} else {
		// No filter - scan all events in range
		fmt.Fprintf(os.Stderr, "Querying all events in ledgers %d-%d...\n", startLedger, endLedger)
		events, err = eventStore.GetEventsByLedgerRange(startLedger, endLedger)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
			os.Exit(1)
		}
	}

	elapsed := time.Since(startTime)

	// Defensive limit
	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	// Always show detailed stats
	if hierarchicalResult != nil {
		fmt.Fprintf(os.Stderr, "\n=== Hierarchical L1+L2 Query Results ===\n")
		fmt.Fprintf(os.Stderr, "  Matching ledgers (L1): %d\n", hierarchicalResult.MatchingLedgers)
		fmt.Fprintf(os.Stderr, "  Matching events (L2):  %d\n", hierarchicalResult.MatchingEvents)
		fmt.Fprintf(os.Stderr, "  Events fetched:        %d\n", hierarchicalResult.EventsFetched)

		fmt.Fprintf(os.Stderr, "\n=== Timing Breakdown ===\n")
		fmt.Fprintf(os.Stderr, "  L1 bitmap lookup:  %s\n", formatDuration(hierarchicalResult.L1LookupTime))
		fmt.Fprintf(os.Stderr, "  L2 bitmap lookup:  %s\n", formatDuration(hierarchicalResult.L2LookupTime))
		fmt.Fprintf(os.Stderr, "  Event fetch:       %s (MultiGet)\n", formatDuration(hierarchicalResult.EventFetchTime))
		fmt.Fprintf(os.Stderr, "  Total time:        %s\n", formatDuration(hierarchicalResult.TotalTime))

		if hierarchicalResult.TotalTime > 0 {
			l1Pct := float64(hierarchicalResult.L1LookupTime) / float64(hierarchicalResult.TotalTime) * 100
			l2Pct := float64(hierarchicalResult.L2LookupTime) / float64(hierarchicalResult.TotalTime) * 100
			fetchPct := float64(hierarchicalResult.EventFetchTime) / float64(hierarchicalResult.TotalTime) * 100
			fmt.Fprintf(os.Stderr, "  Time distribution: L1=%.1f%%, L2=%.1f%%, fetch=%.1f%%\n", l1Pct, l2Pct, fetchPct)
		}
	} else if queryResult != nil {
		fmt.Fprintf(os.Stderr, "\n=== L1 Bitmap Query Results ===\n")
		fmt.Fprintf(os.Stderr, "  Ledger range:      %d ledgers\n", queryResult.LedgerRange)
		fmt.Fprintf(os.Stderr, "  Segments queried:  %d\n", queryResult.SegmentsQueried)
		fmt.Fprintf(os.Stderr, "  Matching ledgers:  %d\n", queryResult.MatchingLedgers)
		fmt.Fprintf(os.Stderr, "  Events scanned:    %d\n", queryResult.EventsScanned)
		fmt.Fprintf(os.Stderr, "  Events returned:   %d\n", queryResult.EventsReturned)

		if queryResult.LedgerRange > 0 {
			ledgerSelectivity := float64(queryResult.MatchingLedgers) / float64(queryResult.LedgerRange) * 100
			fmt.Fprintf(os.Stderr, "  Ledger selectivity: %.4f%% (%.0fx reduction)\n",
				ledgerSelectivity,
				float64(queryResult.LedgerRange)/float64(max(queryResult.MatchingLedgers, 1)))
		}

		fmt.Fprintf(os.Stderr, "\n=== Timing Breakdown ===\n")
		fmt.Fprintf(os.Stderr, "  Bitmap lookup:     %s\n", formatDuration(queryResult.BitmapLookupTime))
		fmt.Fprintf(os.Stderr, "  Event fetch:       %s (iterate + filter)\n", formatDuration(queryResult.EventFetchTime))
		fmt.Fprintf(os.Stderr, "  Total time:        %s\n", formatDuration(queryResult.TotalTime))

		if queryResult.TotalTime > 0 {
			bitmapPct := float64(queryResult.BitmapLookupTime) / float64(queryResult.TotalTime) * 100
			fetchPct := float64(queryResult.EventFetchTime) / float64(queryResult.TotalTime) * 100
			fmt.Fprintf(os.Stderr, "  Time distribution: bitmap=%.1f%%, fetch=%.1f%%\n", bitmapPct, fetchPct)
		}

		if queryResult.MatchingLedgers > 0 {
			avgPerLedger := queryResult.EventFetchTime / time.Duration(queryResult.MatchingLedgers)
			fmt.Fprintf(os.Stderr, "  Avg fetch/ledger:  %s\n", formatDuration(avgPerLedger))
		}
	} else {
		fmt.Fprintf(os.Stderr, "Found %d events in %s\n", len(events), formatElapsed(elapsed))
	}

	fmt.Fprintf(os.Stderr, "\n")

	output, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(output))
}
