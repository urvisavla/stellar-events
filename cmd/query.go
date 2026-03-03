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
	topic0 := fs.String("topic0", "", "Topic0 (base64) - positional filter at position 0")
	topic1 := fs.String("topic1", "", "Topic1 (base64) - positional filter at position 1")
	topic2 := fs.String("topic2", "", "Topic2 (base64) - positional filter at position 2")
	topic3 := fs.String("topic3", "", "Topic3 (base64) - positional filter at position 3")
	limit := fs.Int("limit", 0, "Max results (0 = use config default)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: query <start> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Queries contract events.\n\n")
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  <start>           Start ledger (required)\n")
		fmt.Fprintf(os.Stderr, "  Range is start + %d (max_ledger_range from config)\n\n", cfg.Query.MaxLedgerRange)
		fmt.Fprintf(os.Stderr, "Filter options:\n")
		fmt.Fprintf(os.Stderr, "  --contract <id>   Filter by contract ID (C... strkey format)\n")
		fmt.Fprintf(os.Stderr, "  --topics <list>   Comma-separated topics (base64), position-independent AND\n")
		fmt.Fprintf(os.Stderr, "  --topic0 <val>    Filter by topic0 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic1 <val>    Filter by topic1 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic2 <val>    Filter by topic2 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --topic3 <val>    Filter by topic3 (base64) - positional\n")
		fmt.Fprintf(os.Stderr, "  --limit <n>       Max results (default: %d from config)\n", cfg.Query.DefaultLimit)
		fmt.Fprintf(os.Stderr, "\nTiming stats are always shown.\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  query 55000000                    # Query all events in range\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 --contract C...    # Filter by contract\n")
		fmt.Fprintf(os.Stderr, "  query 55000000 --topic0 <b64>     # Filter by positional topic\n")
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

	cmdQuery(cfg, uint32(startLedger), uint32(endLedger), *contract, *topics, *topic0, *topic1, *topic2, *topic3, queryLimit)
}

func cmdQuery(cfg *config.Config, startLedger, endLedger uint32, contractID, topicsCSV, topic0, topic1, topic2, topic3 string, limit int) {
	eventStore, err := openEventStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	// Build contractIDs and topicGroups for the query.
	// Empty filters → QueryEvents falls back to a range scan automatically.
	var contractIDs [][]byte
	var topicGroups [4][][]byte

	if contractID != "" {
		contractBytes, decErr := decodeContractID(contractID)
		if decErr != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid contract ID (expected C... format): %v\n", decErr)
			os.Exit(2)
		}
		contractIDs = [][]byte{contractBytes}
	}

	hasPositionalTopics := topic0 != "" || topic1 != "" || topic2 != "" || topic3 != ""
	if hasPositionalTopics || topicsCSV == "" {
		// Use positional topic flags (or no topics at all)
		for i, t := range []string{topic0, topic1, topic2, topic3} {
			if t != "" {
				topicBytes, decErr := decodeBase64(t)
				if decErr != nil {
					fmt.Fprintf(os.Stderr, "Error: invalid topic%d: %v\n", i, decErr)
					os.Exit(2)
				}
				topicGroups[i] = [][]byte{topicBytes}
			}
		}
	} else {
		// --topics CSV flag: parse as positional (topic0,topic1,topic2,topic3)
		for i, t := range strings.Split(topicsCSV, ",") {
			if i >= 4 {
				break
			}
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			topicBytes, decErr := decodeBase64(t)
			if decErr != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid topic (expected base64): %v\n", decErr)
				os.Exit(2)
			}
			topicGroups[i] = [][]byte{topicBytes}
		}
	}

	fmt.Fprintf(os.Stderr, "Querying events in ledgers %d-%d...\n", startLedger, endLedger)

	stats, events, err := eventStore.QueryEvents(contractIDs, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}

	printUnifiedResult(toUnifiedResult(stats))

	if events == nil {
		fmt.Println("[]")
		return
	}

	output, jsonErr := json.MarshalIndent(events, "", "  ")
	if jsonErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal events: %v\n", jsonErr)
		os.Exit(1)
	}
	fmt.Println(string(output))
}

// unifiedQueryResult holds query results in a common format for display.
type unifiedQueryResult struct {
	LedgerRange        uint32
	SegmentsTouched    int
	IndexMatches       int
	MatchUnitName      string
	EventsScanned      int
	EventsReturned     int
	IndexBytesRead     int64
	EventBytesRead     int64
	IndexLookupTime    time.Duration
	EventFetchTime     time.Duration
	DecompressTime     time.Duration
	EventDiskReadTime  time.Duration
	GroupsDecompressed int
	DecodeTime         time.Duration
	FilterTime         time.Duration
	TotalTime          time.Duration
}

// toUnifiedResult converts a QueryResult to unifiedQueryResult for display.
func toUnifiedResult(r *store.QueryResult) *unifiedQueryResult {
	return &unifiedQueryResult{
		LedgerRange:        r.LedgerRange,
		SegmentsTouched:    r.SegmentsTouched,
		IndexMatches:       r.MatchingLocalIDs,
		MatchUnitName:      "local IDs",
		EventsScanned:      r.EventsScanned,
		EventsReturned:     r.EventsReturned,
		IndexBytesRead:     r.IndexBytesRead,
		EventBytesRead:     r.EventBytesRead,
		IndexLookupTime:    r.IndexLookupTime,
		EventFetchTime:     r.EventFetchTime,
		DecompressTime:     r.DecompressTime,
		EventDiskReadTime:  r.EventDiskReadTime,
		GroupsDecompressed: r.GroupsDecompressed,
		DecodeTime:         r.DecodeTime,
		FilterTime:         r.FilterTime,
		TotalTime:          r.TotalTime,
	}
}

// printUnifiedResult prints query stats in a unified format
func printUnifiedResult(r *unifiedQueryResult) {
	fmt.Fprintf(os.Stderr, "\n=== Query Results ===\n")
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


