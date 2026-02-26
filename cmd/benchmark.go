package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Benchmark Data Format
// =============================================================================

// BenchmarkData contains test data for benchmark queries
type BenchmarkData struct {
	// Ledger range for queries
	StartLedger uint32 `json:"start_ledger"`
	EndLedger   uint32 `json:"end_ledger"`

	// Contract IDs grouped by cardinality (strkey format C...)
	Contracts ContractData `json:"contracts"`

	// Topics for each position (0-3), each grouped by cardinality (base64 encoded)
	Topic0 TopicData `json:"topic0"`
	Topic1 TopicData `json:"topic1"`
	Topic2 TopicData `json:"topic2"`
	Topic3 TopicData `json:"topic3"`
}

// ContractData holds contract IDs by cardinality
type ContractData struct {
	High   []string `json:"high"`   // High cardinality (many events)
	Medium []string `json:"medium"` // Medium cardinality
	Low    []string `json:"low"`    // Low cardinality (few events)
}

// TopicData holds topics by cardinality for a specific position
type TopicData struct {
	High   []string `json:"high"`   // High cardinality (many events)
	Medium []string `json:"medium"` // Medium cardinality
	Low    []string `json:"low"`    // Low cardinality (few events)
}

// TopicWithPosition holds a topic value with its position
type TopicWithPosition struct {
	Position int    // 0-3
	Value    string // base64 encoded
	Card     string // cardinality label
}

// =============================================================================
// Benchmark Results
// =============================================================================

// QuerySpec describes a query to benchmark
type QuerySpec struct {
	Name        string     // Human-readable name
	ContractIDs []string   // Contract IDs (OR logic, strkey format)
	Topics      [][]string // Topics[position] = []values (OR per position, base64)
}

// BenchmarkResult holds timing results for a query
type BenchmarkResult struct {
	// Query identification
	Query       QuerySpec
	IndexType   string
	StartLedger uint32 // Ledger range used for this query
	EndLedger   uint32

	// Test config
	Iterations int

	// Timing (p50/p99 only)
	P50Time            time.Duration
	P99Time            time.Duration
	P99IndexTime          time.Duration // P99 time for index lookup
	P99IndexReadTime      time.Duration // P99 time reading index from storage (I/O)
	P99IndexDecodeTime    time.Duration // P99 time decoding index (CPU)
	P99IndexIntersectTime time.Duration // P99 time intersecting index results
	P99EventTime          time.Duration // P99 time for event operations (total)
	P99EventFetchTime  time.Duration // P99 time fetching events from storage (I/O)
	P99EventDecodeTime time.Duration // P99 time decoding events (CPU)
	P99EventFilterTime time.Duration // P99 time filtering events (CPU)
	P99EventDecompressTime time.Duration // P99 time decompressing event blobs
	P99EventDiskReadTime   time.Duration // P99 time on event disk I/O

	// Index stats
	SegmentsTouched   int   // Number of segments touched by the query
	IndexMatches     int   // TOIDs or ledgers matched by index
	IndexBytes       int64 // Bytes read from index
	SmallestListSize int   // Size of smallest posting list (posting list only)
	LargestListSize  int   // Size of largest posting list (posting list only)

	// Event stats
	EventsReturned     int   // Events returned after filtering
	EventsScanned      int   // Events scanned from storage
	EventBytes         int64 // Bytes read from event storage
	GroupsDecompressed int   // Number of group blocks decompressed

	Error string
}

// =============================================================================
// Benchmark Command
// =============================================================================

func runBenchmark(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	dataFile := fs.String("data", "", "Benchmark data file (JSON)")
	indexTypes := fs.String("index", "all", "Index types to benchmark: rocksdb,flat-idx,flat-all,all")
	iterations := fs.Int("iterations", 5, "Number of iterations per query")
	warmup := fs.Int("warmup", 1, "Warmup iterations (not counted)")
	outputFormat := fs.String("format", "table", "Output format: table, csv, json")
	generateData := fs.Bool("generate", false, "Generate sample benchmark data file")
	maxCombinations := fs.Int("max-combinations", 50, "Maximum query combinations to test")
	seed := fs.Int64("seed", 0, "Random seed for combination selection (0 = use time)")
	limit := fs.Int("limit", 1000, "Max events to fetch per query")
	timeout := fs.Duration("timeout", 30*time.Second, "Timeout per query (e.g., 10s, 1m)")
	fixedRange := fs.Bool("fixed-range", false, "Use fixed ledger range for all queries (no random sampling)")
	logFile := fs.String("log", "benchmark.log", "Log file for query details (use 'none' to disable)")
	outputFile := fs.String("output", "", "Output file for results (writes incrementally; use 'none' to disable)")
	coldCache := fs.Bool("cold", false, "Purge OS page cache before each query iteration (requires sudo)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: benchmark [options]\n\n")
		fmt.Fprintf(os.Stderr, "Benchmarks query performance across different index types.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  benchmark --generate > benchmark_data.json  # Generate sample data file\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data benchmark_data.json        # Run benchmarks\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data data.json --index rocksdb  # Only test bitmap32\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data data.json --format csv     # CSV output\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	// Generate sample data file
	if *generateData {
		generateSampleData()
		return
	}

	// Require data file
	if *dataFile == "" {
		fmt.Fprintf(os.Stderr, "Error: --data is required\n\n")
		fs.Usage()
		os.Exit(2)
	}

	// Load benchmark data
	data, err := loadBenchmarkData(*dataFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading benchmark data: %v\n", err)
		os.Exit(1)
	}

	// Store original range for random sampling, cap individual queries to max_ledger_range
	originalStartLedger := data.StartLedger
	originalEndLedger := data.EndLedger
	maxLedgerRange := uint32(cfg.Query.MaxLedgerRange)

	// If the data range is smaller than max_ledger_range, just use it as-is
	if originalEndLedger-originalStartLedger <= maxLedgerRange {
		// Range fits, no random sampling needed
		fmt.Fprintf(os.Stderr, "Data range (%d ledgers) fits within max_ledger_range (%d)\n",
			originalEndLedger-originalStartLedger, maxLedgerRange)
	} else {
		fmt.Fprintf(os.Stderr, "Data range: %d-%d (%d ledgers), will sample random %d-ledger windows\n",
			originalStartLedger, originalEndLedger, originalEndLedger-originalStartLedger, maxLedgerRange)
	}

	// Parse index types
	// Map user-facing names to internal names
	indexNameToInternal := map[string]string{
		"rocksdb":  "bitmap32",
		"flat-idx": "segment-index",
		"flat-all": "segment-data",
	}
	internalToDisplay := map[string]string{
		"bitmap32":       "rocksdb",
		"segment-index":   "flat-idx",
		"segment-data": "flat-all",
	}

	var indexes []string
	if *indexTypes == "all" {
		indexes = []string{"bitmap32", "segment-index", "segment-data"}
	} else {
		for _, name := range strings.Split(*indexTypes, ",") {
			internal, ok := indexNameToInternal[name]
			if !ok {
				fmt.Fprintf(os.Stderr, "Error: invalid index type: %s (valid: rocksdb, flat-idx, flat-all)\n", name)
				os.Exit(2)
			}
			indexes = append(indexes, internal)
		}
	}

	// Build display names for each index
	displayIndexes := make([]string, len(indexes))
	for i, idx := range indexes {
		displayIndexes[i] = internalToDisplay[idx]
	}

	// Set random seed
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	rand.Seed(*seed)

	// Open event store
	eventStore, err := openEventStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	// Create log writer if enabled
	var logWriter *os.File
	if *logFile != "none" && *logFile != "" {
		logWriter, err = os.Create(*logFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log file: %v\n", err)
			os.Exit(1)
		}
		defer logWriter.Close()
		// Write CSV header
		fmt.Fprintln(logWriter, "timestamp,query_name,index_type,events_returned,index_matches,segments_touched,index_bytes,event_bytes,groups_decompressed,p99_idx_read_ms,p99_idx_decode_ms,p99_idx_intersect_ms,p99_idx_ms,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,evt_decompress_ms,evt_disk_read_ms,p99_total_ms,error")
		logWriter.Sync()
	}

	// Create output writer if enabled (writes results incrementally)
	var outputWriter *os.File
	outputFileFormat := *outputFormat
	if *outputFile != "none" && *outputFile != "" {
		// Auto-detect format from file extension if format is default
		if outputFileFormat == "table" {
			if strings.HasSuffix(*outputFile, ".csv") {
				outputFileFormat = "csv"
			} else if strings.HasSuffix(*outputFile, ".json") || strings.HasSuffix(*outputFile, ".jsonl") {
				outputFileFormat = "json"
			} else {
				outputFileFormat = "csv" // Default to CSV for file output
			}
		}
		outputWriter, err = os.Create(*outputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create output file: %v\n", err)
			os.Exit(1)
		}
		defer outputWriter.Close()
		// Write CSV header for results
		if outputFileFormat == "csv" {
			fmt.Fprintln(outputWriter, "query_name,contract_id,topic0,topic1,topic2,topic3,index_type,start_ledger,end_ledger,p50_total_ms,p99_total_ms,p99_idx_ms,idx_read_ms,idx_decode_ms,idx_intersect_ms,segments_touched,index_matches,index_bytes,smallest_list,largest_list,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,evt_decompress_ms,evt_disk_read_ms,events_returned,events_scanned,event_bytes,groups_decompressed,iterations,error")
			outputWriter.Sync()
		}
		fmt.Fprintf(os.Stderr, "Output file: %s (format: %s)\n", *outputFile, outputFileFormat)
	}

	// Generate query combinations
	queries := generateQueryCombinations(data, *maxCombinations)
	multiValueCount := 0
	for _, q := range queries {
		if len(q.ContractIDs) > 1 {
			multiValueCount++
			continue
		}
		for _, tv := range q.Topics {
			if len(tv) > 1 {
				multiValueCount++
				break
			}
		}
	}
	fmt.Fprintf(os.Stderr, "Generated %d query combinations (%d multi-value OR)\n", len(queries), multiValueCount)
	fmt.Fprintf(os.Stderr, "Benchmarking with %d iterations per query (+%d warmup)\n", *iterations, *warmup)
	fmt.Fprintf(os.Stderr, "Index types: %v\n", indexes)
	fmt.Fprintf(os.Stderr, "Ledger range: %d - %d (max per query: %d)\n\n", originalStartLedger, originalEndLedger, maxLedgerRange)

	// Acquire sudo credentials once upfront for cold cache mode
	if *coldCache {
		fmt.Fprintf(os.Stderr, "Cold cache mode: acquiring sudo credentials for page cache purge...\n")
		cmd := exec.Command("sudo", "-v")
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to acquire sudo credentials: %v\n", err)
			os.Exit(1)
		}
	}

	// Run benchmarks
	var results []BenchmarkResult
	totalQueries := len(queries) * len(indexes)
	completed := 0

	// Pre-compute fixed range if requested
	var fixedStart, fixedEnd uint32
	if *fixedRange {
		fixedStart = originalStartLedger
		fixedEnd = originalEndLedger
		dataRange := originalEndLedger - originalStartLedger
		if dataRange > maxLedgerRange {
			fixedEnd = originalStartLedger + maxLedgerRange
		}
		fmt.Fprintf(os.Stderr, "Fixed range mode: all queries use [%d-%d]\n", fixedStart, fixedEnd)
	}

	for _, q := range queries {
		// Pick ledger range for this query (same for all index types within a query)
		queryStart := originalStartLedger
		queryEnd := originalEndLedger
		if *fixedRange {
			queryStart = fixedStart
			queryEnd = fixedEnd
		} else {
			dataRange := originalEndLedger - originalStartLedger
			if dataRange > maxLedgerRange {
				// Pick random start within the range that allows full maxLedgerRange window
				maxStart := originalEndLedger - maxLedgerRange
				queryStart = originalStartLedger + uint32(rand.Intn(int(maxStart-originalStartLedger+1)))
				queryEnd = queryStart + maxLedgerRange
			}
		}

		for _, idxType := range indexes {
			completed++
			displayName := internalToDisplay[idxType]
			fmt.Fprintf(os.Stderr, "\r[%d/%d] Running: %s (%s) [%d-%d]...    ", completed, totalQueries, q.Name, displayName, queryStart, queryEnd)

			// Create a temporary data struct with the random range
			queryData := &BenchmarkData{
				StartLedger: queryStart,
				EndLedger:   queryEnd,
			}
			result := runQueryBenchmark(eventStore, queryData, q, idxType, displayName, *iterations, *warmup, *limit, *timeout, *coldCache)
			results = append(results, result)

			// Log query result
			if logWriter != nil {
				errStr := result.Error
				fmt.Fprintf(logWriter, "%s,%s,%s,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%s\n",
					time.Now().Format(time.RFC3339),
					result.Query.Name,
					result.IndexType,
					result.EventsReturned,
					result.IndexMatches,
					result.SegmentsTouched,
					result.IndexBytes,
					result.EventBytes,
					result.GroupsDecompressed,
					float64(result.P99IndexReadTime.Microseconds())/1000.0,
					float64(result.P99IndexDecodeTime.Microseconds())/1000.0,
					float64(result.P99IndexIntersectTime.Microseconds())/1000.0,
					float64(result.P99IndexTime.Microseconds())/1000.0,
					float64(result.P99EventTime.Microseconds())/1000.0,
					float64(result.P99EventFetchTime.Microseconds())/1000.0,
					float64(result.P99EventDecodeTime.Microseconds())/1000.0,
					float64(result.P99EventFilterTime.Microseconds())/1000.0,
					float64(result.P99EventDecompressTime.Microseconds())/1000.0,
					float64(result.P99EventDiskReadTime.Microseconds())/1000.0,
					float64(result.P99Time.Microseconds())/1000.0,
					errStr,
				)
				logWriter.Sync() // Flush to disk immediately
			}

			// Write result to output file incrementally
			if outputWriter != nil {
				writeResultIncremental(outputWriter, result, outputFileFormat)
				outputWriter.Sync() // Flush to disk immediately
			}
		}
	}
	fmt.Fprintf(os.Stderr, "\r[%d/%d] Complete!                                          \n\n", totalQueries, totalQueries)

	// Output results to stdout (skip if already written to file, except for table format summary)
	if outputWriter != nil {
		fmt.Fprintf(os.Stderr, "Results written to: %s\n", *outputFile)
		// Still print summary statistics for table format
		if *outputFormat == "table" {
			printSummaryStats(results, indexes)
		}
	} else {
		switch *outputFormat {
		case "csv":
			outputCSV(results)
		case "json":
			outputJSON(results)
		default:
			outputTable(results, displayIndexes)
		}
	}
}

// =============================================================================
// Data Loading
// =============================================================================

func loadBenchmarkData(path string) (*BenchmarkData, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data BenchmarkData
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		return nil, err
	}

	return &data, nil
}

func generateSampleData() {
	sample := BenchmarkData{
		StartLedger: 56000000,
		EndLedger:   57000000,
		Contracts: ContractData{
			High: []string{
				"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", // Example high-cardinality contract
			},
			Medium: []string{
				"CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP",
			},
			Low: []string{
				"CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUWDA",
			},
		},
		// Topic0 is typically event type (transfer, mint, burn, etc.)
		Topic0: TopicData{
			High: []string{
				"AAAADwAAAAh0cmFuc2Zlcg==", // "transfer" - very common
			},
			Medium: []string{
				"AAAADwAAAARtaW50", // "mint"
				"AAAADwAAAARidXJu", // "burn"
			},
			Low: []string{
				"AAAADwAAAAdkZXBvc2l0", // "deposit" - less common
			},
		},
		// Topic1 is often source address/identifier
		Topic1: TopicData{
			High: []string{
				"AAAAEgAAAAAAAAAAFvELLeYuDeGJOpfhVQQSnrSkU6sk3FgzUm3FdPBeFjY=", // common address
			},
			Medium: []string{},
			Low: []string{
				"AAAAEgAAAAAAAAAAAZRyYo7njrknFNItA5CWPCTZJ+oAmZlIbokfrC2qnCE=", // rare address
			},
		},
		// Topic2 is often destination address/identifier
		Topic2: TopicData{
			High:   []string{},
			Medium: []string{},
			Low: []string{
				"AAAADgAAAAZuYXRpdmUAAA==", // specific asset type
			},
		},
		// Topic3 is optional extra data
		Topic3: TopicData{
			High: []string{
				"AAAACgAAAAAAAAAAAAAAAAAAAAA=", // common amount/value
			},
			Medium: []string{
				"AAAACgAAAAAAAAAAAAAAAAABhqA=", // medium amount
			},
			Low: []string{
				"AAAACgAAAAAAAAAAAAAPQkA=", // rare amount
			},
		},
	}

	output, _ := json.MarshalIndent(sample, "", "  ")
	fmt.Println(string(output))
}

// =============================================================================
// Query Generation
// =============================================================================

func generateQueryCombinations(data *BenchmarkData, maxCombinations int) []QuerySpec {
	var queries []QuerySpec

	// Flatten contracts
	allContracts := append(append(data.Contracts.High, data.Contracts.Medium...), data.Contracts.Low...)

	// Collect topics with position info
	var allTopics []TopicWithPosition
	addTopics := func(td TopicData, pos int) {
		for _, t := range td.High {
			allTopics = append(allTopics, TopicWithPosition{Position: pos, Value: t, Card: "high"})
		}
		for _, t := range td.Medium {
			allTopics = append(allTopics, TopicWithPosition{Position: pos, Value: t, Card: "med"})
		}
		for _, t := range td.Low {
			allTopics = append(allTopics, TopicWithPosition{Position: pos, Value: t, Card: "low"})
		}
	}
	addTopics(data.Topic0, 0)
	addTopics(data.Topic1, 1)
	addTopics(data.Topic2, 2)
	addTopics(data.Topic3, 3)

	contractCardLabel := func(idx int) string {
		if idx < len(data.Contracts.High) {
			return "high"
		} else if idx < len(data.Contracts.High)+len(data.Contracts.Medium) {
			return "med"
		}
		return "low"
	}

	// Helper to create positional topic spec: Topics[position] = []values
	makeTopics := func(topics ...TopicWithPosition) [][]string {
		result := make([][]string, 4)
		for _, t := range topics {
			result[t.Position] = append(result[t.Position], t.Value)
		}
		return result
	}

	// 1. Contract-only queries (single contract)
	for i, c := range allContracts {
		card := contractCardLabel(i)
		queries = append(queries, QuerySpec{
			Name:        fmt.Sprintf("contract-%s", card),
			ContractIDs: []string{c},
		})
	}

	// 2. Single topic queries (with position info)
	for _, t := range allTopics {
		queries = append(queries, QuerySpec{
			Name:   fmt.Sprintf("t%d-%s", t.Position, t.Card),
			Topics: makeTopics(t),
		})
	}

	// 3. Contract + single topic combinations
	for ci, c := range allContracts {
		cCard := contractCardLabel(ci)
		for _, t := range allTopics {
			queries = append(queries, QuerySpec{
				Name:        fmt.Sprintf("c-%s+t%d-%s", cCard, t.Position, t.Card),
				ContractIDs: []string{c},
				Topics:      makeTopics(t),
			})
		}
	}

	// 4. Two topic combinations (different positions)
	for i := 0; i < len(allTopics); i++ {
		for j := i + 1; j < len(allTopics); j++ {
			t1, t2 := allTopics[i], allTopics[j]
			if t1.Position == t2.Position {
				continue
			}
			queries = append(queries, QuerySpec{
				Name:   fmt.Sprintf("t%d-%s+t%d-%s", t1.Position, t1.Card, t2.Position, t2.Card),
				Topics: makeTopics(t1, t2),
			})
		}
	}

	// 5. Contract + two topics (different positions)
	for ci, c := range allContracts {
		cCard := contractCardLabel(ci)
		for i := 0; i < len(allTopics); i++ {
			for j := i + 1; j < len(allTopics); j++ {
				t1, t2 := allTopics[i], allTopics[j]
				if t1.Position == t2.Position {
					continue
				}
				queries = append(queries, QuerySpec{
					Name:        fmt.Sprintf("c-%s+t%d+t%d", cCard, t1.Position, t2.Position),
					ContractIDs: []string{c},
					Topics:      makeTopics(t1, t2),
				})
			}
		}
	}

	// 6. Three topic combinations (different positions)
	for i := 0; i < len(allTopics); i++ {
		for j := i + 1; j < len(allTopics); j++ {
			for k := j + 1; k < len(allTopics); k++ {
				t1, t2, t3 := allTopics[i], allTopics[j], allTopics[k]
				if t1.Position == t2.Position || t1.Position == t3.Position || t2.Position == t3.Position {
					continue
				}
				queries = append(queries, QuerySpec{
					Name:   fmt.Sprintf("t%d+t%d+t%d", t1.Position, t2.Position, t3.Position),
					Topics: makeTopics(t1, t2, t3),
				})
			}
		}
	}

	// 7. Four topic combinations (all different positions)
	for i := 0; i < len(allTopics); i++ {
		for j := i + 1; j < len(allTopics); j++ {
			for k := j + 1; k < len(allTopics); k++ {
				for l := k + 1; l < len(allTopics); l++ {
					t1, t2, t3, t4 := allTopics[i], allTopics[j], allTopics[k], allTopics[l]
					positions := map[int]bool{t1.Position: true, t2.Position: true, t3.Position: true, t4.Position: true}
					if len(positions) != 4 {
						continue
					}
					queries = append(queries, QuerySpec{
						Name:   "t0+t1+t2+t3",
						Topics: makeTopics(t1, t2, t3, t4),
					})
				}
			}
		}
	}

	// 8. Contract + three topics
	for _, c := range allContracts {
		for i := 0; i < len(allTopics); i++ {
			for j := i + 1; j < len(allTopics); j++ {
				for k := j + 1; k < len(allTopics); k++ {
					t1, t2, t3 := allTopics[i], allTopics[j], allTopics[k]
					if t1.Position == t2.Position || t1.Position == t3.Position || t2.Position == t3.Position {
						continue
					}
					queries = append(queries, QuerySpec{
						Name:        fmt.Sprintf("c+t%d+t%d+t%d", t1.Position, t2.Position, t3.Position),
						ContractIDs: []string{c},
						Topics:      makeTopics(t1, t2, t3),
					})
				}
			}
		}
	}

	// 9. Multi-contract queries (OR of 2-3 contracts, no topics)
	if len(allContracts) >= 2 {
		for i := 0; i < len(allContracts); i++ {
			for j := i + 1; j < len(allContracts); j++ {
				queries = append(queries, QuerySpec{
					Name:        fmt.Sprintf("multi-c(%s+%s)", contractCardLabel(i), contractCardLabel(j)),
					ContractIDs: []string{allContracts[i], allContracts[j]},
				})
			}
		}
		if len(allContracts) >= 3 {
			queries = append(queries, QuerySpec{
				Name:        "multi-c(all3)",
				ContractIDs: allContracts[:3],
			})
		}
	}

	// 10. Multi-topic-per-position queries (2-3 values OR'd at one position)
	topicsByPos := make(map[int][]TopicWithPosition)
	for _, t := range allTopics {
		topicsByPos[t.Position] = append(topicsByPos[t.Position], t)
	}
	for pos, posTopics := range topicsByPos {
		if len(posTopics) >= 2 {
			for i := 0; i < len(posTopics); i++ {
				for j := i + 1; j < len(posTopics); j++ {
					topicsSpec := make([][]string, 4)
					topicsSpec[pos] = []string{posTopics[i].Value, posTopics[j].Value}
					queries = append(queries, QuerySpec{
						Name:   fmt.Sprintf("multi-t%d(%s+%s)", pos, posTopics[i].Card, posTopics[j].Card),
						Topics: topicsSpec,
					})
				}
			}
			if len(posTopics) >= 3 {
				topicsSpec := make([][]string, 4)
				topicsSpec[pos] = []string{posTopics[0].Value, posTopics[1].Value, posTopics[2].Value}
				queries = append(queries, QuerySpec{
					Name:   fmt.Sprintf("multi-t%d(all3)", pos),
					Topics: topicsSpec,
				})
			}
		}
	}

	// 11. Mixed: multi-contract + single topic
	if len(allContracts) >= 2 {
		for _, t := range allTopics {
			queries = append(queries, QuerySpec{
				Name:        fmt.Sprintf("multi-c+t%d-%s", t.Position, t.Card),
				ContractIDs: allContracts[:2],
				Topics:      makeTopics(t),
			})
		}
	}

	// 12. Single contract + multi-topic at one position
	for ci, c := range allContracts {
		cCard := contractCardLabel(ci)
		for pos, posTopics := range topicsByPos {
			if len(posTopics) >= 2 {
				topicsSpec := make([][]string, 4)
				topicsSpec[pos] = []string{posTopics[0].Value, posTopics[1].Value}
				queries = append(queries, QuerySpec{
					Name:        fmt.Sprintf("c-%s+multi-t%d", cCard, pos),
					ContractIDs: []string{c},
					Topics:      topicsSpec,
				})
			}
		}
	}

	// 13. Worst-case: 3 contracts + up to 3 values at each topic position
	// Uses whatever positions have data (doesn't require all 4 positions to have 3+ topics)
	if len(allContracts) >= 3 {
		topicsSpec := make([][]string, 4)
		var nameParts []string
		hasAnyTopics := false
		for pos := 0; pos < 4; pos++ {
			n := len(topicsByPos[pos])
			if n == 0 {
				continue
			}
			if n > 3 {
				n = 3
			}
			vals := make([]string, n)
			for i := 0; i < n; i++ {
				vals[i] = topicsByPos[pos][i].Value
			}
			topicsSpec[pos] = vals
			nameParts = append(nameParts, fmt.Sprintf("%dt%d", n, pos))
			hasAnyTopics = true
		}
		if hasAnyTopics {
			queries = append(queries, QuerySpec{
				Name:        fmt.Sprintf("worst-3c+%s", strings.Join(nameParts, "+")),
				ContractIDs: allContracts[:3],
				Topics:      topicsSpec,
			})
		}
	}

	// Classify queries into three tiers:
	// 1. singleTerm: one contract OR one topic (types 1-2) — always included
	// 2. multiValueOR: multiple values in any field, i.e. OR within a group (types 9-12) — always included
	// 3. singleValueAND: single value per field, multiple fields AND'd (types 3-8) — fill remaining
	var singleTerm []QuerySpec
	var multiValueOR []QuerySpec
	var singleValueAND []QuerySpec
	for _, q := range queries {
		totalTopicValues := 0
		maxTopicPerPos := 0
		for _, tv := range q.Topics {
			totalTopicValues += len(tv)
			if len(tv) > maxTopicPerPos {
				maxTopicPerPos = len(tv)
			}
		}
		isSingleTerm := (len(q.ContractIDs) <= 1 && totalTopicValues == 0) || (len(q.ContractIDs) == 0 && totalTopicValues == 1)
		isMultiValue := len(q.ContractIDs) > 1 || maxTopicPerPos > 1

		if isSingleTerm {
			singleTerm = append(singleTerm, q)
		} else if isMultiValue {
			multiValueOR = append(multiValueOR, q)
		} else {
			singleValueAND = append(singleValueAND, q)
		}
	}

	// Extract worst-case queries from multiValueOR — always include them
	var worstCase []QuerySpec
	var remainingMV []QuerySpec
	for _, q := range multiValueOR {
		if strings.HasPrefix(q.Name, "worst-") {
			worstCase = append(worstCase, q)
		} else {
			remainingMV = append(remainingMV, q)
		}
	}
	multiValueOR = remainingMV

	// Always include all single-term + worst-case queries
	result := append(singleTerm, worstCase...)

	// Shuffle + cap remaining multi-value OR queries at 25% of max
	mvLimit := maxCombinations / 4
	if mvLimit < 20 {
		mvLimit = 20
	}
	rand.Shuffle(len(multiValueOR), func(i, j int) {
		multiValueOR[i], multiValueOR[j] = multiValueOR[j], multiValueOR[i]
	})
	if len(multiValueOR) > mvLimit {
		multiValueOR = multiValueOR[:mvLimit]
	}
	result = append(result, multiValueOR...)

	// Fill remaining with single-value AND queries
	remaining := maxCombinations - len(result)
	if remaining > 0 && len(singleValueAND) > 0 {
		rand.Shuffle(len(singleValueAND), func(i, j int) {
			singleValueAND[i], singleValueAND[j] = singleValueAND[j], singleValueAND[i]
		})
		if len(singleValueAND) > remaining {
			singleValueAND = singleValueAND[:remaining]
		}
		result = append(result, singleValueAND...)
	}

	return result
}

// =============================================================================
// Benchmark Execution
// =============================================================================

// purgePageCache drops the OS page cache. Requires privileges.
// Linux: echo 3 > /proc/sys/vm/drop_caches
// macOS: purge command
func purgePageCache() error {
	if runtime.GOOS == "linux" {
		cmd := exec.Command("sh", "-c", "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'")
		return cmd.Run()
	} else if runtime.GOOS == "darwin" {
		cmd := exec.Command("sh", "-c", "sync && sudo purge")
		return cmd.Run()
	}
	return fmt.Errorf("unsupported OS for cache purge: %s", runtime.GOOS)
}

func runQueryBenchmark(eventStore *store.EventStore, data *BenchmarkData, spec QuerySpec, indexType, displayName string, iterations, warmup, limit int, timeout time.Duration, coldCache bool) BenchmarkResult {
	result := BenchmarkResult{
		Query:       spec,
		IndexType:   displayName,
		StartLedger: data.StartLedger,
		EndLedger:   data.EndLedger,
		Iterations:  iterations,
	}

	// Track all timing data together per iteration so p99 values are correlated
	type iterationTiming struct {
		total          time.Duration
		index          time.Duration
		indexRead      time.Duration
		indexDecode    time.Duration
		indexIntersect time.Duration
		event          time.Duration
		eventFetch     time.Duration
		eventDecode    time.Duration
		eventFilter    time.Duration
		eventDecompress time.Duration
		eventDiskRead   time.Duration
	}
	var timings []iterationTiming

	// Decode contract IDs
	var contractBytes [][]byte
	for _, cid := range spec.ContractIDs {
		decoded, err := decodeContractID(cid)
		if err != nil {
			result.Error = fmt.Sprintf("invalid contract ID: %v", err)
			return result
		}
		contractBytes = append(contractBytes, decoded)
	}

	// Decode topics: Topics[position] = []values -> topicGroups[position] = [][]byte
	var topicGroups [4][][]byte
	for pos, values := range spec.Topics {
		if pos >= 4 {
			break
		}
		for _, t := range values {
			decoded, err := decodeBase64(t)
			if err != nil {
				result.Error = fmt.Sprintf("invalid topic: %v", err)
				return result
			}
			topicGroups[pos] = append(topicGroups[pos], decoded)
		}
	}

	// Warmup runs (with timeout)
	for i := 0; i < warmup; i++ {
		if coldCache {
			if err := purgePageCache(); err != nil {
				fmt.Fprintf(os.Stderr, "\nWarning: failed to purge page cache: %v\n", err)
			}
		}
		resultChan := make(chan *QueryResult, 1)
		go func() {
			resultChan <- executeQueryBenchmark(eventStore, data.StartLedger, data.EndLedger, contractBytes, topicGroups, indexType, limit)
		}()
		select {
		case <-resultChan:
			// Warmup complete
		case <-time.After(timeout):
			// Warmup timed out - wait for goroutine to finish to avoid race
			<-resultChan
		}
	}

	// Benchmark runs
	for i := 0; i < iterations; i++ {
		if coldCache {
			if err := purgePageCache(); err != nil {
				fmt.Fprintf(os.Stderr, "\nWarning: failed to purge page cache: %v\n", err)
			}
		}
		start := time.Now()

		resultChan := make(chan *QueryResult, 1)
		go func() {
			resultChan <- executeQueryBenchmark(eventStore, data.StartLedger, data.EndLedger, contractBytes, topicGroups, indexType, limit)
		}()

		var qr *QueryResult
		var timedOut bool
		select {
		case qr = <-resultChan:
			// Query completed in time
		case <-time.After(timeout):
			timedOut = true
			// Must wait for goroutine to finish to avoid RocksDB race conditions
			qr = <-resultChan
		}

		elapsed := time.Since(start)

		makeIterTiming := func(qr *QueryResult) iterationTiming {
			return iterationTiming{
				total: elapsed, index: qr.IndexTime, indexRead: qr.IndexReadTime, indexDecode: qr.IndexDecodeTime, indexIntersect: qr.IndexIntersectTime,
				event: qr.EventTime, eventFetch: qr.EventFetchTime, eventDecode: qr.EventDecodeTime, eventFilter: qr.EventFilterTime,
				eventDecompress: qr.EventDecompressTime, eventDiskRead: qr.EventDiskReadTime,
			}
		}

		if timedOut {
			result.Error = "timeout"
			// Still record results from this slow query
			if qr != nil && qr.Error == nil {
				populateQueryStats(&result, qr)
				timings = append(timings, makeIterTiming(qr))
			} else {
				timings = append(timings, iterationTiming{total: elapsed})
			}
			// Stop further iterations - this query is too slow
			break
		} else if qr != nil {
			if qr.Error != nil {
				result.Error = qr.Error.Error()
				timings = append(timings, iterationTiming{total: elapsed})
			} else {
				populateQueryStats(&result, qr)
				timings = append(timings, makeIterTiming(qr))
			}
		}
	}

	// Calculate statistics - sort by total time so p50/p99 index/event come from same iteration
	if len(timings) > 0 {
		sort.Slice(timings, func(i, j int) bool { return timings[i].total < timings[j].total })

		result.P50Time = timings[len(timings)/2].total

		// P99 values all come from the same iteration
		p99Idx := int(float64(len(timings)) * 0.99)
		result.P99Time = timings[p99Idx].total
		result.P99IndexTime = timings[p99Idx].index
		result.P99IndexReadTime = timings[p99Idx].indexRead
		result.P99IndexDecodeTime = timings[p99Idx].indexDecode
		result.P99IndexIntersectTime = timings[p99Idx].indexIntersect
		result.P99EventTime = timings[p99Idx].event
		result.P99EventFetchTime = timings[p99Idx].eventFetch
		result.P99EventDecodeTime = timings[p99Idx].eventDecode
		result.P99EventFilterTime = timings[p99Idx].eventFilter
		result.P99EventDecompressTime = timings[p99Idx].eventDecompress
		result.P99EventDiskReadTime = timings[p99Idx].eventDiskRead
	}

	return result
}

// populateQueryStats copies stats from a QueryResult into a BenchmarkResult.
func populateQueryStats(result *BenchmarkResult, qr *QueryResult) {
	result.EventsReturned = qr.EventsReturned
	result.EventsScanned = qr.EventsScanned
	result.SegmentsTouched = qr.SegmentsTouched
	result.IndexMatches = qr.IndexMatches
	result.IndexBytes = qr.IndexBytes
	result.EventBytes = qr.EventBytes
	result.GroupsDecompressed = qr.GroupsDecompressed
	result.SmallestListSize = qr.SmallestListSize
	result.LargestListSize = qr.LargestListSize
}

// QueryResult holds basic query stats for benchmarking
type QueryResult struct {
	// Event stats
	EventsReturned int
	EventsScanned  int

	// Index stats
	SegmentsTouched   int   // Number of segments touched by the query
	IndexMatches     int   // TOIDs or ledgers matched by index
	IndexBytes       int64 // Bytes read from index
	SmallestListSize int   // Size of smallest posting list (posting list only)
	LargestListSize  int   // Size of largest posting list (posting list only)

	// Event I/O
	EventBytes int64 // Bytes read from event storage

	// Index timing
	IndexTime          time.Duration // Time spent on index operations (total)
	IndexReadTime      time.Duration // Time spent reading index from storage (I/O)
	IndexDecodeTime    time.Duration // Time spent decoding index (CPU)
	IndexIntersectTime time.Duration // Time spent intersecting index results

	// Event timing
	EventTime            time.Duration // Time spent on event operations (total)
	EventFetchTime       time.Duration // Time spent fetching events from storage (I/O)
	EventDecodeTime      time.Duration // Time spent decoding events (CPU)
	EventFilterTime      time.Duration // Time spent filtering events (CPU)
	EventDecompressTime  time.Duration // Time spent decompressing event blobs (zstd/dict)
	EventDiskReadTime    time.Duration // Time spent on disk I/O for event data
	GroupsDecompressed   int           // Number of group blocks decompressed

	Error error
}

func executeQueryBenchmark(eventStore *store.EventStore, startLedger, endLedger uint32, contractIDs [][]byte, topicGroups [4][][]byte, indexType string, limit int) *QueryResult {
	// Determine if this is a multi-value query (OR within any group)
	isMultiValue := len(contractIDs) > 1
	if !isMultiValue {
		for _, tg := range topicGroups {
			if len(tg) > 1 {
				isMultiValue = true
				break
			}
		}
	}

	// For single-value queries, use the original (optimized) functions
	if !isMultiValue {
		var singleContract []byte
		if len(contractIDs) == 1 {
			singleContract = contractIDs[0]
		}

		switch indexType {
		case "bitmap32":
			return executeBitmap32QueryBenchmark(eventStore, startLedger, endLedger, singleContract, topicGroups, limit)
		case "segment-index":
			return executeSegmentIndexQueryBenchmark(eventStore, startLedger, endLedger, singleContract, topicGroups, limit)
		case "segment-data":
			return executeSegmentDataQueryBenchmark(eventStore, startLedger, endLedger, singleContract, topicGroups, limit)
		}
		return nil
	}

	// Multi-value: use new multi-filter functions
	switch indexType {
	case "bitmap32":
		return executeBitmap32MultiFilterBenchmark(eventStore, startLedger, endLedger, contractIDs, topicGroups, limit)
	case "segment-index":
		return executeSegmentIndexMultiFilterBenchmark(eventStore, startLedger, endLedger, contractIDs, topicGroups, limit)
	case "segment-data":
		return executeSegmentDataMultiFilterBenchmark(eventStore, startLedger, endLedger, contractIDs, topicGroups, limit)
	}
	return nil
}

func executeBitmap32QueryBenchmark(eventStore *store.EventStore, startLedger, endLedger uint32, contractID []byte, topicGroups [4][][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithBitmap32EventIndex(contractID, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned:     len(events),
		EventsScanned:      stats.EventsScanned,
		SegmentsTouched:     stats.SegmentsTouched,
		IndexBytes:         stats.IndexBytesRead,
		EventBytes:         stats.EventBytesRead,
		IndexMatches:       stats.MatchingLocalIDs,
		IndexTime:          stats.IndexLookupTime,
		IndexReadTime:      stats.IndexReadTime,
		IndexDecodeTime:    stats.IndexDecodeTime,
		IndexIntersectTime: stats.IndexIntersectTime,
		EventTime:          stats.EventFetchTime,
		EventFetchTime:     stats.EventFetchTime,
		EventDecodeTime:    stats.DecodeTime,
		EventFilterTime:    stats.FilterTime,
	}
}

func executeBitmap32MultiFilterBenchmark(eventStore *store.EventStore, startLedger, endLedger uint32, contractIDs [][]byte, topicGroups [4][][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithBitmap32MultiFilter(contractIDs, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned:     len(events),
		EventsScanned:      stats.EventsScanned,
		SegmentsTouched:     stats.SegmentsTouched,
		IndexBytes:         stats.IndexBytesRead,
		EventBytes:         stats.EventBytesRead,
		IndexMatches:       stats.MatchingLocalIDs,
		IndexTime:          stats.IndexLookupTime,
		IndexReadTime:      stats.IndexReadTime,
		IndexDecodeTime:    stats.IndexDecodeTime,
		IndexIntersectTime: stats.IndexIntersectTime,
		EventTime:          stats.EventFetchTime,
		EventFetchTime:     stats.EventFetchTime,
		EventDecodeTime:    stats.DecodeTime,
		EventFilterTime:    stats.FilterTime,
	}
}

func executeSegmentIndexQueryBenchmark(eventStore *store.EventStore, startLedger, endLedger uint32, contractID []byte, topicGroups [4][][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithSegmentIndex(contractID, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned:      len(events),
		EventsScanned:       stats.EventsScanned,
		SegmentsTouched:      stats.SegmentsTouched,
		IndexBytes:          stats.IndexBytesRead,
		EventBytes:          stats.EventBytesRead,
		IndexMatches:        stats.MatchingLocalIDs,
		IndexTime:           stats.IndexLookupTime,
		IndexReadTime:       stats.IndexReadTime,
		IndexDecodeTime:     stats.IndexDecodeTime,
		IndexIntersectTime:  stats.IndexIntersectTime,

		EventTime:           stats.EventFetchTime,
		EventFetchTime:      stats.EventFetchTime,
		EventDecodeTime:     stats.DecodeTime,
		EventFilterTime:     stats.FilterTime,
		EventDecompressTime: stats.DecompressTime,
		EventDiskReadTime:   stats.EventDiskReadTime,
		GroupsDecompressed:  stats.GroupsDecompressed,
	}
}

func executeSegmentIndexMultiFilterBenchmark(eventStore *store.EventStore, startLedger, endLedger uint32, contractIDs [][]byte, topicGroups [4][][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithSegmentIndexMultiFilter(contractIDs, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned:      len(events),
		EventsScanned:       stats.EventsScanned,
		SegmentsTouched:      stats.SegmentsTouched,
		IndexBytes:          stats.IndexBytesRead,
		EventBytes:          stats.EventBytesRead,
		IndexMatches:        stats.MatchingLocalIDs,
		IndexTime:           stats.IndexLookupTime,
		IndexReadTime:       stats.IndexReadTime,
		IndexDecodeTime:     stats.IndexDecodeTime,
		IndexIntersectTime:  stats.IndexIntersectTime,

		EventTime:           stats.EventFetchTime,
		EventFetchTime:      stats.EventFetchTime,
		EventDecodeTime:     stats.DecodeTime,
		EventFilterTime:     stats.FilterTime,
		EventDecompressTime: stats.DecompressTime,
		EventDiskReadTime:   stats.EventDiskReadTime,
		GroupsDecompressed:  stats.GroupsDecompressed,
	}
}

func executeSegmentDataQueryBenchmark(eventStore *store.EventStore, startLedger, endLedger uint32, contractID []byte, topicGroups [4][][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithSegmentData(contractID, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned:      len(events),
		EventsScanned:       stats.EventsScanned,
		SegmentsTouched:      stats.SegmentsTouched,
		IndexBytes:          stats.IndexBytesRead,
		EventBytes:          stats.EventBytesRead,
		IndexMatches:        stats.MatchingLocalIDs,
		IndexTime:           stats.IndexLookupTime,
		IndexReadTime:       stats.IndexReadTime,
		IndexDecodeTime:     stats.IndexDecodeTime,
		IndexIntersectTime:  stats.IndexIntersectTime,

		EventTime:           stats.EventFetchTime,
		EventFetchTime:      stats.EventFetchTime,
		EventDecodeTime:     stats.DecodeTime,
		EventFilterTime:     stats.FilterTime,
		EventDecompressTime: stats.DecompressTime,
		EventDiskReadTime:   stats.EventDiskReadTime,
		GroupsDecompressed:  stats.GroupsDecompressed,
	}
}

func executeSegmentDataMultiFilterBenchmark(eventStore *store.EventStore, startLedger, endLedger uint32, contractIDs [][]byte, topicGroups [4][][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithSegmentDataMultiFilter(contractIDs, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned:      len(events),
		EventsScanned:       stats.EventsScanned,
		SegmentsTouched:      stats.SegmentsTouched,
		IndexBytes:          stats.IndexBytesRead,
		EventBytes:          stats.EventBytesRead,
		IndexMatches:        stats.MatchingLocalIDs,
		IndexTime:           stats.IndexLookupTime,
		IndexReadTime:       stats.IndexReadTime,
		IndexDecodeTime:     stats.IndexDecodeTime,
		IndexIntersectTime:  stats.IndexIntersectTime,

		EventTime:           stats.EventFetchTime,
		EventFetchTime:      stats.EventFetchTime,
		EventDecodeTime:     stats.DecodeTime,
		EventFilterTime:     stats.FilterTime,
		EventDecompressTime: stats.DecompressTime,
		EventDiskReadTime:   stats.EventDiskReadTime,
		GroupsDecompressed:  stats.GroupsDecompressed,
	}
}

// =============================================================================
// Output Formatting
// =============================================================================

func outputTable(results []BenchmarkResult, indexes []string) {
	// Group results by query
	queryResults := make(map[string]map[string]BenchmarkResult)
	var queryNames []string

	for _, r := range results {
		key := r.Query.Name
		if len(r.Query.ContractIDs) > 0 {
			key += ":" + r.Query.ContractIDs[0][:8]
		}
		if _, ok := queryResults[key]; !ok {
			queryResults[key] = make(map[string]BenchmarkResult)
			queryNames = append(queryNames, key)
		}
		queryResults[key][r.IndexType] = r
	}

	// Print header
	fmt.Printf("\n%-40s", "Query")
	for _, idx := range indexes {
		fmt.Printf(" | %8s p99", idx)
	}
	fmt.Printf(" | %6s/%6s | %4s | %8s | %8s | %8s\n", "EvtRet", "Scaned", "Segs", "IdxMatch", "IdxBytes", "EvtBytes")

	fmt.Print(strings.Repeat("-", 40))
	for range indexes {
		fmt.Print("-+-" + strings.Repeat("-", 12))
	}
	fmt.Println("-+-" + strings.Repeat("-", 13) + "-+-" + strings.Repeat("-", 4) + "-+-" + strings.Repeat("-", 8) + "-+-" + strings.Repeat("-", 8) + "-+-" + strings.Repeat("-", 8))

	// Print results
	for _, name := range queryNames {
		qr := queryResults[name]
		fmt.Printf("%-40s", truncateBenchmarkStr(name, 40))

		var eventsReturned, eventsScanned, idxMatches, segmentsTouched int
		var idxBytes, evtBytes int64
		for _, idx := range indexes {
			if r, ok := qr[idx]; ok {
				if r.Error != "" {
					fmt.Printf(" | %12s", "ERROR")
				} else {
					fmt.Printf(" | %12s", formatBenchmarkDuration(r.P99Time))
				}
				eventsReturned = r.EventsReturned
				eventsScanned = r.EventsScanned
				idxMatches = r.IndexMatches
				segmentsTouched = r.SegmentsTouched
				idxBytes = r.IndexBytes
				evtBytes = r.EventBytes
			} else {
				fmt.Printf(" | %12s", "N/A")
			}
		}
		fmt.Printf(" | %6d/%6d | %4d | %8d | %8s | %8s\n", eventsReturned, eventsScanned, segmentsTouched, idxMatches, formatBytes(idxBytes), formatBytes(evtBytes))
	}

	// Summary statistics
	printSummaryStats(results, indexes)
}

func printSummaryStats(results []BenchmarkResult, indexes []string) {
	fmt.Println()
	fmt.Println("=== Summary Statistics ===")
	for _, idx := range indexes {
		var p50s, p99s []time.Duration
		for _, r := range results {
			if r.IndexType == idx && r.Error == "" {
				p50s = append(p50s, r.P50Time)
				p99s = append(p99s, r.P99Time)
			}
		}
		if len(p50s) > 0 {
			sort.Slice(p50s, func(i, j int) bool { return p50s[i] < p50s[j] })
			sort.Slice(p99s, func(i, j int) bool { return p99s[i] < p99s[j] })
			medP50 := p50s[len(p50s)/2]
			medP99 := p99s[len(p99s)/2]
			fmt.Printf("  %s: median_p50=%s, median_p99=%s (n=%d)\n", idx, formatBenchmarkDuration(medP50), formatBenchmarkDuration(medP99), len(p50s))
		}
	}
}

func outputCSV(results []BenchmarkResult) {
	// Header: query | timing | index | event | test
	fmt.Println("query_name,contract_id,topic0,topic1,topic2,topic3,index_type,p50_total_ms,p99_total_ms,p99_idx_ms,idx_read_ms,idx_decode_ms,idx_intersect_ms,segments_touched,index_matches,index_bytes,smallest_list,largest_list,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,evt_decompress_ms,evt_disk_read_ms,events_returned,events_scanned,event_bytes,groups_decompressed,iterations,error")

	for _, r := range results {
		contractID := strings.Join(r.Query.ContractIDs, "|")
		if contractID == "" {
			contractID = "-"
		}

		topics := make([]string, 4)
		for i := 0; i < 4; i++ {
			if i < len(r.Query.Topics) && len(r.Query.Topics[i]) > 0 {
				topics[i] = strings.Join(r.Query.Topics[i], "|")
			} else {
				topics[i] = "-"
			}
		}

		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%d,%d,%s\n",
			r.Query.Name,
			contractID,
			topics[0], topics[1], topics[2], topics[3],
			r.IndexType,
			float64(r.P50Time.Microseconds())/1000.0,
			float64(r.P99Time.Microseconds())/1000.0,
			float64(r.P99IndexTime.Microseconds())/1000.0,
			float64(r.P99IndexReadTime.Microseconds())/1000.0,
			float64(r.P99IndexDecodeTime.Microseconds())/1000.0,
			float64(r.P99IndexIntersectTime.Microseconds())/1000.0,
			r.SegmentsTouched,
			r.IndexMatches,
			formatBytes(r.IndexBytes),
			r.SmallestListSize,
			r.LargestListSize,
			float64(r.P99EventTime.Microseconds())/1000.0,
			float64(r.P99EventFetchTime.Microseconds())/1000.0,
			float64(r.P99EventDecodeTime.Microseconds())/1000.0,
			float64(r.P99EventFilterTime.Microseconds())/1000.0,
			float64(r.P99EventDecompressTime.Microseconds())/1000.0,
			float64(r.P99EventDiskReadTime.Microseconds())/1000.0,
			r.EventsReturned,
			r.EventsScanned,
			formatBytes(r.EventBytes),
			r.GroupsDecompressed,
			r.Iterations,
			r.Error,
		)
	}
}

func outputJSON(results []BenchmarkResult) {
	type jsonResult struct {
		// Query identification
		QueryName   string     `json:"query_name"`
		ContractIDs []string   `json:"contract_ids,omitempty"`
		Topics      [][]string `json:"topics,omitempty"`
		IndexType   string     `json:"index_type"`

		// Timing
		P50Ms float64 `json:"p50_total_ms"`
		P99Ms float64 `json:"p99_total_ms"`

		// Index stats
		P99IndexMs       float64 `json:"p99_idx_ms"`
		IdxReadMs        float64 `json:"idx_read_ms"`
		IdxDecodeMs      float64 `json:"idx_decode_ms"`
		IdxIntersectMs   float64 `json:"idx_intersect_ms"`
		SegmentsTouched   int     `json:"segments_touched"`
		IndexMatches     int     `json:"index_matches"`
		IndexBytes       string  `json:"index_bytes"`
		SmallestListSize int     `json:"smallest_list_size,omitempty"`
		LargestListSize  int     `json:"largest_list_size,omitempty"`

		// Event stats
		P99EventMs        float64 `json:"p99_evt_ms"`
		EvtFetchMs        float64 `json:"evt_fetch_ms"`
		EvtDecodeMs       float64 `json:"evt_decode_ms"`
		EvtFilterMs       float64 `json:"evt_filter_ms"`
		EvtDecompressMs    float64 `json:"evt_decompress_ms"`
		EvtDiskReadMs      float64 `json:"evt_disk_read_ms"`
		EventsReturned     int     `json:"events_returned"`
		EventsScanned      int     `json:"events_scanned"`
		EventBytes         string  `json:"event_bytes"`
		GroupsDecompressed int     `json:"groups_decompressed"`

		// Test config
		Iterations int    `json:"iterations"`
		Error      string `json:"error,omitempty"`
	}

	var jsonResults []jsonResult
	for _, r := range results {
		jr := jsonResult{
			QueryName:        r.Query.Name,
			ContractIDs:      r.Query.ContractIDs,
			Topics:           r.Query.Topics,
			IndexType:        r.IndexType,
			P50Ms:            float64(r.P50Time.Microseconds()) / 1000.0,
			P99Ms:            float64(r.P99Time.Microseconds()) / 1000.0,
			P99IndexMs:       float64(r.P99IndexTime.Microseconds()) / 1000.0,
			IdxReadMs:        float64(r.P99IndexReadTime.Microseconds()) / 1000.0,
			IdxDecodeMs:      float64(r.P99IndexDecodeTime.Microseconds()) / 1000.0,
			IdxIntersectMs:   float64(r.P99IndexIntersectTime.Microseconds()) / 1000.0,
			SegmentsTouched:   r.SegmentsTouched,
			IndexMatches:     r.IndexMatches,
			IndexBytes:       formatBytes(r.IndexBytes),
			SmallestListSize: r.SmallestListSize,
			LargestListSize:  r.LargestListSize,
			P99EventMs:       float64(r.P99EventTime.Microseconds()) / 1000.0,
			EvtFetchMs:       float64(r.P99EventFetchTime.Microseconds()) / 1000.0,
			EvtDecodeMs:      float64(r.P99EventDecodeTime.Microseconds()) / 1000.0,
			EvtFilterMs:      float64(r.P99EventFilterTime.Microseconds()) / 1000.0,
			EvtDecompressMs:    float64(r.P99EventDecompressTime.Microseconds()) / 1000.0,
			EvtDiskReadMs:      float64(r.P99EventDiskReadTime.Microseconds()) / 1000.0,
			EventsReturned:     r.EventsReturned,
			EventsScanned:      r.EventsScanned,
			EventBytes:         formatBytes(r.EventBytes),
			GroupsDecompressed: r.GroupsDecompressed,
			Iterations:         r.Iterations,
			Error:              r.Error,
		}
		jsonResults = append(jsonResults, jr)
	}

	output, _ := json.MarshalIndent(jsonResults, "", "  ")
	fmt.Println(string(output))
}

func writeResultIncremental(w *os.File, r BenchmarkResult, format string) {
	switch format {
	case "json":
		// Write as JSON Lines (one JSON object per line)
		jr := struct {
			// Query identification
			QueryName   string     `json:"query_name"`
			ContractIDs []string   `json:"contract_ids,omitempty"`
			Topics      [][]string `json:"topics,omitempty"`
			IndexType   string     `json:"index_type"`
			StartLedger uint32     `json:"start_ledger"`
			EndLedger   uint32     `json:"end_ledger"`

			// Timing
			P50Ms float64 `json:"p50_total_ms"`
			P99Ms float64 `json:"p99_total_ms"`

			// Index stats
			P99IndexMs       float64 `json:"p99_idx_ms"`
			IdxReadMs        float64 `json:"idx_read_ms"`
			IdxDecodeMs      float64 `json:"idx_decode_ms"`
			IdxIntersectMs   float64 `json:"idx_intersect_ms"`
			SegmentsTouched   int     `json:"segments_touched"`
			IndexMatches     int     `json:"index_matches"`
			IndexBytes       string  `json:"index_bytes"`
			SmallestListSize int     `json:"smallest_list_size,omitempty"`
			LargestListSize  int     `json:"largest_list_size,omitempty"`

			// Event stats
			P99EventMs      float64 `json:"p99_evt_ms"`
			EvtFetchMs      float64 `json:"evt_fetch_ms"`
			EvtDecodeMs     float64 `json:"evt_decode_ms"`
			EvtFilterMs     float64 `json:"evt_filter_ms"`
			EvtDecompressMs    float64 `json:"evt_decompress_ms"`
			EvtDiskReadMs      float64 `json:"evt_disk_read_ms"`
			EventsReturned     int     `json:"events_returned"`
			EventsScanned      int     `json:"events_scanned"`
			EventBytes         string  `json:"event_bytes"`
			GroupsDecompressed int     `json:"groups_decompressed"`

			// Test config
			Iterations int    `json:"iterations"`
			Error      string `json:"error,omitempty"`
		}{
			QueryName:        r.Query.Name,
			ContractIDs:      r.Query.ContractIDs,
			Topics:           r.Query.Topics,
			IndexType:        r.IndexType,
			StartLedger:      r.StartLedger,
			EndLedger:        r.EndLedger,
			P50Ms:            float64(r.P50Time.Microseconds()) / 1000.0,
			P99Ms:            float64(r.P99Time.Microseconds()) / 1000.0,
			P99IndexMs:       float64(r.P99IndexTime.Microseconds()) / 1000.0,
			IdxReadMs:        float64(r.P99IndexReadTime.Microseconds()) / 1000.0,
			IdxDecodeMs:      float64(r.P99IndexDecodeTime.Microseconds()) / 1000.0,
			IdxIntersectMs:   float64(r.P99IndexIntersectTime.Microseconds()) / 1000.0,
			SegmentsTouched:   r.SegmentsTouched,
			IndexMatches:     r.IndexMatches,
			IndexBytes:       formatBytes(r.IndexBytes),
			SmallestListSize: r.SmallestListSize,
			LargestListSize:  r.LargestListSize,
			P99EventMs:       float64(r.P99EventTime.Microseconds()) / 1000.0,
			EvtFetchMs:       float64(r.P99EventFetchTime.Microseconds()) / 1000.0,
			EvtDecodeMs:      float64(r.P99EventDecodeTime.Microseconds()) / 1000.0,
			EvtFilterMs:      float64(r.P99EventFilterTime.Microseconds()) / 1000.0,
			EvtDecompressMs:    float64(r.P99EventDecompressTime.Microseconds()) / 1000.0,
			EvtDiskReadMs:      float64(r.P99EventDiskReadTime.Microseconds()) / 1000.0,
			EventsReturned:     r.EventsReturned,
			EventsScanned:      r.EventsScanned,
			EventBytes:         formatBytes(r.EventBytes),
			GroupsDecompressed: r.GroupsDecompressed,
			Iterations:         r.Iterations,
			Error:              r.Error,
		}
		line, _ := json.Marshal(jr)
		fmt.Fprintln(w, string(line))
	default:
		// CSV format
		contractID := strings.Join(r.Query.ContractIDs, "|")
		if contractID == "" {
			contractID = "-"
		}
		topics := make([]string, 4)
		for i := 0; i < 4; i++ {
			if i < len(r.Query.Topics) && len(r.Query.Topics[i]) > 0 {
				topics[i] = strings.Join(r.Query.Topics[i], "|")
			} else {
				topics[i] = "-"
			}
		}
		fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%d,%d,%s\n",
			r.Query.Name,
			contractID,
			topics[0], topics[1], topics[2], topics[3],
			r.IndexType,
			r.StartLedger,
			r.EndLedger,
			float64(r.P50Time.Microseconds())/1000.0,
			float64(r.P99Time.Microseconds())/1000.0,
			float64(r.P99IndexTime.Microseconds())/1000.0,
			float64(r.P99IndexReadTime.Microseconds())/1000.0,
			float64(r.P99IndexDecodeTime.Microseconds())/1000.0,
			float64(r.P99IndexIntersectTime.Microseconds())/1000.0,
			r.SegmentsTouched,
			r.IndexMatches,
			formatBytes(r.IndexBytes),
			r.SmallestListSize,
			r.LargestListSize,
			float64(r.P99EventTime.Microseconds())/1000.0,
			float64(r.P99EventFetchTime.Microseconds())/1000.0,
			float64(r.P99EventDecodeTime.Microseconds())/1000.0,
			float64(r.P99EventFilterTime.Microseconds())/1000.0,
			float64(r.P99EventDecompressTime.Microseconds())/1000.0,
			float64(r.P99EventDiskReadTime.Microseconds())/1000.0,
			r.EventsReturned,
			r.EventsScanned,
			formatBytes(r.EventBytes),
			r.GroupsDecompressed,
			r.Iterations,
			r.Error,
		)
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func formatBenchmarkDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.1fus", float64(d.Nanoseconds())/1000.0)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func truncateBenchmarkStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
