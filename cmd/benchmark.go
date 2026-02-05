package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
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
	Name       string   // Human-readable name
	ContractID string   // Contract ID (strkey format) or empty
	Topics     []string // Topic values (base64) or empty
}

// BenchmarkResult holds timing results for a query
type BenchmarkResult struct {
	Query          QuerySpec
	IndexType      string
	StartLedger    uint32 // Ledger range used for this query
	EndLedger      uint32
	Iterations     int
	TotalTime      time.Duration
	AvgTime        time.Duration
	MinTime        time.Duration
	MaxTime        time.Duration
	P50Time        time.Duration
	P99Time        time.Duration
	P99IndexTime   time.Duration // P99 time for index lookup
	P99EventTime   time.Duration // P99 time for event fetch
	EventsReturned int
	EventsScanned  int
	LedgersFound   int
	IndexMatches   int
	IndexBytes     int64
	EventBytes     int64
	Error          string

	// Multi-filter optimization stats (posting list only)
	SmallestListSize int // Size of smallest posting list
	LargestListSize  int // Size of largest posting list
	SkippedBuckets   int // Buckets skipped due to guided intersection
}

// =============================================================================
// Benchmark Command
// =============================================================================

func runBenchmark(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	dataFile := fs.String("data", "", "Benchmark data file (JSON)")
	indexTypes := fs.String("index", "all", "Index types to benchmark: posting,bitmap32,bitmap64,all")
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

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: benchmark [options]\n\n")
		fmt.Fprintf(os.Stderr, "Benchmarks query performance across different index types.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  benchmark --generate > benchmark_data.json  # Generate sample data file\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data benchmark_data.json        # Run benchmarks\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data data.json --index posting  # Only test posting list\n")
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
	var indexes []string
	if *indexTypes == "all" {
		indexes = []string{"posting", "posting-parallel", "bitmap32", "bitmap64", "bitmap64-parallel"}
	} else {
		indexes = strings.Split(*indexTypes, ",")
	}

	// Validate index types
	validIndexes := map[string]bool{"posting": true, "posting-parallel": true, "bitmap32": true, "bitmap64": true, "bitmap64-parallel": true}
	for _, idx := range indexes {
		if !validIndexes[idx] {
			fmt.Fprintf(os.Stderr, "Error: invalid index type: %s\n", idx)
			os.Exit(2)
		}
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
		fmt.Fprintln(logWriter, "timestamp,query_name,index_type,start_ledger,end_ledger,events_returned,events_scanned,index_matches,index_bytes,event_bytes,avg_ms,error")
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
			fmt.Fprintln(outputWriter, "query_name,contract_id,topic0,topic1,topic2,topic3,index_type,start_ledger,end_ledger,avg_ms,p99_ms,p99_idx_ms,p99_evt_ms,events_scanned,index_matches,index_bytes,event_bytes,events_returned,error")
			outputWriter.Sync()
		}
		fmt.Fprintf(os.Stderr, "Output file: %s (format: %s)\n", *outputFile, outputFileFormat)
	}

	// Generate query combinations
	queries := generateQueryCombinations(data, *maxCombinations)
	fmt.Fprintf(os.Stderr, "Generated %d query combinations\n", len(queries))
	fmt.Fprintf(os.Stderr, "Benchmarking with %d iterations per query (+%d warmup)\n", *iterations, *warmup)
	fmt.Fprintf(os.Stderr, "Index types: %v\n", indexes)
	fmt.Fprintf(os.Stderr, "Ledger range: %d - %d (max per query: %d)\n\n", originalStartLedger, originalEndLedger, maxLedgerRange)

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
			fmt.Fprintf(os.Stderr, "\r[%d/%d] Running: %s (%s) [%d-%d]...    ", completed, totalQueries, q.Name, idxType, queryStart, queryEnd)

			// Create a temporary data struct with the random range
			queryData := &BenchmarkData{
				StartLedger: queryStart,
				EndLedger:   queryEnd,
			}
			result := runQueryBenchmark(eventStore, queryData, q, idxType, *iterations, *warmup, *limit, *timeout)
			results = append(results, result)

			// Log query result
			if logWriter != nil {
				errStr := result.Error
				fmt.Fprintf(logWriter, "%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%.3f,%s\n",
					time.Now().Format(time.RFC3339),
					result.Query.Name,
					result.IndexType,
					result.StartLedger,
					result.EndLedger,
					result.EventsReturned,
					result.EventsScanned,
					result.IndexMatches,
					result.IndexBytes,
					result.EventBytes,
					float64(result.AvgTime.Microseconds())/1000.0,
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
			outputTable(results, indexes)
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
			High:   []string{},
			Medium: []string{},
			Low:    []string{},
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

	// 1. Contract-only queries
	for i, c := range allContracts {
		card := contractCardLabel(i)
		queries = append(queries, QuerySpec{
			Name:       fmt.Sprintf("contract-%s", card),
			ContractID: c,
		})
	}

	// 2. Single topic queries (with position info)
	for _, t := range allTopics {
		queries = append(queries, QuerySpec{
			Name:   fmt.Sprintf("t%d-%s", t.Position, t.Card),
			Topics: []string{t.Value},
		})
	}

	// 3. Contract + single topic combinations
	for ci, c := range allContracts {
		cCard := contractCardLabel(ci)
		for _, t := range allTopics {
			queries = append(queries, QuerySpec{
				Name:       fmt.Sprintf("c-%s+t%d-%s", cCard, t.Position, t.Card),
				ContractID: c,
				Topics:     []string{t.Value},
			})
		}
	}

	// 4. Two topic combinations (different positions)
	for i := 0; i < len(allTopics); i++ {
		for j := i + 1; j < len(allTopics); j++ {
			t1, t2 := allTopics[i], allTopics[j]
			// Skip if same position (can't have two topic0s)
			if t1.Position == t2.Position {
				continue
			}
			queries = append(queries, QuerySpec{
				Name:   fmt.Sprintf("t%d-%s+t%d-%s", t1.Position, t1.Card, t2.Position, t2.Card),
				Topics: []string{t1.Value, t2.Value},
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
					Name:       fmt.Sprintf("c-%s+t%d+t%d", cCard, t1.Position, t2.Position),
					ContractID: c,
					Topics:     []string{t1.Value, t2.Value},
				})
			}
		}
	}

	// 6. Three topic combinations (different positions)
	for i := 0; i < len(allTopics); i++ {
		for j := i + 1; j < len(allTopics); j++ {
			for k := j + 1; k < len(allTopics); k++ {
				t1, t2, t3 := allTopics[i], allTopics[j], allTopics[k]
				// Skip if any positions are the same
				if t1.Position == t2.Position || t1.Position == t3.Position || t2.Position == t3.Position {
					continue
				}
				queries = append(queries, QuerySpec{
					Name:   fmt.Sprintf("t%d+t%d+t%d", t1.Position, t2.Position, t3.Position),
					Topics: []string{t1.Value, t2.Value, t3.Value},
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
					// Must have all different positions (0,1,2,3)
					positions := map[int]bool{t1.Position: true, t2.Position: true, t3.Position: true, t4.Position: true}
					if len(positions) != 4 {
						continue
					}
					queries = append(queries, QuerySpec{
						Name:   "t0+t1+t2+t3",
						Topics: []string{t1.Value, t2.Value, t3.Value, t4.Value},
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
						Name:       fmt.Sprintf("c+t%d+t%d+t%d", t1.Position, t2.Position, t3.Position),
						ContractID: c,
						Topics:     []string{t1.Value, t2.Value, t3.Value},
					})
				}
			}
		}
	}

	// Separate single-filter queries (always keep these) from multi-filter queries
	var singleFilter []QuerySpec
	var multiFilter []QuerySpec
	for _, q := range queries {
		isSingle := (q.ContractID != "" && len(q.Topics) == 0) || (q.ContractID == "" && len(q.Topics) == 1)
		if isSingle {
			singleFilter = append(singleFilter, q)
		} else {
			multiFilter = append(multiFilter, q)
		}
	}

	// Always include all single-filter queries, then add multi-filter up to max
	result := singleFilter
	remaining := maxCombinations - len(singleFilter)
	if remaining > 0 && len(multiFilter) > 0 {
		// Shuffle multi-filter queries and take up to remaining
		rand.Shuffle(len(multiFilter), func(i, j int) {
			multiFilter[i], multiFilter[j] = multiFilter[j], multiFilter[i]
		})
		if len(multiFilter) > remaining {
			multiFilter = multiFilter[:remaining]
		}
		result = append(result, multiFilter...)
	}

	return result
}

// =============================================================================
// Benchmark Execution
// =============================================================================

func runQueryBenchmark(eventStore *store.RocksDBEventStore, data *BenchmarkData, spec QuerySpec, indexType string, iterations, warmup, limit int, timeout time.Duration) BenchmarkResult {
	result := BenchmarkResult{
		Query:       spec,
		IndexType:   indexType,
		StartLedger: data.StartLedger,
		EndLedger:   data.EndLedger,
		Iterations:  iterations,
	}

	// Track all timing data together per iteration so p99 values are correlated
	type iterationTiming struct {
		total time.Duration
		index time.Duration
		event time.Duration
	}
	var timings []iterationTiming

	// Decode contract ID if present
	var contractBytes []byte
	if spec.ContractID != "" {
		var err error
		contractBytes, err = decodeContractID(spec.ContractID)
		if err != nil {
			result.Error = fmt.Sprintf("invalid contract ID: %v", err)
			return result
		}
	}

	// Decode topics
	var topicBytes [][]byte
	for _, t := range spec.Topics {
		decoded, err := decodeBase64(t)
		if err != nil {
			result.Error = fmt.Sprintf("invalid topic: %v", err)
			return result
		}
		topicBytes = append(topicBytes, decoded)
	}

	// Warmup runs (with timeout)
	for i := 0; i < warmup; i++ {
		resultChan := make(chan *QueryResult, 1)
		go func() {
			resultChan <- executeQueryBenchmark(eventStore, data.StartLedger, data.EndLedger, contractBytes, topicBytes, indexType, limit)
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
		start := time.Now()

		resultChan := make(chan *QueryResult, 1)
		go func() {
			resultChan <- executeQueryBenchmark(eventStore, data.StartLedger, data.EndLedger, contractBytes, topicBytes, indexType, limit)
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

		if timedOut {
			result.Error = "timeout"
			// Still record results from this slow query
			if qr != nil && qr.Error == nil {
				result.EventsReturned = qr.EventsReturned
				result.EventsScanned = qr.EventsScanned
				result.LedgersFound = qr.LedgersFound
				result.IndexMatches = qr.IndexMatches
				result.IndexBytes = qr.IndexBytes
				result.EventBytes = qr.EventBytes
				result.SmallestListSize = qr.SmallestListSize
				result.LargestListSize = qr.LargestListSize
				result.SkippedBuckets = qr.SkippedBuckets
				timings = append(timings, iterationTiming{total: elapsed, index: qr.IndexTime, event: qr.EventFetchTime})
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
				result.EventsReturned = qr.EventsReturned
				result.EventsScanned = qr.EventsScanned
				result.LedgersFound = qr.LedgersFound
				result.IndexMatches = qr.IndexMatches
				result.IndexBytes = qr.IndexBytes
				result.EventBytes = qr.EventBytes
				result.SmallestListSize = qr.SmallestListSize
				result.LargestListSize = qr.LargestListSize
				result.SkippedBuckets = qr.SkippedBuckets
				timings = append(timings, iterationTiming{total: elapsed, index: qr.IndexTime, event: qr.EventFetchTime})
			}
		}
	}

	// Calculate statistics - sort by total time so p99 index/event come from same iteration
	if len(timings) > 0 {
		sort.Slice(timings, func(i, j int) bool { return timings[i].total < timings[j].total })

		var total time.Duration
		for _, t := range timings {
			total += t.total
		}

		result.TotalTime = total
		result.AvgTime = total / time.Duration(len(timings))
		result.MinTime = timings[0].total
		result.MaxTime = timings[len(timings)-1].total
		result.P50Time = timings[len(timings)/2].total

		// P99 values all come from the same iteration
		p99Idx := int(float64(len(timings)) * 0.99)
		result.P99Time = timings[p99Idx].total
		result.P99IndexTime = timings[p99Idx].index
		result.P99EventTime = timings[p99Idx].event
	}

	return result
}

// QueryResult holds basic query stats for benchmarking
type QueryResult struct {
	EventsReturned int
	EventsScanned  int
	LedgersFound   int
	IndexBytes     int64
	EventBytes     int64
	IndexMatches   int           // TOIDs or ledgers matched by index before fetching events
	IndexTime      time.Duration // Time spent reading index
	EventFetchTime time.Duration // Time spent fetching events
	Error          error

	// Multi-filter optimization stats (posting list only)
	SmallestListSize int // Size of smallest posting list
	LargestListSize  int // Size of largest posting list
	SkippedBuckets   int // Buckets skipped due to guided intersection
}

func executeQueryBenchmark(eventStore *store.RocksDBEventStore, startLedger, endLedger uint32, contractID []byte, topics [][]byte, indexType string, limit int) *QueryResult {
	switch indexType {
	case "posting":
		eventStore.ParallelPostingReads = false
		return executePostingQueryBenchmark(eventStore, startLedger, endLedger, contractID, topics, limit)
	case "posting-parallel":
		eventStore.ParallelPostingReads = true
		result := executePostingQueryBenchmark(eventStore, startLedger, endLedger, contractID, topics, limit)
		eventStore.ParallelPostingReads = false
		return result
	case "bitmap32":
		return executeBitmap32QueryBenchmark(eventStore, startLedger, endLedger, contractID, topics, limit)
	case "bitmap64":
		eventStore.ParallelBitmap64 = false
		return executeBitmap64QueryBenchmark(eventStore, startLedger, endLedger, contractID, topics, limit)
	case "bitmap64-parallel":
		eventStore.ParallelBitmap64 = true
		result := executeBitmap64QueryBenchmark(eventStore, startLedger, endLedger, contractID, topics, limit)
		eventStore.ParallelBitmap64 = false
		return result
	}
	return nil
}

func executePostingQueryBenchmark(eventStore *store.RocksDBEventStore, startLedger, endLedger uint32, contractID []byte, topics [][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithPostingListTiming(contractID, topics, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned:   len(events),
		EventsScanned:    stats.EventsScanned,
		LedgersFound:     stats.UniqueLedgers,
		IndexBytes:       stats.PostingListBytes,
		EventBytes:       stats.EventBytesRead,
		IndexMatches:     stats.TOIDsAfterIntersect,
		IndexTime:        stats.PostingListTime + stats.IntersectTime,
		EventFetchTime:   stats.EventFetchTime + stats.DecodeTime + stats.FilterTime,
		SmallestListSize: stats.SmallestListSize,
		LargestListSize:  stats.LargestListSize,
		SkippedBuckets:   stats.SkippedBuckets,
	}
}

func executeBitmap32QueryBenchmark(eventStore *store.RocksDBEventStore, startLedger, endLedger uint32, contractID []byte, topics [][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithBitmap(contractID, topics, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned: len(events),
		EventsScanned:  stats.EventsScanned,
		LedgersFound:   stats.MatchingLedgers,
		IndexBytes:     stats.IndexBytesRead,
		EventBytes:     stats.EventBytesRead,
		IndexMatches:   stats.MatchingLedgers,
		IndexTime:      stats.IndexLookupTime,
		EventFetchTime: stats.EventFetchTime,
	}
}

func executeBitmap64QueryBenchmark(eventStore *store.RocksDBEventStore, startLedger, endLedger uint32, contractID []byte, topics [][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEventsWithBitmap64(contractID, topics, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned: len(events),
		EventsScanned:  stats.EventsScanned,
		LedgersFound:   stats.MatchingTOIDs, // TOIDs are operation-level granularity
		IndexBytes:     stats.IndexBytesRead,
		EventBytes:     stats.EventBytesRead,
		IndexMatches:   stats.MatchingTOIDs,
		IndexTime:      stats.IndexLookupTime,
		EventFetchTime: stats.EventFetchTime,
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
		if r.Query.ContractID != "" {
			key += ":" + r.Query.ContractID[:8]
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
		fmt.Printf(" | %12s", idx)
	}
	fmt.Printf(" | %6s/%6s | %8s | %8s | %8s\n", "EvtRet", "Scaned", "IdxMatch", "IdxBytes", "EvtBytes")

	fmt.Print(strings.Repeat("-", 40))
	for range indexes {
		fmt.Print("-+-" + strings.Repeat("-", 12))
	}
	fmt.Println("-+-" + strings.Repeat("-", 13) + "-+-" + strings.Repeat("-", 8) + "-+-" + strings.Repeat("-", 8) + "-+-" + strings.Repeat("-", 8))

	// Print results
	for _, name := range queryNames {
		qr := queryResults[name]
		fmt.Printf("%-40s", truncateBenchmarkStr(name, 40))

		var eventsReturned, eventsScanned, idxMatches int
		var idxBytes, evtBytes int64
		for _, idx := range indexes {
			if r, ok := qr[idx]; ok {
				if r.Error != "" {
					fmt.Printf(" | %12s", "ERROR")
				} else {
					fmt.Printf(" | %12s", formatBenchmarkDuration(r.AvgTime))
				}
				eventsReturned = r.EventsReturned
				eventsScanned = r.EventsScanned
				idxMatches = r.IndexMatches
				idxBytes = r.IndexBytes
				evtBytes = r.EventBytes
			} else {
				fmt.Printf(" | %12s", "N/A")
			}
		}
		fmt.Printf(" | %6d/%6d | %8d | %8s | %8s\n", eventsReturned, eventsScanned, idxMatches, formatBytes(idxBytes), formatBytes(evtBytes))
	}

	// Summary statistics
	printSummaryStats(results, indexes)
}

func printSummaryStats(results []BenchmarkResult, indexes []string) {
	fmt.Println()
	fmt.Println("=== Summary Statistics ===")
	for _, idx := range indexes {
		var total, count int64
		var min, max time.Duration = time.Hour, 0
		for _, r := range results {
			if r.IndexType == idx && r.Error == "" {
				total += r.AvgTime.Nanoseconds()
				count++
				if r.MinTime < min {
					min = r.MinTime
				}
				if r.MaxTime > max {
					max = r.MaxTime
				}
			}
		}
		if count > 0 {
			avg := time.Duration(total / count)
			fmt.Printf("  %s: avg=%s, min=%s, max=%s (n=%d)\n", idx, formatBenchmarkDuration(avg), formatBenchmarkDuration(min), formatBenchmarkDuration(max), count)
		}
	}
}

func outputCSV(results []BenchmarkResult) {
	// Header with separate topic columns and timing breakdown
	fmt.Println("query_name,contract_id,topic0,topic1,topic2,topic3,index_type,avg_ms,p99_ms,p99_idx_ms,p99_evt_ms,events_scanned,index_matches,index_bytes,event_bytes,events_returned,smallest_list,largest_list,skipped_buckets,error")

	for _, r := range results {
		contractID := r.Query.ContractID
		if contractID == "" {
			contractID = "-"
		}

		// Separate topics into columns
		topics := make([]string, 4)
		for i := 0; i < 4; i++ {
			if i < len(r.Query.Topics) {
				topics[i] = r.Query.Topics[i]
			} else {
				topics[i] = "-"
			}
		}

		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%s,%d,%d,%d,%d,%s\n",
			r.Query.Name,
			contractID,
			topics[0],
			topics[1],
			topics[2],
			topics[3],
			r.IndexType,
			float64(r.AvgTime.Microseconds())/1000.0,
			float64(r.P99Time.Microseconds())/1000.0,
			float64(r.P99IndexTime.Microseconds())/1000.0,
			float64(r.P99EventTime.Microseconds())/1000.0,
			r.EventsScanned,
			r.IndexMatches,
			formatBytes(r.IndexBytes),
			formatBytes(r.EventBytes),
			r.EventsReturned,
			r.SmallestListSize,
			r.LargestListSize,
			r.SkippedBuckets,
			r.Error,
		)
	}
}

func outputJSON(results []BenchmarkResult) {
	type jsonResult struct {
		QueryName        string   `json:"query_name"`
		ContractID       string   `json:"contract_id,omitempty"`
		Topics           []string `json:"topics,omitempty"`
		IndexType        string   `json:"index_type"`
		AvgMs            float64  `json:"avg_ms"`
		MinMs            float64  `json:"min_ms"`
		MaxMs            float64  `json:"max_ms"`
		P50Ms            float64  `json:"p50_ms"`
		P99Ms            float64  `json:"p99_ms"`
		P99IndexMs       float64  `json:"p99_index_ms"`
		P99EventMs       float64  `json:"p99_event_ms"`
		EventsReturned   int      `json:"events_returned"`
		EventsScanned    int      `json:"events_scanned"`
		IndexMatches     int      `json:"index_matches"`
		IndexBytes       int64    `json:"index_bytes"`
		IndexBytesHR     string   `json:"index_bytes_human"`
		EventBytes       int64    `json:"event_bytes"`
		EventBytesHR     string   `json:"event_bytes_human"`
		Iterations       int      `json:"iterations"`
		SmallestListSize int      `json:"smallest_list_size,omitempty"`
		LargestListSize  int      `json:"largest_list_size,omitempty"`
		SkippedBuckets   int      `json:"skipped_buckets,omitempty"`
		Error            string   `json:"error,omitempty"`
	}

	var jsonResults []jsonResult
	for _, r := range results {
		jr := jsonResult{
			QueryName:        r.Query.Name,
			ContractID:       r.Query.ContractID,
			Topics:           r.Query.Topics,
			IndexType:        r.IndexType,
			AvgMs:            float64(r.AvgTime.Microseconds()) / 1000.0,
			MinMs:            float64(r.MinTime.Microseconds()) / 1000.0,
			MaxMs:            float64(r.MaxTime.Microseconds()) / 1000.0,
			P50Ms:            float64(r.P50Time.Microseconds()) / 1000.0,
			P99Ms:            float64(r.P99Time.Microseconds()) / 1000.0,
			P99IndexMs:       float64(r.P99IndexTime.Microseconds()) / 1000.0,
			P99EventMs:       float64(r.P99EventTime.Microseconds()) / 1000.0,
			EventsReturned:   r.EventsReturned,
			EventsScanned:    r.EventsScanned,
			IndexMatches:     r.IndexMatches,
			IndexBytes:       r.IndexBytes,
			IndexBytesHR:     formatBytes(r.IndexBytes),
			EventBytes:       r.EventBytes,
			EventBytesHR:     formatBytes(r.EventBytes),
			Iterations:       r.Iterations,
			SmallestListSize: r.SmallestListSize,
			LargestListSize:  r.LargestListSize,
			SkippedBuckets:   r.SkippedBuckets,
			Error:            r.Error,
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
			QueryName        string   `json:"query_name"`
			ContractID       string   `json:"contract_id,omitempty"`
			Topics           []string `json:"topics,omitempty"`
			IndexType        string   `json:"index_type"`
			StartLedger      uint32   `json:"start_ledger"`
			EndLedger        uint32   `json:"end_ledger"`
			AvgMs            float64  `json:"avg_ms"`
			MinMs            float64  `json:"min_ms"`
			MaxMs            float64  `json:"max_ms"`
			P50Ms            float64  `json:"p50_ms"`
			P99Ms            float64  `json:"p99_ms"`
			P99IndexMs       float64  `json:"p99_index_ms"`
			P99EventMs       float64  `json:"p99_event_ms"`
			EventsReturned   int      `json:"events_returned"`
			EventsScanned    int      `json:"events_scanned"`
			IndexMatches     int      `json:"index_matches"`
			IndexBytes       int64    `json:"index_bytes"`
			IndexBytesHR     string   `json:"index_bytes_human"`
			EventBytes       int64    `json:"event_bytes"`
			EventBytesHR     string   `json:"event_bytes_human"`
			Iterations       int      `json:"iterations"`
			SmallestListSize int      `json:"smallest_list_size,omitempty"`
			LargestListSize  int      `json:"largest_list_size,omitempty"`
			SkippedBuckets   int      `json:"skipped_buckets,omitempty"`
			Error            string   `json:"error,omitempty"`
		}{
			QueryName:        r.Query.Name,
			ContractID:       r.Query.ContractID,
			Topics:           r.Query.Topics,
			IndexType:        r.IndexType,
			StartLedger:      r.StartLedger,
			EndLedger:        r.EndLedger,
			AvgMs:            float64(r.AvgTime.Microseconds()) / 1000.0,
			MinMs:            float64(r.MinTime.Microseconds()) / 1000.0,
			MaxMs:            float64(r.MaxTime.Microseconds()) / 1000.0,
			P50Ms:            float64(r.P50Time.Microseconds()) / 1000.0,
			P99Ms:            float64(r.P99Time.Microseconds()) / 1000.0,
			P99IndexMs:       float64(r.P99IndexTime.Microseconds()) / 1000.0,
			P99EventMs:       float64(r.P99EventTime.Microseconds()) / 1000.0,
			EventsReturned:   r.EventsReturned,
			EventsScanned:    r.EventsScanned,
			IndexMatches:     r.IndexMatches,
			IndexBytes:       r.IndexBytes,
			IndexBytesHR:     formatBytes(r.IndexBytes),
			EventBytes:       r.EventBytes,
			EventBytesHR:     formatBytes(r.EventBytes),
			Iterations:       r.Iterations,
			SmallestListSize: r.SmallestListSize,
			LargestListSize:  r.LargestListSize,
			SkippedBuckets:   r.SkippedBuckets,
			Error:            r.Error,
		}
		line, _ := json.Marshal(jr)
		fmt.Fprintln(w, string(line))
	default:
		// CSV format
		contractID := r.Query.ContractID
		if contractID == "" {
			contractID = "-"
		}
		topics := make([]string, 4)
		for i := 0; i < 4; i++ {
			if i < len(r.Query.Topics) {
				topics[i] = r.Query.Topics[i]
			} else {
				topics[i] = "-"
			}
		}
		fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%s,%d,%d,%d,%d,%s\n",
			r.Query.Name,
			contractID,
			topics[0],
			topics[1],
			topics[2],
			topics[3],
			r.IndexType,
			r.StartLedger,
			r.EndLedger,
			float64(r.AvgTime.Microseconds())/1000.0,
			float64(r.P99Time.Microseconds())/1000.0,
			float64(r.P99IndexTime.Microseconds())/1000.0,
			float64(r.P99EventTime.Microseconds())/1000.0,
			r.EventsScanned,
			r.IndexMatches,
			formatBytes(r.IndexBytes),
			formatBytes(r.EventBytes),
			r.EventsReturned,
			r.SmallestListSize,
			r.LargestListSize,
			r.SkippedBuckets,
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
