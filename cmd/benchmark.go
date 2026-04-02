package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
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
	Name        string     `json:"name"`
	ContractIDs []string   `json:"contract_ids,omitempty"`
	Topics      [][]string `json:"topics,omitempty"` // Topics[position] = []values (OR per position, base64)
}

// QueryPlan is a fully resolved, reproducible benchmark plan.
// Generated from BenchmarkData + seed, can be saved and reloaded.
type QueryPlan struct {
	StartLedger uint32      `json:"start_ledger"`
	EndLedger   uint32      `json:"end_ledger"`
	Queries     []QuerySpec `json:"queries"`
}

// BenchmarkResult holds timing results for a query
type BenchmarkResult struct {
	// Query identification
	Query       QuerySpec
	Datastore   string
	StartLedger uint32 // Ledger range used for this query
	EndLedger   uint32

	// Test config
	Iterations int

	// Timing
	P50Time                time.Duration
	P99Time                time.Duration
	P99IndexTime           time.Duration // P99 total index time
	P99IndexLookupTime     time.Duration // P99 MPHF hash + slot lookup
	P99IndexDecodeTime     time.Duration // P99 bitmap decode (FromBuffer)
	P99IndexIntersectTime  time.Duration // P99 trim + AND/OR
	P99EventTime           time.Duration // P99 total event time (fetch + decode)
	P99EventFetchTime      time.Duration // P99 event I/O
	P99EventDecodeTime     time.Duration // P99 event decode
	P99EventFilterTime     time.Duration // P99 post-filter
	AvgTime                time.Duration // Average total time
	AvgIndexTime           time.Duration
	AvgIndexLookupTime     time.Duration
	AvgIndexDecodeTime     time.Duration
	AvgIndexIntersectTime  time.Duration
	AvgEventTime           time.Duration
	AvgEventFetchTime      time.Duration
	AvgEventDecodeTime     time.Duration
	AvgEventFilterTime     time.Duration

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

	dataFile := fs.String("data", "", "Benchmark data file (JSON with contracts/topics by cardinality)")
	queriesFile := fs.String("queries", "", "Pre-generated query plan file (JSON, from --generate-queries)")
	generateQueries := fs.String("generate-queries", "", "Generate query plan from --data and write to this file")
	datastoreFlag := fs.String("datastore", "all", "Datastore to benchmark: rocksdb,flatfiles,all")
	iterations := fs.Int("iterations", 5, "Number of iterations per query")
	warmup := fs.Int("warmup", 1, "Warmup iterations (not counted)")
	outputFormat := fs.String("format", "table", "Output format: table, csv, json")
	generateData := fs.Bool("generate", false, "Generate sample benchmark data file")
	generateDataFromEvents := fs.String("generate-data", "", "Auto-generate benchmark data from actual events (writes JSON to file)")
	scanSegments := fs.Int("scan-segments", 0, "Max segments to scan for --generate-data (0 = all)")
	validate := fs.Bool("validate", false, "Validate queries by running each once, report/skip queries returning 0 results")
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
		fmt.Fprintf(os.Stderr, "Benchmarks query performance across different datastores.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  benchmark --generate > benchmark_data.json              # Generate sample data file\n")
		fmt.Fprintf(os.Stderr, "  benchmark --generate-data out.json                      # Auto-generate from actual events\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data data.json --generate-queries plan.json  # Generate fixed query plan\n")
		fmt.Fprintf(os.Stderr, "  benchmark --queries plan.json                            # Run from fixed query plan\n")
		fmt.Fprintf(os.Stderr, "  benchmark --queries plan.json --validate                 # Validate queries for 0-result\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data benchmark_data.json                     # Run benchmarks\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data data.json --datastore flatfiles         # Only test flatfiles\n")
		fmt.Fprintf(os.Stderr, "  benchmark --data data.json --format csv                  # CSV output\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	// Generate sample data file
	if *generateData {
		generateSampleData()
		return
	}

	// Auto-generate benchmark data from actual events using index distribution
	if *generateDataFromEvents != "" {
		eventStore, err := openStore(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
			os.Exit(1)
		}
		defer eventStore.Close()

		data, err := generateBenchmarkDataFromEvents(eventStore, cfg.Storage.SegmentPath, *scanSegments)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating benchmark data: %v\n", err)
			os.Exit(1)
		}

		output, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling benchmark data: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*generateDataFromEvents, output, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing benchmark data: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Wrote benchmark data to %s\n", *generateDataFromEvents)
		fmt.Fprintf(os.Stderr, "  Ledger range: %d - %d\n", data.StartLedger, data.EndLedger)
		fmt.Fprintf(os.Stderr, "  Contracts: %d high, %d med, %d low\n",
			len(data.Contracts.High), len(data.Contracts.Medium), len(data.Contracts.Low))
		for i, td := range []TopicData{data.Topic0, data.Topic1, data.Topic2, data.Topic3} {
			fmt.Fprintf(os.Stderr, "  Topic%d: %d high, %d med, %d low\n",
				i, len(td.High), len(td.Medium), len(td.Low))
		}
		return
	}

	// --- Resolve query plan: either from --queries file or generated from --data ---
	var plan QueryPlan

	// Set random seed (used for query generation and ledger range sampling)
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	rand.Seed(*seed)
	fmt.Fprintf(os.Stderr, "Random seed: %d (use --seed %d to reproduce)\n", *seed, *seed)

	if *queriesFile != "" {
		// Load pre-generated query plan
		planData, readErr := os.ReadFile(*queriesFile)
		if readErr != nil {
			fmt.Fprintf(os.Stderr, "Error reading query plan: %v\n", readErr)
			os.Exit(1)
		}
		if jsonErr := json.Unmarshal(planData, &plan); jsonErr != nil {
			fmt.Fprintf(os.Stderr, "Error parsing query plan: %v\n", jsonErr)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Loaded %d queries from %s\n", len(plan.Queries), *queriesFile)
	} else if *dataFile != "" {
		// Generate from benchmark data
		data, loadErr := loadBenchmarkData(*dataFile)
		if loadErr != nil {
			fmt.Fprintf(os.Stderr, "Error loading benchmark data: %v\n", loadErr)
			os.Exit(1)
		}

		plan = QueryPlan{
			StartLedger: data.StartLedger,
			EndLedger:   data.EndLedger,
			Queries:     generateQueryCombinations(data, *maxCombinations),
		}

		// If --generate-queries, write the plan and exit
		if *generateQueries != "" {
			planJSON, jsonErr := json.MarshalIndent(plan, "", "  ")
			if jsonErr != nil {
				fmt.Fprintf(os.Stderr, "Error marshaling query plan: %v\n", jsonErr)
				os.Exit(1)
			}
			if writeErr := os.WriteFile(*generateQueries, planJSON, 0644); writeErr != nil {
				fmt.Fprintf(os.Stderr, "Error writing query plan: %v\n", writeErr)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "Wrote %d queries to %s\n", len(plan.Queries), *generateQueries)
			return
		}
	} else {
		fmt.Fprintf(os.Stderr, "Error: --data or --queries is required\n\n")
		fs.Usage()
		os.Exit(2)
	}

	// Validate queries if requested
	if *validate {
		eventStore, err := openStore(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open event store: %v\n", err)
			os.Exit(1)
		}
		defer eventStore.Close()

		fmt.Fprintf(os.Stderr, "Validating %d queries (ledger range %d-%d)...\n",
			len(plan.Queries), plan.StartLedger, plan.EndLedger)

		var valid, zero, errCount int
		for _, q := range plan.Queries {
			var contractBytes [][]byte
			var decodeErr bool
			for _, cid := range q.ContractIDs {
				decoded, decErr := decodeContractID(cid)
				if decErr != nil {
					fmt.Fprintf(os.Stderr, "  %-40s ERROR: %v\n", q.Name, decErr)
					errCount++
					decodeErr = true
					break
				}
				contractBytes = append(contractBytes, decoded)
			}
			if decodeErr {
				continue
			}
			var topicGroups [4][][]byte
			for pos, values := range q.Topics {
				if pos >= 4 {
					break
				}
				for _, t := range values {
					decoded, decErr := decodeBase64(t)
					if decErr != nil {
						fmt.Fprintf(os.Stderr, "  %-40s ERROR: %v\n", q.Name, decErr)
						errCount++
						decodeErr = true
						break
					}
					topicGroups[pos] = append(topicGroups[pos], decoded)
				}
				if decodeErr {
					break
				}
			}
			if decodeErr {
				continue
			}

			qr := executeQueryBenchmark(eventStore, plan.StartLedger, plan.EndLedger, contractBytes, topicGroups, 1)
			if qr.Error != nil {
				fmt.Fprintf(os.Stderr, "  %-40s ERROR: %v\n", q.Name, qr.Error)
				errCount++
			} else if qr.EventsReturned == 0 {
				fmt.Fprintf(os.Stderr, "  %-40s ZERO results (index_matches=%d)\n", q.Name, qr.IndexMatches)
				zero++
			} else {
				fmt.Fprintf(os.Stderr, "  %-40s OK (%d results, %d index_matches)\n", q.Name, qr.EventsReturned, qr.IndexMatches)
				valid++
			}
		}
		fmt.Fprintf(os.Stderr, "\nValidation: %d OK, %d zero-result, %d errors (total %d)\n",
			valid, zero, errCount, len(plan.Queries))
		return
	}

	originalStartLedger := plan.StartLedger
	originalEndLedger := plan.EndLedger
	queries := plan.Queries
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

	// Parse datastores — only include backends that are actually enabled in config
	var datastores []string
	if *datastoreFlag == "all" {
		if cfg.Storage.RocksDB {
			datastores = append(datastores, "rocksdb")
		}
		if cfg.Storage.SegmentFiles {
			datastores = append(datastores, "flatfiles")
		}
		if len(datastores) == 0 {
			fmt.Fprintf(os.Stderr, "Error: no storage backends enabled in config\n")
			os.Exit(2)
		}
	} else {
		for _, name := range strings.Split(*datastoreFlag, ",") {
			if name != "rocksdb" && name != "flatfiles" {
				fmt.Fprintf(os.Stderr, "Error: invalid datastore: %s (valid: rocksdb, flatfiles)\n", name)
				os.Exit(2)
			}
			datastores = append(datastores, name)
		}
	}

	// Open per-datastore stores
	stores := make(map[string]*store.Store)
	for _, ds := range datastores {
		s, err := openStoreForDatastore(cfg, ds)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open %s store: %v\n", ds, err)
			os.Exit(1)
		}
		stores[ds] = s
	}
	defer func() {
		for _, s := range stores {
			s.Close()
		}
	}()

	// Create log writer if enabled
	var err error
	var logWriter *os.File
	if *logFile != "none" && *logFile != "" {
		logWriter, err = os.Create(*logFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log file: %v\n", err)
			os.Exit(1)
		}
		defer logWriter.Close()
		// Write CSV header
		fmt.Fprintln(logWriter, "timestamp,query_name,datastore,events_returned,index_matches,segments_touched,index_bytes,event_bytes,groups_decompressed,p99_idx_lookup_ms,p99_idx_decode_ms,p99_idx_intersect_ms,p99_idx_ms,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,p99_total_ms,avg_idx_lookup_ms,avg_idx_decode_ms,avg_idx_intersect_ms,avg_idx_ms,avg_evt_ms,avg_evt_fetch_ms,avg_evt_decode_ms,avg_evt_filter_ms,avg_total_ms,error")
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
			fmt.Fprintln(outputWriter, "query_name,contract_id,topic0,topic1,topic2,topic3,datastore,start_ledger,end_ledger,p50_total_ms,p99_total_ms,p99_idx_ms,idx_lookup_ms,idx_decode_ms,idx_intersect_ms,segments_touched,index_matches,index_bytes,smallest_list,largest_list,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,events_returned,events_scanned,event_bytes,groups_decompressed,iterations,error")
			outputWriter.Sync()
		}
		fmt.Fprintf(os.Stderr, "Output file: %s (format: %s)\n", *outputFile, outputFileFormat)
	}
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
	fmt.Fprintf(os.Stderr, "Datastores: %v\n", datastores)
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
	totalQueries := len(queries) * len(datastores)
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

		for _, ds := range datastores {
			completed++
			fmt.Fprintf(os.Stderr, "\r[%d/%d] Running: %s (%s) [%d-%d]...    ", completed, totalQueries, q.Name, ds, queryStart, queryEnd)

			// Create a temporary data struct with the random range
			queryData := &BenchmarkData{
				StartLedger: queryStart,
				EndLedger:   queryEnd,
			}
			result := runQueryBenchmark(stores[ds], queryData, q, ds, *iterations, *warmup, *limit, *timeout, *coldCache)
			results = append(results, result)

			// Log query result
			if logWriter != nil {
				errStr := result.Error
				fmt.Fprintf(logWriter, "%s,%s,%s,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%s\n",
					time.Now().Format(time.RFC3339),
					result.Query.Name,
					result.Datastore,
					result.EventsReturned,
					result.IndexMatches,
					result.SegmentsTouched,
					result.IndexBytes,
					result.EventBytes,
					result.GroupsDecompressed,
					float64(result.P99IndexLookupTime.Microseconds())/1000.0,
					float64(result.P99IndexDecodeTime.Microseconds())/1000.0,
					float64(result.P99IndexIntersectTime.Microseconds())/1000.0,
					float64(result.P99IndexTime.Microseconds())/1000.0,
					float64(result.P99EventTime.Microseconds())/1000.0,
					float64(result.P99EventFetchTime.Microseconds())/1000.0,
					float64(result.P99EventDecodeTime.Microseconds())/1000.0,
					float64(result.P99EventFilterTime.Microseconds())/1000.0,
					float64(result.P99Time.Microseconds())/1000.0,
					float64(result.AvgIndexLookupTime.Microseconds())/1000.0,
					float64(result.AvgIndexDecodeTime.Microseconds())/1000.0,
					float64(result.AvgIndexIntersectTime.Microseconds())/1000.0,
					float64(result.AvgIndexTime.Microseconds())/1000.0,
					float64(result.AvgEventTime.Microseconds())/1000.0,
					float64(result.AvgEventFetchTime.Microseconds())/1000.0,
					float64(result.AvgEventDecodeTime.Microseconds())/1000.0,
					float64(result.AvgEventFilterTime.Microseconds())/1000.0,
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
			printSummaryStats(results, datastores)
		}
	} else {
		switch *outputFormat {
		case "csv":
			outputCSV(results)
		case "json":
			outputJSON(results)
		default:
			outputTable(results, datastores)
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
// Auto-generate Benchmark Data from Events
// =============================================================================

// generateBenchmarkDataFromEvents scans actual stored events and builds BenchmarkData
// with values guaranteed to co-occur. Works with any backend (flatfiles or RocksDB).
func generateBenchmarkDataFromEvents(eventStore *store.Store, segmentPath string, maxSegments int) (*BenchmarkData, error) {
	// Discover ledger range by scanning segment directories
	coldPath := filepath.Join(segmentPath, "cold")
	entries, err := os.ReadDir(coldPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read segment directory %s: %w", coldPath, err)
	}

	// Collect all segment IDs
	var segIDs []uint32
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var segID uint32
		if _, err := fmt.Sscanf(e.Name(), "%d", &segID); err != nil {
			continue
		}
		segIDs = append(segIDs, segID)
	}
	if len(segIDs) == 0 {
		return nil, fmt.Errorf("no segments found in %s", coldPath)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	// Limit number of segments to scan
	if maxSegments > 0 && len(segIDs) > maxSegments {
		fmt.Fprintf(os.Stderr, "Limiting scan to %d of %d segments\n", maxSegments, len(segIDs))
		segIDs = segIDs[:maxSegments]
	}

	minSeg := segIDs[0]
	maxSeg := segIDs[len(segIDs)-1]
	startLedger := minSeg * store.SegmentSize
	endLedger := (maxSeg+1)*store.SegmentSize - 1

	fmt.Fprintf(os.Stderr, "Scanning events in ledger range %d-%d (%d segments)...\n",
		startLedger, endLedger, len(segIDs))

	// Scan events and build frequency maps
	contractCounts := make(map[string]int)
	topicCounts := [4]map[string]int{
		make(map[string]int),
		make(map[string]int),
		make(map[string]int),
		make(map[string]int),
	}

	// Scan in batches of 100k ledgers to avoid loading everything into memory
	const batchSize uint32 = 100_000
	totalEvents := 0
	for batchStart := startLedger; batchStart <= endLedger; batchStart += batchSize {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > endLedger {
			batchEnd = endLedger
		}

		_, events, err := eventStore.QueryEvents(nil, [4][][]byte{}, batchStart, batchEnd, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to scan events %d-%d: %w", batchStart, batchEnd, err)
		}

		for _, ev := range events {
			if ev.ContractID != "" {
				contractCounts[ev.ContractID]++
			}
			for pos, topic := range ev.Topics {
				if pos >= 4 {
					break
				}
				if topic != "" {
					topicCounts[pos][topic]++
				}
			}
		}
		totalEvents += len(events)
		fmt.Fprintf(os.Stderr, "  Scanned ledgers %d-%d: %d events (total: %d)\n",
			batchStart, batchEnd, len(events), totalEvents)
	}

	fmt.Fprintf(os.Stderr, "Total: %d events, %d contracts, %d/%d/%d/%d topics\n",
		totalEvents, len(contractCounts),
		len(topicCounts[0]), len(topicCounts[1]), len(topicCounts[2]), len(topicCounts[3]))

	data := &BenchmarkData{
		StartLedger: startLedger,
		EndLedger:   endLedger,
	}

	// Sort by count descending and classify into tiers: top 2 = high, next 3 = med, next 5 = low
	classifyMap := func(counts map[string]int) (high, med, low []string) {
		type entry struct {
			key   string
			count int
		}
		sorted := make([]entry, 0, len(counts))
		for k, v := range counts {
			sorted = append(sorted, entry{k, v})
		}
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].count > sorted[j].count })

		for i, e := range sorted {
			if i >= 10 {
				break
			}
			switch {
			case i < 2:
				high = append(high, e.key)
			case i < 5:
				med = append(med, e.key)
			default:
				low = append(low, e.key)
			}
		}
		return
	}

	h, m, l := classifyMap(contractCounts)
	data.Contracts = ContractData{High: h, Medium: m, Low: l}

	for pos := 0; pos < 4; pos++ {
		h, m, l := classifyMap(topicCounts[pos])
		td := TopicData{High: h, Medium: m, Low: l}
		switch pos {
		case 0:
			data.Topic0 = td
		case 1:
			data.Topic1 = td
		case 2:
			data.Topic2 = td
		case 3:
			data.Topic3 = td
		}
	}

	return data, nil
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

func runQueryBenchmark(eventStore *store.Store, data *BenchmarkData, spec QuerySpec, datastore string, iterations, warmup, limit int, timeout time.Duration, coldCache bool) BenchmarkResult {
	result := BenchmarkResult{
		Query:       spec,
		Datastore:   datastore,
		StartLedger: data.StartLedger,
		EndLedger:   data.EndLedger,
		Iterations:  iterations,
	}

	// Track all timing data together per iteration so p99 values are correlated
	type iterationTiming struct {
		total          time.Duration
		index          time.Duration
		indexLookup      time.Duration
		indexDecode    time.Duration
		indexIntersect time.Duration
		event          time.Duration
		eventFetch     time.Duration
		eventDecode    time.Duration
		eventFilter    time.Duration
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
			eventStore.PurgeQueryCache()
			if err := purgePageCache(); err != nil {
				fmt.Fprintf(os.Stderr, "\nWarning: failed to purge page cache: %v\n", err)
			}
		}
		resultChan := make(chan *QueryResult, 1)
		go func() {
			resultChan <- executeQueryBenchmark(eventStore, data.StartLedger, data.EndLedger, contractBytes, topicGroups, limit)
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
			eventStore.PurgeQueryCache()
			if err := purgePageCache(); err != nil {
				fmt.Fprintf(os.Stderr, "\nWarning: failed to purge page cache: %v\n", err)
			}
		}
		start := time.Now()

		resultChan := make(chan *QueryResult, 1)
		go func() {
			resultChan <- executeQueryBenchmark(eventStore, data.StartLedger, data.EndLedger, contractBytes, topicGroups, limit)
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
				total: elapsed, index: qr.IndexTime, indexLookup: qr.IndexLookupTime, indexDecode: qr.IndexDecodeTime, indexIntersect: qr.IndexIntersectTime,
				event: qr.EventTime, eventFetch: qr.EventFetchTime, eventDecode: qr.EventDecodeTime, eventFilter: qr.EventFilterTime,
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
		result.P99IndexLookupTime = timings[p99Idx].indexLookup
		result.P99IndexDecodeTime = timings[p99Idx].indexDecode
		result.P99IndexIntersectTime = timings[p99Idx].indexIntersect
		result.P99EventTime = timings[p99Idx].event
		result.P99EventFetchTime = timings[p99Idx].eventFetch
		result.P99EventDecodeTime = timings[p99Idx].eventDecode
		result.P99EventFilterTime = timings[p99Idx].eventFilter

		// Average across all iterations
		n := time.Duration(len(timings))
		for _, t := range timings {
			result.AvgTime += t.total
			result.AvgIndexTime += t.index
			result.AvgIndexLookupTime += t.indexLookup
			result.AvgIndexDecodeTime += t.indexDecode
			result.AvgIndexIntersectTime += t.indexIntersect
			result.AvgEventTime += t.event
			result.AvgEventFetchTime += t.eventFetch
			result.AvgEventDecodeTime += t.eventDecode
			result.AvgEventFilterTime += t.eventFilter
		}
		result.AvgTime /= n
		result.AvgIndexTime /= n
		result.AvgIndexLookupTime /= n
		result.AvgIndexDecodeTime /= n
		result.AvgIndexIntersectTime /= n
		result.AvgEventTime /= n
		result.AvgEventFetchTime /= n
		result.AvgEventDecodeTime /= n
		result.AvgEventFilterTime /= n
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
	IndexLookupTime      time.Duration // Time spent reading index from storage (I/O)
	IndexDecodeTime    time.Duration // Time spent decoding index (CPU)
	IndexIntersectTime time.Duration // Time spent intersecting index results

	// Event timing
	EventTime            time.Duration // Time spent on event operations (total)
	EventFetchTime       time.Duration // Time spent fetching events from storage (I/O)
	EventDecodeTime      time.Duration // Time spent decoding events (CPU)
	EventFilterTime      time.Duration // Time spent filtering events (CPU)
	GroupsDecompressed   int           // Number of group blocks decompressed

	Error error
}

func executeQueryBenchmark(eventStore *store.Store, startLedger, endLedger uint32, contractIDs [][]byte, topicGroups [4][][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEvents(contractIDs, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	fmt.Fprintf(os.Stderr, "[debug] store.TotalTime=%v idx=%v evt=%v decode=%v filter=%v matches=%d events=%d\n",
		stats.TotalTime, stats.IndexLookupTime, stats.EventFetchTime, stats.DecodeTime, stats.FilterTime,
		stats.MatchingLocalIDs, len(events))

	return &QueryResult{
		EventsReturned:      len(events),
		EventsScanned:       stats.EventsScanned,
		SegmentsTouched:     stats.SegmentsTouched,
		IndexBytes:          stats.IndexBytesRead,
		EventBytes:          stats.EventBytesRead,
		IndexMatches:        stats.MatchingLocalIDs,
		IndexTime:           stats.IndexLookupTime,
		IndexLookupTime:       stats.IndexTermLookupTime,
		IndexDecodeTime:     stats.IndexDecodeTime,
		IndexIntersectTime:  stats.IndexIntersectTime,
		EventTime:           stats.EventFetchTime + stats.DecodeTime,
		EventFetchTime:      stats.EventFetchTime,
		EventDecodeTime:     stats.DecodeTime,
		EventFilterTime:     stats.FilterTime,
		GroupsDecompressed:  stats.GroupsDecompressed,
	}
}

// =============================================================================
// Output Formatting
// =============================================================================

func outputTable(results []BenchmarkResult, datastores []string) {
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
		queryResults[key][r.Datastore] = r
	}

	// Print header
	fmt.Printf("\n%-40s", "Query")
	for _, ds := range datastores {
		fmt.Printf(" | %8s p99", ds)
	}
	fmt.Printf(" | %6s/%6s | %4s | %8s | %8s | %8s\n", "EvtRet", "Scaned", "Segs", "IdxMatch", "IdxBytes", "EvtBytes")

	fmt.Print(strings.Repeat("-", 40))
	for range datastores {
		fmt.Print("-+-" + strings.Repeat("-", 12))
	}
	fmt.Println("-+-" + strings.Repeat("-", 13) + "-+-" + strings.Repeat("-", 4) + "-+-" + strings.Repeat("-", 8) + "-+-" + strings.Repeat("-", 8) + "-+-" + strings.Repeat("-", 8))

	// Print results
	for _, name := range queryNames {
		qr := queryResults[name]
		fmt.Printf("%-40s", truncateBenchmarkStr(name, 40))

		var eventsReturned, eventsScanned, idxMatches, segmentsTouched int
		var idxBytes, evtBytes int64
		for _, ds := range datastores {
			if r, ok := qr[ds]; ok {
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
	printSummaryStats(results, datastores)
}

func printSummaryStats(results []BenchmarkResult, datastores []string) {
	fmt.Println()
	fmt.Println("=== Summary Statistics ===")
	for _, ds := range datastores {
		var p50s, p99s []time.Duration
		for _, r := range results {
			if r.Datastore == ds && r.Error == "" {
				p50s = append(p50s, r.P50Time)
				p99s = append(p99s, r.P99Time)
			}
		}
		if len(p50s) > 0 {
			sort.Slice(p50s, func(i, j int) bool { return p50s[i] < p50s[j] })
			sort.Slice(p99s, func(i, j int) bool { return p99s[i] < p99s[j] })
			medP50 := p50s[len(p50s)/2]
			medP99 := p99s[len(p99s)/2]
			fmt.Printf("  %s: median_p50=%s, median_p99=%s (n=%d)\n", ds, formatBenchmarkDuration(medP50), formatBenchmarkDuration(medP99), len(p50s))
		}
	}
}

func outputCSV(results []BenchmarkResult) {
	// Header: query | timing | index | event | test
	fmt.Println("query_name,contract_id,topic0,topic1,topic2,topic3,datastore,p50_total_ms,p99_total_ms,p99_idx_ms,idx_lookup_ms,idx_decode_ms,idx_intersect_ms,segments_touched,index_matches,index_bytes,smallest_list,largest_list,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,events_returned,events_scanned,event_bytes,groups_decompressed,iterations,error")

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

		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%d,%d,%s\n",
			r.Query.Name,
			contractID,
			topics[0], topics[1], topics[2], topics[3],
			r.Datastore,
			float64(r.P50Time.Microseconds())/1000.0,
			float64(r.P99Time.Microseconds())/1000.0,
			float64(r.P99IndexTime.Microseconds())/1000.0,
			float64(r.P99IndexLookupTime.Microseconds())/1000.0,
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
		Datastore   string     `json:"datastore"`

		// Timing
		P50Ms float64 `json:"p50_total_ms"`
		P99Ms float64 `json:"p99_total_ms"`

		// Index stats
		P99IndexMs       float64 `json:"p99_idx_ms"`
		IdxLookupMs        float64 `json:"idx_lookup_ms"`
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
			Datastore:        r.Datastore,
			P50Ms:            float64(r.P50Time.Microseconds()) / 1000.0,
			P99Ms:            float64(r.P99Time.Microseconds()) / 1000.0,
			P99IndexMs:       float64(r.P99IndexTime.Microseconds()) / 1000.0,
			IdxLookupMs:        float64(r.P99IndexLookupTime.Microseconds()) / 1000.0,
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
			Datastore   string     `json:"datastore"`
			StartLedger uint32     `json:"start_ledger"`
			EndLedger   uint32     `json:"end_ledger"`

			// Timing
			P50Ms float64 `json:"p50_total_ms"`
			P99Ms float64 `json:"p99_total_ms"`

			// Index stats
			P99IndexMs       float64 `json:"p99_idx_ms"`
			IdxLookupMs        float64 `json:"idx_lookup_ms"`
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
			Datastore:        r.Datastore,
			StartLedger:      r.StartLedger,
			EndLedger:        r.EndLedger,
			P50Ms:            float64(r.P50Time.Microseconds()) / 1000.0,
			P99Ms:            float64(r.P99Time.Microseconds()) / 1000.0,
			P99IndexMs:       float64(r.P99IndexTime.Microseconds()) / 1000.0,
			IdxLookupMs:        float64(r.P99IndexLookupTime.Microseconds()) / 1000.0,
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
		fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%d,%d,%s,%d,%d,%s\n",
			r.Query.Name,
			contractID,
			topics[0], topics[1], topics[2], topics[3],
			r.Datastore,
			r.StartLedger,
			r.EndLedger,
			float64(r.P50Time.Microseconds())/1000.0,
			float64(r.P99Time.Microseconds())/1000.0,
			float64(r.P99IndexTime.Microseconds())/1000.0,
			float64(r.P99IndexLookupTime.Microseconds())/1000.0,
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
