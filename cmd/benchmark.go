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

		data, err := generateBenchmarkDataFromEvents(eventStore, cfg.Storage.SegmentPath)
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

	// Open event store
	eventStore, err := openStore(cfg)
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
		fmt.Fprintln(logWriter, "timestamp,query_name,datastore,events_returned,index_matches,segments_touched,index_bytes,event_bytes,groups_decompressed,p99_idx_read_ms,p99_idx_decode_ms,p99_idx_intersect_ms,p99_idx_ms,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,p99_total_ms,error")
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
			fmt.Fprintln(outputWriter, "query_name,contract_id,topic0,topic1,topic2,topic3,datastore,start_ledger,end_ledger,p50_total_ms,p99_total_ms,p99_idx_ms,idx_read_ms,idx_decode_ms,idx_intersect_ms,segments_touched,index_matches,index_bytes,smallest_list,largest_list,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,events_returned,events_scanned,event_bytes,groups_decompressed,iterations,error")
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
			result := runQueryBenchmark(eventStore, queryData, q, ds, *iterations, *warmup, *limit, *timeout, *coldCache)
			results = append(results, result)

			// Log query result
			if logWriter != nil {
				errStr := result.Error
				fmt.Fprintf(logWriter, "%s,%s,%s,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%s\n",
					time.Now().Format(time.RFC3339),
					result.Query.Name,
					result.Datastore,
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
func generateBenchmarkDataFromEvents(eventStore *store.Store, segmentPath string) (*BenchmarkData, error) {
	// Discover ledger range by scanning segment directories
	coldPath := filepath.Join(segmentPath, "cold")
	entries, err := os.ReadDir(coldPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read segment directory %s: %w", coldPath, err)
	}

	var minSeg, maxSeg uint32
	first := true
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var segID uint32
		if _, err := fmt.Sscanf(e.Name(), "%d", &segID); err != nil {
			continue
		}
		if first || segID < minSeg {
			minSeg = segID
		}
		if first || segID > maxSeg {
			maxSeg = segID
		}
		first = false
	}
	if first {
		return nil, fmt.Errorf("no segments found in %s", coldPath)
	}

	startLedger := minSeg * store.SegmentSize
	endLedger := (maxSeg+1)*store.SegmentSize - 1

	fmt.Fprintf(os.Stderr, "Scanning events in ledger range %d-%d (%d segments)...\n",
		startLedger, endLedger, maxSeg-minSeg+1)

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

	// Helper: pick first value from a tier, empty string if unavailable
	first := func(vals []string) string {
		if len(vals) > 0 {
			return vals[0]
		}
		return ""
	}
	second := func(vals []string) string {
		if len(vals) >= 2 {
			return vals[1]
		}
		return ""
	}

	// Contract values
	cHigh := first(data.Contracts.High)
	cHigh2 := second(data.Contracts.High)
	cMed := first(data.Contracts.Medium)
	cLow := first(data.Contracts.Low)

	// Topic values per position: [pos] -> TopicData
	topicDatas := [4]TopicData{data.Topic0, data.Topic1, data.Topic2, data.Topic3}
	tHigh := [4]string{}   // first high per position
	tHigh2 := [4]string{}  // second high per position
	tMed := [4]string{}    // first med per position
	tLow := [4]string{}    // first low per position
	for pos := 0; pos < 4; pos++ {
		tHigh[pos] = first(topicDatas[pos].High)
		tHigh2[pos] = second(topicDatas[pos].High)
		tMed[pos] = first(topicDatas[pos].Medium)
		tLow[pos] = first(topicDatas[pos].Low)
	}

	// Helper: create positional topic spec from (position, values...) pairs
	makeTopics := func(pvs ...struct {
		pos  int
		vals []string
	}) [][]string {
		result := make([][]string, 4)
		for _, pv := range pvs {
			result[pv.pos] = pv.vals
		}
		return result
	}
	pv := func(pos int, vals ...string) struct {
		pos  int
		vals []string
	} {
		// Filter empty values
		var filtered []string
		for _, v := range vals {
			if v != "" {
				filtered = append(filtered, v)
			}
		}
		return struct {
			pos  int
			vals []string
		}{pos, filtered}
	}

	// Identify which topic positions have data (high tier)
	var activePos []int
	for pos := 0; pos < 4; pos++ {
		if tHigh[pos] != "" {
			activePos = append(activePos, pos)
		}
	}

	// =========================================================================
	// Section 1: Single-field baselines (high, med, low)
	// =========================================================================
	if cHigh != "" {
		queries = append(queries, QuerySpec{Name: "contract-high", ContractIDs: []string{cHigh}})
	}
	if cMed != "" {
		queries = append(queries, QuerySpec{Name: "contract-med", ContractIDs: []string{cMed}})
	}
	if cLow != "" {
		queries = append(queries, QuerySpec{Name: "contract-low", ContractIDs: []string{cLow}})
	}
	for _, pos := range activePos {
		queries = append(queries, QuerySpec{
			Name:   fmt.Sprintf("t%d-high", pos),
			Topics: makeTopics(pv(pos, tHigh[pos])),
		})
	}
	for _, pos := range activePos {
		if tMed[pos] != "" {
			queries = append(queries, QuerySpec{
				Name:   fmt.Sprintf("t%d-med", pos),
				Topics: makeTopics(pv(pos, tMed[pos])),
			})
		}
	}
	for _, pos := range activePos {
		if tLow[pos] != "" {
			queries = append(queries, QuerySpec{
				Name:   fmt.Sprintf("t%d-low", pos),
				Topics: makeTopics(pv(pos, tLow[pos])),
			})
		}
	}

	// =========================================================================
	// Section 2: Same-field OR — worst case
	// =========================================================================
	// Multi-contract OR
	if cHigh != "" && cHigh2 != "" {
		queries = append(queries, QuerySpec{
			Name:        "multi-c(high+high)",
			ContractIDs: []string{cHigh, cHigh2},
		})
	}
	if cHigh != "" && cMed != "" {
		queries = append(queries, QuerySpec{
			Name:        "multi-c(high+med)",
			ContractIDs: []string{cHigh, cMed},
		})
	}
	// Multi-topic OR per position (high+high)
	for _, pos := range activePos {
		if tHigh2[pos] != "" {
			queries = append(queries, QuerySpec{
				Name:   fmt.Sprintf("multi-t%d(high+high)", pos),
				Topics: makeTopics(pv(pos, tHigh[pos], tHigh2[pos])),
			})
		}
	}
	// Multi-topic OR per position (high+med)
	for _, pos := range activePos {
		if tMed[pos] != "" {
			queries = append(queries, QuerySpec{
				Name:   fmt.Sprintf("multi-t%d(high+med)", pos),
				Topics: makeTopics(pv(pos, tHigh[pos], tMed[pos])),
			})
		}
	}

	// =========================================================================
	// Section 3: Cross-field AND — worst case (high × high)
	// =========================================================================
	// Contract + one topic (high × high)
	for _, pos := range activePos {
		if cHigh != "" {
			queries = append(queries, QuerySpec{
				Name:        fmt.Sprintf("c-high+t%d-high", pos),
				ContractIDs: []string{cHigh},
				Topics:      makeTopics(pv(pos, tHigh[pos])),
			})
		}
	}
	// Two topics AND (high × high, all pairs)
	for i := 0; i < len(activePos); i++ {
		for j := i + 1; j < len(activePos); j++ {
			p1, p2 := activePos[i], activePos[j]
			queries = append(queries, QuerySpec{
				Name:   fmt.Sprintf("t%d-high+t%d-high", p1, p2),
				Topics: makeTopics(pv(p1, tHigh[p1]), pv(p2, tHigh[p2])),
			})
		}
	}
	// Contract + two topics AND
	if cHigh != "" && len(activePos) >= 2 {
		for i := 0; i < len(activePos); i++ {
			for j := i + 1; j < len(activePos); j++ {
				p1, p2 := activePos[i], activePos[j]
				queries = append(queries, QuerySpec{
					Name:        fmt.Sprintf("c-high+t%d+t%d", p1, p2),
					ContractIDs: []string{cHigh},
					Topics:      makeTopics(pv(p1, tHigh[p1]), pv(p2, tHigh[p2])),
				})
			}
		}
	}
	// Three topics AND
	if len(activePos) >= 3 {
		for i := 0; i < len(activePos); i++ {
			for j := i + 1; j < len(activePos); j++ {
				for k := j + 1; k < len(activePos); k++ {
					p1, p2, p3 := activePos[i], activePos[j], activePos[k]
					queries = append(queries, QuerySpec{
						Name:   fmt.Sprintf("t%d+t%d+t%d", p1, p2, p3),
						Topics: makeTopics(pv(p1, tHigh[p1]), pv(p2, tHigh[p2]), pv(p3, tHigh[p3])),
					})
				}
			}
		}
	}

	// =========================================================================
	// Section 4: Combined OR+AND — worst case
	// =========================================================================
	// Multi-contract OR + topic AND
	if cHigh != "" && cHigh2 != "" && len(activePos) > 0 {
		queries = append(queries, QuerySpec{
			Name:        fmt.Sprintf("multi-c(high+high)+t%d-high", activePos[0]),
			ContractIDs: []string{cHigh, cHigh2},
			Topics:      makeTopics(pv(activePos[0], tHigh[activePos[0]])),
		})
	}
	// Contract AND + multi-topic OR
	if cHigh != "" && len(activePos) > 0 && tHigh2[activePos[0]] != "" {
		queries = append(queries, QuerySpec{
			Name:        fmt.Sprintf("c-high+multi-t%d(high+high)", activePos[0]),
			ContractIDs: []string{cHigh},
			Topics:      makeTopics(pv(activePos[0], tHigh[activePos[0]], tHigh2[activePos[0]])),
		})
	}
	// Both OR'd: multi-contract + multi-topic
	if cHigh != "" && cHigh2 != "" && len(activePos) > 0 && tHigh2[activePos[len(activePos)-1]] != "" {
		lastPos := activePos[len(activePos)-1]
		queries = append(queries, QuerySpec{
			Name:        fmt.Sprintf("multi-c(high+high)+multi-t%d(high+high)", lastPos),
			ContractIDs: []string{cHigh, cHigh2},
			Topics:      makeTopics(pv(lastPos, tHigh[lastPos], tHigh2[lastPos])),
		})
	}
	// All contracts OR'd + topic
	if len(data.Contracts.High) > 1 && len(activePos) > 0 {
		allContracts := make([]string, 0, len(data.Contracts.High)+len(data.Contracts.Medium))
		allContracts = append(allContracts, data.Contracts.High...)
		allContracts = append(allContracts, data.Contracts.Medium...)
		if len(allContracts) > 1 {
			lastPos := activePos[len(activePos)-1]
			queries = append(queries, QuerySpec{
				Name:        fmt.Sprintf("multi-c(%d)+t%d-high", len(allContracts), lastPos),
				ContractIDs: allContracts,
				Topics:      makeTopics(pv(lastPos, tHigh[lastPos])),
			})
		}
	}
	// Contract + all topics at one position OR'd
	if cHigh != "" && len(activePos) > 0 {
		pos := activePos[0]
		allTopicVals := make([]string, 0)
		for _, v := range topicDatas[pos].High {
			allTopicVals = append(allTopicVals, v)
		}
		for _, v := range topicDatas[pos].Medium {
			allTopicVals = append(allTopicVals, v)
		}
		if len(allTopicVals) > 1 {
			queries = append(queries, QuerySpec{
				Name:        fmt.Sprintf("c-high+multi-t%d(%d)", pos, len(allTopicVals)),
				ContractIDs: []string{cHigh},
				Topics:      makeTopics(pv(pos, allTopicVals...)),
			})
		}
	}

	// =========================================================================
	// Section 5: Worst-case extremes
	// =========================================================================
	// worst-all: all contracts OR'd + all topic positions with all values OR'd
	{
		allContracts := make([]string, 0)
		allContracts = append(allContracts, data.Contracts.High...)
		allContracts = append(allContracts, data.Contracts.Medium...)
		topicsSpec := make([][]string, 4)
		var hasTopics bool
		for pos := 0; pos < 4; pos++ {
			var vals []string
			vals = append(vals, topicDatas[pos].High...)
			vals = append(vals, topicDatas[pos].Medium...)
			if len(vals) > 0 {
				topicsSpec[pos] = vals
				hasTopics = true
			}
		}
		if len(allContracts) > 0 && hasTopics {
			queries = append(queries, QuerySpec{
				Name:        "worst-all",
				ContractIDs: allContracts,
				Topics:      topicsSpec,
			})
		}
	}
	// worst-and-all: high contract + high topic at every active position
	if cHigh != "" && len(activePos) >= 2 {
		pvs := make([]struct {
			pos  int
			vals []string
		}, 0, len(activePos))
		for _, pos := range activePos {
			pvs = append(pvs, pv(pos, tHigh[pos]))
		}
		queries = append(queries, QuerySpec{
			Name:        "worst-and-all",
			ContractIDs: []string{cHigh},
			Topics:      makeTopics(pvs...),
		})
	}

	return queries
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
		indexRead      time.Duration
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
				total: elapsed, index: qr.IndexTime, indexRead: qr.IndexReadTime, indexDecode: qr.IndexDecodeTime, indexIntersect: qr.IndexIntersectTime,
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
		result.P99IndexReadTime = timings[p99Idx].indexRead
		result.P99IndexDecodeTime = timings[p99Idx].indexDecode
		result.P99IndexIntersectTime = timings[p99Idx].indexIntersect
		result.P99EventTime = timings[p99Idx].event
		result.P99EventFetchTime = timings[p99Idx].eventFetch
		result.P99EventDecodeTime = timings[p99Idx].eventDecode
		result.P99EventFilterTime = timings[p99Idx].eventFilter
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
	GroupsDecompressed   int           // Number of group blocks decompressed

	Error error
}

func executeQueryBenchmark(eventStore *store.Store, startLedger, endLedger uint32, contractIDs [][]byte, topicGroups [4][][]byte, limit int) *QueryResult {
	stats, events, err := eventStore.QueryEvents(contractIDs, topicGroups, startLedger, endLedger, limit)
	if err != nil {
		return &QueryResult{Error: err}
	}

	return &QueryResult{
		EventsReturned:      len(events),
		EventsScanned:       stats.EventsScanned,
		SegmentsTouched:     stats.SegmentsTouched,
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
	fmt.Println("query_name,contract_id,topic0,topic1,topic2,topic3,datastore,p50_total_ms,p99_total_ms,p99_idx_ms,idx_read_ms,idx_decode_ms,idx_intersect_ms,segments_touched,index_matches,index_bytes,smallest_list,largest_list,p99_evt_ms,evt_fetch_ms,evt_decode_ms,evt_filter_ms,events_returned,events_scanned,event_bytes,groups_decompressed,iterations,error")

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
