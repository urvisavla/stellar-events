package main

import (
	"fmt"
	"os"

	"github.com/urvisavla/stellar-events/internal/config"
)

// =============================================================================
// Usage / Main
// =============================================================================

func printUsage() {
	fmt.Fprintf(os.Stderr, "Stellar Events - Extract and query Soroban contract events\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  ingest         Ingest events via hot files, converting to cold on segment boundaries\n")
	fmt.Fprintf(os.Stderr, "  backfill       Bulk-ingest historical events directly to cold segment files\n")
	fmt.Fprintf(os.Stderr, "  query          Query events from RocksDB\n")
	fmt.Fprintf(os.Stderr, "  stats          Show database statistics\n")
	fmt.Fprintf(os.Stderr, "  benchmark      Benchmark query performance across index types\n")
	fmt.Fprintf(os.Stderr, "  inspect        Inspect segment directories and report term counts\n")
	fmt.Fprintf(os.Stderr, "\nConfiguration:\n")
	fmt.Fprintf(os.Stderr, "  Requires stellar-events.toml or config.toml in current directory\n")
	fmt.Fprintf(os.Stderr, "  See configs/stellar-events.example.toml for reference\n")
	fmt.Fprintf(os.Stderr, "\nExamples:\n")
	fmt.Fprintf(os.Stderr, "  %s ingest --start 1000 --end 2000        # hot→cold ingestion\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s backfill --start 1000 --end 2000      # bulk cold-only backfill\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s query 55000000 56000000 --contract <base64_id>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s stats\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s benchmark --data benchmark_data.json\n", os.Args[0])
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	command := os.Args[1]
	args := os.Args[2:]

	// Help without config
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
		runIngest(cfg, args)
	case "backfill":
		runBackfill(cfg, args)
	case "query":
		runQuery(cfg, args)
	case "stats":
		runStats(cfg, args)
	case "benchmark":
		runBenchmark(cfg, args)
	case "inspect":
		runInspect(cfg, args)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(2)
	}
}
