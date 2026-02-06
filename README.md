# Stellar Events

High-performance event indexing and querying system for Stellar blockchain contract events.

## Overview

Stellar Events ingests contract events from Stellar ledger files and stores them in RocksDB with multiple index types optimized for different query patterns. The system supports both V1 (TOID-based) and V2 (sequential ID-based) storage formats.

## Prerequisites

### RocksDB

**macOS (Homebrew):**
```bash
brew install rocksdb
```

**Ubuntu/Debian:**
```bash
apt-get install librocksdb-dev
```

## Installation

```bash
make deps
make build
```

## Storage Architecture

### Column Families

Events are stored in RocksDB with separate column families for primary storage and indexes:

| Column Family | Purpose |
|---------------|---------|
| `events` | Primary event storage (XDR or binary format) |
| `default` | Metadata (last processed ledger, etc.) |
| `unique` | Unique value counts for statistics |

### V1 Index Column Families

| Column Family | Purpose |
|---------------|---------|
| `contracts_pl`, `topics_pl` | Posting list indexes (TOID-based) |
| `contracts_bm`, `topics_bm` | 32-bit ledger-level bitmap indexes |
| `contracts_bm64`, `topics_bm64` | 64-bit event-level bitmap indexes |

### V2 Index Column Families

| Column Family | Purpose |
|---------------|---------|
| `contracts_bm32`, `topics_bm32` | 32-bit event-level bitmap indexes |
| `contracts_plv2`, `topics_plv2` | V2 posting list indexes (local ID-based) |

V1 and V2 indexes are mutually exclusive due to different event key formats.

## Key Structures

### V1 Event Key (10 bytes)

Used when V1 indexes are enabled:

```
[TOID:8][event_index:2]
```

**TOID (Transaction Order ID):**
- Bits 63-32: ledger sequence (32 bits)
- Bits 31-12: transaction index (20 bits, max 1,048,575)
- Bits 11-0: operation index (12 bits, max 4,095)

**event_index:** 16-bit index within the operation

Multiple events can share the same TOID (same operation), requiring range scans during fetch.

### V2 Event Key (6 bytes)

Used when V2 indexes are enabled:

```
[ledger:4][event_seq:2]
```

- **ledger**: 32-bit ledger sequence number
- **event_seq**: 16-bit sequential event number within the ledger (0, 1, 2, ...)

Each V2 key maps to exactly one event, enabling point lookups instead of range scans.

### Index Key (36 bytes)

Used by all posting list indexes:

```
[term_key:32][bucket_id:4]
```

- **term_key**: SHA-256 hash of the indexed value (contract ID or topic XDR)
- **bucket_id**: ledger / 10,000

## Bucketing

All indexes are partitioned into buckets of 10,000 ledgers each (~14 hours at ~5 seconds/ledger).

**Benefits:**
- Bounded memory usage during ingestion
- Efficient range queries (only relevant buckets scanned)
- Incremental updates (new data only affects current bucket)
- Natural time-based partitioning

### Local IDs (V2)

Within a bucket, events are identified by 32-bit local IDs:

```
local_id = (ledger_offset << 16) | event_seq
```

- **ledger_offset**: ledger - bucket_start (0-9,999, fits in 14 bits)
- **event_seq**: sequential event number within ledger (0-65,535)

This compact 32-bit representation enables efficient bitmap and posting list storage while maintaining event-level granularity.

## Index Design

### Posting Lists

Posting lists map index terms (contract IDs, topics) to event locations using delta-varint encoding for compression.

**V1 Format:** Stores 64-bit TOIDs
```
[count:varint][first_toid:8][delta1:varint][delta2:varint]...
```

**V2 Format:** Stores 32-bit local IDs
```
[count:varint][first_id:8][delta1:varint][delta2:varint]...
```

Delta encoding exploits the sequential nature of events — consecutive events have small deltas, often encoding in 1-2 bytes instead of 4-8.

**Query Optimizations:**
- Parallel reads: contract and topic posting lists read concurrently
- Guided intersection: lists sorted by size, smallest intersected first
- Streaming: single-filter queries fetch bucket-by-bucket with early termination

### Bitmap Indexes

Bitmap indexes use Roaring Bitmaps for space-efficient set operations.

**32-bit Ledger Bitmap:** Maps terms to ledger numbers. Returns matching ledgers, then events within those ledgers are scanned. Good for high-selectivity queries.

**64-bit Event Bitmap:** Maps terms to TOIDs. Event-level granularity but with 64-bit storage overhead.

**32-bit Event Bitmap (V2):** Maps terms to local IDs. Event-level granularity with compact 32-bit storage. Best query performance when combined with V2 event keys.

## Configuration

### Index Selection

In `config.toml` under `[ingestion]`:

```toml
[ingestion]
# V1 indexes (10-byte TOID-based event keys)
bitmap_indexes = false        # ledger-level bitmap
bitmap64_indexes = false      # event-level bitmap (64-bit TOIDs)
posting_list_indexes = false  # TOID-based posting lists

# V2 indexes (6-byte sequential event keys)
v2_indexes = true             # enables bitmap32-event + posting-v2
```

V1 and V2 cannot be enabled simultaneously — they use incompatible event key formats.

### Storage Options

```toml
[storage]
db_path = "./events.db"
event_format = "binary"       # "xdr" or "binary" (binary is faster for queries)
compression = "zstd"
```

### Performance Tuning

```toml
[ingestion]
workers = 8                   # parallel ledger readers
batch_size = 100              # ledgers per write batch
index_flush_interval = 10000  # flush indexes every N ledgers
```

## Usage

### Ingestion

```bash
# Ingest ledger range
./stellar-events ingest --start 56000000 --end 57000000

# Resume from last processed ledger
./stellar-events ingest --end 57000000
```

### Query

```bash
# Query by contract ID
./stellar-events query --start 56000000 --end 56100000 --contract <strkey>

# Query by topic
./stellar-events query --start 56000000 --end 56100000 --topic0 <base64>

# Combined filters
./stellar-events query --start 56000000 --end 56100000 --contract <strkey> --topic0 <base64>
```

### Statistics

```bash
./stellar-events stats
```

Shows event counts, ledger range, and column family sizes.

## Benchmarking

### 1. Generate Test Data File

Create a benchmark data file with contract IDs and topics to test:

```bash
./stellar-events benchmark --generate > benchmark_test.json
```

Edit `benchmark_test.json` to include actual contract IDs and topics from your data, categorized by cardinality (high/medium/low event counts).

### 2. Run Benchmarks

```bash
# Test all index types
./stellar-events benchmark --data benchmark_test.json --index all

# Test specific index type
./stellar-events benchmark --data benchmark_test.json --index posting-v2

# With custom options
./stellar-events benchmark --data benchmark_test.json \
  --index posting-v2 \
  --iterations 10 \
  --warmup 2 \
  --limit 1000 \
  --timeout 30s
```

### 3. Output Formats

```bash
# Table format (default, to stdout)
./stellar-events benchmark --data benchmark_test.json

# CSV format
./stellar-events benchmark --data benchmark_test.json --format csv

# JSON format
./stellar-events benchmark --data benchmark_test.json --format json

# Write results to file
./stellar-events benchmark --data benchmark_test.json --output results.csv
```

### Available Index Types

| Index Type | Description |
|------------|-------------|
| `posting` | V1 posting list (sequential reads) |
| `posting-parallel` | V1 posting list (parallel reads + guided intersection) |
| `posting-v2` | V2 posting list (parallel + point gets) |
| `bitmap32` | Ledger-level bitmap |
| `bitmap64` | Event-level bitmap (TOID-based) |
| `bitmap32-event` | Event-level bitmap (local ID-based, V2) |
| `all` | Run all index types |

### Benchmark Options

| Flag | Description |
|------|-------------|
| `--data` | Path to benchmark data JSON file (required) |
| `--index` | Index type(s) to test (default: all) |
| `--iterations` | Number of iterations per query (default: 5) |
| `--warmup` | Warmup iterations not counted (default: 1) |
| `--limit` | Max events to fetch per query (default: 1000) |
| `--timeout` | Timeout per query (default: 30s) |
| `--fixed-range` | Use same ledger range for all queries |
| `--max-combinations` | Max query combinations to test (default: 50) |
| `--seed` | Random seed for reproducibility |
| `--log` | Log file for per-query details |
| `--output` | Output file for results |
| `--format` | Output format: table, csv, json |

### Benchmark Output Metrics

| Metric | Description |
|--------|-------------|
| P50/P99 Time | Query latency percentiles |
| Index Matches | TOIDs, local IDs, or ledgers matched by index |
| Events Scanned | Events read from storage |
| Events Returned | Events after filtering |
| Index Bytes | Bytes read from index |
| Event Bytes | Bytes read from event storage |
| Index Read Time | I/O time reading index |
| Index Decode Time | CPU time decoding index |

### Tips

- Use `--fixed-range` when comparing index types to ensure identical query ranges
- Use `--warmup 2` to prime OS and RocksDB caches
- Check `--log` output for per-query debugging
- V2 indexes (`posting-v2`, `bitmap32-event`) require data ingested with `v2_indexes = true`

## Performance Comparison

| Index Type | Granularity | Event Fetch | Best For |
|------------|-------------|-------------|----------|
| `bitmap32` | Ledger | Scan all events in ledger | High selectivity (few ledgers match) |
| `bitmap64` | Operation | Range scan per TOID | Medium selectivity |
| `bitmap32-event` | Event | Point get | Low selectivity, V2 data |
| `posting` | Operation | Range scan per TOID | Multi-filter queries |
| `posting-v2` | Event | Point get | Multi-filter queries, V2 data |

V2 indexes provide the best query performance due to point gets, but require re-ingesting data with `v2_indexes = true`.

## Project Structure

```
stellar-events/
├── cmd/                      # CLI commands
├── internal/
│   ├── config/               # TOML configuration
│   ├── index/                # Bitmap and posting list indexes
│   ├── ingest/               # Parallel ingestion pipeline
│   ├── query/                # Query types and engine
│   ├── reader/               # Ledger file reader
│   └── store/                # RocksDB storage layer
├── configs/
│   └── config.example.toml   # Example configuration
└── Makefile
```
