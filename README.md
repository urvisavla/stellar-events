# Stellar Events

A Go CLI tool to extract Soroban contract events from Stellar ledger files and store them in RocksDB for fast querying. Uses roaring bitmap indexes for efficient range queries across millions of ledgers.

## Features

- Extract events from chunked ledger files (zstd-compressed XDR)
- Store events in RocksDB with roaring bitmap indexes
- Query by ledger range, contract ID, or topics
- Parallel ingestion pipeline with configurable workers
- Progress tracking with performance snapshots
- Resume capability for interrupted ingestion
- Configurable RocksDB tuning for optimal performance

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
# Install dependencies
make deps

# Build (auto-detects macOS vs Linux)
make build

# Or build for specific platform
make build-macos
make build-linux
```

### Manual Build (if not using Make)

RocksDB requires CGO:

```bash
# macOS (Homebrew)
CGO_ENABLED=1 \
CGO_CFLAGS="-I$(brew --prefix)/include" \
CGO_LDFLAGS="-L$(brew --prefix)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
go build -o stellar-events ./cmd/

# Linux
CGO_ENABLED=1 \
CGO_CFLAGS="-I/usr/include" \
CGO_LDFLAGS="-L/usr/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
go build -o stellar-events ./cmd/
```

## Configuration

Copy the example config and modify as needed:

```bash
cp configs/config.example.toml config.toml
```

The tool looks for `config.toml` in the current directory.

## Usage

### Ingest Events

```bash
# Ingest using config settings
./stellar-events ingest

# Override ledger range
./stellar-events ingest --start 1000 --end 2000
```

### Query Events

```bash
# Query by ledger range
./stellar-events query --start 100 --end 200

# Query by contract ID (base64)
./stellar-events query --start 100 --end 200 --contract <base64_id>

# Query by topic (base64)
./stellar-events query --start 100 --end 200 --topic0 <base64_value>

# Use bitmap index for fast queries (default)
./stellar-events query --start 100 --end 200 --contract <id> --bitmap

# Show query statistics
./stellar-events query --start 100 --end 200 --stats
```

### Other Commands

```bash
# Show database statistics
./stellar-events stats
./stellar-events stats --storage --indexes

# Show available ledger ranges in source data
./stellar-events ledgers

# Run manual RocksDB compaction
./stellar-events compact

# Benchmark RocksDB vs ledger file performance
./stellar-events benchmark
```

## Progress Tracking

Ingestion automatically creates progress files:

| File | Description |
|------|-------------|
| `progress_<timestamp>.json` | Current state, updated every 5 seconds |
| `progress_<timestamp>_history.jsonl` | Historical snapshots for trend analysis |

Snapshots are recorded every 1M ledgers and include throughput, timing breakdown, storage metrics, and RocksDB state.

## RocksDB Tuning

```toml
[storage.rocksdb]
# Write performance
write_buffer_size_mb = 64
max_write_buffer_number = 2

# Read performance
block_cache_size_mb = 64
bloom_filter_bits_per_key = 10

# Compression
compression = "lz4"
bottommost_compression = "zstd"
```

### Final Compaction

After ingestion, manual compaction runs by default to merge L0 files and apply compression. Disable for faster ingestion:

```toml
[ingestion]
final_compaction = false
```

## Project Structure

```
stellar-events/
├── cmd/
│   └── main.go              # CLI entry point, all commands
├── internal/
│   ├── auth/                # Authentication (placeholder for future use)
│   ├── config/              # TOML configuration loading
│   ├── index/               # Roaring bitmap indexes
│   ├── ingest/              # Event extraction pipeline
│   ├── progress/            # Ingestion progress tracking
│   ├── reader/              # Ledger file reader (chunked, zstd)
│   └── store/               # RocksDB storage and queries
├── configs/
│   └── config.example.toml  # Example configuration
├── Makefile                 # Build automation
└── config.toml              # Runtime configuration
```

## Storage Architecture

### RocksDB Column Families

| Column Family | Purpose |
|---------------|---------|
| `default` | Metadata (last_processed_ledger, etc.) |
| `events` | Primary event storage (raw XDR) |
| `unique` | Unique value indexes with counts |
| `bitmap` | Roaring bitmap inverted indexes |

### Binary Key Format

Events use a 10-byte binary key: `[ledger:4][tx:2][op:2][event:2]`

### Bitmap Indexes

Roaring bitmap indexes provide fast range queries:
- **L1 (Ledger-level)**: Which ledgers contain events for a given contract/topic
- **L2 (Event-level)**: Which events within a ledger match (optional, for precise lookups)

Segments of 1M ledgers are used to limit write amplification while amortizing overhead.

## Source Data Format

The tool reads from ledger chunk files:

```
<ledger_dir>/
└── chunks/
    ├── 0000/
    │   ├── 000000.data    # Zstd-compressed XDR records
    │   ├── 000000.index   # Byte offsets for random access
    │   └── ...
    └── ...
```

- Each chunk contains 10,000 ledgers
- Data files contain zstd-compressed LedgerCloseMeta XDR
- Index files contain byte offsets for random access

## Event Data

Events are stored as raw XDR for minimal overhead. When queried, they're decoded to include:

| Field | Description |
|-------|-------------|
| `ledger_sequence` | Ledger number |
| `transaction_index` | Transaction position in ledger |
| `operation_index` | Operation position (if applicable) |
| `event_index` | Event position in transaction |
| `contract_id` | Base64-encoded contract ID |
| `type` | `contract`, `system`, or `diagnostic` |
| `topics` | Array of base64-encoded topic values |
| `data` | Base64-encoded event data |
