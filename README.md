# Stellar Events

A Go tool to extract Soroban contract events from Stellar ledger files and store them in RocksDB for fast querying.

## Features

- Extract events from ledger files
- Store events in RocksDB with secondary indexes
- Query by ledger, contract ID, transaction hash, event type, or topics
- Progress tracking with performance snapshots
- Resume capability for interrupted ingestion
- Configurable RocksDB tuning for optimal performance
- Final compaction for read-optimized storage

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
go mod download

# macOS with Homebrew
CGO_CFLAGS="-I/opt/homebrew/include" \
CGO_LDFLAGS="-L/opt/homebrew/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
go build -o stellar-events ./cmd/

# Linux (adjust paths as needed)
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

# Resume from a previous progress file
./stellar-events ingest --resume progress_20240118T143052.json
```

### Query Events

```bash
# Query by ledger
./stellar-events query --ledger 12345
./stellar-events query --start 100 --end 200

# Query by contract ID (base64)
./stellar-events query --contract <base64_id>

# Query by transaction hash (hex)
./stellar-events query --txhash <hex_hash>

# Query by event type
./stellar-events query --type contract
./stellar-events query --type system
./stellar-events query --type diagnostic

# Query by topic (base64)
./stellar-events query --topic0 <base64_value>
./stellar-events query --topic1 <base64_value>
```

### Database Statistics

```bash
# Basic stats
./stellar-events stats

# Include RocksDB storage metrics
./stellar-events stats --storage

# Include index entry counts
./stellar-events stats --indexes

# Show source ledger data stats
./stellar-events stats --source

# Dump all stats to JSON
./stellar-events stats --storage --indexes --dump stats.json
```

## Progress Tracking

Ingestion automatically creates progress files for monitoring and resume capability:

| File | Description |
|------|-------------|
| `progress_<timestamp>.json` | Current state, updated every 5 seconds |
| `progress_<timestamp>.jsonl` | Historical snapshots for trend analysis |
| `progress_<timestamp>_summary.txt` | Final summary after ingestion |

### Snapshot Metrics

Snapshots are recorded every 1M ledgers and include:

- Throughput (ledgers/sec, events/sec)
- Timing breakdown (read vs write time)
- Storage metrics (SST size, memtable)
- RocksDB state (L0 files, compaction status)

## Ingestion Summary

After ingestion completes, a summary is printed with:

- **Results**: Ledgers, events, transactions processed
- **Performance**: Average throughput rates
- **Storage**: Raw data size, RocksDB size, compression savings
- **Compaction**: Before/after metrics if final compaction ran
- **RocksDB State**: File counts by level

## RocksDB Tuning

The config supports tuning for different workloads:

```toml
[storage.rocksdb]
# Write performance
write_buffer_size_mb = 64
max_write_buffer_number = 2

# Read performance
block_cache_size_mb = 64
bloom_filter_bits_per_key = 10

# Compaction (increase for write-heavy ingestion)
level0_file_num_compaction_trigger = 4
max_background_compactions = 4

# Compression
compression = "lz4"
bottommost_compression = "zstd"
```

### Final Compaction

After ingestion, manual compaction runs by default to:
- Merge L0 files for faster reads
- Apply bottommost compression
- Reclaim space from deleted tombstones

Disable for faster ingestion if you'll compact later:
```toml
[ingestion]
final_compaction = false
```

## Project Structure

```
stellar-events/
├── cmd/
│   └── main.go                  # CLI entry point
├── internal/
│   ├── config/
│   │   └── config.go            # TOML config loading
│   ├── reader/
│   │   └── ledger.go            # Ledger file reader
│   ├── ingest/
│   │   └── extractor.go         # Event extraction from XDR
│   ├── store/
│   │   └── rocksdb.go           # RocksDB storage & queries
│   └── progress/
│       └── tracker.go           # Ingestion progress tracking
├── configs/
│   └── config.example.toml
├── go.mod
└── README.md
```

## Secondary Indexes

The following indexes are created for efficient querying:

| Index | Key Format | Description |
|-------|-----------|-------------|
| Primary | `events:<ledger>:<tx>:<event>` | All events by ledger |
| Contract | `contract:<id>:<ledger>:<tx>:<event>` | Events by contract ID |
| TxHash | `txhash:<hash>:<event>` | Events by transaction |
| Type | `type:<type>:<ledger>:<tx>:<event>` | Events by type |
| Topic0-3 | `topic0:<value>:<ledger>:<tx>:<event>` | Events by topic position |

## Source Data Format

The tool reads from ledger chunk files:

```
<ledger_dir>/
└── chunks/
    ├── 0000/
    │   ├── 000000.data    # Compressed ledger data
    │   ├── 000000.index   # Byte offsets
    │   └── ...
    └── ...
```

- Each chunk contains 10,000 ledgers
- Data files contain zstd-compressed XDR records
- Index files contain byte offsets for random access

## Event Data Structure

Events are stored as JSON with the following fields:

| Field | Description |
|-------|-------------|
| `ledger_sequence` | Ledger number |
| `transaction_index` | Transaction position in ledger |
| `operation_index` | Operation position (if applicable) |
| `event_index` | Event position in transaction |
| `contract_id` | Base64-encoded contract ID |
| `type` | `contract`, `system`, or `diagnostic` |
| `event_stage` | Event stage for transaction events |
| `topics` | Array of base64-encoded topic values |
| `data` | Base64-encoded event data |
| `transaction_hash` | Hex transaction hash |
| `successful` | Whether transaction succeeded |