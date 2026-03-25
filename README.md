# Stellar Events

High-performance event indexing and querying system for Stellar blockchain contract events.

## Installation

```bash
make deps
make build
```

## Design

### Segments

Events are partitioned into **segments** of 10,000 ledgers each (~14 hours at ~5s/ledger). Each segment is identified by `segmentID = ledger / 10,000`.

Within a segment, each event gets a **dense local ID** — a sequential 32-bit integer assigned in ingestion order. Ledger offsets embedded in `events.pack` appData map dense IDs back to `(ledger, eventSeq)` pairs.

### Storage

Events are stored as self-contained flat file directories per segment under `<segment_path>/cold/`. Each segment directory contains all data needed for queries — no external database required.

**SegmentReader caching**: The query path caches opened resources to avoid repeated file opens across queries:
- **mmap cache** — mmap'd `.pack` files
- **hash cache** — streamhash index objects (`.hash` files)
- **eventstore reader cache** — packfile readers for event data
- **appData cache** — ledger offset arrays from `events.pack`
- **record size cache** — packfile record sizes (block size) from trailers
- `PurgeCache()` closes and clears all caches, used by cold-cache benchmarks to ensure OS page cache purge is effective

### Segment Directory Layout

```
<segment_path>/cold/NNNNNN/
  events.pack    — packfile: block-compressed events + ledger offset appData
  index.hash     — streamhash MPHF for all fields (contracts + topics 0-3)
  index.pack     — unified roaring bitmaps for all fields
```

## File Formats

### events.pack — Packfile Format

Block-compressed event data stored as a packfile with an embedded ledger offset array.

**Structure (from EOF):**
- **Trailer** (64 bytes at EOF): magic `"SLCH"`, version, record count, total items, record size, index size, appData size, SHA-256 content hash, CRC32C
- **Records**: block-compressed (zstd, default 128 events/block) event data
- **Offset index**: FOR-128 encoded (delta-coded record sizes in groups of 128, CRC32C per group). Compact and fast to decode for random access.
- **AppData**: 40,000-byte ledger offset array (10,000 × uint32 LE cumulative counts) — replaces separate `segment.meta`

**Speculative tail read**: `Open()` reads the last 256KB on open, which usually captures trailer + index + appData in a single I/O, avoiding extra reads.

### index.hash — Unified MPHF

A [streamhash](https://github.com/tamirms/streamhash) minimal perfect hash function mapping 32-byte composite keys to dense slots. A single hash file covers all 5 fields (contracts + topic0–topic3).

**Key format**: `[xxh3(composite):16][termHash:16]` where `composite = [termHash:16][fieldIndex:1]`

### index.pack — Unified Bitmap Pack

Roaring bitmaps for all fields packed into a single file, followed by an offset trailer.

**Per-record**: `[fingerprint:4][fieldIndex:1][bitmap bytes]`
- **fingerprint** = first 4 bytes of xxh3 hash, used for false-positive rejection
- **fieldIndex** = 0 (contracts), 1–4 (topic0–topic3)

**Trailer**: (N+1) uint64 LE offsets. Bitmap `i` spans bytes `offset[i]..offset[i+1]`.

**Lookup:**
1. `slot = hash.Query(queryKey)` — O(1) with fingerprint check
2. Read `offsets[slot]` and `offsets[slot+1]` from the pack trailer
3. Verify fingerprint and fieldIndex match
4. `roaring.UnmarshalBinary(bitmapBytes)` → set of matching dense local IDs

## Packfile Read Optimizations

Three read paths for different access patterns:

1. **Sequential range reads** (`ReadRange`): Coalesces consecutive records into single `ReadAt` calls using pooled 1MB buffers. Minimizes syscalls for large contiguous scans.
2. **Scattered reads** (`ReadItems`): Parallel I/O across goroutines (default 8). Partitions requested indices into batches, each batch coalesces adjacent records. Work-stealing via atomic counter.
3. **Single item reads** (`ReadItem`): Reads and decompresses only the containing record (128 events). O(1) record location via offset index.

Other optimizations:
- **Speculative open**: `Open()` returns immediately; all I/O (stat, trailer parse, index decode, appData read) runs in a background goroutine
- **Speculative tail read**: Last 256KB read on open usually captures trailer + index + appData in single I/O, avoiding extra reads
- **Pooled decoders**: `sync.Pool` of zstd decompressors (each owns a C-allocated `ZSTD_DCtx`) avoids repeated allocation
- **Pooled read buffers**: 1MB `sync.Pool` buffers reused across `ReadRange`/`ReadItems` calls
- **FOR-128 offset index**: Frame-of-Reference delta encoding of record sizes in groups of 128, with CRC32C per group

## Configuration

```toml
[source]
ledger_dir = "./data/ledgers"
network = "mainnet"

[storage]
segment_files = true               # flat file segment backend (default)
segment_path = "./lfs/events"      # base directory for segment files
compress_data = true               # zstd compress event data blocks
block_size = 128                   # events per compression block

[ingestion]
disable_fsync = true               # skip fsync during writes
progress_file = "progress.json"    # progress tracking (empty = disabled)
final_compaction = false           # run compaction after ingestion
compute_stats = false              # compute event stats after ingestion
unique_indexes = true              # maintain unique value counts
workers = 0                        # 0 = all CPUs
batch_size = 100                   # ledgers per write batch
queue_size = 0                     # 0 = workers * 2

[query]
max_ledger_range = 10000
default_limit = 100
```

## Usage

### Ingestion

```bash
# Ingest ledger range
./stellar-events ingest --start 56000000 --end 57000000

# Resume from last processed ledger
./stellar-events ingest --end 57000000
```

### Backfill

```bash
# Bulk historical ingestion directly to cold segments
./stellar-events backfill --start 56000000 --end 57000000

# Auto-detect end ledger from source data
./stellar-events backfill --start 56000000
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

### Benchmark

```bash
# Generate test data (contracts/topics by cardinality) from indexed segments
./stellar-events benchmark --generate-data data.json

# Generate query plan from test data
./stellar-events benchmark --data data.json --generate-queries queries.json

# Run benchmark with pre-generated queries
./stellar-events benchmark --data data.json --queries queries.json

# Validate query results (check for zero-result queries)
./stellar-events benchmark --data data.json --queries queries.json --validate
```

### Inspect

```bash
# Inspect all segments
./stellar-events inspect

# Inspect a single segment with verbose stats
./stellar-events inspect --segment 5600 --verbose
```

### Statistics

```bash
./stellar-events stats
```

## Project Structure

```
stellar-events/
├── cmd/                        # CLI commands
├── internal/
│   ├── config/                 # TOML configuration
│   ├── event/                  # Event types
│   ├── ingest/                 # Parallel ingestion pipeline
│   ├── progress/               # Progress tracking
│   ├── query/                  # Query types and engine
│   ├── store/                  # Segment file storage, bitmap indexes, caching
│   └── zstd/                   # Zstd compression wrapper
├── configs/
│   └── config.example.toml
└── Makefile
```
