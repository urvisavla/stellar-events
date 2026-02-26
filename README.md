# Stellar Events

High-performance event indexing and querying system for Stellar blockchain contract events.

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

## Design

### Segments

Events are partitioned into **segments** of 10,000 ledgers each (~14 hours at ~5s/ledger). Each segment is identified by `segmentID = ledger / 10,000`.

Within a segment, each event gets a **dense local ID** — a sequential 32-bit integer assigned in ingestion order. The `segment.meta` file maps dense IDs back to `(ledger, eventSeq)` pairs.

### Storage Modes

Two storage backends can be used independently or together:

| Mode | Config | Description |
|------|--------|-------------|
| **RocksDB** | `storage.rocksdb = true` | Events + bitmap indexes in RocksDB column families |
| **Segment files** | `storage.segment_files = true` | Self-contained flat file directories per segment |

Segment file mode produces a directory per segment with all data needed for queries without RocksDB.

### Segment Directory Layout

```
<segment_path>/NNNNNN/
  events.dat         — block-compressed event data
  events.idx         — block offset index for events.dat
  contracts.hash     — MPHF index (contract term hash → slot)
  contracts.pack     — roaring bitmaps ordered by MPHF slot
  topic0.hash        — MPHF index for topic position 0
  topic0.pack
  topic1.hash
  topic1.pack
  topic2.hash
  topic2.pack
  topic3.hash
  topic3.pack
  segment.meta       — dense ID → (ledger, seq) cumulative array
```

## File Formats

### events.dat — Block-Compressed Event Data

Events are grouped into blocks (default 128 events), each independently zstd-compressed. This enables random access to any event by reading and decompressing only its block.

**Block layout (uncompressed):**
```
[event₀ bytes][event₁ bytes]...[eventₙ₋₁ bytes][FOR index]
```

The **FOR (Frame-of-Reference) index** is a compact encoding of per-event sizes appended to the raw event data. Layout within the block:

```
Offset (from block end)    Content
─────────────────────────────────────────────
end - 1                    W (1 byte) — bits per residual
end - 1 - packSize         packed residuals (ceil(W×N/8) bytes)
end - 1 - packSize - 4     min_size (4 bytes, uint32 LE)
```

Encoding:
1. `min_size` = min event size in block, `max_size` = max
2. `W` = `max(1, bits.Len32(max_size - min_size))`
3. Each event's size stored as `(size - min_size)` in W bits, packed little-endian
4. `packSize` = `ceil(W × N / 8)`

To read event `i` in a block: decode all sizes from the FOR index, compute prefix sum to get byte offset and length.

### events.idx — Block Offset Index

Fixed-size header followed by an array of block offsets into `events.dat`.

```
Offset  Size    Type        Field
──────────────────────────────────────────────
0       4       uint32 LE   magic (0x45494458 = "EIDX")
4       4       uint32 LE   total_events
8       4       uint32 LE   block_size (events per block)
12      4       uint32 LE   flags (bit 0: 1=uncompressed)
16      4       uint32 LE   block_count
20      4       —           reserved (zero)
24      8×(block_count+1)   int64 LE offsets
```

`offsets[i]` is the byte offset in `events.dat` where block `i` starts. `offsets[block_count]` is the total data size (sentinel).

**Read path for event at index `i`:**
1. `blockIdx = i / block_size`, `localIdx = i % block_size`
2. Read compressed bytes from `offsets[blockIdx]..offsets[blockIdx+1]`
3. Decompress, decode FOR index, prefix-sum to locate event `localIdx`

### .hash / .pack — MPHF Bitmap Index

Each index type (contracts, topic0–topic3) is stored as a `.hash` + `.pack` pair.

**.hash** — A [streamhash](https://github.com/tamirms/streamhash) minimal perfect hash function mapping 32-byte term keys (SHA-256 of contract ID or topic XDR) to dense slot indices.

**.pack** — Roaring bitmaps ordered by MPHF slot, followed by an offset trailer:
```
[bitmap₀ bytes][bitmap₁ bytes]...[bitmapₙ₋₁ bytes]
[offset₀: uint64 LE][offset₁: uint64 LE]...[offsetₙ: uint64 LE]
```

The trailer has N+1 entries; `offset[N]` is the sentinel (end of last bitmap). Bitmap `i` spans bytes `offset[i]..offset[i+1]`.

**Lookup:**
1. `slot = hash.Query(termKey)` — O(1), with fingerprint check for false positives
2. Read `offsets[slot]` and `offsets[slot+1]` from the pack trailer
3. `pread(pack, offset, length)` → roaring bitmap bytes
4. `roaring.UnmarshalBinary(bytes)` → set of matching dense local IDs

### segment.meta — Ledger Map

A fixed 40,000-byte cumulative count array (10,000 × uint32 LE).

Entry `i` = total events in ledgers at offsets 0 through `i` (inclusive cumulative sum). Used to convert between dense local IDs and `(ledger, eventSeq)` pairs:

- **Ledger range → ID range** — O(1) lookup for query range trimming
- **Dense ID → (ledger, seq)** — O(log N) binary search for result decoding

## Configuration

```toml
[source]
ledger_dir = "./data/ledgers"
network = "mainnet"

[storage]
db_path = "./rocksdb/events"
rocksdb = true                 # write to RocksDB
segment_files = false          # write segment flat files
segment_path = "./data/segments"
compress_data = false          # zstd compress events.dat blocks
block_size = 128               # events per compression block

[ingestion]
workers = 0                    # 0 = all CPUs
batch_size = 100               # ledgers per write batch
unique_indexes = false         # maintain unique value counts

[query]
max_ledger_range = 100000
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

## Project Structure

```
stellar-events/
├── cmd/                        # CLI commands
├── internal/
│   ├── config/                 # TOML configuration
│   ├── eventstore/             # Block-compressed event file reader/writer
│   ├── ingest/                 # Parallel ingestion pipeline
│   ├── packfile/               # FOR (Frame-of-Reference) codec
│   ├── query/                  # Query types and engine
│   ├── reader/                 # Ledger file reader
│   ├── store/                  # RocksDB storage, bitmap indexes, segment files
│   └── zstd/                   # Zstd compression wrapper
├── configs/
│   └── config.example.toml
└── Makefile
```
