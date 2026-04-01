# Proposal: Use Packfile LiveWriter for Hot Segment Event Storage

## Background

Hot segments are the in-progress segments during event ingestion. Events are written per-ledger and must be queryable before the segment is finalized (frozen). When a segment is complete (10,000 ledgers), it is frozen into the cold format: `events.pack` (packfile) + `index.hash` + `index.pack` (MPHF bitmap index).

Currently there are two hot segment event storage backends, with a third (LiveWriter) recently prototyped:

| | Flat Files | RocksDB | LiveWriter (new) |
|---|---|---|---|
| **Hot format** | `events.dat` + `events.idx` | cfEvents column family | `events.pack` (packfile, unfrozen) |
| **Freeze step** | Read all events back, re-pack into `events.pack` | Iterate cfEvents, re-pack into `events.pack` | `Freeze()` — writes index + trailer in-place |
| **Crash recovery** | Track file length, truncate partial writes | WAL handles partial writes | Packfile Checkpoint (caller-persisted) + `OpenLive` truncates to last sync |
| **Query API (hot)** | Custom: mmap `events.idx` + pread `events.dat` | RocksDB `GetCF` / `BatchedMultiGetCF` | `eventstore.Reader` (same as cold) |
| **Query API (cold)** | `eventstore.Reader` on `events.pack` | `eventstore.Reader` on `events.pack` | `eventstore.Reader` on `events.pack` |

## Problem

Both the flat file and RocksDB paths require a **re-packing step at freeze time**: reading all events from the hot format and writing them into a packfile. For a segment with ~9M events, this takes 3-10 seconds and is pure overhead — the event data is identical, just reformatted.

Additionally, the hot and cold query paths use different APIs, requiring separate reader implementations (`HotSegmentReader`, `RocksDBHotSegmentReader`, `SegmentReader`) and routing logic in `HybridReader`.

## Proposal

Use `packfile.LiveWriter` ([tamirms/event-analysis#3](https://github.com/tamirms/event-analysis/pull/3)) for hot segment event storage. Events are written directly to an `events.pack` file that is incrementally built during ingestion and finalized in-place at segment boundary.

### How it works

**During ingestion:**
```
WriteLedger(events) → lw.Append(encodedEvent) for each event
```
Events are appended to the packfile's internal buffer. Full records (128 events) are flushed to disk automatically. The file is a valid (partial) packfile at all times.

**At segment boundary (freeze):**
```
lw.Freeze(ledgerOffsData)  →  writes packfile index + trailer in-place
os.Rename(hot/NNNNNN/events.pack, cold/NNNNNN/events.pack)
WriteSegmentDir(...)  →  builds index.hash + index.pack from bitmaps
```
No event re-reading. No re-packing. `Freeze()` appends ~100KB of metadata to the existing file.

**Checkpointing and crash recovery:**

The packfile library provides the checkpointing mechanism but does **not** persist the checkpoint — that is the caller's responsibility. `lw.Sync()` fsyncs the file and returns a `Checkpoint` struct. The caller must persist this checkpoint along with application state atomically.

```
// After writing a batch of ledgers:
cp, _ := lw.Sync()        →  fsyncs file, returns Checkpoint
persistCheckpoint(cp, appState)  →  caller writes to checkpoint file or RocksDB
```

The `Checkpoint` contains:
- `Offsets []int64` — byte offsets of each flushed record on disk
- `EndOfData int64` — byte position where the last complete record ends (everything after this is a partial/torn write)
- `Digests []byte` — incremental SHA-256 content hash state
- `RecordSize` / `Format` — validated against config on recovery

The caller must also persist application-level state alongside the checkpoint:
- `lastCommittedLedger` — resume ingestion from here + 1
- `ledgerOffsData` — cumulative event counts per ledger (embedded in events.pack appData on Freeze)
- `nextEventID` / `cumulativeEvents` / `ledgersWritten` — writer state

On recovery:
```
cp, appState := loadCheckpoint()
lw, _ := packfile.OpenLive(path, cp, opts)  →  truncates file to EndOfData, discards partial writes
// Restore application state from appState
// Resume ingestion from appState.lastCommittedLedger + 1
```

`OpenLive` truncates the file to `EndOfData` (removing any bytes written after the last complete record), fsyncs, and reconstructs the writer state from the checkpoint. Only complete records (multiples of RecordSize items) survive — partial records from a crash are discarded. The caller then replays ledgers from `cp.TotalItems()` onward.

**Checkpoint frequency:** Configurable. Default: at segment boundaries (every 10,000 ledgers). More frequent checkpointing (e.g., every 1,000 ledgers) reduces replay work after a crash at the cost of more fsync calls.

**Querying hot segments:**
```
eventstore.Open(hot/NNNNNN/events.pack)  →  same API as cold segments
```
After `Close()` (which calls `Freeze()`), the file is a standard packfile readable by the same `eventstore.Reader` used for cold segments.

### What changes

- **Event storage**: `events.dat` + `events.idx` (flat files) or `cfEvents` (RocksDB) replaced by `events.pack` via LiveWriter
- **Freeze**: Re-packing step eliminated. `Freeze()` finalizes the file in-place (~1ms vs 3-10s)
- **Query path**: Hot and cold segments use the same `eventstore.Reader` API. No separate `HotSegmentReader` needed for events
- **Crash recovery**: Packfile Checkpoint + caller-persisted application state replaces manual file length tracking or RocksDB WAL (for events). The packfile handles file truncation; the caller handles state persistence.

### What stays the same

- **Index deltas**: Still written to `index_deltas.dat` during ingestion for bitmap rebuild and MPHF index build
- **In-memory bitmaps**: Maintained via `IndexStore.AddContractEvent` / `AddTopicEvent` during ingestion
- **MPHF index build**: Still happens at freeze time from flushed bitmaps (`WriteSegmentDir`)
- **Ledger offsets**: Tracked in memory, embedded in `events.pack` appData on Freeze. Must be persisted in checkpoint for crash recovery.
- **RocksDB**: Still available for index-side operations (bitmap snapshots, index deltas CF) if needed

### Advantages

1. **No freeze re-pack** — eliminates 3-10s of I/O at every segment boundary
2. **Uniform query API** — same `eventstore.Reader` for hot and cold, fewer reader implementations
3. **Built-in crash recovery** — Checkpoint/Sync/OpenLive handles partial writes
4. **Simpler architecture** — one file format for events throughout the lifecycle
5. **Concurrent reads during ingestion** — LiveWriter's RWMutex allows query access to in-progress segments (future: HTTP query endpoint alongside ingestion)

### Trade-offs

1. **Newer code path** — LiveWriter is less battle-tested than flat files or RocksDB for event storage (mitigated by 998-line test suite in the PR)
2. **Compression timing** — events are compressed during ingestion (per-record) rather than in a batch at freeze time. Per-ledger write latency may increase slightly with compression enabled
3. **Dependency on packfile PR** — requires [tamirms/event-analysis#3](https://github.com/tamirms/event-analysis/pull/3) to be merged

### Ingestion performance

Measured with flat file hot writer on mainnet data (ledgers 59M-60M). Numbers are indicative of relative cost distribution; absolute values will change with the final storage design.

**Backfill (fsync disabled):**
- Write throughput: ~370K events/sec per segment
- Per-ledger write latency: P50=1.9ms, P99=4.3ms

**Live ingestion (fsync enabled, per-ledger durability):**
- Write throughput: ~230K events/sec per segment
- Per-ledger write latency: P50=3.5ms, P99=8.3ms

**Write time breakdown (per segment, ~8.5M events):**

| Component | Time | % |
|---|---|---|
| Bitmap add | 9.3s | 37% |
| Hash (xxhash128) | 6.2s | 25% |
| Delta write | 3.3s | 13% |
| Encode | 2.7s | 11% |
| Event write | 1.7s | 7% |
| Other | 1.8s | 7% |
| **Total write** | **~25s** | |
| + Fsync (live only) | +11s | |

**Per-ledger write latency:**

| | Backfill | Live |
|---|---|---|
| P50 | 1.9ms | 3.5ms |
| P99 | 4.3ms | 8.3ms |
| Max | 68ms | 33ms |

**Memory:** ~500 MB per segment for in-memory bitmap index (2.6M terms × 200 bytes/entry). 85% of terms are topic2 with cardinality 1 — a tiered storage optimization could reduce this to ~130 MB.

**Freeze overhead:**

| Backend | Freeze time | Notes |
|---|---|---|
| Flat files | ~3s | Read events back + repack into events.pack |
| RocksDB | ~10s | Iterate cfEvents + repack into events.pack |
| LiveWriter | ~1ms | Freeze in-place (write index + trailer only) |

### Backend comparison

| Metric | Flat Files | RocksDB | LiveWriter |
|---|---|---|---|
| Freeze | ~3s (read + repack) | ~10s (iterate + repack) | ~1ms (in-place) |
| Query API (hot) | Custom pread | BatchedMultiGetCF | eventstore.Reader |
| Query API (cold) | eventstore.Reader | eventstore.Reader | eventstore.Reader |
| Crash recovery | Manual truncation | RocksDB WAL | Packfile Checkpoint (caller-persisted) |

### Implementation status

`LiveHotSegmentWriter` and `LiveHotSegmentReader` are implemented and functional. Selectable via `hot_writer = "live"` in `config.toml`. Flat file and RocksDB paths remain available as `hot_writer = "flatfile"` and `hot_writer = "rocksdb"`.

### Recommendation

Adopt `packfile.LiveWriter` as the default hot segment event storage backend. Keep flat file and RocksDB paths available for comparison but deprecate them once LiveWriter is validated in production.
