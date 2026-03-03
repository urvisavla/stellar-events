package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// EventStore manages storing and querying events.
// Coordinates RocksDB storage, in-memory bitmap indexes, and flat file segments.
type EventStore struct {
	rocksDB *RocksDBBackend // nil when RocksDB disabled

	// Index coordinator (owns in-memory bitmap + delegates to IndexFlusher)
	indexStore *IndexStore

	// Flat file segment directory path (empty = disabled)
	segmentPath string

	// Unified query backend (RocksDB or flat files)
	queryBackend QueryBackend

	// Segment data writer for flat file event storage (nil = disabled)
	segmentDataWriter *SegmentDataWriter

	// Tracks the last segment ID seen during StoreEvents for auto-finalization.
	lastSegmentID uint32
	hasLastSegment bool
}

// EventStoreOptions configures an EventStore.
type EventStoreOptions struct {
	// RocksDB (optional — nil disables RocksDB)
	DBPath    string
	RocksOpts *RocksDBOptions

	// Flat file segments
	SegmentPath       string // base directory for segment files (empty = disabled)
	WriteSegmentFiles bool   // write index + data files during ingest
	CompressData      bool   // zstd-compress event data in segment files
	BlockSize         int    // group size for compressed event blocks
}

// NewEventStore creates an EventStore from the given options.
func NewEventStore(opts EventStoreOptions) (*EventStore, error) {
	es := &EventStore{
		segmentPath: opts.SegmentPath,
	}

	if opts.RocksOpts != nil {
		backend, err := NewRocksDBBackend(opts.DBPath, opts.RocksOpts)
		if err != nil {
			return nil, err
		}
		es.rocksDB = backend
		es.indexStore = NewIndexStore(backend.bitmapStore, backend.bitmapStore, backend.bitmapStore)
	} else {
		flusher := &SegmentIndexFlusher{}
		es.indexStore = NewIndexStore(flusher, nil, nil)
	}

	// Configure index write targets
	segPath := ""
	if opts.WriteSegmentFiles {
		segPath = opts.SegmentPath
	}
	es.indexStore.SetWriteConfig(segPath, es.rocksDB != nil)

	// Initialize segment data writer if segment files are enabled
	if opts.WriteSegmentFiles && opts.SegmentPath != "" {
		es.segmentDataWriter = NewSegmentDataWriter(opts.SegmentPath, opts.CompressData, opts.BlockSize)
	}

	// Initialize query backend
	if opts.SegmentPath != "" {
		es.queryBackend = NewSegmentReader(opts.SegmentPath)
	} else if es.rocksDB != nil {
		es.queryBackend = NewRocksDBReader(
			es.indexStore.bitmap,
			NewRocksDBEventFetcher(es.rocksDB.db, es.rocksDB.ro, es.rocksDB.cfEvents),
			es.rocksDB.bitmapStore,
		)
	}

	return es, nil
}

// RocksDB returns the underlying RocksDBBackend for direct access to stats methods.
func (es *EventStore) RocksDB() *RocksDBBackend {
	return es.rocksDB
}

// StoreEvents stores events with optional index updates based on options.
// Automatically finalizes completed segments when a segment boundary is crossed.
// Returns the number of bytes written.
func (es *EventStore) StoreEvents(events []*event.IngestEvent, opts *StoreOptions) (int64, error) {
	var totalBytes int64

	if opts == nil {
		opts = &StoreOptions{}
	}

	var kvs []EventKV
	countUpdates := make(map[string]uint64)
	var maxLedger uint32

	for _, ev := range events {
		if es.indexStore != nil {
			segmentID := SegmentID(ev.LedgerSequence)
			if ev.LedgerSequence > maxLedger {
				maxLedger = ev.LedgerSequence
			}

			// Auto-finalize completed segment on boundary crossing.
			if es.hasLastSegment && segmentID != es.lastSegmentID {
				if err := es.finalizeCompletedSegment(es.lastSegmentID); err != nil {
					return 0, fmt.Errorf("failed to finalize segment %d: %w", es.lastSegmentID, err)
				}
			}
			es.lastSegmentID = segmentID
			es.hasLastSegment = true

			denseLocalID := es.indexStore.AssignDenseLocalID(segmentID, ev.LedgerSequence)

			key := EncodeKey(segmentID, denseLocalID)
			value := event.EncodeBinaryEvent(ev)
			if es.rocksDB != nil {
				kvs = append(kvs, EventKV{Key: key, Value: value})
			}
			totalBytes += int64(len(value))

			if len(ev.ContractID) > 0 {
				es.indexStore.AddContractEvent(ev.ContractID, segmentID, denseLocalID)
			}

			for pos, topicBytes := range ev.Topics {
				es.indexStore.AddTopicEvent(pos, topicBytes, segmentID, denseLocalID)
			}

			if es.segmentDataWriter != nil {
				if !es.segmentDataWriter.IsActive() {
					if err := es.segmentDataWriter.StartChunk(segmentID); err != nil {
						return 0, fmt.Errorf("failed to start segment data chunk %d: %w", segmentID, err)
					}
				}
				v4Data := event.EncodeBinaryEvent(ev)
				if err := es.segmentDataWriter.AppendEvent(denseLocalID, v4Data); err != nil {
					return 0, fmt.Errorf("failed to append event to file store: %w", err)
				}
			}
		}

		if opts.UniqueIndexes {
			if len(ev.ContractID) > 0 {
				uk := string(uniqueKey(UniqueTypeContract, ev.ContractID))
				countUpdates[uk]++
			}

			for i, topicBytes := range ev.Topics {
				if i > 3 {
					break
				}
				var uniqueType byte
				switch i {
				case 0:
					uniqueType = UniqueTypeTopic0
				case 1:
					uniqueType = UniqueTypeTopic1
				case 2:
					uniqueType = UniqueTypeTopic2
				case 3:
					uniqueType = UniqueTypeTopic3
				}
				uk := string(uniqueKey(uniqueType, topicBytes))
				countUpdates[uk]++
			}
		}
	}

	if es.rocksDB != nil && (len(kvs) > 0 || len(countUpdates) > 0) {
		if err := es.rocksDB.WriteEventBatch(kvs, countUpdates); err != nil {
			return 0, err
		}
		if maxLedger > 0 {
			if err := es.rocksDB.SetLastProcessedLedger(maxLedger); err != nil {
				return 0, fmt.Errorf("failed to update last processed ledger: %w", err)
			}
		}
	}

	return totalBytes, nil
}

// finalizeCompletedSegment flushes in-memory bitmap indexes (for both RocksDB
// and flat file paths) and, when segment files are configured, writes flat file
// indexes for the given segment.
func (es *EventStore) finalizeCompletedSegment(segmentID uint32) error {
	if err := es.indexStore.Flush(); err != nil {
		return fmt.Errorf("flush bitmap indexes: %w", err)
	}
	if es.segmentPath == "" {
		return nil
	}
	return FinalizeSegment(es.indexStore, es.segmentPath, segmentID, es.segmentDataWriter)
}


// Finalize flushes in-memory bitmap indexes and finalizes the last segment.
// Must be called after ingestion completes to ensure the final segment is written.
func (es *EventStore) Finalize() error {
	if !es.hasLastSegment {
		return nil
	}
	return es.finalizeCompletedSegment(es.lastSegmentID)
}

// QueryEvents executes a query via the query backend.
// When filters (contractIDs / topicGroups) are provided, uses bitmap indexes to
// find matching events. Otherwise falls back to a sequential range scan.
func (es *EventStore) QueryEvents(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error) {
	hasFilters := len(contractIDs) > 0
	if !hasFilters {
		for _, tg := range topicGroups {
			if len(tg) > 0 {
				hasFilters = true
				break
			}
		}
	}

	if !hasFilters {
		return es.queryBackend.FetchByRange(startLedger, endLedger, limit)
	}

	totalStart := time.Now()
	result := &QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	perSegment, err := collectBitmaps(es.queryBackend, contractIDs, topicGroups, startLedger, endLedger, result)
	if err != nil {
		return nil, nil, err
	}

	if result.MatchingLocalIDs == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	events, err := es.queryBackend.FetchByIDs(perSegment, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// Close closes the event store.
func (es *EventStore) Close() {
	if es.indexStore != nil {
		es.indexStore.Close()
	}

	if es.queryBackend != nil {
		es.queryBackend.Close()
	}

	if es.rocksDB != nil {
		es.rocksDB.Close()
	}
}

// collectBitmaps loads bitmaps for a query and intersects them per segment.
// Contract IDs are OR'd within group 0, topic values are OR'd within each
// positional group (1-4), and the resulting group bitmaps are AND'd per segment.
// Returns the per-segment intersection bitmaps and populates index stats on result.
func collectBitmaps(
	loader BitmapLoader,
	contractIDs [][]byte,
	topicGroups [4][][]byte,
	startLedger, endLedger uint32,
	result *QueryResult,
) (map[uint32]*roaring.Bitmap, error) {
	segments := GetSegmentsForRange(startLedger, endLedger)
	result.SegmentsTouched = len(segments)

	indexStart := time.Now()

	// Collect per-group, per-segment bitmaps.
	// groupIdx 0 = contracts, 1-4 = topic positions.
	type groupSegBitmaps struct {
		bitmaps map[uint32][]*roaring.Bitmap
	}
	groups := make(map[int]*groupSegBitmaps)

	// Query contract bitmaps (OR within contracts group).
	if len(contractIDs) > 0 {
		groups[0] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, cid := range contractIDs {
			termKey := ContractTermKey(cid)
			for _, segID := range segments {
				bm, stats, err := loader.LoadTermBitmap(segID, 0, termKey, startLedger, endLedger)
				if err != nil {
					continue
				}
				result.IndexBytesRead += stats.BytesRead
				result.IndexReadTime += stats.ReadTime
				result.IndexDecodeTime += stats.DecodeTime
				if bm != nil && !bm.IsEmpty() {
					result.SegmentsScanned++
					groups[0].bitmaps[segID] = append(groups[0].bitmaps[segID], bm)
				}
			}
		}
	}

	// Query topic bitmaps per position (OR within each position group).
	for pos, tg := range topicGroups {
		if len(tg) == 0 {
			continue
		}
		groupIdx := pos + 1
		groups[groupIdx] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, topicXDR := range tg {
			if len(topicXDR) == 0 {
				continue
			}
			termKey := TopicTermKey(topicXDR)
			for _, segID := range segments {
				bm, stats, err := loader.LoadTermBitmap(segID, groupIdx, termKey, startLedger, endLedger)
				if err != nil {
					continue
				}
				result.IndexBytesRead += stats.BytesRead
				result.IndexReadTime += stats.ReadTime
				result.IndexDecodeTime += stats.DecodeTime
				if bm != nil && !bm.IsEmpty() {
					result.SegmentsScanned++
					groups[groupIdx].bitmaps[segID] = append(groups[groupIdx].bitmaps[segID], bm)
				}
			}
		}
	}

	// Collect all segment IDs across all groups.
	allSegIDs := make(map[uint32]bool)
	for _, g := range groups {
		for segID := range g.bitmaps {
			allSegIDs[segID] = true
		}
	}

	// Parallel intersection: parallel OR within groups, then AND across groups.
	intersectStart := time.Now()
	perSegment := make(map[uint32]*roaring.Bitmap)

	type groupBitmaps struct {
		perSeg map[uint32][]*roaring.Bitmap
	}
	groupList := make([]groupBitmaps, 0, len(groups))
	for _, g := range groups {
		groupList = append(groupList, groupBitmaps{perSeg: g.bitmaps})
	}

	type segAndResult struct {
		segID uint32
		bm    *roaring.Bitmap
	}
	segIDList := make([]uint32, 0, len(allSegIDs))
	for segID := range allSegIDs {
		segIDList = append(segIDList, segID)
	}
	andResults := make([]segAndResult, len(segIDList))
	var andWg sync.WaitGroup
	for i, segID := range segIDList {
		andWg.Add(1)
		go func(idx int, sid uint32) {
			defer andWg.Done()
			for _, g := range groupList {
				if len(g.perSeg[sid]) == 0 {
					return
				}
			}
			groupUnions := make([]*roaring.Bitmap, len(groupList))
			var orWg sync.WaitGroup
			for gIdx := range groupList {
				bms := groupList[gIdx].perSeg[sid]
				if len(bms) == 1 {
					groupUnions[gIdx] = bms[0]
				} else {
					orWg.Add(1)
					go func(oi int, bmaps []*roaring.Bitmap) {
						defer orWg.Done()
						groupUnions[oi] = roaring.FastOr(bmaps...)
					}(gIdx, bms)
				}
			}
			orWg.Wait()
			intersected := roaring.FastAnd(groupUnions...)
			if !intersected.IsEmpty() {
				andResults[idx] = segAndResult{segID: sid, bm: intersected}
			}
		}(i, segID)
	}
	andWg.Wait()
	for _, sr := range andResults {
		if sr.bm != nil {
			perSegment[sr.segID] = sr.bm
			result.MatchingLocalIDs += int(sr.bm.GetCardinality())
		}
	}
	result.IndexIntersectTime = time.Since(intersectStart)
	result.IndexLookupTime = time.Since(indexStart)

	return perSegment, nil
}
