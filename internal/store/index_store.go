package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/RoaringBitmap/roaring"
)

// flushedSegmentData holds bitmap terms for a single segment, cached after Flush().
type flushedSegmentData struct {
	contracts []SegmentTermData
	topics    [4][]SegmentTermData
}

// IndexStore coordinates the in-memory bitmap index and the RocksDB bitmap store.
// It owns the eventBitmap32Index (in-memory) and delegates persistence to BitmapEventSeqStore.
type IndexStore struct {
	bitmap *eventBitmap32Index
	db     *BitmapEventSeqStore

	// Write targets during Flush
	segmentPath    string // base dir for segment files (empty = file writes disabled)
	writeToRocksDB bool   // write bitmaps and ledger maps to RocksDB (default: true)

	// Cache of flushed bitmap data, keyed by segment ID.
	// Populated by Flush() when writeToRocksDB=false, consumed by PopSegmentTerms().
	flushedTerms map[uint32]*flushedSegmentData
}

// NewIndexStore creates a new IndexStore wrapping a BitmapEventSeqStore.
// The BitmapEventSeqStore implements both bitmap32Loader and ledgerMapLoader interfaces.
func NewIndexStore(db *BitmapEventSeqStore) *IndexStore {
	s := &IndexStore{db: db, writeToRocksDB: true}
	s.bitmap = newEventBitmap32Index(db, db) // db implements both loader interfaces
	return s
}

// SetWriteConfig configures where bitmaps and ledger maps are written during Flush().
// segmentPath: base directory for segment flat files (empty = file writes disabled).
// writeToRocksDB: if true, bitmaps and ledger maps are written to RocksDB.
func (s *IndexStore) SetWriteConfig(segmentPath string, writeToRocksDB bool) {
	s.segmentPath = segmentPath
	s.writeToRocksDB = writeToRocksDB
}

// Flush persists all hot segments and ledger maps.
// Gets data from the in-memory index, passes to BitmapEventSeqStore for RocksDB persistence,
// then writes flat files if configured.
func (s *IndexStore) Flush() error {
	segments, counters, err := s.bitmap.GetAndClearAllSegments()
	if err != nil {
		return fmt.Errorf("failed to get event bitmap32 segments: %w", err)
	}

	if len(segments) == 0 && len(counters) == 0 {
		return nil
	}

	// Persist to RocksDB (if enabled) and get back ledger map data
	ledgerMapData, err := s.db.Flush(segments, counters, s.writeToRocksDB)
	if err != nil {
		return err
	}

	// Cache bitmap chunks organized by segment ID for WriteSegmentDir.
	// The pipeline must not do periodic mid-segment flushes when segment files
	// are enabled, so each term appears at most once — no merge needed.
	s.flushedTerms = make(map[uint32]*flushedSegmentData)
	for _, seg := range segments {
		segmentID := binary.BigEndian.Uint32(seg.Key[32:36])
		fd, ok := s.flushedTerms[segmentID]
		if !ok {
			fd = &flushedSegmentData{}
			s.flushedTerms[segmentID] = fd
		}
		var termHash [32]byte
		copy(termHash[:], seg.Key[:32])
		td := SegmentTermData{TermHash: termHash, BitmapData: seg.Data}
		if seg.FieldIndex == 0 {
			fd.contracts = append(fd.contracts, td)
		} else {
			pos := seg.FieldIndex - 1 // 1→topic0, 2→topic1, 3→topic2, 4→topic3
			fd.topics[pos] = append(fd.topics[pos], td)
		}
	}

	// Write ledger maps to segment flat files if configured
	if s.segmentPath != "" {
		for segmentID, data := range ledgerMapData {
			dirPath := filepath.Join(s.segmentPath, fmt.Sprintf("%06d", segmentID))
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return fmt.Errorf("failed to create segment dir %s: %w", dirPath, err)
			}
			if err := writeFileAtomic(filepath.Join(dirPath, LedgerMapFileName), data); err != nil {
				return fmt.Errorf("failed to write ledger map file for segment %d: %w", segmentID, err)
			}
		}
	}

	return nil
}

// PopSegmentTerms returns cached bitmap terms for a segment and removes them from the cache.
// Returns nil if no cached data exists (caller should fall back to RocksDB scan).
func (s *IndexStore) PopSegmentTerms(segmentID uint32) *flushedSegmentData {
	if s.flushedTerms == nil {
		return nil
	}
	fd := s.flushedTerms[segmentID]
	delete(s.flushedTerms, segmentID)
	return fd
}

// =============================================================================
// Write Operations (delegate to in-memory bitmap)
// =============================================================================

// AddContractEvent adds an event to the contract index using a dense local ID.
func (s *IndexStore) AddContractEvent(contractID []byte, segmentID uint32, denseLocalID uint32) {
	s.bitmap.AddContractEvent(contractID, segmentID, denseLocalID)
}

// AddTopicEvent adds an event to the topic index (positional) using a dense local ID.
func (s *IndexStore) AddTopicEvent(pos int, topic []byte, segmentID uint32, denseLocalID uint32) {
	s.bitmap.AddTopicEvent(pos, topic, segmentID, denseLocalID)
}

// AssignDenseLocalID assigns the next dense local ID for an event within a segment.
func (s *IndexStore) AssignDenseLocalID(segmentID uint32, ledger uint32) uint32 {
	return s.bitmap.AssignDenseLocalID(segmentID, ledger)
}

// InitializeDenseCounter loads the ledger map for a segment from storage and sets
// the nextDenseID to continue from the persisted state. Called on startup.
func (s *IndexStore) InitializeDenseCounter(segmentID uint32, nextDenseID uint32) {
	s.bitmap.InitializeDenseCounter(segmentID, nextDenseID)
}

// =============================================================================
// Query Operations
// =============================================================================

// QueryEventKeysWithStats returns per-segment bitmaps of matching local IDs.
func (s *IndexStore) QueryEventKeysWithStats(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32) (*EventSeqQueryResult, error) {
	result := &EventSeqQueryResult{
		PerSegment: make(map[uint32]*roaring.Bitmap),
	}

	type segmentBitmaps struct {
		bitmaps []*roaring.Bitmap
	}
	allSegments := make(map[uint32]*segmentBitmaps)

	// Count expected filter terms so we can skip segments missing any term
	expectedTerms := 0

	// Query contract index if specified
	if len(contractID) > 0 {
		expectedTerms++
		termKey := ContractTermKey(contractID)
		perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(true, termKey, -1, startLedger, endLedger)
		if err != nil {
			return nil, fmt.Errorf("contract bitmap32 query failed: %w", err)
		}
		result.BytesRead += bytesRead
		result.Segments += segments
		result.ReadTime += readTime
		result.DecodeTime += decodeTime

		for segID, bm := range perSeg {
			if _, ok := allSegments[segID]; !ok {
				allSegments[segID] = &segmentBitmaps{}
			}
			allSegments[segID].bitmaps = append(allSegments[segID].bitmaps, bm)
		}
	}

	// Query topic indexes (positional)
	for pos, tg := range topicGroups {
		for _, topic := range tg {
			if len(topic) == 0 {
				continue
			}
			expectedTerms++
			termKey := TopicTermKey(topic)
			perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(false, termKey, pos, startLedger, endLedger)
			if err != nil {
				return nil, fmt.Errorf("topic bitmap32 query failed: %w", err)
			}
			result.BytesRead += bytesRead
			result.Segments += segments
			result.ReadTime += readTime
			result.DecodeTime += decodeTime

			for segID, bm := range perSeg {
				if _, ok := allSegments[segID]; !ok {
					allSegments[segID] = &segmentBitmaps{}
				}
				allSegments[segID].bitmaps = append(allSegments[segID].bitmaps, bm)
			}
		}
	}

	// Intersect per-segment bitmaps
	intersectStart := time.Now()
	for segID, sb := range allSegments {
		if len(sb.bitmaps) < expectedTerms {
			continue
		}
		intersected := roaring.FastAnd(sb.bitmaps...)
		if !intersected.IsEmpty() {
			result.PerSegment[segID] = intersected
			result.TotalCount += intersected.GetCardinality()
		}
	}
	result.IntersectTime = time.Since(intersectStart)

	return result, nil
}

// QueryEventKeysMultiFilter returns per-segment bitmaps with multi-value OR/AND filtering.
// contractIDs: multiple contract IDs (OR within group)
// topicGroups: per-position topic values (OR within position, AND across positions)
// Semantics: (contract1 OR contract2 OR ...) AND (topic0val1 OR topic0val2 OR ...) AND ...
func (s *IndexStore) QueryEventKeysMultiFilter(
	contractIDs [][]byte,
	topicGroups [4][][]byte,
	startLedger, endLedger uint32,
) (*EventSeqQueryResult, error) {
	result := &EventSeqQueryResult{
		PerSegment: make(map[uint32]*roaring.Bitmap),
	}

	// Collect per-group, per-segment bitmaps
	// groupIdx 0 = contracts, 1-4 = topic positions
	type groupSegBitmaps struct {
		bitmaps map[uint32][]*roaring.Bitmap // segmentID -> bitmaps to OR
	}
	groups := make(map[int]*groupSegBitmaps)

	// Query contract bitmaps (OR within contracts group)
	if len(contractIDs) > 0 {
		groups[0] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, cid := range contractIDs {
			termKey := ContractTermKey(cid)
			perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(true, termKey, -1, startLedger, endLedger)
			if err != nil {
				return nil, fmt.Errorf("contract bitmap32 multi-filter query failed: %w", err)
			}
			result.BytesRead += bytesRead
			result.Segments += segments
			result.ReadTime += readTime
			result.DecodeTime += decodeTime

			for segID, bm := range perSeg {
				groups[0].bitmaps[segID] = append(groups[0].bitmaps[segID], bm)
			}
		}
	}

	// Query topic bitmaps per position (OR within each position group)
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
			perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(false, termKey, pos, startLedger, endLedger)
			if err != nil {
				return nil, fmt.Errorf("topic bitmap32 multi-filter query failed: %w", err)
			}
			result.BytesRead += bytesRead
			result.Segments += segments
			result.ReadTime += readTime
			result.DecodeTime += decodeTime

			for segID, bm := range perSeg {
				groups[groupIdx].bitmaps[segID] = append(groups[groupIdx].bitmaps[segID], bm)
			}
		}
	}

	// Collect all segment IDs across all groups
	allSegIDs := make(map[uint32]bool)
	for _, g := range groups {
		for segID := range g.bitmaps {
			allSegIDs[segID] = true
		}
	}

	// For each segment: OR within each group, then AND across groups
	intersectStart := time.Now()
	for segID := range allSegIDs {
		var groupUnions []*roaring.Bitmap

		for _, g := range groups {
			bms := g.bitmaps[segID]
			if len(bms) == 0 {
				// This group has no data for this segment -> AND produces empty
				groupUnions = nil
				break
			}
			if len(bms) == 1 {
				groupUnions = append(groupUnions, bms[0])
			} else {
				groupUnions = append(groupUnions, roaring.FastOr(bms...))
			}
		}

		if len(groupUnions) == 0 {
			continue
		}

		intersected := roaring.FastAnd(groupUnions...)
		if !intersected.IsEmpty() {
			result.PerSegment[segID] = intersected
			result.TotalCount += intersected.GetCardinality()
		}
	}
	result.IntersectTime = time.Since(intersectStart)

	return result, nil
}

// =============================================================================
// Accessors (delegate to in-memory bitmap)
// =============================================================================

// GetHotSegmentStats returns statistics about hot segments.
func (s *IndexStore) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	return s.bitmap.GetHotSegmentStats()
}

// GetCurrentSegmentID returns the current segment ID being written to.
func (s *IndexStore) GetCurrentSegmentID() uint32 {
	return s.bitmap.GetCurrentSegmentID()
}

// ClearAll clears all hot segments and segment counters.
func (s *IndexStore) ClearAll() {
	s.bitmap.ClearAll()
}

// LoadSegmentLedgerMap loads a ledger map for a segment from RocksDB.
func (s *IndexStore) LoadSegmentLedgerMap(segmentID uint32) (*SegmentLedgerMap, error) {
	return s.db.LoadSegmentLedgerMap(segmentID)
}

// Close releases resources.
func (s *IndexStore) Close() error {
	return s.db.Close()
}
