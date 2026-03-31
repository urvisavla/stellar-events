package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/linxGnu/grocksdb"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// RocksDBHotSegmentReader implements QueryBackend by reading hot segment data
// from RocksDB column families and rebuilding bitmaps from index_deltas CF.
// Parallel to HotSegmentReader for benchmarking.
type RocksDBHotSegmentReader struct {
	backend   *RocksDBBackend
	available map[uint32]bool
	loaded    *rocksDBHotState
	mu        sync.Mutex
}

type rocksDBHotState struct {
	segmentID  uint32
	contracts  map[[16]byte]*roaring.Bitmap
	topics     [4]map[[16]byte]*roaring.Bitmap
	ledgerOffs *SegmentLedgerOffsets
	eventCount int
}

var _ QueryBackend = (*RocksDBHotSegmentReader)(nil)

// NewRocksDBHotSegmentReader discovers hot segments by scanning the index_deltas CF.
func NewRocksDBHotSegmentReader(backend *RocksDBBackend) (*RocksDBHotSegmentReader, error) {
	reader := &RocksDBHotSegmentReader{
		backend:   backend,
		available: make(map[uint32]bool),
	}

	// Discover segments by scanning index_deltas CF
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := backend.db.NewIteratorCF(ro, backend.cfIndexDeltas)
	defer it.Close()

	it.SeekToFirst()
	for it.Valid() {
		key := it.Key()
		keyData := key.Data()
		if len(keyData) < 8 {
			key.Free()
			break
		}
		segID := binary.BigEndian.Uint32(keyData[0:4])
		key.Free()

		reader.available[segID] = true

		// Skip to the next segment
		nextKey := EncodeKey(segID+1, 0)
		it.Seek(nextKey)
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("scan index_deltas for segment discovery: %w", err)
	}

	if len(reader.available) > 0 {
		segIDs := make([]uint32, 0, len(reader.available))
		for id := range reader.available {
			segIDs = append(segIDs, id)
		}
		sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })
		labels := make([]string, len(segIDs))
		for i, id := range segIDs {
			labels[i] = fmt.Sprintf("%06d", id)
		}
		fmt.Fprintf(os.Stderr, "[hot-rocksdb] found %d hot segment(s): %s\n",
			len(segIDs), strings.Join(labels, " "))
	}

	return reader, nil
}

// getSegment lazily loads a single hot segment from RocksDB.
func (r *RocksDBHotSegmentReader) getSegment(segmentID uint32) (*rocksDBHotState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.loaded != nil && r.loaded.segmentID == segmentID {
		return r.loaded, nil
	}

	if r.loaded != nil {
		return nil, fmt.Errorf("query range spans multiple hot segments (%06d, %06d); narrow --start/--end to a single segment",
			r.loaded.segmentID, segmentID)
	}

	if !r.available[segmentID] {
		return nil, nil
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	loadStart := time.Now()
	state, err := r.loadHotSegment(segmentID)
	if err != nil {
		delete(r.available, segmentID)
		return nil, err
	}
	loadTime := time.Since(loadStart)

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	actualDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
	fmt.Fprintf(os.Stderr, "[hot-rocksdb] loaded segment %06d in %v, actual heap delta: %s\n",
		segmentID, loadTime, formatBytesStore(actualDelta))

	r.loaded = state
	return state, nil
}

func (r *RocksDBHotSegmentReader) loadHotSegment(segID uint32) (*rocksDBHotState, error) {
	// TODO: enable bitmap snapshot fast path once validated
	// state, err := r.loadFromBitmapSnapshots(segID)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "[hot-rocksdb %06d] bitmap_snapshots failed, falling back: %v\n", segID, err)
	// }
	// if state != nil {
	// 	return state, nil
	// }

	return r.loadFromIndexDeltas(segID)
}

func (r *RocksDBHotSegmentReader) loadFromIndexDeltas(segID uint32) (*rocksDBHotState, error) {
	rebuildStart := time.Now()
	fmt.Fprintf(os.Stderr, "[hot-rocksdb %06d] rebuilding bitmaps from index_deltas CF...\n", segID)

	contracts := make(map[[16]byte]*roaring.Bitmap)
	var topics [4]map[[16]byte]*roaring.Bitmap
	for i := range topics {
		topics[i] = make(map[[16]byte]*roaring.Bitmap)
	}

	// Prefix-scan index_deltas CF for this segment
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := r.backend.db.NewIteratorCF(ro, r.backend.cfIndexDeltas)
	defer it.Close()

	startKey := EncodeKey(segID, 0)
	endKey := EncodeKey(segID+1, 0)

	var numDeltas int
	var deltaCountPerField [5]int

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key()
		keyData := key.Data()
		if len(keyData) < 8 || bytes.Compare(keyData, endKey) >= 0 {
			key.Free()
			break
		}
		key.Free()

		value := it.Value()
		valData := value.Data()
		if len(valData) < indexDeltaSize {
			value.Free()
			continue
		}

		fieldIndex := valData[0]
		var termHash [16]byte
		copy(termHash[:], valData[1:17])
		eventID := binary.LittleEndian.Uint32(valData[17:21])
		value.Free()

		numDeltas++

		if fieldIndex == 0 {
			deltaCountPerField[0]++
			bm, ok := contracts[termHash]
			if !ok {
				bm = roaring.New()
				contracts[termHash] = bm
			}
			bm.Add(eventID)
		} else if fieldIndex >= 1 && fieldIndex <= 4 {
			idx := fieldIndex - 1
			deltaCountPerField[fieldIndex]++
			bm, ok := topics[idx][termHash]
			if !ok {
				bm = roaring.New()
				topics[idx][termHash] = bm
			}
			bm.Add(eventID)
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterate index_deltas for segment %d: %w", segID, err)
	}

	// Run-optimize all bitmaps to convert array/bitmap containers to
	// run-length encoding where beneficial. Reduces memory and speeds up Clone().
	for _, bm := range contracts {
		bm.RunOptimize()
	}
	for i := range topics {
		for _, bm := range topics[i] {
			bm.RunOptimize()
		}
	}

	fmt.Fprintf(os.Stderr, "[hot-rocksdb %06d] index_deltas: %d entries (contracts: %d, topic0: %d, topic1: %d, topic2: %d, topic3: %d)\n",
		segID, numDeltas, deltaCountPerField[0], deltaCountPerField[1], deltaCountPerField[2], deltaCountPerField[3], deltaCountPerField[4])

	// Load ledger offsets from default CF
	loKey := SegmentLedgerOffsetsKey(segID)
	loValue, err := r.backend.db.GetCF(r.backend.ro, r.backend.cfDefault, loKey)
	if err != nil {
		return nil, fmt.Errorf("load ledger offsets for segment %d: %w", segID, err)
	}
	defer loValue.Free()

	if loValue.Size() == 0 {
		return nil, fmt.Errorf("no ledger offsets for hot segment %d", segID)
	}

	paddedData := make([]byte, SegmentLedgerOffsetsSize)
	copy(paddedData, loValue.Data())
	ledgerOffs := &SegmentLedgerOffsets{SegmentID: segID, Data: paddedData}

	// Derive event count from max bitmap dense ID (TotalEvents() returns 0
	// when the cumulative array is incomplete due to empty ledgers not being
	// written during ingestion).
	eventCount := 0
	for _, bm := range contracts {
		if !bm.IsEmpty() {
			if m := int(bm.Maximum()) + 1; m > eventCount {
				eventCount = m
			}
		}
	}
	for i := range topics {
		for _, bm := range topics[i] {
			if !bm.IsEmpty() {
				if m := int(bm.Maximum()) + 1; m > eventCount {
					eventCount = m
				}
			}
		}
	}
	if te := int(ledgerOffs.TotalEvents()); te > eventCount {
		eventCount = te
	}

	rebuildTime := time.Since(rebuildStart)
	fmt.Fprintf(os.Stderr, "[hot-rocksdb %06d] events: %d, rebuild time: %v\n", segID, eventCount, rebuildTime)

	return &rocksDBHotState{
		segmentID:  segID,
		contracts:  contracts,
		topics:     topics,
		ledgerOffs: ledgerOffs,
		eventCount: eventCount,
	}, nil
}

// loadFromBitmapSnapshots loads pre-serialized bitmaps from the bitmap_snapshots CF.
// Returns nil state (no error) if no snapshots exist for this segment.
func (r *RocksDBHotSegmentReader) loadFromBitmapSnapshots(segID uint32) (*rocksDBHotState, error) {
	loadStart := time.Now()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := r.backend.db.NewIteratorCF(ro, r.backend.cfBitmapSnapshots)
	defer it.Close()

	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, segID)
	endPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(endPrefix, segID+1)

	contracts := make(map[[16]byte]*roaring.Bitmap)
	var topics [4]map[[16]byte]*roaring.Bitmap
	for i := range topics {
		topics[i] = make(map[[16]byte]*roaring.Bitmap)
	}

	var termCount int
	var totalBytes int64

	for it.Seek(prefix); it.Valid(); it.Next() {
		key := it.Key()
		keyData := key.Data()
		if len(keyData) < 21 || bytes.Compare(keyData[:4], endPrefix) >= 0 {
			key.Free()
			break
		}

		fieldIndex := keyData[4]
		var termHash [16]byte
		copy(termHash[:], keyData[5:21])
		key.Free()

		value := it.Value()
		valData := make([]byte, value.Size())
		copy(valData, value.Data())
		totalBytes += int64(value.Size())
		value.Free()

		bm := roaring.New()
		if _, err := bm.FromBuffer(valData); err != nil {
			return nil, fmt.Errorf("decode bitmap snapshot field %d: %w", fieldIndex, err)
		}

		termCount++

		if fieldIndex == 0 {
			contracts[termHash] = bm
		} else if fieldIndex >= 1 && fieldIndex <= 4 {
			topics[fieldIndex-1][termHash] = bm
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterate bitmap_snapshots for segment %d: %w", segID, err)
	}

	if termCount == 0 {
		return nil, nil // no snapshots, fall back to delta replay
	}

	// Load ledger offsets from default CF
	loKey := SegmentLedgerOffsetsKey(segID)
	loValue, err := r.backend.db.GetCF(r.backend.ro, r.backend.cfDefault, loKey)
	if err != nil {
		return nil, fmt.Errorf("load ledger offsets for segment %d: %w", segID, err)
	}
	defer loValue.Free()

	if loValue.Size() == 0 {
		return nil, fmt.Errorf("no ledger offsets for hot segment %d", segID)
	}

	paddedData := make([]byte, SegmentLedgerOffsetsSize)
	copy(paddedData, loValue.Data())
	ledgerOffs := &SegmentLedgerOffsets{SegmentID: segID, Data: paddedData}

	// Derive event count from bitmap max IDs
	eventCount := 0
	for _, bm := range contracts {
		if !bm.IsEmpty() {
			if m := int(bm.Maximum()) + 1; m > eventCount {
				eventCount = m
			}
		}
	}
	for i := range topics {
		for _, bm := range topics[i] {
			if !bm.IsEmpty() {
				if m := int(bm.Maximum()) + 1; m > eventCount {
					eventCount = m
				}
			}
		}
	}
	if te := int(ledgerOffs.TotalEvents()); te > eventCount {
		eventCount = te
	}

	loadTime := time.Since(loadStart)
	fmt.Fprintf(os.Stderr, "[hot-rocksdb %06d] loaded %d bitmap snapshots (%s) in %v (fast path)\n",
		segID, termCount, formatBytesStore(totalBytes), loadTime)

	return &rocksDBHotState{
		segmentID:  segID,
		contracts:  contracts,
		topics:     topics,
		ledgerOffs: ledgerOffs,
		eventCount: eventCount,
	}, nil
}

// HasSegment returns true if the given segment ID is available in RocksDB.
func (r *RocksDBHotSegmentReader) HasSegment(segmentID uint32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.available[segmentID]
}

// HasSegments returns true if any hot segments are available.
func (r *RocksDBHotSegmentReader) HasSegments() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.available) > 0
}

// LoadTermBitmap loads a bitmap for a specific term in a hot segment, trimmed to ledger range.
func (r *RocksDBHotSegmentReader) LoadTermBitmap(segmentID uint32, fieldIndex int, termKey [16]byte,
	startLedger, endLedger uint32) (*roaring.Bitmap, BitmapLoadStats, error) {

	totalStart := time.Now()
	stats := BitmapLoadStats{}

	seg, err := r.getSegment(segmentID)
	if err != nil {
		return nil, stats, err
	}
	if seg == nil {
		stats.TotalTime = time.Since(totalStart)
		return nil, stats, nil
	}

	var bm *roaring.Bitmap
	if fieldIndex == 0 {
		bm = seg.contracts[termKey]
	} else if fieldIndex >= 1 && fieldIndex <= 4 {
		bm = seg.topics[fieldIndex-1][termKey]
	}

	if bm == nil || bm.IsEmpty() {
		stats.TotalTime = time.Since(totalStart)
		return nil, stats, nil
	}

	result := bm.Clone()

	// Trim to ledger range
	segmentStart := segmentID * SegmentSize
	var startOff uint16
	if startLedger > segmentStart {
		startOff = uint16(startLedger - segmentStart)
	}
	endOff := uint16(SegmentSize - 1)
	if endLedger < segmentStart+SegmentSize-1 {
		endOff = uint16(endLedger - segmentStart)
	}

	needsTrim := startOff > 0 || endOff < uint16(SegmentSize-1)
	if needsTrim && seg.ledgerOffs != nil {
		startLocalID, endLocalID := seg.ledgerOffs.LedgerRangeToIDRange(startOff, endOff)
		if startLocalID > 0 {
			result.RemoveRange(0, uint64(startLocalID))
		}
		if !result.IsEmpty() {
			if maxVal := result.Maximum(); endLocalID < maxVal {
				result.RemoveRange(uint64(endLocalID)+1, uint64(maxVal)+1)
			}
		}
	}

	stats.TotalTime = time.Since(totalStart)
	if result.IsEmpty() {
		return nil, stats, nil
	}

	return result, stats, nil
}

// FetchByIDs resolves dense IDs to events via BatchedMultiGetCF.
func (r *RocksDBHotSegmentReader) FetchByIDs(perSegment map[uint32]*roaring.Bitmap, limit int, result *QueryResult) ([]*query.Event, error) {
	segIDs := make([]uint32, 0, len(perSegment))
	for segID := range perSegment {
		segIDs = append(segIDs, segID)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	fetchCap := result.MatchingLocalIDs
	if limit > 0 && fetchCap > limit {
		fetchCap = limit
	}

	// Collect all keys and their metadata for a single BatchedMultiGetCF call.
	type keyMeta struct {
		segID   uint32
		denseID uint32
	}
	keys := make([][]byte, 0, fetchCap)
	metas := make([]keyMeta, 0, fetchCap)

	for _, segID := range segIDs {
		if len(keys) >= fetchCap {
			break
		}

		seg, err := r.getSegment(segID)
		if err != nil {
			return nil, err
		}
		if seg == nil {
			continue
		}

		bitmapIter := perSegment[segID].Iterator()
		for bitmapIter.HasNext() {
			if len(keys) >= fetchCap {
				break
			}
			denseID := bitmapIter.Next()
			if int(denseID) >= seg.eventCount {
				continue
			}
			keys = append(keys, EncodeKey(segID, denseID))
			metas = append(metas, keyMeta{segID: segID, denseID: denseID})
		}
	}

	if len(keys) == 0 {
		result.EventsReturned = 0
		return nil, nil
	}

	// Single batched fetch — keys are already sorted by (segID, denseID).
	fetchStart := time.Now()
	values, err := r.backend.db.BatchedMultiGetCF(r.backend.ro, r.backend.cfEvents, true, keys...)
	result.EventFetchTime += time.Since(fetchStart)
	if err != nil {
		return nil, fmt.Errorf("batched multi-get events: %w", err)
	}
	defer values.Destroy()

	events := make([]*query.Event, 0, len(keys))
	for i, val := range values {
		data := val.Data()
		if len(data) == 0 {
			continue
		}

		result.EventBytesRead += int64(len(data))
		result.EventsScanned++

		seg, _ := r.getSegment(metas[i].segID)
		ledger, eventSeq := seg.ledgerOffs.DenseIDToLedgerAndSeq(metas[i].denseID)

		decStart := time.Now()
		ev, err := event.DecodeBinaryToQueryEvent(data, ledger, eventSeq)
		result.DecodeTime += time.Since(decStart)

		if err != nil {
			continue
		}

		events = append(events, ev)
	}

	result.EventsReturned = len(events)
	return events, nil
}

// FetchByRange retrieves all events in a ledger range from hot RocksDB segments.
func (r *RocksDBHotSegmentReader) FetchByRange(startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	segments := GetSegmentsForRange(startLedger, endLedger)
	result.SegmentsTouched = len(segments)

	fetchCap := 0
	if limit > 0 {
		fetchCap = limit
	}

	// Collect all keys and metadata for a single BatchedMultiGetCF call.
	type keyMeta struct {
		segID   uint32
		denseID uint32
	}
	var keys [][]byte
	var metas []keyMeta

	for _, segID := range segments {
		if fetchCap > 0 && len(keys) >= fetchCap {
			break
		}

		seg, err := r.getSegment(segID)
		if err != nil {
			return nil, nil, err
		}
		if seg == nil {
			continue
		}

		segmentStart := segID * SegmentSize
		var startOff uint16
		if startLedger > segmentStart {
			startOff = uint16(startLedger - segmentStart)
		}
		endOff := uint16(SegmentSize - 1)
		if endLedger < segmentStart+SegmentSize-1 {
			endOff = uint16(endLedger - segmentStart)
		}

		startID, endID := seg.ledgerOffs.LedgerRangeToIDRange(startOff, endOff)
		if endID < startID {
			continue
		}

		count := int(endID - startID + 1)
		if fetchCap > 0 && count > fetchCap-len(keys) {
			count = fetchCap - len(keys)
		}
		result.MatchingLocalIDs += count

		for i := 0; i < count; i++ {
			denseID := startID + uint32(i)
			keys = append(keys, EncodeKey(segID, denseID))
			metas = append(metas, keyMeta{segID: segID, denseID: denseID})
		}
	}

	if len(keys) == 0 {
		result.EventsReturned = 0
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// Single batched fetch — keys are sorted by (segID, denseID).
	fetchStart := time.Now()
	values, err := r.backend.db.BatchedMultiGetCF(r.backend.ro, r.backend.cfEvents, true, keys...)
	result.EventFetchTime += time.Since(fetchStart)
	if err != nil {
		return nil, nil, fmt.Errorf("batched multi-get events: %w", err)
	}
	defer values.Destroy()

	events := make([]*query.Event, 0, len(keys))
	for i, val := range values {
		data := val.Data()
		if len(data) == 0 {
			continue
		}

		result.EventBytesRead += int64(len(data))
		result.EventsScanned++

		seg, _ := r.getSegment(metas[i].segID)
		ledger, eventSeq := seg.ledgerOffs.DenseIDToLedgerAndSeq(metas[i].denseID)

		decStart := time.Now()
		ev, err := event.DecodeBinaryToQueryEvent(data, ledger, eventSeq)
		result.DecodeTime += time.Since(decStart)

		if err != nil {
			continue
		}

		events = append(events, ev)
	}

	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// readEvent reads a single event by dense ID from RocksDB events CF.
func (r *RocksDBHotSegmentReader) readEvent(seg *rocksDBHotState, segID, denseID uint32) ([]byte, error) {
	if int(denseID) >= seg.eventCount {
		return nil, fmt.Errorf("denseID %d out of range (eventCount=%d)", denseID, seg.eventCount)
	}

	key := EncodeKey(segID, denseID)
	value, err := r.backend.db.GetCF(r.backend.ro, r.backend.cfEvents, key)
	if err != nil {
		return nil, fmt.Errorf("rocksdb get event: %w", err)
	}
	defer value.Free()

	if value.Size() == 0 {
		return nil, fmt.Errorf("no event data for segment %d denseID %d", segID, denseID)
	}

	// Copy data since value is freed after return
	data := make([]byte, value.Size())
	copy(data, value.Data())
	return data, nil
}

// Close releases resources.
func (r *RocksDBHotSegmentReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loaded = nil
	return nil
}
