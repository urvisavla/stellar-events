package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring"
	grocksdb "github.com/linxGnu/grocksdb"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// encodeIndexKeyWithSegment creates a 36-byte index key from term key and segment ID.
func encodeIndexKeyWithSegment(termKey [32]byte, segmentID uint32) []byte {
	key := make([]byte, IndexKeySize)
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], segmentID)
	return key
}

// encodeTopicIndexKey creates a 37-byte RocksDB key for topic CF entries.
// Format: [pos:1][termHash:32][segmentID:4]
func encodeTopicIndexKey(pos int, termKey [32]byte, segmentID uint32) []byte {
	key := make([]byte, 37)
	key[0] = byte(pos)
	copy(key[1:33], termKey[:])
	binary.BigEndian.PutUint32(key[33:37], segmentID)
	return key
}

// =============================================================================
// RocksDBIndexStore — RocksDB I/O for bitmap indexes and ledger offsetss
// =============================================================================

// RocksDBIndexStore implements IndexFlusher, bitmap32Loader, and ledgerOffsetsLoader
// backed by RocksDB column families. Pure I/O layer — does NOT own the in-memory bitmap index.
type RocksDBIndexStore struct {
	db          *grocksdb.DB
	cfContracts *grocksdb.ColumnFamilyHandle
	cfTopics    *grocksdb.ColumnFamilyHandle
	cfDefault   *grocksdb.ColumnFamilyHandle

	wo *grocksdb.WriteOptions
	ro *grocksdb.ReadOptions
}

// NewRocksDBIndexStore creates a new RocksDB-backed index store.
func NewRocksDBIndexStore(db *grocksdb.DB, cfDefault, cfContracts, cfTopics *grocksdb.ColumnFamilyHandle) (*RocksDBIndexStore, error) {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)

	return &RocksDBIndexStore{
		db:          db,
		cfDefault:   cfDefault,
		cfContracts: cfContracts,
		cfTopics:    cfTopics,
		wo:          wo,
		ro:          grocksdb.NewDefaultReadOptions(),
	}, nil
}

func (s *RocksDBIndexStore) getCF(isContract bool) *grocksdb.ColumnFamilyHandle {
	if isContract {
		return s.cfContracts
	}
	return s.cfTopics
}

// Close releases resources.
func (s *RocksDBIndexStore) Close() error {
	s.wo.Destroy()
	s.ro.Destroy()
	return nil
}

// LoadSegmentLedgerOffsets implements the ledgerOffsetsLoader interface.
func (s *RocksDBIndexStore) LoadSegmentLedgerOffsets(segmentID uint32) (*SegmentLedgerOffsets, error) {
	key := SegmentLedgerOffsetsKey(segmentID)
	data, err := s.db.GetCF(s.ro, s.cfDefault, key)
	if err != nil {
		return nil, fmt.Errorf("failed to load ledger offsets for segment %d: %w", segmentID, err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, nil
	}

	dataCopy := make([]byte, data.Size())
	copy(dataCopy, data.Data())

	return &SegmentLedgerOffsets{
		SegmentID: segmentID,
		Data:      dataCopy,
	}, nil
}

// LoadBitmap32SegmentWithTiming loads a segment using FromBuffer for near-zero-cost decode.
// fieldIndex: 0=contracts, 1-4=topic positions 0-3.
func (s *RocksDBIndexStore) LoadBitmap32SegmentWithTiming(fieldIndex int, termKey [32]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	isContract := fieldIndex == 0
	var dbKey []byte
	if isContract {
		dbKey = encodeIndexKeyWithSegment(termKey, segmentID)
	} else {
		dbKey = encodeTopicIndexKey(fieldIndex-1, termKey, segmentID)
	}
	cf := s.getCF(isContract)

	readStart := time.Now()
	data, err := s.db.GetCF(s.ro, cf, dbKey)
	readTime := time.Since(readStart)
	if err != nil {
		return nil, 0, readTime, 0, fmt.Errorf("failed to get bitmap: %w", err)
	}

	if data.Size() == 0 {
		data.Free()
		return nil, 0, readTime, 0, nil
	}

	bytesRead := int64(data.Size())
	decodeStart := time.Now()

	dataCopy := make([]byte, data.Size())
	copy(dataCopy, data.Data())
	data.Free()

	bitmap := roaring.New()
	_, err = bitmap.FromBuffer(dataCopy)
	decodeTime := time.Since(decodeStart)
	if err != nil {
		return nil, bytesRead, readTime, decodeTime, fmt.Errorf("failed to decode bitmap via FromBuffer: %w", err)
	}

	return bitmap, bytesRead, readTime, decodeTime, nil
}

func (s *RocksDBIndexStore) loadBitmap32FromCF(cf *grocksdb.ColumnFamilyHandle, key []byte) (*roaring.Bitmap, error) {
	data, err := s.db.GetCF(s.ro, cf, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get bitmap: %w", err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, nil
	}

	bitmap := roaring.New()
	_, err = bitmap.FromBuffer(data.Data())
	if err != nil {
		return nil, fmt.Errorf("failed to decode bitmap: %w", err)
	}

	return bitmap.Clone(), nil
}

// Flush persists bitmap segments and ledger offsetss to RocksDB, merging with existing data.
// writeToStore controls whether data is actually written (allows RocksDB for reads only).
func (s *RocksDBIndexStore) Flush(
	segments []bitmapChunk,
	counters map[uint32]*segmentEventCounter,
	writeToStore bool,
) (map[uint32][]byte, error) {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, seg := range segments {
		isContract := seg.FieldIndex == 0
		cf := s.getCF(isContract)

		var dbKey []byte
		if isContract {
			dbKey = seg.Key
		} else {
			pos := seg.FieldIndex - 1
			var termKey [32]byte
			copy(termKey[:], seg.Key[0:32])
			segmentID := binary.BigEndian.Uint32(seg.Key[32:36])
			dbKey = encodeTopicIndexKey(pos, termKey, segmentID)
		}

		if writeToStore {
			existingBitmap, err := s.loadBitmap32FromCF(cf, dbKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load existing bitmap for merge: %w", err)
			}

			var finalData []byte
			if existingBitmap != nil {
				newBitmap := roaring.New()
				_, err := newBitmap.FromBuffer(seg.Data)
				if err != nil {
					return nil, fmt.Errorf("failed to decode new bitmap: %w", err)
				}
				existingBitmap.Or(newBitmap)
				existingBitmap.RunOptimize()
				finalData, err = existingBitmap.ToBytes()
				if err != nil {
					return nil, fmt.Errorf("failed to serialize merged bitmap: %w", err)
				}
			} else {
				finalData = seg.Data
			}

			batch.PutCF(cf, dbKey, finalData)
		}
	}

	ledgerOffsetsData := make(map[uint32][]byte, len(counters))
	for segmentID, counter := range counters {
		lmKey := SegmentLedgerOffsetsKey(segmentID)

		var existingBytes []byte
		if writeToStore {
			existingData, err := s.db.GetCF(s.ro, s.cfDefault, lmKey)
			if err != nil {
				return nil, fmt.Errorf("failed to read existing ledger offsets for segment %d: %w", segmentID, err)
			}
			if existingData.Size() > 0 {
				existingBytes = make([]byte, existingData.Size())
				copy(existingBytes, existingData.Data())
			}
			existingData.Free()
		}

		newCounts := make(map[uint16]uint32)
		for i := uint32(0); i < SegmentSize; i++ {
			if counter.eventCounts[i] > 0 {
				newCounts[uint16(i)] = counter.eventCounts[i]
			}
		}

		var finalData []byte
		if len(existingBytes) > 0 {
			finalData = MergeSegmentLedgerOffsetsData(existingBytes, newCounts)
		} else {
			finalData = EncodeSegmentLedgerOffsets(segmentID, counter.eventCounts)
		}

		ledgerOffsetsData[segmentID] = finalData

		if writeToStore {
			batch.PutCF(s.cfDefault, lmKey, finalData)
		}
	}

	if batch.Count() > 0 {
		if err := s.db.Write(s.wo, batch); err != nil {
			return nil, fmt.Errorf("failed to write bitmap segments: %w", err)
		}
	}

	return ledgerOffsetsData, nil
}

// RocksDBReader implements QueryBackend backed by RocksDB.
// Loads bitmaps from the in-memory bitmap engine (hot cache) and RocksDBIndexStore (cold storage),
// fetches events via RocksDB iterators, and resolves dense IDs via ledger offsetss.
type RocksDBReader struct {
	bitmap   *eventBitmap32Index
	fetcher  EventFetcher
	lmLoader LedgerOffsetsLoader
}

// NewRocksDBReader creates a QueryBackend backed by RocksDB.
func NewRocksDBReader(bitmap *eventBitmap32Index, fetcher EventFetcher, lmLoader LedgerOffsetsLoader) *RocksDBReader {
	return &RocksDBReader{bitmap: bitmap, fetcher: fetcher, lmLoader: lmLoader}
}

// LoadTermBitmap loads a bitmap for a specific term+segment, trimmed to ledger range.
// fieldIndex: 0=contracts, 1-4=topic positions 0-3.
func (l *RocksDBReader) LoadTermBitmap(segmentID uint32, fieldIndex int, termKey [32]byte,
	startLedger, endLedger uint32) (*roaring.Bitmap, BitmapLoadStats, error) {

	// Load bitmap for this single segment (checks hot cache first, then RocksDB).
	bm, segBytes, readTime, decodeTime, fromCache, err := l.bitmap.getSegmentWithStats(fieldIndex, termKey, segmentID)
	if err != nil {
		return nil, BitmapLoadStats{}, err
	}

	stats := BitmapLoadStats{
		BytesRead:  segBytes,
		ReadTime:   readTime,
		DecodeTime: decodeTime,
	}

	if bm == nil || bm.IsEmpty() {
		return nil, stats, nil
	}

	// Trim to ledger range using dense ID boundaries from ledger offsets.
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
	if needsTrim && l.bitmap.ledgerOffsetsLoader != nil {
		lm, lmErr := l.bitmap.ledgerOffsetsLoader.LoadSegmentLedgerOffsets(segmentID)
		if lmErr != nil {
			return nil, stats, fmt.Errorf("failed to load ledger offsets for segment %d: %w", segmentID, lmErr)
		}

		var startLocalID, endLocalID uint32
		if lm != nil {
			startLocalID, endLocalID = lm.LedgerRangeToIDRange(startOff, endOff)
		} else {
			endLocalID = bm.Maximum()
		}

		// Clone if from hot cache (shared reference), otherwise operate in-place.
		trimmed := bm
		if fromCache {
			trimmed = bm.Clone()
		}
		if startLocalID > 0 {
			trimmed.RemoveRange(0, uint64(startLocalID))
		}
		if !trimmed.IsEmpty() {
			if maxVal := trimmed.Maximum(); endLocalID < maxVal {
				trimmed.RemoveRange(uint64(endLocalID)+1, uint64(maxVal)+1)
			}
		}
		bm = trimmed
	} else if fromCache {
		bm = bm.Clone()
	}

	if bm.IsEmpty() {
		return nil, stats, nil
	}

	return bm, stats, nil
}

// RocksDBEventFetcher creates event iterators backed by RocksDB's cfEvents column family.
type RocksDBEventFetcher struct {
	db       *grocksdb.DB
	ro       *grocksdb.ReadOptions
	cfEvents *grocksdb.ColumnFamilyHandle
}

// NewRocksDBEventFetcher creates an EventFetcher for RocksDB event storage.
func NewRocksDBEventFetcher(db *grocksdb.DB, ro *grocksdb.ReadOptions, cfEvents *grocksdb.ColumnFamilyHandle) *RocksDBEventFetcher {
	return &RocksDBEventFetcher{db: db, ro: ro, cfEvents: cfEvents}
}

// NewEventIterator creates a new RocksDB-backed event iterator.
func (f *RocksDBEventFetcher) NewEventIterator() EventIterator {
	return &RocksDBEventIterator{iter: f.db.NewIteratorCF(f.ro, f.cfEvents)}
}

// RocksDBEventIterator wraps grocksdb.Iterator to implement EventIterator.
type RocksDBEventIterator struct {
	iter *grocksdb.Iterator
}

func (it *RocksDBEventIterator) Seek(key []byte) { it.iter.Seek(key) }
func (it *RocksDBEventIterator) Next()           { it.iter.Next() }
func (it *RocksDBEventIterator) Valid() bool     { return it.iter.Valid() }

func (it *RocksDBEventIterator) Key() []byte {
	k := it.iter.Key()
	if k == nil {
		return nil
	}
	return k.Data()
}

func (it *RocksDBEventIterator) Value() []byte {
	v := it.iter.Value()
	if v == nil {
		return nil
	}
	return v.Data()
}

func (it *RocksDBEventIterator) Close() { it.iter.Close() }


// FetchByIDs resolves dense IDs from per-segment bitmaps to events via RocksDB point lookups.
func (q *RocksDBReader) FetchByIDs(perSegment map[uint32]*roaring.Bitmap, limit int, result *QueryResult) ([]*query.Event, error) {
	segIDs := make([]uint32, 0, len(perSegment))
	for segID := range perSegment {
		segIDs = append(segIDs, segID)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	fetchCap := result.MatchingLocalIDs
	if limit > 0 && fetchCap > limit {
		fetchCap = limit
	}

	iter := q.fetcher.NewEventIterator()
	defer iter.Close()

	events := make([]*query.Event, 0, fetchCap)

	for _, segID := range segIDs {
		if len(events) >= fetchCap {
			break
		}

		lm, err := q.lmLoader.LoadSegmentLedgerOffsets(segID)
		if err != nil {
			return nil, fmt.Errorf("failed to load ledger offsets for segment %d: %w", segID, err)
		}
		if lm == nil {
			continue
		}

		bitmapIter := perSegment[segID].Iterator()
		for bitmapIter.HasNext() {
			if len(events) >= fetchCap {
				break
			}

			denseID := bitmapIter.Next()
			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
			key := EncodeKey(segID, denseID)

			seekStart := time.Now()
			iter.Seek(key)
			if !iter.Valid() {
				result.EventFetchTime += time.Since(seekStart)
				break
			}

			iterKey := iter.Key()
			if !bytes.Equal(iterKey, key) {
				result.EventFetchTime += time.Since(seekStart)
				continue
			}

			iterVal := iter.Value()
			if len(iterVal) == 0 {
				result.EventFetchTime += time.Since(seekStart)
				continue
			}

			valueCopy := make([]byte, len(iterVal))
			copy(valueCopy, iterVal)
			result.EventFetchTime += time.Since(seekStart)

			result.EventBytesRead += int64(len(valueCopy))
			result.EventsScanned++

			decStart := time.Now()
			ev, decErr := event.DecodeBinaryToQueryEvent(valueCopy, ledger, eventSeq)
			result.DecodeTime += time.Since(decStart)

			if decErr != nil {
				continue
			}

			events = append(events, ev)
		}
	}

	result.EventsReturned = len(events)
	return events, nil
}

// FetchByRange retrieves all events in a ledger range from RocksDB via sequential scan.
func (q *RocksDBReader) FetchByRange(startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	startKey := EncodeKey(SegmentID(startLedger), 0)

	iter := q.fetcher.NewEventIterator()
	defer iter.Close()

	diskStart := time.Now()
	iter.Seek(startKey)
	result.EventFetchTime += time.Since(diskStart)

	var currentSegID = ^uint32(0)
	var lm *SegmentLedgerOffsets
	var events []*query.Event

	for iter.Valid() {
		if limit > 0 && len(events) >= limit {
			break
		}

		key := iter.Key()
		if len(key) < 8 {
			break
		}

		segID, denseID := DecodeKey(key)

		if segID > SegmentID(endLedger) {
			break
		}

		if segID != currentSegID {
			currentSegID = segID
			lm, _ = q.lmLoader.LoadSegmentLedgerOffsets(segID)
		}
		if lm == nil {
			diskStart = time.Now()
			iter.Next()
			result.EventFetchTime += time.Since(diskStart)
			continue
		}

		ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
		if ledger < startLedger {
			diskStart = time.Now()
			iter.Next()
			result.EventFetchTime += time.Since(diskStart)
			continue
		}
		if ledger > endLedger {
			break
		}

		valueData := iter.Value()
		result.EventBytesRead += int64(len(valueData))

		decStart := time.Now()
		ev, err := event.DecodeBinaryToQueryEvent(valueData, ledger, eventSeq)
		result.DecodeTime += time.Since(decStart)

		result.EventsScanned++

		if err != nil {
			diskStart = time.Now()
			iter.Next()
			result.EventFetchTime += time.Since(diskStart)
			continue
		}
		events = append(events, ev)

		diskStart = time.Now()
		iter.Next()
		result.EventFetchTime += time.Since(diskStart)
	}

	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// Close is a no-op — underlying resources are owned by RocksDBBackend.
func (q *RocksDBReader) Close() error { return nil }

// Verify interface compliance at compile time.
var (
	_ QueryBackend    = (*RocksDBReader)(nil)
	_ EventFetcher    = (*RocksDBEventFetcher)(nil)
	_ EventIterator   = (*RocksDBEventIterator)(nil)
	_ LedgerOffsetsLoader = (*RocksDBIndexStore)(nil)
)
