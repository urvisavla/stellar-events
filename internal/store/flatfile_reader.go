package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/tamirms/streamhash"
	"github.com/zeebo/xxh3"

	"github.com/tamir/events-analysis/eventstore"
	"github.com/tamir/events-analysis/packfile"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// SegmentReader implements QueryBackend for flat file segments (MPHF .hash + .pack).
// Uses cached readers to avoid repeated opens; mapped files are cached for reuse.
type SegmentReader struct {
	basePath string

	mu              sync.Mutex
	mmapCache       map[string]*MmapFile          // path -> mmap'd file (.meta, .pack, ledgermap)
	hashCache       map[string]*streamhash.Index  // path -> streamhash index (.hash files)
	eventCache      map[string]*eventstore.Reader // path -> eventstore reader (event data)
	appDataCache    map[string][]byte             // path -> cached appData from events.pack
	recordSizeCache map[string]int                // path -> packfile record size (block size)
}

// NewSegmentReader creates a new reader for segment flat file indexes.
func NewSegmentReader(basePath string) *SegmentReader {
	return &SegmentReader{
		basePath:        basePath,
		mmapCache:       make(map[string]*MmapFile),
		hashCache:       make(map[string]*streamhash.Index),
		eventCache:      make(map[string]*eventstore.Reader),
		appDataCache:    make(map[string][]byte),
		recordSizeCache: make(map[string]int),
	}
}

// Close releases all cached readers held by this reader.
func (r *SegmentReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, mm := range r.mmapCache {
		mm.Close()
	}
	for _, hi := range r.hashCache {
		hi.Close()
	}
	for _, er := range r.eventCache {
		er.Close()
	}
	r.mmapCache = nil
	r.hashCache = nil
	r.eventCache = nil
	return nil
}

// PurgeCache closes and clears all cached mmaps, indexes, and readers so that
// subsequent queries must re-open files from disk. Used by cold-cache benchmarks
// to ensure OS page cache purge is effective (mmap'd pages can't be evicted).
func (r *SegmentReader) PurgeCache() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, mm := range r.mmapCache {
		mm.Close()
	}
	for _, hi := range r.hashCache {
		hi.Close()
	}
	for _, er := range r.eventCache {
		er.Close()
	}
	r.mmapCache = make(map[string]*MmapFile)
	r.hashCache = make(map[string]*streamhash.Index)
	r.eventCache = make(map[string]*eventstore.Reader)
	r.appDataCache = make(map[string][]byte)
	r.recordSizeCache = make(map[string]int)
}

// LoadTermBitmap loads a bitmap for a specific term in a specific segment,
// trimmed to the given ledger range. Returns nil bitmap if the term doesn't exist.
// fieldIndex: 0=contracts, 1-4=topic positions 0-3.
// Implements BitmapLoader.
func (r *SegmentReader) LoadTermBitmap(segmentID uint32, fieldIndex int, termKey [16]byte,
	startLedger, endLedger uint32) (*roaring.Bitmap, BitmapLoadStats, error) {

	totalStart := time.Now()
	bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segmentID, fieldIndex, termKey)
	if err != nil {
		return nil, BitmapLoadStats{}, err
	}

	stats := BitmapLoadStats{
		BytesRead:  bytesRead,
		ReadTime:   readTime,
		DecodeTime: decodeTime,
	}

	if bm == nil || bm.IsEmpty() {
		stats.TotalTime = time.Since(totalStart)
		return nil, stats, nil
	}

	// Trim to ledger range.
	bm = r.trimToLedgerRange(segmentID, bm, startLedger, endLedger)
	stats.TotalTime = time.Since(totalStart)
	if bm == nil || bm.IsEmpty() {
		return nil, stats, nil
	}

	return bm, stats, nil
}

// LoadSegmentLedgerOffsets loads a ledger offsets from segment flat files via mmap cache.
// Implements LedgerOffsetsLoader.
func (r *SegmentReader) LoadSegmentLedgerOffsets(segmentID uint32) (*SegmentLedgerOffsets, error) {
	return r.loadLedgerOffsetsFromFile(segmentID)
}

// getMmap returns a cached mmap for the given path, opening it on first access.
func (r *SegmentReader) getMmap(path string) (*MmapFile, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if mm, ok := r.mmapCache[path]; ok {
		return mm, nil
	}
	mm, err := OpenMmap(path)
	if err != nil {
		return nil, err
	}
	r.mmapCache[path] = mm
	return mm, nil
}

// getHashIndex returns a cached streamhash index for the given path.
func (r *SegmentReader) getHashIndex(path string) (*streamhash.Index, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if hi, ok := r.hashCache[path]; ok {
		return hi, nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	hi, err := streamhash.Open(path)
	if err != nil {
		return nil, err
	}
	r.hashCache[path] = hi
	return hi, nil
}

// getEventstoreReader returns a cached eventstore reader for the given path.
func (r *SegmentReader) getEventstoreReader(dirPath string) (*eventstore.Reader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if er, ok := r.eventCache[dirPath]; ok {
		return er, nil
	}
	eventsPath := filepath.Join(dirPath, EventsFileName)
	if _, err := os.Stat(eventsPath); err != nil {
		return nil, err
	}
	er := eventstore.Open(eventsPath)
	r.eventCache[dirPath] = er

	// Cache record size from packfile trailer (used for GroupsDecompressed counting)
	if _, ok := r.recordSizeCache[dirPath]; !ok {
		pr := packfile.Open(eventsPath)
		if trailer, err := pr.Trailer(); err == nil && trailer.RecordSize > 0 {
			r.recordSizeCache[dirPath] = int(trailer.RecordSize)
		}
		pr.Close()
	}

	return er, nil
}

// getRecordSize returns the cached record size for a segment directory.
// Returns 0 if not cached (e.g. uncompressed or never opened).
func (r *SegmentReader) getRecordSize(dirPath string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recordSizeCache[dirPath]
}

// getAppData returns cached appData from an events.pack file, reading it on first access
// via a temporary packfile.Reader.
func (r *SegmentReader) getAppData(eventsPath string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if data, ok := r.appDataCache[eventsPath]; ok {
		return data, nil
	}
	pr := packfile.Open(eventsPath)
	defer pr.Close()
	data, err := pr.AppData()
	if err != nil {
		return nil, err
	}
	// Cache a copy so we don't depend on the reader after close
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	r.appDataCache[eventsPath] = dataCopy
	return dataCopy, nil
}

// loadLedgerOffsetsFromFile reads ledger offsets from events.pack appData.
func (r *SegmentReader) loadLedgerOffsetsFromFile(segmentID uint32) (*SegmentLedgerOffsets, error) {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(r.basePath, dirName)
	eventsPath := filepath.Join(dirPath, EventsFileName)

	appData, err := r.getAppData(eventsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read appData for ledger offsets: %w", err)
	}

	if len(appData) != SegmentLedgerOffsetsSize {
		return nil, fmt.Errorf("invalid ledger offsets size in appData: got %d, expected %d", len(appData), SegmentLedgerOffsetsSize)
	}

	return &SegmentLedgerOffsets{SegmentID: segmentID, Data: appData}, nil
}

// loadBitmapFromFile loads a bitmap from the unified segment MPHF index files via reader cache.
// fieldIndex: 0=contracts, 1-4=topic positions 0-3.
// Uses 32-byte query keys: [PreHash(compositeKey):16][termHash:16].
func (r *SegmentReader) loadBitmapFromFile(segmentID uint32, fieldIndex int, termKey [16]byte) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(r.basePath, dirName)

	hashPath := filepath.Join(dirPath, "index.hash")
	packPath := filepath.Join(dirPath, "index.pack")

	// Build 32-byte query key: [PreHash(compositeKey):16][termHash:16]
	queryKey := makeStreamhashKey(termKey, byte(fieldIndex))

	// Get cached MPHF index
	readStart := time.Now()
	hashIdx, err := r.getHashIndex(hashPath)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	// O(1) MPHF lookup (no streamhash fingerprint — false positives caught by pack-level xxh3 check)
	slot, err := hashIdx.Query(queryKey)
	if err != nil {
		return nil, 0, time.Since(readStart), 0, nil
	}

	// Get cached pack mmap
	packMmap, err := r.getMmap(packPath)
	if err != nil {
		return nil, 0, time.Since(readStart), 0, err
	}

	// Read offsets from trailer
	numKeys := hashIdx.NumKeys()
	trailerSize := (numKeys + 1) * 8
	fileSize := uint64(packMmap.Len())
	if fileSize < trailerSize {
		return nil, 0, time.Since(readStart), 0, fmt.Errorf("pack file too small: %d < trailer %d", fileSize, trailerSize)
	}
	trailerStart := fileSize - trailerSize

	offStart := trailerStart + slot*8
	offEnd := trailerStart + (slot+1)*8
	if offEnd+8 > fileSize {
		return nil, 0, time.Since(readStart), 0, fmt.Errorf("offset out of bounds in pack trailer")
	}

	bitmapStart := binary.LittleEndian.Uint64(packMmap.data[offStart : offStart+8])
	bitmapEnd := binary.LittleEndian.Uint64(packMmap.data[offEnd : offEnd+8])

	if bitmapEnd < bitmapStart || bitmapEnd > trailerStart {
		return nil, 0, time.Since(readStart), 0, fmt.Errorf("invalid bitmap offsets: [%d, %d)", bitmapStart, bitmapEnd)
	}

	readTime := time.Since(readStart)

	recordBytes := packMmap.data[bitmapStart:bitmapEnd]
	bytesRead := int64(bitmapEnd - bitmapStart)

	if len(recordBytes) < 5 {
		return nil, bytesRead, readTime, 0, nil // record too small
	}

	// Verify 4-byte fingerprint
	expectedFP := xxh3.Hash(queryKey)
	var expectedFPBytes [4]byte
	binary.LittleEndian.PutUint32(expectedFPBytes[:], uint32(expectedFP))
	if recordBytes[0] != expectedFPBytes[0] || recordBytes[1] != expectedFPBytes[1] ||
		recordBytes[2] != expectedFPBytes[2] || recordBytes[3] != expectedFPBytes[3] {
		return nil, bytesRead, readTime, 0, nil // fingerprint mismatch — term not found
	}

	// Verify 1-byte field index
	if recordBytes[4] != byte(fieldIndex) {
		return nil, bytesRead, readTime, 0, nil // wrong field
	}

	// Actual bitmap bytes start after 5-byte prefix
	bitmapBytes := recordBytes[5:]

	// Decode bitmap
	decodeStart := time.Now()
	bm := roaring.New()
	if _, err = bm.FromBuffer(bitmapBytes); err != nil {
		return nil, bytesRead, readTime, 0, fmt.Errorf("failed to decode bitmap: %w", err)
	}
	decodeTime := time.Since(decodeStart)

	return bm, bytesRead, readTime, decodeTime, nil
}

// trimToLedgerRange trims a bitmap to the dense ID range corresponding to the requested ledger range.
func (r *SegmentReader) trimToLedgerRange(segID uint32, bm *roaring.Bitmap, startLedger, endLedger uint32) *roaring.Bitmap {
	segmentStart := segID * SegmentSize

	var startOff uint16
	if startLedger > segmentStart {
		startOff = uint16(startLedger - segmentStart)
	}
	endOff := uint16(SegmentSize - 1)
	if endLedger < segmentStart+SegmentSize-1 {
		endOff = uint16(endLedger - segmentStart)
	}

	needsTrim := startOff > 0 || endOff < uint16(SegmentSize-1)
	if !needsTrim {
		return bm
	}

	lm, err := r.loadLedgerOffsetsFromFile(segID)
	if err != nil {
		// Can't trim without ledger offsets — return full bitmap
		return bm
	}

	startLocalID, endLocalID := lm.LedgerRangeToIDRange(startOff, endOff)

	// Operate in-place: bitmap is freshly loaded from mmap'd pack file, not shared.
	// FromBuffer bitmaps are copy-on-write, so only modified containers are materialized.
	trimmed := bm
	if startLocalID > 0 {
		trimmed.RemoveRange(0, uint64(startLocalID))
	}
	if !trimmed.IsEmpty() {
		if maxVal := trimmed.Maximum(); endLocalID < maxVal {
			trimmed.RemoveRange(uint64(endLocalID)+1, uint64(maxVal)+1)
		}
	}

	return trimmed
}

// FetchByRange reads all events in a ledger range via sequential scan (no index).
// Uses the ledger offsets to determine the dense ID range per segment and reads sequentially.
func (r *SegmentReader) FetchByRange(startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error) {
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
	events := make([]*query.Event, 0)

	for _, segID := range segments {
		if fetchCap > 0 && len(events) >= fetchCap {
			break
		}

		lm, err := r.loadLedgerOffsetsFromFile(segID)
		if err != nil {
			continue // segment may not exist
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

		startID, endID := lm.LedgerRangeToIDRange(startOff, endOff)
		if endID < startID {
			continue
		}

		count := int(endID - startID + 1)
		if fetchCap > 0 && count > fetchCap-len(events) {
			count = fetchCap - len(events)
		}

		result.MatchingLocalIDs += count

		// Open eventstore reader for this segment
		dirName := fmt.Sprintf("%06d", segID)
		eventsPath := filepath.Join(r.basePath, dirName)

		readStart := time.Now()
		er, err := r.getEventstoreReader(eventsPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open eventstore for segment %d: %w", segID, err)
		}

		// Count groups (records/blocks) decompressed for this segment
		if recSize := r.getRecordSize(eventsPath); recSize > 0 {
			firstRec := int(startID) / recSize
			lastRec := (int(startID) + count - 1) / recSize
			result.GroupsDecompressed += lastRec - firstRec + 1
		}

		// Sequential range read using eventstore.ReadEvents
		eventIdx := 0
		for blob, readErr := range er.ReadEvents(int(startID), count) {
			if readErr != nil {
				return nil, nil, fmt.Errorf("failed to read events from segment %d: %w", segID, readErr)
			}

			denseID := startID + uint32(eventIdx)
			eventIdx++

			// Copy the blob since it's only valid until the next iteration
			blobCopy := make([]byte, len(blob))
			copy(blobCopy, blob)

			result.EventBytesRead += int64(len(blobCopy))
			result.EventsScanned++

			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)

			decStart := time.Now()
			ev, err := event.DecodeBinaryToQueryEvent(blobCopy, ledger, eventSeq)
			result.DecodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, ev)
		}
		result.EventFetchTime += time.Since(readStart)
	}

	// Subtract decode time so EventFetchTime reflects pure I/O + decompression
	result.EventFetchTime -= result.DecodeTime

	if fetchCap > 0 && len(events) > fetchCap {
		events = events[:fetchCap]
	}

	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// FetchByIDs reads events from flat files using pre-resolved bitmap IDs.
// Uses eventstore.ReadIndices for scattered access.
func (r *SegmentReader) FetchByIDs(perSegment map[uint32]*roaring.Bitmap, limit int, result *QueryResult) ([]*query.Event, error) {
	segIDs := make([]uint32, 0, len(perSegment))
	for segID := range perSegment {
		segIDs = append(segIDs, segID)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	fetchCap := result.MatchingLocalIDs
	if limit > 0 && fetchCap > limit {
		fetchCap = limit
	}
	events := make([]*query.Event, 0, fetchCap)

	for _, segID := range segIDs {
		if len(events) >= fetchCap {
			break
		}

		bitmap := perSegment[segID]

		// Collect dense IDs for this segment (capped at remaining fetch limit)
		idCap := int(bitmap.GetCardinality())
		if remaining := fetchCap - len(events); remaining < idCap {
			idCap = remaining
		}
		denseIDs := make([]uint32, 0, idCap)
		bitmapIter := bitmap.Iterator()
		for bitmapIter.HasNext() {
			if len(denseIDs)+len(events) >= fetchCap {
				break
			}
			denseIDs = append(denseIDs, bitmapIter.Next())
		}

		if len(denseIDs) == 0 {
			continue
		}

		// Load ledger offsets for this segment (needed for ledger/seq resolution)
		lm, err := r.loadLedgerOffsetsFromFile(segID)
		if err != nil {
			return nil, fmt.Errorf("failed to load ledger offsets for segment %d: %w", segID, err)
		}

		// Open eventstore reader
		dirName := fmt.Sprintf("%06d", segID)
		eventsPath := filepath.Join(r.basePath, dirName)

		readStart := time.Now()
		er, err := r.getEventstoreReader(eventsPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open eventstore for segment %d: %w", segID, err)
		}

		// Convert uint32 dense IDs to sorted int indices for ReadIndices
		indices := make([]int, len(denseIDs))
		for i, id := range denseIDs {
			indices[i] = int(id)
		}

		// Count distinct records (blocks) that will be decompressed
		if recSize := r.getRecordSize(eventsPath); recSize > 0 {
			prevRec := -1
			for _, idx := range indices {
				rec := idx / recSize
				if rec != prevRec {
					result.GroupsDecompressed++
					prevRec = rec
				}
			}
		}

		// Use ReadIndices for parallel scattered event fetch
		eventIdx := 0
		for blob, readErr := range er.ReadIndices(context.Background(), indices) {
			if readErr != nil {
				return nil, fmt.Errorf("failed to read events from segment %d: %w", segID, readErr)
			}

			denseID := denseIDs[eventIdx]
			eventIdx++

			result.EventBytesRead += int64(len(blob))
			result.EventsScanned++

			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)

			decStart := time.Now()
			ev, err := event.DecodeBinaryToQueryEvent(blob, ledger, eventSeq)
			result.DecodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, ev)
		}
		result.EventFetchTime += time.Since(readStart)
	}

	// Subtract decode time so EventFetchTime reflects pure I/O + decompression
	result.EventFetchTime -= result.DecodeTime

	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	result.EventsReturned = len(events)
	return events, nil
}

// Verify interface compliance at compile time.
var _ QueryBackend = (*SegmentReader)(nil)
