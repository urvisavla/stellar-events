package store

import (
	"bytes"
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

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/eventstore"
	"github.com/urvisavla/stellar-events/internal/query"
)

// SegmentReader reads flat file indexes (MPHF .hash + .pack) for queries.
// Uses cached readers to avoid repeated opens; mapped files are cached for reuse.
type SegmentReader struct {
	basePath string
	es       *EventStore

	mu            sync.Mutex
	mmapCache     map[string]*MmapFile         // path -> mmap'd file (for segment.meta)
	hashCache     map[string]*streamhash.Index  // path -> streamhash index (for .hash files)
	packMmapCache map[string]*MmapFile          // path -> mmap'd .pack file
	eventCache    map[string]*eventstore.Reader  // path -> eventstore reader (for event data)
}

// NewSegmentReader creates a new reader for segment flat file indexes.
func NewSegmentReader(basePath string, es *EventStore) *SegmentReader {
	return &SegmentReader{
		basePath:      basePath,
		es:            es,
		mmapCache:     make(map[string]*MmapFile),
		hashCache:     make(map[string]*streamhash.Index),
		packMmapCache: make(map[string]*MmapFile),
		eventCache:    make(map[string]*eventstore.Reader),
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
	for _, pm := range r.packMmapCache {
		pm.Close()
	}
	for _, er := range r.eventCache {
		er.Close()
	}
	r.mmapCache = nil
	r.hashCache = nil
	r.packMmapCache = nil
	r.eventCache = nil
	return nil
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

// getPackMmap returns a cached mmap for a .pack file.
func (r *SegmentReader) getPackMmap(path string) (*MmapFile, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if pm, ok := r.packMmapCache[path]; ok {
		return pm, nil
	}
	pm, err := OpenMmap(path)
	if err != nil {
		return nil, err
	}
	r.packMmapCache[path] = pm
	return pm, nil
}

// getEventstoreReader returns a cached eventstore reader for the given path.
func (r *SegmentReader) getEventstoreReader(dirPath string) (*eventstore.Reader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if er, ok := r.eventCache[dirPath]; ok {
		return er, nil
	}
	if _, err := os.Stat(filepath.Join(dirPath, EventsIndexFileName)); err != nil {
		return nil, err
	}
	er := eventstore.Open(dirPath)
	r.eventCache[dirPath] = er
	return er, nil
}

// QueryEvents queries events using flat file segment indexes (single-value per filter).
func (r *SegmentReader) QueryEvents(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	segments := GetSegmentsForRange(startLedger, endLedger)
	result.SegmentsTouched = len(segments)

	indexStart := time.Now()

	// Collect per-segment bitmaps, AND across filter terms
	type segmentBitmaps struct {
		bitmaps []*roaring.Bitmap
	}
	allSegments := make(map[uint32]*segmentBitmaps)
	expectedTerms := 0

	// Query contract term
	if len(contractID) > 0 {
		expectedTerms++
		termKey := ContractTermKey(contractID)
		for _, segID := range segments {
			bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segID, true, termKey, -1)
			if err != nil {
				continue // segment file may not exist
			}
			result.IndexBytesRead += bytesRead
			result.IndexReadTime += readTime
			result.IndexDecodeTime += decodeTime
			if bm != nil && !bm.IsEmpty() {
				// Trim to ledger range before intersection (matches bitmap32 behavior)
				bm = r.trimToLedgerRange(segID, bm, startLedger, endLedger)
				if bm == nil || bm.IsEmpty() {
					continue
				}
				result.SegmentsScanned++
				if _, ok := allSegments[segID]; !ok {
					allSegments[segID] = &segmentBitmaps{}
				}
				allSegments[segID].bitmaps = append(allSegments[segID].bitmaps, bm)
			}
		}
	}

	// Query topic terms (positional)
	for pos, tg := range topicGroups {
		for _, topic := range tg {
			if len(topic) == 0 {
				continue
			}
			expectedTerms++
			termKey := TopicTermKey(topic)
			for _, segID := range segments {
				bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segID, false, termKey, pos)
				if err != nil {
					continue
				}
				result.IndexBytesRead += bytesRead
				result.IndexReadTime += readTime
				result.IndexDecodeTime += decodeTime
				if bm != nil && !bm.IsEmpty() {
					bm = r.trimToLedgerRange(segID, bm, startLedger, endLedger)
					if bm == nil || bm.IsEmpty() {
						continue
					}
					result.SegmentsScanned++
					if _, ok := allSegments[segID]; !ok {
						allSegments[segID] = &segmentBitmaps{}
					}
					allSegments[segID].bitmaps = append(allSegments[segID].bitmaps, bm)
				}
			}
		}
	}

	// Intersect per-segment bitmaps (bitmaps already trimmed to ledger range)
	intersectStart := time.Now()
	perSegment := make(map[uint32]*roaring.Bitmap)
	for segID, sb := range allSegments {
		if len(sb.bitmaps) < expectedTerms {
			continue
		}
		bm := roaring.FastAnd(sb.bitmaps...)
		if !bm.IsEmpty() {
			perSegment[segID] = bm
			result.MatchingLocalIDs += int(bm.GetCardinality())
		}
	}
	result.IndexIntersectTime = time.Since(intersectStart)
	result.IndexLookupTime = time.Since(indexStart)

	if result.MatchingLocalIDs == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// Resolve dense IDs to event keys and fetch from RocksDB
	events, err := r.fetchEvents(perSegment, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// QueryEventsMultiFilter queries events with multi-value OR/AND filters.
func (r *SegmentReader) QueryEventsMultiFilter(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	segments := GetSegmentsForRange(startLedger, endLedger)
	result.SegmentsTouched = len(segments)

	indexStart := time.Now()

	// Collect per-group, per-segment bitmaps
	// groupIdx 0 = contracts, 1-4 = topic positions
	type groupSegBitmaps struct {
		bitmaps map[uint32][]*roaring.Bitmap
	}
	groups := make(map[int]*groupSegBitmaps)

	// Query contract bitmaps (OR within contracts group)
	if len(contractIDs) > 0 {
		groups[0] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, cid := range contractIDs {
			termKey := ContractTermKey(cid)
			for _, segID := range segments {
				bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segID, true, termKey, -1)
				if err != nil {
					continue
				}
				result.IndexBytesRead += bytesRead
				result.IndexReadTime += readTime
				result.IndexDecodeTime += decodeTime
				if bm != nil && !bm.IsEmpty() {
					bm = r.trimToLedgerRange(segID, bm, startLedger, endLedger)
					if bm == nil || bm.IsEmpty() {
						continue
					}
					result.SegmentsScanned++
					groups[0].bitmaps[segID] = append(groups[0].bitmaps[segID], bm)
				}
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
			for _, segID := range segments {
				bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segID, false, termKey, pos)
				if err != nil {
					continue
				}
				result.IndexBytesRead += bytesRead
				result.IndexReadTime += readTime
				result.IndexDecodeTime += decodeTime
				if bm != nil && !bm.IsEmpty() {
					bm = r.trimToLedgerRange(segID, bm, startLedger, endLedger)
					if bm == nil || bm.IsEmpty() {
						continue
					}
					result.SegmentsScanned++
					groups[groupIdx].bitmaps[segID] = append(groups[groupIdx].bitmaps[segID], bm)
				}
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

	// For each segment: OR within each group, then AND across groups (bitmaps already trimmed)
	intersectStart := time.Now()
	perSegment := make(map[uint32]*roaring.Bitmap)
	for segID := range allSegIDs {
		var groupUnions []*roaring.Bitmap

		for _, g := range groups {
			bms := g.bitmaps[segID]
			if len(bms) == 0 {
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
			perSegment[segID] = intersected
			result.MatchingLocalIDs += int(intersected.GetCardinality())
		}
	}
	result.IndexIntersectTime = time.Since(intersectStart)
	result.IndexLookupTime = time.Since(indexStart)

	if result.MatchingLocalIDs == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	events, err := r.fetchEvents(perSegment, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// loadLedgerMapFromFile reads a ledger map from segment flat files via mmap cache.
func (r *SegmentReader) loadLedgerMapFromFile(segmentID uint32) (*SegmentLedgerMap, error) {
	dirName := fmt.Sprintf("%06d", segmentID)
	path := filepath.Join(r.basePath, dirName, LedgerMapFileName)

	mm, err := r.getMmap(path)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap ledger map: %w", err)
	}

	if mm.Len() != SegmentLedgerMapSize {
		return nil, fmt.Errorf("invalid ledger map size: got %d, expected %d", mm.Len(), SegmentLedgerMapSize)
	}

	// Data points directly into the mmap region (zero-copy).
	return &SegmentLedgerMap{SegmentID: segmentID, Data: mm.data}, nil
}

// loadBitmapFromFile loads a bitmap from segment MPHF index files via reader cache.
// For contracts: reads contracts.hash + contracts.pack
// For topics: reads topic{pos}.hash + topic{pos}.pack
func (r *SegmentReader) loadBitmapFromFile(segmentID uint32, isContract bool, termKey [32]byte, pos int) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(r.basePath, dirName)

	var name string
	if isContract {
		name = "contracts"
	} else {
		name = fmt.Sprintf("topic%d", pos)
	}

	hashPath := filepath.Join(dirPath, name+".hash")
	packPath := filepath.Join(dirPath, name+".pack")

	// Get cached MPHF index
	readStart := time.Now()
	hashIdx, err := r.getHashIndex(hashPath)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	// O(1) MPHF lookup with fingerprint check
	slot, err := hashIdx.Query(termKey[:])
	if err != nil {
		// Fingerprint mismatch or not found — term not in index
		return nil, 0, time.Since(readStart), 0, nil
	}

	// Get cached pack mmap
	packMmap, err := r.getPackMmap(packPath)
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

	bitmapBytes := packMmap.data[bitmapStart:bitmapEnd]
	bytesRead := int64(bitmapEnd - bitmapStart)

	// Decode bitmap
	decodeStart := time.Now()
	bm := roaring.New()
	if err = bm.UnmarshalBinary(bitmapBytes); err != nil {
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

	lm, err := r.loadLedgerMapFromFile(segID)
	if err != nil {
		// Can't trim without ledger map — return full bitmap
		return bm
	}

	startLocalID, endLocalID := lm.LedgerRangeToIDRange(startOff, endOff)

	trimmed := bm.Clone()
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

// fetchEvents resolves dense IDs to (ledger, eventSeq) and fetches events from RocksDB.
// Mirrors the parallel worker pattern from QueryEventsWithBitmap32EventIndex.
func (r *SegmentReader) fetchEvents(perSegment map[uint32]*roaring.Bitmap, limit int, result *Bitmap32EventQueryResult) ([]*query.Event, error) {
	type bitmapEvtMeta struct {
		ledger   uint32
		eventSeq uint16
	}

	segIDs := make([]uint32, 0, len(perSegment))
	for segID := range perSegment {
		segIDs = append(segIDs, segID)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	fetchCap := result.MatchingLocalIDs
	if limit > 0 && fetchCap > limit {
		fetchCap = limit
	}
	allKeys := make([][]byte, 0, fetchCap)
	allMetas := make([]bitmapEvtMeta, 0, fetchCap)

	for _, segID := range segIDs {
		if len(allKeys) >= fetchCap {
			break
		}

		lm, err := r.loadLedgerMapFromFile(segID)
		if err != nil {
			return nil, fmt.Errorf("failed to load ledger map for segment %d: %w", segID, err)
		}
		if lm == nil {
			continue
		}

		bitmap := perSegment[segID]
		bitmapIter := bitmap.Iterator()
		for bitmapIter.HasNext() {
			if len(allKeys) >= fetchCap {
				break
			}
			denseID := bitmapIter.Next()
			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
			allKeys = append(allKeys, event.EncodeKeyV2(ledger, eventSeq))
			allMetas = append(allMetas, bitmapEvtMeta{ledger: ledger, eventSeq: eventSeq})
		}
	}

	// Parallel fetch using iterators
	const maxWorkers = 4
	numWorkers := maxWorkers
	if len(allKeys) < numWorkers {
		numWorkers = len(allKeys)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	type workerResult struct {
		events     []*query.Event
		fetchTime  time.Duration
		decodeTime time.Duration
		filterTime time.Duration
		bytesRead  int64
		scanned    int
	}

	results := make([]workerResult, numWorkers)
	chunkSize := (len(allKeys) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > len(allKeys) {
			end = len(allKeys)
		}
		if start >= end {
			continue
		}

		wg.Add(1)
		go func(workerID, start, end int) {
			defer wg.Done()
			wr := &results[workerID]
			wr.events = make([]*query.Event, 0, end-start)

			dbIter := r.es.db.NewIteratorCF(r.es.ro, r.es.cfEvents)
			defer dbIter.Close()

			for i := start; i < end; i++ {
				key := allKeys[i]
				meta := allMetas[i]

				seekStart := time.Now()
				dbIter.Seek(key)
				if !dbIter.Valid() {
					wr.fetchTime += time.Since(seekStart)
					break
				}

				iterKey := dbIter.Key()
				if iterKey == nil || !bytes.Equal(iterKey.Data(), key) {
					wr.fetchTime += time.Since(seekStart)
					continue
				}

				iterVal := dbIter.Value()
				if iterVal == nil {
					wr.fetchTime += time.Since(seekStart)
					continue
				}
				valData := iterVal.Data()
				if len(valData) == 0 {
					wr.fetchTime += time.Since(seekStart)
					continue
				}

				valueCopy := make([]byte, len(valData))
				copy(valueCopy, valData)
				wr.fetchTime += time.Since(seekStart)

				wr.bytesRead += int64(len(valueCopy))
				wr.scanned++

				// Decode to query.Event
				decStart := time.Now()
				var ev *query.Event
				var decErr error
				ev, decErr = parseRawXDRToQueryEvent(valueCopy, meta.ledger, 0, 0, meta.eventSeq)
				wr.decodeTime += time.Since(decStart)

				if decErr != nil {
					continue
				}

				wr.events = append(wr.events, ev)
			}
		}(w, start, end)
	}

	wg.Wait()

	// Merge results — use max of per-worker times since workers run in parallel
	var fetchTime, decodeTime, filterTime time.Duration
	events := make([]*query.Event, 0, fetchCap)
	for _, wr := range results {
		events = append(events, wr.events...)
		if wr.fetchTime > fetchTime {
			fetchTime = wr.fetchTime
		}
		if wr.decodeTime > decodeTime {
			decodeTime = wr.decodeTime
		}
		if wr.filterTime > filterTime {
			filterTime = wr.filterTime
		}
		result.EventBytesRead += wr.bytesRead
		result.EventsScanned += wr.scanned
	}
	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventsReturned = len(events)

	return events, nil
}

// QueryEventsFromSegmentData queries events using flat file bitmap indexes + eventstore (no RocksDB).
func (r *SegmentReader) QueryEventsFromSegmentData(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	segments := GetSegmentsForRange(startLedger, endLedger)
	result.SegmentsTouched = len(segments)

	indexStart := time.Now()

	type segmentBitmaps struct {
		bitmaps []*roaring.Bitmap
	}
	allSegments := make(map[uint32]*segmentBitmaps)
	expectedTerms := 0

	if len(contractID) > 0 {
		expectedTerms++
		termKey := ContractTermKey(contractID)
		for _, segID := range segments {
			bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segID, true, termKey, -1)
			if err != nil {
				continue
			}
			result.IndexBytesRead += bytesRead
			result.IndexReadTime += readTime
			result.IndexDecodeTime += decodeTime
			if bm != nil && !bm.IsEmpty() {
				bm = r.trimToLedgerRange(segID, bm, startLedger, endLedger)
				if bm == nil || bm.IsEmpty() {
					continue
				}
				result.SegmentsScanned++
				if _, ok := allSegments[segID]; !ok {
					allSegments[segID] = &segmentBitmaps{}
				}
				allSegments[segID].bitmaps = append(allSegments[segID].bitmaps, bm)
			}
		}
	}

	for pos, tg := range topicGroups {
		for _, topic := range tg {
			if len(topic) == 0 {
				continue
			}
			expectedTerms++
			termKey := TopicTermKey(topic)
			for _, segID := range segments {
				bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segID, false, termKey, pos)
				if err != nil {
					continue
				}
				result.IndexBytesRead += bytesRead
				result.IndexReadTime += readTime
				result.IndexDecodeTime += decodeTime
				if bm != nil && !bm.IsEmpty() {
					bm = r.trimToLedgerRange(segID, bm, startLedger, endLedger)
					if bm == nil || bm.IsEmpty() {
						continue
					}
					result.SegmentsScanned++
					if _, ok := allSegments[segID]; !ok {
						allSegments[segID] = &segmentBitmaps{}
					}
					allSegments[segID].bitmaps = append(allSegments[segID].bitmaps, bm)
				}
			}
		}
	}

	intersectStart := time.Now()
	perSegment := make(map[uint32]*roaring.Bitmap)
	for segID, sb := range allSegments {
		if len(sb.bitmaps) < expectedTerms {
			continue
		}
		bm := roaring.FastAnd(sb.bitmaps...)
		if !bm.IsEmpty() {
			perSegment[segID] = bm
			result.MatchingLocalIDs += int(bm.GetCardinality())
		}
	}
	result.IndexIntersectTime = time.Since(intersectStart)
	result.IndexLookupTime = time.Since(indexStart)

	if result.MatchingLocalIDs == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	events, err := r.fetchEventsFromSegmentData(perSegment, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// QueryEventsFromSegmentDataMultiFilter queries events from eventstore with multi-value OR/AND filters.
func (r *SegmentReader) QueryEventsFromSegmentDataMultiFilter(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	segments := GetSegmentsForRange(startLedger, endLedger)
	result.SegmentsTouched = len(segments)

	indexStart := time.Now()

	type groupSegBitmaps struct {
		bitmaps map[uint32][]*roaring.Bitmap
	}
	groups := make(map[int]*groupSegBitmaps)

	if len(contractIDs) > 0 {
		groups[0] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, cid := range contractIDs {
			termKey := ContractTermKey(cid)
			for _, segID := range segments {
				bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segID, true, termKey, -1)
				if err != nil {
					continue
				}
				result.IndexBytesRead += bytesRead
				result.IndexReadTime += readTime
				result.IndexDecodeTime += decodeTime
				if bm != nil && !bm.IsEmpty() {
					bm = r.trimToLedgerRange(segID, bm, startLedger, endLedger)
					if bm == nil || bm.IsEmpty() {
						continue
					}
					result.SegmentsScanned++
					groups[0].bitmaps[segID] = append(groups[0].bitmaps[segID], bm)
				}
			}
		}
	}

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
				bm, bytesRead, readTime, decodeTime, err := r.loadBitmapFromFile(segID, false, termKey, pos)
				if err != nil {
					continue
				}
				result.IndexBytesRead += bytesRead
				result.IndexReadTime += readTime
				result.IndexDecodeTime += decodeTime
				if bm != nil && !bm.IsEmpty() {
					bm = r.trimToLedgerRange(segID, bm, startLedger, endLedger)
					if bm == nil || bm.IsEmpty() {
						continue
					}
					result.SegmentsScanned++
					groups[groupIdx].bitmaps[segID] = append(groups[groupIdx].bitmaps[segID], bm)
				}
			}
		}
	}

	allSegIDs := make(map[uint32]bool)
	for _, g := range groups {
		for segID := range g.bitmaps {
			allSegIDs[segID] = true
		}
	}

	intersectStart := time.Now()
	perSegment := make(map[uint32]*roaring.Bitmap)
	for segID := range allSegIDs {
		var groupUnions []*roaring.Bitmap
		for _, g := range groups {
			bms := g.bitmaps[segID]
			if len(bms) == 0 {
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
			perSegment[segID] = intersected
			result.MatchingLocalIDs += int(intersected.GetCardinality())
		}
	}
	result.IndexIntersectTime = time.Since(intersectStart)
	result.IndexLookupTime = time.Since(indexStart)

	if result.MatchingLocalIDs == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	events, err := r.fetchEventsFromSegmentData(perSegment, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// GetEventsInRange reads all events in a ledger range from eventstore (no index, no RocksDB).
// Uses the ledger map to determine the dense ID range per segment and reads sequentially.
func (r *SegmentReader) GetEventsInRange(startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
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

		lm, err := r.loadLedgerMapFromFile(segID)
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

		// Sequential range read using eventstore.ReadEvents
		var ioStats eventstore.ReadStats
		eventIdx := 0
		for blob, readErr := range er.ReadEvents(int(startID), count, &ioStats) {
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
			ev, err := event.DecodeBinaryToQueryEventV4(blobCopy, ledger, eventSeq)
			result.DecodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, ev)
		}
		result.EventFetchTime += time.Since(readStart)
		result.DecompressTime += ioStats.DecompressTime
		result.EventDiskReadTime += ioStats.DiskReadTime
		result.GroupsDecompressed += ioStats.BlocksRead
	}

	if fetchCap > 0 && len(events) > fetchCap {
		events = events[:fetchCap]
	}

	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// fetchEventsFromSegmentData reads events from eventstore (no RocksDB).
// Uses eventstore.ReadIndices for parallel scattered access.
func (r *SegmentReader) fetchEventsFromSegmentData(perSegment map[uint32]*roaring.Bitmap, limit int, result *Bitmap32EventQueryResult) ([]*query.Event, error) {
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

		// Collect dense IDs for this segment (sorted by bitmap iterator)
		denseIDs := make([]uint32, 0, bitmap.GetCardinality())
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

		// Load ledger map for this segment (needed for ledger/seq resolution)
		lm, err := r.loadLedgerMapFromFile(segID)
		if err != nil {
			return nil, fmt.Errorf("failed to load ledger map for segment %d: %w", segID, err)
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

		// Use ReadIndices for parallel scattered event fetch
		var ioStats eventstore.ReadStats
		eventIdx := 0
		for blob, readErr := range er.ReadIndices(context.Background(), indices, &ioStats) {
			if readErr != nil {
				return nil, fmt.Errorf("failed to read events from segment %d: %w", segID, readErr)
			}

			denseID := denseIDs[eventIdx]
			eventIdx++

			result.EventBytesRead += int64(len(blob))
			result.EventsScanned++

			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)

			decStart := time.Now()
			ev, err := event.DecodeBinaryToQueryEventV4(blob, ledger, eventSeq)
			result.DecodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, ev)
		}
		result.EventFetchTime += time.Since(readStart)
		result.DecompressTime += ioStats.DecompressTime
		result.EventDiskReadTime += ioStats.DiskReadTime
		result.GroupsDecompressed += ioStats.BlocksRead
	}

	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	result.EventsReturned = len(events)
	return events, nil
}

// SegmentIndexToUnified converts a Bitmap32EventQueryResult from segment index queries to UnifiedQueryResult.
func SegmentIndexToUnified(r *Bitmap32EventQueryResult) *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:          "segment-index",
		LedgerRange:        r.LedgerRange,
		SegmentsTouched:    r.SegmentsTouched,
		IndexMatches:       r.MatchingLocalIDs,
		MatchUnitName:      "local IDs",
		EventsScanned:      r.EventsScanned,
		EventsReturned:     r.EventsReturned,
		IndexBytesRead:     r.IndexBytesRead,
		EventBytesRead:     r.EventBytesRead,
		IndexLookupTime:    r.IndexLookupTime,
		EventFetchTime:     r.EventFetchTime,
		DecompressTime:     r.DecompressTime,
		EventDiskReadTime:  r.EventDiskReadTime,
		GroupsDecompressed: r.GroupsDecompressed,
		DecodeTime:         r.DecodeTime,
		FilterTime:         r.FilterTime,
		TotalTime:          r.TotalTime,
	}
}

// SegmentDataToUnified converts a Bitmap32EventQueryResult from segment file store queries to UnifiedQueryResult.
func SegmentDataToUnified(r *Bitmap32EventQueryResult) *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:          "segment-data",
		LedgerRange:        r.LedgerRange,
		SegmentsTouched:    r.SegmentsTouched,
		IndexMatches:       r.MatchingLocalIDs,
		MatchUnitName:      "local IDs",
		EventsScanned:      r.EventsScanned,
		EventsReturned:     r.EventsReturned,
		IndexBytesRead:     r.IndexBytesRead,
		EventBytesRead:     r.EventBytesRead,
		IndexLookupTime:    r.IndexLookupTime,
		EventFetchTime:     r.EventFetchTime,
		DecompressTime:     r.DecompressTime,
		EventDiskReadTime:  r.EventDiskReadTime,
		GroupsDecompressed: r.GroupsDecompressed,
		DecodeTime:         r.DecodeTime,
		FilterTime:         r.FilterTime,
		TotalTime:          r.TotalTime,
	}
}
