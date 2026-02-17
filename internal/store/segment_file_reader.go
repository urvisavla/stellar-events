package store

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// SegmentFileReader reads flat file indexes (.idx/.pack) for queries.
// Uses mmap to avoid repeated syscalls; mapped files are cached for reuse.
type SegmentFileReader struct {
	basePath string
	es       *RocksDBEventStore

	mu        sync.Mutex
	mmapCache map[string]*MmapFile // path -> mmap'd file, reused across queries
}

// NewSegmentFileReader creates a new reader for segment flat file indexes.
func NewSegmentFileReader(basePath string, es *RocksDBEventStore) *SegmentFileReader {
	return &SegmentFileReader{
		basePath:  basePath,
		es:        es,
		mmapCache: make(map[string]*MmapFile),
	}
}

// Close releases all mmap'd files held by this reader.
func (r *SegmentFileReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, mm := range r.mmapCache {
		mm.Close()
	}
	r.mmapCache = nil
	return nil
}

// getMmap returns a cached mmap for the given path, opening it on first access.
func (r *SegmentFileReader) getMmap(path string) (*MmapFile, error) {
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

// QueryEvents queries events using flat file segment indexes (single-value per filter).
func (r *SegmentFileReader) QueryEvents(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	buckets := GetBucketsForRange(startLedger, endLedger)
	result.BucketsTouched = len(buckets)

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
		for _, segID := range buckets {
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
			termKey := TopicTermKey(pos, topic)
			for _, segID := range buckets {
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
	events, err := r.fetchEvents(perSegment, contractID, nil, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// QueryEventsMultiFilter queries events with multi-value OR/AND filters.
func (r *SegmentFileReader) QueryEventsMultiFilter(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	buckets := GetBucketsForRange(startLedger, endLedger)
	result.BucketsTouched = len(buckets)

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
			for _, segID := range buckets {
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
			termKey := TopicTermKey(pos, topicXDR)
			for _, segID := range buckets {
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

	events, err := r.fetchEvents(perSegment, nil, contractIDs, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// loadLedgerMapFromFile reads a ledger map from segment flat files via mmap cache.
func (r *SegmentFileReader) loadLedgerMapFromFile(segmentID uint32) (*BucketLedgerMap, error) {
	dirName := fmt.Sprintf("%06d", segmentID)
	path := filepath.Join(r.basePath, dirName, LedgerMapFileName)

	mm, err := r.getMmap(path)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap ledger map: %w", err)
	}

	if mm.Len() != BucketLedgerMapSize {
		return nil, fmt.Errorf("invalid ledger map size: got %d, expected %d", mm.Len(), BucketLedgerMapSize)
	}

	// Data points directly into the mmap region (zero-copy).
	return &BucketLedgerMap{BucketID: segmentID, Data: mm.data}, nil
}

// loadBitmapFromFile loads a bitmap from segment flat files via mmap cache.
// For contracts: reads contracts.idx/.pack
// For topics: reads topic{pos}.idx/.pack, falls back to topics.idx/.pack
func (r *SegmentFileReader) loadBitmapFromFile(segmentID uint32, isContract bool, termKey [16]byte, pos int) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(r.basePath, dirName)

	var idxName string
	if isContract {
		idxName = "contracts"
	} else {
		idxName = fmt.Sprintf("topic%d", pos)
	}

	idxPath := filepath.Join(dirPath, idxName+".idx")
	packPath := filepath.Join(dirPath, idxName+".pack")

	// Try per-position file first; fall back to combined topics
	readStart := time.Now()
	var idxFile *IndexFile
	idxMm, err := r.getMmap(idxPath)
	if err == nil {
		idxFile, err = OpenIndexFileMmap(idxMm)
	}
	if err != nil && !isContract {
		// Fall back to combined topics.idx
		idxName = "topics"
		idxPath = filepath.Join(dirPath, idxName+".idx")
		packPath = filepath.Join(dirPath, idxName+".pack")
		idxMm, err = r.getMmap(idxPath)
		if err == nil {
			idxFile, err = OpenIndexFileMmap(idxMm)
		}
	}
	readTime := time.Since(readStart)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	// Look up the term (binary search — pure CPU, no I/O)
	entry := idxFile.LookupTerm(termKey)
	if entry == nil {
		return nil, 0, readTime, 0, nil
	}

	// Read bitmap data from pack file (zero-copy slice from mmap)
	packStart := time.Now()
	packMm, err := r.getMmap(packPath)
	if err != nil {
		return nil, 0, readTime, 0, err
	}
	data, err := ReadBitmapFromPackMmap(packMm, entry.PackOffset, entry.PackLength)
	packReadTime := time.Since(packStart)
	if err != nil {
		return nil, 0, readTime, 0, err
	}
	bytesRead := int64(entry.PackLength)

	// Decode bitmap (UnmarshalBinary copies data, safe with read-only mmap)
	decodeStart := time.Now()
	bm := roaring.New()
	if err = bm.UnmarshalBinary(data); err != nil {
		return nil, bytesRead, readTime + packReadTime, 0, fmt.Errorf("failed to decode bitmap: %w", err)
	}
	decodeTime := time.Since(decodeStart)

	return bm, bytesRead, readTime + packReadTime, decodeTime, nil
}

// trimToLedgerRange trims a bitmap to the dense ID range corresponding to the requested ledger range.
func (r *SegmentFileReader) trimToLedgerRange(segID uint32, bm *roaring.Bitmap, startLedger, endLedger uint32) *roaring.Bitmap {
	segmentStart := segID * BucketSize

	var startOff uint16
	if startLedger > segmentStart {
		startOff = uint16(startLedger - segmentStart)
	}
	endOff := uint16(BucketSize - 1)
	if endLedger < segmentStart+BucketSize-1 {
		endOff = uint16(endLedger - segmentStart)
	}

	needsTrim := startOff > 0 || endOff < uint16(BucketSize-1)
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
func (r *SegmentFileReader) fetchEvents(perSegment map[uint32]*roaring.Bitmap, singleContractID []byte, multiContractIDs [][]byte, limit int, result *Bitmap32EventQueryResult) ([]*query.Event, error) {
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

				// Post-filter: contract ID (topic filtering handled by positional index)
				filterStart := time.Now()
				if r.es.eventFormat == "binary" {
					if len(singleContractID) > 0 {
						header := event.ParseBinaryHeader(valueCopy)
						if header != nil && !header.MatchesContractID(singleContractID) {
							wr.filterTime += time.Since(filterStart)
							continue
						}
					} else if len(multiContractIDs) > 0 {
						header := event.ParseBinaryHeader(valueCopy)
						if header != nil {
							contractMatch := false
							for _, cid := range multiContractIDs {
								if header.MatchesContractID(cid) {
									contractMatch = true
									break
								}
							}
							if !contractMatch {
								wr.filterTime += time.Since(filterStart)
								continue
							}
						}
					}
				}
				wr.filterTime += time.Since(filterStart)

				// Decode to query.Event
				decStart := time.Now()
				var ev *query.Event
				var decErr error
				if r.es.eventFormat == "binary" {
					ev, decErr = event.DecodeBinaryToQueryEventV2(valueCopy, meta.ledger, meta.eventSeq)
				} else {
					ev, decErr = parseRawXDRToQueryEvent(valueCopy, meta.ledger, 0, 0, meta.eventSeq)
				}
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

// QueryEventsFromVolume queries events using flat file bitmap indexes + flat file event volume (no RocksDB).
func (r *SegmentFileReader) QueryEventsFromVolume(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	buckets := GetBucketsForRange(startLedger, endLedger)
	result.BucketsTouched = len(buckets)

	indexStart := time.Now()

	type segmentBitmaps struct {
		bitmaps []*roaring.Bitmap
	}
	allSegments := make(map[uint32]*segmentBitmaps)
	expectedTerms := 0

	if len(contractID) > 0 {
		expectedTerms++
		termKey := ContractTermKey(contractID)
		for _, segID := range buckets {
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
			termKey := TopicTermKey(pos, topic)
			for _, segID := range buckets {
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

	events, err := r.fetchEventsFromVolume(perSegment, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// QueryEventsFromVolumeMultiFilter queries events from volume with multi-value OR/AND filters.
func (r *SegmentFileReader) QueryEventsFromVolumeMultiFilter(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	buckets := GetBucketsForRange(startLedger, endLedger)
	result.BucketsTouched = len(buckets)

	indexStart := time.Now()

	type groupSegBitmaps struct {
		bitmaps map[uint32][]*roaring.Bitmap
	}
	groups := make(map[int]*groupSegBitmaps)

	if len(contractIDs) > 0 {
		groups[0] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, cid := range contractIDs {
			termKey := ContractTermKey(cid)
			for _, segID := range buckets {
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
			termKey := TopicTermKey(pos, topicXDR)
			for _, segID := range buckets {
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

	events, err := r.fetchEventsFromVolume(perSegment, limit, result)
	if err != nil {
		return nil, nil, err
	}

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// GetEventsInRange reads all events in a ledger range from flat file volumes (no index, no RocksDB).
// Uses the ledger map to determine the dense ID range per bucket and reads sequentially.
func (r *SegmentFileReader) GetEventsInRange(startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	buckets := GetBucketsForRange(startLedger, endLedger)
	result.BucketsTouched = len(buckets)

	fetchCap := 0
	if limit > 0 {
		fetchCap = limit
	}
	events := make([]*query.Event, 0)

	for _, segID := range buckets {
		if fetchCap > 0 && len(events) >= fetchCap {
			break
		}

		lm, err := r.loadLedgerMapFromFile(segID)
		if err != nil {
			continue // segment may not exist
		}

		segmentStart := segID * BucketSize

		var startOff uint16
		if startLedger > segmentStart {
			startOff = uint16(startLedger - segmentStart)
		}
		endOff := uint16(BucketSize - 1)
		if endLedger < segmentStart+BucketSize-1 {
			endOff = uint16(endLedger - segmentStart)
		}

		startID, endID := lm.LedgerRangeToIDRange(startOff, endOff)
		if endID < startID {
			continue
		}

		// Build sequential dense IDs slice
		count := int(endID - startID + 1)
		if fetchCap > 0 && count > fetchCap-len(events) {
			count = fetchCap - len(events)
		}
		denseIDs := make([]uint32, count)
		for i := 0; i < count; i++ {
			denseIDs[i] = startID + uint32(i)
		}

		result.MatchingLocalIDs += count

		// Batch read events from volume (via mmap cache)
		readStart := time.Now()
		eventBlobs, volTiming, err := r.readEventsFromVolumeMmap(segID, denseIDs)
		readTime := time.Since(readStart)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read events from volume for segment %d: %w", segID, err)
		}

		result.EventFetchTime += readTime
		if volTiming != nil {
			result.DecompressTime += volTiming.DecompressTime
			result.EventDiskReadTime += volTiming.DiskReadTime
			result.GroupsDecompressed += volTiming.GroupsDecompressed
		}

		// Decode events
		for _, denseID := range denseIDs {
			blob, ok := eventBlobs[denseID]
			if !ok {
				continue
			}

			result.EventBytesRead += int64(len(blob))
			result.EventsScanned++

			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)

			decStart := time.Now()
			ev, err := event.DecodeBinaryToQueryEvent(blob, ledger, 0, 0, eventSeq)
			result.DecodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, ev)
		}
	}

	if fetchCap > 0 && len(events) > fetchCap {
		events = events[:fetchCap]
	}

	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// fetchEventsFromVolume reads events from flat file event volumes (no RocksDB).
// Uses O(1) positional lookup per event via the offset array.
func (r *SegmentFileReader) fetchEventsFromVolume(perSegment map[uint32]*roaring.Bitmap, limit int, result *Bitmap32EventQueryResult) ([]*query.Event, error) {
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

		// Collect dense IDs for this segment
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

		// Batch read events from volume (via mmap cache)
		readStart := time.Now()
		eventBlobs, volTiming, err := r.readEventsFromVolumeMmap(segID, denseIDs)
		readTime := time.Since(readStart)
		if err != nil {
			return nil, fmt.Errorf("failed to read events from volume for segment %d: %w", segID, err)
		}

		result.EventFetchTime += readTime
		if volTiming != nil {
			result.DecompressTime += volTiming.DecompressTime
			result.EventDiskReadTime += volTiming.DiskReadTime
			result.GroupsDecompressed += volTiming.GroupsDecompressed
		}

		// Decode events
		for _, denseID := range denseIDs {
			blob, ok := eventBlobs[denseID]
			if !ok {
				continue
			}

			result.EventBytesRead += int64(len(blob))
			result.EventsScanned++

			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)

			decStart := time.Now()
			ev, err := event.DecodeBinaryToQueryEvent(blob, ledger, 0, 0, eventSeq)
			result.DecodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, ev)
		}
	}

	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	result.EventsReturned = len(events)
	return events, nil
}

// readEventsFromVolumeMmap reads events from mmap'd volume files via the cache.
func (r *SegmentFileReader) readEventsFromVolumeMmap(segID uint32, denseIDs []uint32) (map[uint32][]byte, *VolumeReadTiming, error) {
	dirName := fmt.Sprintf("%06d", segID)
	dirPath := filepath.Join(r.basePath, dirName)

	offsetsPath := filepath.Join(dirPath, EventOffsetsFileName)
	eventsPath := filepath.Join(dirPath, EventsFileName)

	offsetsMm, err := r.getMmap(offsetsPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to mmap event_offsets.dat: %w", err)
	}
	eventsMm, err := r.getMmap(eventsPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to mmap events.dat: %w", err)
	}

	hdr, err := parseOffsetsHeaderMmap(offsetsMm)
	if err != nil {
		return nil, nil, err
	}

	// Load dictionary for dict-compressed volumes
	var dictMm *MmapFile
	if hdr.dictCompressed {
		dictPath := filepath.Join(dirPath, EventsDictFileName)
		dictMm, err = r.getMmap(dictPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to mmap events.dict: %w", err)
		}
	}

	return ReadEventsFromVolumeMmap(offsetsMm, eventsMm, hdr, dictMm, denseIDs)
}

// SegmentFileToUnified converts a Bitmap32EventQueryResult from segment-file queries to UnifiedQueryResult.
func SegmentFileToUnified(r *Bitmap32EventQueryResult) *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:         "segment-file",
		LedgerRange:       r.LedgerRange,
		BucketsTouched:    r.BucketsTouched,
		IndexMatches:      r.MatchingLocalIDs,
		MatchUnitName:     "local IDs",
		EventsScanned:     r.EventsScanned,
		EventsReturned:    r.EventsReturned,
		IndexBytesRead:    r.IndexBytesRead,
		EventBytesRead:    r.EventBytesRead,
		IndexLookupTime:   r.IndexLookupTime,
		EventFetchTime:    r.EventFetchTime,
		DecompressTime:     r.DecompressTime,
		EventDiskReadTime:  r.EventDiskReadTime,
		GroupsDecompressed: r.GroupsDecompressed,
		DecodeTime:        r.DecodeTime,
		FilterTime:        r.FilterTime,
		TotalTime:         r.TotalTime,
	}
}

// SegmentVolumeToUnified converts a Bitmap32EventQueryResult from segment-volume queries to UnifiedQueryResult.
func SegmentVolumeToUnified(r *Bitmap32EventQueryResult) *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:         "segment-volume",
		LedgerRange:       r.LedgerRange,
		BucketsTouched:    r.BucketsTouched,
		IndexMatches:      r.MatchingLocalIDs,
		MatchUnitName:     "local IDs",
		EventsScanned:     r.EventsScanned,
		EventsReturned:    r.EventsReturned,
		IndexBytesRead:    r.IndexBytesRead,
		EventBytesRead:    r.EventBytesRead,
		IndexLookupTime:   r.IndexLookupTime,
		EventFetchTime:    r.EventFetchTime,
		DecompressTime:     r.DecompressTime,
		EventDiskReadTime:  r.EventDiskReadTime,
		GroupsDecompressed: r.GroupsDecompressed,
		DecodeTime:        r.DecodeTime,
		FilterTime:        r.FilterTime,
		TotalTime:         r.TotalTime,
	}
}
