package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// HotSegmentReader implements QueryBackend by reading hot segment files from disk
// and rebuilding bitmaps from index_deltas.dat. This enables querying hot segments
// without RocksDB or a concurrent ingestion process.
//
// Segments are discovered cheaply at construction (directory scan only) and loaded
// lazily on first access. Only a single segment may be loaded at a time; queries
// that span multiple hot segments return an error.
type HotSegmentReader struct {
	basePath  string            // top-level segment path
	available map[uint32]string // segID → dir path (from dir scan, no I/O)
	loaded    *hotSegmentState  // single loaded segment (nil until first access)
	mu        sync.Mutex
}

type hotSegmentState struct {
	segmentID     uint32
	contracts     map[[16]byte]*roaring.Bitmap
	topics        [4]map[[16]byte]*roaring.Bitmap
	ledgerOffs    *SegmentLedgerOffsets
	eventsIdx     *MmapFile // mmap'd events.idx
	eventsDat     *os.File  // for pread
	eventCount    int       // len(eventsIdx) / 8
	eventsDatSize int64     // cached file size for last-event boundary
}

// NewHotSegmentReader scans basePath/hot/ for hot segment directories but does NOT
// load any segments. Segments are loaded lazily on first access via getSegment.
func NewHotSegmentReader(basePath string) (*HotSegmentReader, error) {
	hotDir := filepath.Join(basePath, hotDirName)
	entries, err := os.ReadDir(hotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return &HotSegmentReader{basePath: basePath, available: make(map[uint32]string)}, nil
		}
		return nil, fmt.Errorf("read hot dir: %w", err)
	}

	reader := &HotSegmentReader{
		basePath:  basePath,
		available: make(map[uint32]string),
	}

	var segIDs []uint32
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		segID64, err := strconv.ParseUint(entry.Name(), 10, 32)
		if err != nil {
			continue
		}
		segID := uint32(segID64)
		reader.available[segID] = filepath.Join(hotDir, entry.Name())
		segIDs = append(segIDs, segID)
	}

	if len(segIDs) > 0 {
		sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })
		labels := make([]string, len(segIDs))
		for i, id := range segIDs {
			labels[i] = fmt.Sprintf("%06d", id)
		}
		fmt.Fprintf(os.Stderr, "[hot] found %d hot segment(s) on disk: %s\n",
			len(segIDs), strings.Join(labels, " "))
	}

	return reader, nil
}

// getSegment lazily loads a single hot segment. Only one segment may be loaded at
// a time; requesting a different segment when one is already loaded returns an error.
func (r *HotSegmentReader) getSegment(segmentID uint32) (*hotSegmentState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Already loaded this segment?
	if r.loaded != nil && r.loaded.segmentID == segmentID {
		return r.loaded, nil
	}

	// Already loaded a DIFFERENT segment? Error — multi-hot not allowed.
	if r.loaded != nil {
		return nil, fmt.Errorf("query range spans multiple hot segments (%06d, %06d); narrow --start/--end to a single segment",
			r.loaded.segmentID, segmentID)
	}

	// Not available on disk?
	segDir, ok := r.available[segmentID]
	if !ok {
		return nil, nil // segment doesn't exist, not an error
	}

	// Load with heap stats + logging
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	loadStart := time.Now()
	state, err := loadHotSegment(segDir, segmentID)
	if err != nil {
		delete(r.available, segmentID)
		return nil, err
	}
	loadTime := time.Since(loadStart)

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	actualDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
	fmt.Fprintf(os.Stderr, "[hot] loaded segment %06d in %v, actual heap delta: %s\n",
		segmentID, loadTime, formatBytesStore(actualDelta))

	r.loaded = state
	return state, nil
}


func loadHotSegment(segDir string, segID uint32) (*hotSegmentState, error) {
	rebuildStart := time.Now()
	fmt.Fprintf(os.Stderr, "[hot %06d] rebuilding bitmaps from index_deltas.dat...\n", segID)

	// Read index_deltas.dat and replay into bitmaps
	deltasPath := filepath.Join(segDir, hotIndexDeltasFile)
	deltasData, err := os.ReadFile(deltasPath)
	if err != nil {
		return nil, fmt.Errorf("read index_deltas.dat: %w", err)
	}

	contracts := make(map[[16]byte]*roaring.Bitmap)
	var topics [4]map[[16]byte]*roaring.Bitmap
	for i := range topics {
		topics[i] = make(map[[16]byte]*roaring.Bitmap)
	}

	numDeltas := len(deltasData) / indexDeltaSize
	var deltaCountPerField [5]int

	for i := 0; i < numDeltas; i++ {
		off := i * indexDeltaSize
		fieldIndex := deltasData[off]
		var termHash [16]byte
		copy(termHash[:], deltasData[off+1:off+17])
		eventID := binary.LittleEndian.Uint32(deltasData[off+17 : off+21])

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

	// Compute per-field stats
	fieldNames := [5]string{"contracts", "topic0", "topic1", "topic2", "topic3"}
	fieldMaps := [5]map[[16]byte]*roaring.Bitmap{contracts, topics[0], topics[1], topics[2], topics[3]}

	var totalTerms int
	var totalBitmapBytes uint64
	var totalOverhead uint64

	fmt.Fprintf(os.Stderr, "[hot %06d] index_deltas: %d entries (contracts: %d, topic0: %d, topic1: %d, topic2: %d, topic3: %d)\n",
		segID, numDeltas, deltaCountPerField[0], deltaCountPerField[1], deltaCountPerField[2], deltaCountPerField[3], deltaCountPerField[4])

	var termCounts [5]int
	var bitmapSizes [5]uint64
	for f := 0; f < 5; f++ {
		termCounts[f] = len(fieldMaps[f])
		for _, bm := range fieldMaps[f] {
			bitmapSizes[f] += bm.GetSizeInBytes()
		}
		totalTerms += termCounts[f]
		totalBitmapBytes += bitmapSizes[f]
		totalOverhead += uint64(termCounts[f]) * perEntryOverhead
	}

	fmt.Fprintf(os.Stderr, "[hot %06d] in-memory terms: contracts=%d, topic0=%d, topic1=%d, topic2=%d, topic3=%d (total: %d)\n",
		segID, termCounts[0], termCounts[1], termCounts[2], termCounts[3], termCounts[4], totalTerms)

	fmt.Fprintf(os.Stderr, "[hot %06d] bitmap data:     %s=%s, %s=%s, %s=%s, %s=%s, %s=%s (total: %s)\n",
		segID,
		fieldNames[0], formatBytesStore(int64(bitmapSizes[0])),
		fieldNames[1], formatBytesStore(int64(bitmapSizes[1])),
		fieldNames[2], formatBytesStore(int64(bitmapSizes[2])),
		fieldNames[3], formatBytesStore(int64(bitmapSizes[3])),
		fieldNames[4], formatBytesStore(int64(bitmapSizes[4])),
		formatBytesStore(int64(totalBitmapBytes)))

	fmt.Fprintf(os.Stderr, "[hot %06d] Go overhead:     %d terms × %d bytes = %s (maps + pointers + bitmap objects)\n",
		segID, totalTerms, perEntryOverhead, formatBytesStore(int64(totalOverhead)))

	totalInMem := totalBitmapBytes + totalOverhead
	fmt.Fprintf(os.Stderr, "[hot %06d] total in-memory: %s\n", segID, formatBytesStore(int64(totalInMem)))

	// Read ledger_offsets.dat and pad to SegmentLedgerOffsetsSize
	ledgerOffsPath := filepath.Join(segDir, hotLedgerOffsFile)
	ledgerOffsRaw, err := os.ReadFile(ledgerOffsPath)
	if err != nil {
		return nil, fmt.Errorf("read ledger_offsets.dat: %w", err)
	}
	paddedData := make([]byte, SegmentLedgerOffsetsSize)
	copy(paddedData, ledgerOffsRaw)
	ledgerOffs := &SegmentLedgerOffsets{SegmentID: segID, Data: paddedData}

	// Mmap events.idx
	eventsIdxPath := filepath.Join(segDir, hotEventsIdxFile)
	eventsIdx, err := OpenMmap(eventsIdxPath)
	if err != nil {
		return nil, fmt.Errorf("mmap events.idx: %w", err)
	}
	eventCount := eventsIdx.Len() / 8

	// Open events.dat for pread and cache its size
	eventsDatPath := filepath.Join(segDir, hotEventsDatFile)
	eventsDat, err := os.Open(eventsDatPath)
	if err != nil {
		eventsIdx.Close()
		return nil, fmt.Errorf("open events.dat: %w", err)
	}
	datFi, err := eventsDat.Stat()
	if err != nil {
		eventsIdx.Close()
		eventsDat.Close()
		return nil, fmt.Errorf("stat events.dat: %w", err)
	}
	eventsDatSize := datFi.Size()

	ledgerEntries := len(ledgerOffsRaw) / 4
	rebuildTime := time.Since(rebuildStart)
	fmt.Fprintf(os.Stderr, "[hot %06d] events: %d, ledger offsets: %d entries\n", segID, eventCount, ledgerEntries)
	fmt.Fprintf(os.Stderr, "[hot %06d] rebuild time: %v\n", segID, rebuildTime)

	return &hotSegmentState{
		segmentID:     segID,
		contracts:     contracts,
		topics:        topics,
		ledgerOffs:    ledgerOffs,
		eventsIdx:     eventsIdx,
		eventsDat:     eventsDat,
		eventCount:    eventCount,
		eventsDatSize: eventsDatSize,
	}, nil
}

// HasSegment returns true if the given segment ID is available on disk (may not be loaded yet).
func (r *HotSegmentReader) HasSegment(segmentID uint32) bool {
	_, ok := r.available[segmentID]
	return ok
}

// HasSegments returns true if any hot segments are available on disk.
func (r *HotSegmentReader) HasSegments() bool {
	return len(r.available) > 0
}

// LoadTermBitmap loads a bitmap for a specific term in a specific hot segment,
// trimmed to the given ledger range.
func (r *HotSegmentReader) LoadTermBitmap(segmentID uint32, fieldIndex int, termKey [16]byte,
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

	// Look up bitmap
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

	// Clone bitmap (caller may mutate)
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

// FetchByIDs resolves dense IDs from per-segment bitmaps to events via pread on hot files.
func (r *HotSegmentReader) FetchByIDs(perSegment map[uint32]*roaring.Bitmap, limit int, result *QueryResult) ([]*query.Event, error) {
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

		seg, err := r.getSegment(segID)
		if err != nil {
			return nil, err
		}
		if seg == nil {
			continue
		}

		bitmapIter := perSegment[segID].Iterator()
		for bitmapIter.HasNext() {
			if len(events) >= fetchCap {
				break
			}

			denseID := bitmapIter.Next()

			fetchStart := time.Now()
			eventData, err := r.readEvent(seg, denseID)
			result.EventFetchTime += time.Since(fetchStart)
			if err != nil {
				continue
			}

			result.EventBytesRead += int64(len(eventData))
			result.EventsScanned++

			ledger, eventSeq := seg.ledgerOffs.DenseIDToLedgerAndSeq(denseID)

			decStart := time.Now()
			ev, err := event.DecodeBinaryToQueryEvent(eventData, ledger, eventSeq)
			result.DecodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, ev)
		}
	}

	result.EventsReturned = len(events)
	return events, nil
}

// FetchByRange retrieves all events in a ledger range from hot segments.
func (r *HotSegmentReader) FetchByRange(startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error) {
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
		if fetchCap > 0 && count > fetchCap-len(events) {
			count = fetchCap - len(events)
		}
		result.MatchingLocalIDs += count

		for i := 0; i < count; i++ {
			denseID := startID + uint32(i)

			fetchStart := time.Now()
			eventData, err := r.readEvent(seg, denseID)
			result.EventFetchTime += time.Since(fetchStart)
			if err != nil {
				continue
			}

			result.EventBytesRead += int64(len(eventData))
			result.EventsScanned++

			ledger, eventSeq := seg.ledgerOffs.DenseIDToLedgerAndSeq(denseID)

			decStart := time.Now()
			ev, err := event.DecodeBinaryToQueryEvent(eventData, ledger, eventSeq)
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

// readEvent reads a single event by dense ID from a hot segment's files.
func (r *HotSegmentReader) readEvent(seg *hotSegmentState, denseID uint32) ([]byte, error) {
	if int(denseID) >= seg.eventCount {
		return nil, fmt.Errorf("denseID %d out of range (eventCount=%d)", denseID, seg.eventCount)
	}

	idxData := seg.eventsIdx.Data()
	offset := binary.LittleEndian.Uint64(idxData[denseID*8 : (denseID+1)*8])

	// Determine event end
	var eventEnd uint64
	if int(denseID+1) < seg.eventCount {
		eventEnd = binary.LittleEndian.Uint64(idxData[(denseID+1)*8 : (denseID+2)*8])
	} else {
		// Last event: use cached file size
		eventEnd = uint64(seg.eventsDatSize)
	}

	size := int(eventEnd - offset)
	if size <= 0 {
		return nil, fmt.Errorf("invalid event size %d at denseID %d", size, denseID)
	}

	buf := make([]byte, size)
	n, err := seg.eventsDat.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("pread events.dat: %w", err)
	}
	if n != size {
		return nil, fmt.Errorf("short read: got %d, want %d", n, size)
	}

	return buf, nil
}

// Close releases all resources held by the hot segment reader.
func (r *HotSegmentReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded != nil {
		if r.loaded.eventsIdx != nil {
			r.loaded.eventsIdx.Close()
		}
		if r.loaded.eventsDat != nil {
			r.loaded.eventsDat.Close()
		}
		r.loaded = nil
	}
	return nil
}

// Verify interface compliance at compile time.
var _ QueryBackend = (*HotSegmentReader)(nil)
