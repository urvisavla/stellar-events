package store

import (
	"context"
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

	"github.com/tamir/events-analysis/eventstore"
	"github.com/tamir/events-analysis/packfile"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// LiveHotSegmentReader implements QueryBackend for hot segments written by
// LiveHotSegmentWriter. Events are read from frozen events.pack files in hot/,
// bitmaps are rebuilt from index_deltas.dat (same as HotSegmentReader).
type LiveHotSegmentReader struct {
	basePath  string            // top-level segment path
	available map[uint32]string // segID → hot dir path
	loaded    *liveHotState     // lazily loaded segment
	mu        sync.Mutex
}

type liveHotState struct {
	segmentID  uint32
	contracts  map[[16]byte]*roaring.Bitmap
	topics     [4]map[[16]byte]*roaring.Bitmap
	ledgerOffs *SegmentLedgerOffsets
	eventCount int
	er         *eventstore.Reader // eventstore reader for events.pack
	hotDir     string             // for cleanup
}

var _ QueryBackend = (*LiveHotSegmentReader)(nil)

// NewLiveHotSegmentReader scans basePath/hot/ for directories containing events.pack.
func NewLiveHotSegmentReader(basePath string) (*LiveHotSegmentReader, error) {
	hotDir := filepath.Join(basePath, hotDirName)
	entries, err := os.ReadDir(hotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return &LiveHotSegmentReader{basePath: basePath, available: make(map[uint32]string)}, nil
		}
		return nil, fmt.Errorf("read hot dir: %w", err)
	}

	reader := &LiveHotSegmentReader{
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
		segDir := filepath.Join(hotDir, entry.Name())
		// Only include if events.pack exists (LiveWriter segment)
		if _, err := os.Stat(filepath.Join(segDir, EventsFileName)); err != nil {
			continue
		}
		segID := uint32(segID64)
		reader.available[segID] = segDir
		segIDs = append(segIDs, segID)
	}

	if len(segIDs) > 0 {
		sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })
		labels := make([]string, len(segIDs))
		for i, id := range segIDs {
			labels[i] = fmt.Sprintf("%06d", id)
		}
		fmt.Fprintf(os.Stderr, "[hot-live] found %d hot segment(s): %s\n",
			len(segIDs), strings.Join(labels, " "))
	}

	return reader, nil
}

func (r *LiveHotSegmentReader) getSegment(segmentID uint32) (*liveHotState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.loaded != nil && r.loaded.segmentID == segmentID {
		return r.loaded, nil
	}
	if r.loaded != nil {
		return nil, fmt.Errorf("query range spans multiple hot segments (%06d, %06d); narrow --start/--end to a single segment",
			r.loaded.segmentID, segmentID)
	}

	segDir, ok := r.available[segmentID]
	if !ok {
		return nil, nil
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	loadStart := time.Now()
	state, err := r.loadSegment(segDir, segmentID)
	if err != nil {
		delete(r.available, segmentID)
		return nil, err
	}
	loadTime := time.Since(loadStart)

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	actualDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
	fmt.Fprintf(os.Stderr, "[hot-live] loaded segment %06d in %v, actual heap delta: %s\n",
		segmentID, loadTime, formatBytesStore(actualDelta))

	r.loaded = state
	return state, nil
}

func (r *LiveHotSegmentReader) loadSegment(segDir string, segID uint32) (*liveHotState, error) {
	rebuildStart := time.Now()
	fmt.Fprintf(os.Stderr, "[hot-live %06d] rebuilding bitmaps from index_deltas.dat...\n", segID)

	// Rebuild bitmaps from index_deltas.dat (same as HotSegmentReader)
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

	fmt.Fprintf(os.Stderr, "[hot-live %06d] index_deltas: %d entries (contracts: %d, topic0: %d, topic1: %d, topic2: %d, topic3: %d)\n",
		segID, numDeltas, deltaCountPerField[0], deltaCountPerField[1], deltaCountPerField[2], deltaCountPerField[3], deltaCountPerField[4])

	// Load ledger offsets from events.pack appData
	eventsPath := filepath.Join(segDir, EventsFileName)
	pr := packfile.Open(eventsPath)
	appData, err := pr.AppData()
	if err != nil {
		pr.Close()
		return nil, fmt.Errorf("read events.pack appData: %w", err)
	}
	paddedData := make([]byte, SegmentLedgerOffsetsSize)
	copy(paddedData, appData)
	pr.Close()

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

	// Open eventstore reader for event fetching
	er := eventstore.Open(eventsPath)

	rebuildTime := time.Since(rebuildStart)
	fmt.Fprintf(os.Stderr, "[hot-live %06d] events: %d, rebuild time: %v\n", segID, eventCount, rebuildTime)

	return &liveHotState{
		segmentID:  segID,
		contracts:  contracts,
		topics:     topics,
		ledgerOffs: ledgerOffs,
		eventCount: eventCount,
		er:         er,
		hotDir:     segDir,
	}, nil
}

// HasSegment returns true if the given segment is available.
func (r *LiveHotSegmentReader) HasSegment(segmentID uint32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.available[segmentID]
	return ok
}

// HasSegments returns true if any hot segments are available.
func (r *LiveHotSegmentReader) HasSegments() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.available) > 0
}

// LoadTermBitmap loads a bitmap for a term in a hot segment, trimmed to ledger range.
func (r *LiveHotSegmentReader) LoadTermBitmap(segmentID uint32, fieldIndex int, termKey [16]byte,
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
	trimStart := time.Now()
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
	stats.TrimTime = time.Since(trimStart)

	stats.TotalTime = time.Since(totalStart)
	if result.IsEmpty() {
		return nil, stats, nil
	}
	return result, stats, nil
}

// FetchByIDs reads events from events.pack using eventstore.ReadIndices.
func (r *LiveHotSegmentReader) FetchByIDs(perSegment map[uint32]*roaring.Bitmap, limit int, result *QueryResult) ([]*query.Event, error) {
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

		bitmap := perSegment[segID]
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

		indices := make([]int, len(denseIDs))
		for i, id := range denseIDs {
			indices[i] = int(id)
		}

		readStart := time.Now()
		eventIdx := 0
		for blob, readErr := range seg.er.ReadIndices(context.Background(), indices) {
			if readErr != nil {
				return nil, fmt.Errorf("read events from hot segment %d: %w", segID, readErr)
			}

			denseID := denseIDs[eventIdx]
			eventIdx++

			result.EventBytesRead += int64(len(blob))
			result.EventsScanned++

			ledger, eventSeq := seg.ledgerOffs.DenseIDToLedgerAndSeq(denseID)

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

	result.EventFetchTime -= result.DecodeTime
	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}
	result.EventsReturned = len(events)
	return events, nil
}

// FetchByRange reads all events in a ledger range from hot segments.
func (r *LiveHotSegmentReader) FetchByRange(startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error) {
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

		readStart := time.Now()
		eventIdx := 0
		for blob, readErr := range seg.er.ReadEvents(int(startID), count) {
			if readErr != nil {
				return nil, nil, fmt.Errorf("read events from hot segment %d: %w", segID, readErr)
			}

			denseID := startID + uint32(eventIdx)
			eventIdx++

			blobCopy := make([]byte, len(blob))
			copy(blobCopy, blob)

			result.EventBytesRead += int64(len(blobCopy))
			result.EventsScanned++

			ledger, eventSeq := seg.ledgerOffs.DenseIDToLedgerAndSeq(denseID)

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

	result.EventFetchTime -= result.DecodeTime
	if fetchCap > 0 && len(events) > fetchCap {
		events = events[:fetchCap]
	}
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// Close releases resources.
func (r *LiveHotSegmentReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded != nil {
		if r.loaded.er != nil {
			r.loaded.er.Close()
		}
		r.loaded = nil
	}
	return nil
}
