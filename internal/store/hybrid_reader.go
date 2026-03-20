package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/urvisavla/stellar-events/internal/query"
)

// HybridReader implements QueryBackend that routes queries per-segment:
//   - Finalized segments → SegmentReader (mmap'd flat files, fast)
//   - Hot/incomplete segments → hotReader (RocksDB or HotSegmentReader)
//
// A segment is considered finalized if its flat file index files exist
// (index.hash, events.pack, etc.).
type HybridReader struct {
	segmentReader *SegmentReader // cold path (finalized segments)
	hotReader     QueryBackend  // hot path (incomplete segments — RocksDB or HotSegmentReader)
	segmentPath   string        // base directory for flat file segments
}

// NewHybridReader creates a QueryBackend that routes between flat files and a hot backend.
func NewHybridReader(segmentReader *SegmentReader, hotReader QueryBackend, segmentPath string) *HybridReader {
	return &HybridReader{
		segmentReader: segmentReader,
		hotReader:     hotReader,
		segmentPath:   segmentPath,
	}
}

// isSegmentFinalized checks if a segment has been finalized by verifying
// the existence of required flat file index files.
func (h *HybridReader) isSegmentFinalized(segmentID uint32) bool {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(h.segmentPath, dirName)

	// Check for index.hash — written by WriteSegmentDir
	hashPath := filepath.Join(dirPath, "index.hash")
	if _, err := os.Stat(hashPath); err != nil {
		return false
	}

	// Check for events.pack — finalized event data (ledger offsets embedded as appData)
	eventsPath := filepath.Join(dirPath, EventsFileName)
	if _, err := os.Stat(eventsPath); err != nil {
		return false
	}

	return true
}

// isSegmentHot checks if a segment exists in the hot reader (if it's a HotSegmentReader).
func (h *HybridReader) isSegmentHot(segmentID uint32) bool {
	if hr, ok := h.hotReader.(*HotSegmentReader); ok {
		return hr.HasSegment(segmentID)
	}
	// For RocksDB hot reader, any non-finalized segment is considered hot
	return true
}

// LoadTermBitmap loads a bitmap for a specific term+segment, routing to the
// appropriate backend based on segment finalization state.
func (h *HybridReader) LoadTermBitmap(segmentID uint32, fieldIndex int, termKey [16]byte,
	startLedger, endLedger uint32) (*roaring.Bitmap, BitmapLoadStats, error) {

	if h.isSegmentFinalized(segmentID) {
		return h.segmentReader.LoadTermBitmap(segmentID, fieldIndex, termKey, startLedger, endLedger)
	}
	if h.isSegmentHot(segmentID) {
		return h.hotReader.LoadTermBitmap(segmentID, fieldIndex, termKey, startLedger, endLedger)
	}
	// Segment exists in neither cold nor hot
	return nil, BitmapLoadStats{}, nil
}

// FetchByIDs splits the per-segment bitmap map into cold (finalized) and hot
// (incomplete) segments, then fetches from each backend.
func (h *HybridReader) FetchByIDs(perSegment map[uint32]*roaring.Bitmap, limit int, result *QueryResult) ([]*query.Event, error) {
	coldSegments := make(map[uint32]*roaring.Bitmap)
	hotSegments := make(map[uint32]*roaring.Bitmap)

	for segID, bm := range perSegment {
		if h.isSegmentFinalized(segID) {
			coldSegments[segID] = bm
		} else {
			hotSegments[segID] = bm
		}
	}

	var allEvents []*query.Event

	// Fetch from cold segments first (lower segment IDs = older data)
	if len(coldSegments) > 0 {
		coldEvents, err := h.segmentReader.FetchByIDs(coldSegments, limit, result)
		if err != nil {
			return nil, fmt.Errorf("cold segment fetch: %w", err)
		}
		allEvents = append(allEvents, coldEvents...)
	}

	// Fetch remaining from hot segments
	if len(hotSegments) > 0 {
		hotLimit := 0
		if limit > 0 {
			hotLimit = limit - len(allEvents)
			if hotLimit <= 0 {
				goto done
			}
		}

		// Adjust result.MatchingLocalIDs for the hot reader's internal cap computation
		hotResult := &QueryResult{
			MatchingLocalIDs: result.MatchingLocalIDs,
		}
		hotEvents, err := h.hotReader.FetchByIDs(hotSegments, hotLimit, hotResult)
		if err != nil {
			return nil, fmt.Errorf("hot segment fetch: %w", err)
		}

		// Merge stats
		result.EventBytesRead += hotResult.EventBytesRead
		result.EventsScanned += hotResult.EventsScanned
		result.EventFetchTime += hotResult.EventFetchTime
		result.DecodeTime += hotResult.DecodeTime

		allEvents = append(allEvents, hotEvents...)
	}

done:
	// Sort all events by segment ID then dense ID order (already sorted within each backend)
	// Events from cold come before hot since cold segments have lower IDs
	if limit > 0 && len(allEvents) > limit {
		allEvents = allEvents[:limit]
	}

	result.EventsReturned = len(allEvents)
	return allEvents, nil
}

// FetchByRange retrieves all events in a ledger range, routing per-segment.
func (h *HybridReader) FetchByRange(startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	segments := GetSegmentsForRange(startLedger, endLedger)
	result.SegmentsTouched = len(segments)

	// Split segments into cold and hot (only include segments that actually exist)
	var coldSegs, hotSegs []uint32
	for _, segID := range segments {
		if h.isSegmentFinalized(segID) {
			coldSegs = append(coldSegs, segID)
		} else if h.isSegmentHot(segID) {
			hotSegs = append(hotSegs, segID)
		}
	}

	// Ensure sorted order
	sort.Slice(coldSegs, func(i, j int) bool { return coldSegs[i] < coldSegs[j] })
	sort.Slice(hotSegs, func(i, j int) bool { return hotSegs[i] < hotSegs[j] })

	var allEvents []*query.Event

	// Fetch cold segments via flat file reader
	if len(coldSegs) > 0 {
		coldStart := coldSegs[0] * SegmentSize
		if startLedger > coldStart {
			coldStart = startLedger
		}
		coldEnd := (coldSegs[len(coldSegs)-1]+1)*SegmentSize - 1
		if endLedger < coldEnd {
			coldEnd = endLedger
		}

		coldResult, coldEvents, err := h.segmentReader.FetchByRange(coldStart, coldEnd, limit)
		if err != nil {
			return nil, nil, fmt.Errorf("cold segment range fetch: %w", err)
		}

		// Merge cold stats
		result.EventBytesRead += coldResult.EventBytesRead
		result.EventsScanned += coldResult.EventsScanned
		result.EventFetchTime += coldResult.EventFetchTime
		result.DecodeTime += coldResult.DecodeTime
		result.MatchingLocalIDs += coldResult.MatchingLocalIDs

		allEvents = append(allEvents, coldEvents...)
	}

	// Fetch hot segments via hot reader
	if len(hotSegs) > 0 {
		hotLimit := 0
		if limit > 0 {
			hotLimit = limit - len(allEvents)
			if hotLimit <= 0 {
				goto done
			}
		}

		hotStart := hotSegs[0] * SegmentSize
		if startLedger > hotStart {
			hotStart = startLedger
		}
		hotEnd := (hotSegs[len(hotSegs)-1]+1)*SegmentSize - 1
		if endLedger < hotEnd {
			hotEnd = endLedger
		}

		hotResult, hotEvents, err := h.hotReader.FetchByRange(hotStart, hotEnd, hotLimit)
		if err != nil {
			return nil, nil, fmt.Errorf("hot segment range fetch: %w", err)
		}

		// Merge hot stats
		result.EventBytesRead += hotResult.EventBytesRead
		result.EventsScanned += hotResult.EventsScanned
		result.EventFetchTime += hotResult.EventFetchTime
		result.DecodeTime += hotResult.DecodeTime
		result.MatchingLocalIDs += hotResult.MatchingLocalIDs

		allEvents = append(allEvents, hotEvents...)
	}

done:
	if limit > 0 && len(allEvents) > limit {
		allEvents = allEvents[:limit]
	}

	result.EventsReturned = len(allEvents)
	result.TotalTime = time.Since(totalStart)
	return result, allEvents, nil
}

// Close releases resources from both backends.
func (h *HybridReader) Close() error {
	var firstErr error
	if err := h.segmentReader.Close(); err != nil {
		firstErr = err
	}
	if err := h.hotReader.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Verify interface compliance at compile time.
var _ QueryBackend = (*HybridReader)(nil)
