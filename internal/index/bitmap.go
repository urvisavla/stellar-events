package index

import (
	"encoding/binary"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// =============================================================================
// Segment Loader Interface
// =============================================================================

// SegmentLoader provides segment loading capability for queries.
// This is implemented by the storage layer (e.g., RocksDBIndexStore).
type SegmentLoader interface {
	// LoadL1Segment loads an L1 bitmap segment from storage.
	// Returns nil, nil if segment doesn't exist.
	LoadL1Segment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error)

	// LoadL2Segment loads an L2 bitmap segment from storage.
	// Returns nil, nil if segment doesn't exist.
	LoadL2Segment(prefix byte, keyValue []byte, ledgerSeq uint32) (*roaring.Bitmap, error)
}

// =============================================================================
// Single-Writer Update Types
// =============================================================================

// UpdateType identifies the type of bitmap update
type UpdateType byte

const (
	UpdateL1Contract UpdateType = iota
	UpdateL1Topic0
	UpdateL1Topic1
	UpdateL1Topic2
	UpdateL1Topic3
	UpdateL2Contract
	UpdateL2Topic0
	UpdateL2Topic1
	UpdateL2Topic2
	UpdateL2Topic3
)

// BitmapUpdate represents a single bitmap update request
type BitmapUpdate struct {
	Type       UpdateType
	KeyValue   []byte
	LedgerSeq  uint32
	EventIndex uint32 // Only for L2 updates (encoded tx:op:event)
}

// =============================================================================
// Bitmap Index (Pure In-Memory)
// =============================================================================

// BitmapIndex manages segmented roaring bitmap indexes in memory.
// It provides Add operations for building indexes and Query operations
// that combine in-memory hot segments with persisted segments via SegmentLoader.
//
// Uses a single-writer goroutine pattern for lock-free updates during ingestion.
// All Add* methods send updates through a channel to the writer goroutine,
// which owns all bitmap data structures and requires no locks for writes.
type BitmapIndex struct {
	// Level 1: Hot segment cache - maps "prefix:keyHash:segmentID" to bitmap of ledgers
	// Owned by writer goroutine during ingestion (no lock needed for writes)
	hotSegments   map[string]*roaring.Bitmap
	hotSegmentsMu sync.RWMutex // Only needed for reads/flush

	// Level 2: Hot cache - maps "L2prefix:keyHash:ledger" to bitmap of event indices
	// Owned by writer goroutine during ingestion (no lock needed for writes)
	hotL2Segments   map[string]*roaring.Bitmap
	hotL2SegmentsMu sync.RWMutex // Only needed for reads/flush

	// Current hot segment ID (the one being actively written)
	currentSegmentID uint32

	// Segment loader for queries (optional, can be nil for write-only mode)
	loader SegmentLoader

	// Single-writer channel pattern for lock-free updates
	updateCh chan *BitmapUpdate
	doneCh   chan struct{}
	wg       sync.WaitGroup
	started  bool

	// Flush coordination: allows pausing writer for safe bitmap serialization
	drainMu   sync.Mutex    // Prevents concurrent drains
	drainReq  chan struct{} // Signal to request drain/resume
	drainResp chan struct{} // Signal that writer has paused
}

// NewBitmapIndex creates a new in-memory bitmap index.
// The loader parameter is optional and used for queries to load persisted segments.
func NewBitmapIndex(loader SegmentLoader) *BitmapIndex {
	return &BitmapIndex{
		hotSegments:   make(map[string]*roaring.Bitmap),
		hotL2Segments: make(map[string]*roaring.Bitmap),
		loader:        loader,
	}
}

// SetLoader sets the segment loader for query operations.
func (bi *BitmapIndex) SetLoader(loader SegmentLoader) {
	bi.loader = loader
}

// =============================================================================
// Single-Writer Lifecycle
// =============================================================================

// Start begins the single-writer goroutine for lock-free updates.
// Must be called before using Add* methods during bulk ingestion.
func (bi *BitmapIndex) Start() {
	if bi.started {
		return
	}
	bi.updateCh = make(chan *BitmapUpdate, 10000) // Buffered for throughput
	bi.doneCh = make(chan struct{})
	bi.drainReq = make(chan struct{})
	bi.drainResp = make(chan struct{})
	bi.started = true
	bi.wg.Add(1)
	go bi.writer()
}

// Stop gracefully shuts down the writer goroutine.
// Waits for all pending updates to be processed.
func (bi *BitmapIndex) Stop() {
	if !bi.started {
		return
	}
	close(bi.updateCh)
	bi.wg.Wait()
	close(bi.doneCh)
	bi.started = false
}

// IsStarted returns whether the writer goroutine is running.
func (bi *BitmapIndex) IsStarted() bool {
	return bi.started
}

// writer is the single goroutine that owns all bitmap data during ingestion.
// It processes updates without any locks since it's the sole writer.
// Supports pausing via drain requests for safe bitmap serialization.
func (bi *BitmapIndex) writer() {
	defer bi.wg.Done()
	for {
		select {
		case update, ok := <-bi.updateCh:
			if !ok {
				return // Channel closed, exit
			}
			bi.applyUpdate(update)
		case <-bi.drainReq:
			// Drain all pending updates from the channel
			draining := true
			for draining {
				select {
				case update, ok := <-bi.updateCh:
					if !ok {
						// Channel closed during drain
						bi.drainResp <- struct{}{}
						return
					}
					bi.applyUpdate(update)
				default:
					// No more pending updates
					draining = false
				}
			}
			// Signal that we're drained and paused
			bi.drainResp <- struct{}{}
			// Wait for resume signal
			<-bi.drainReq
		}
	}
}

// Drain pauses the writer goroutine after processing all pending updates.
// This gives the caller exclusive access to bitmap data for safe serialization.
// Must be paired with Resume() to continue processing.
// Safe to call when writer is not started (no-op).
func (bi *BitmapIndex) Drain() {
	if !bi.started {
		return
	}
	bi.drainMu.Lock() // Prevent concurrent drains
	bi.drainReq <- struct{}{}
	<-bi.drainResp // Wait for writer to pause
	// Writer is now paused, caller has exclusive access to bitmaps
}

// Resume resumes the writer goroutine after a Drain().
// Must be called after Drain() to allow the writer to continue processing updates.
// Safe to call when writer is not started (no-op).
func (bi *BitmapIndex) Resume() {
	if !bi.started {
		return
	}
	bi.drainReq <- struct{}{} // Signal resume
	bi.drainMu.Unlock()
}

// applyUpdate applies a single update without locks (called only from writer goroutine).
func (bi *BitmapIndex) applyUpdate(u *BitmapUpdate) {
	switch u.Type {
	case UpdateL1Contract:
		bi.addToIndexInternal(PrefixContractIndex, u.KeyValue, u.LedgerSeq)
	case UpdateL1Topic0:
		bi.addToIndexInternal(PrefixTopic0Index, u.KeyValue, u.LedgerSeq)
	case UpdateL1Topic1:
		bi.addToIndexInternal(PrefixTopic1Index, u.KeyValue, u.LedgerSeq)
	case UpdateL1Topic2:
		bi.addToIndexInternal(PrefixTopic2Index, u.KeyValue, u.LedgerSeq)
	case UpdateL1Topic3:
		bi.addToIndexInternal(PrefixTopic3Index, u.KeyValue, u.LedgerSeq)
	case UpdateL2Contract:
		bi.addToL2IndexInternal(PrefixContractL2, u.KeyValue, u.LedgerSeq, u.EventIndex)
	case UpdateL2Topic0:
		bi.addToL2IndexInternal(PrefixTopic0L2, u.KeyValue, u.LedgerSeq, u.EventIndex)
	case UpdateL2Topic1:
		bi.addToL2IndexInternal(PrefixTopic1L2, u.KeyValue, u.LedgerSeq, u.EventIndex)
	case UpdateL2Topic2:
		bi.addToL2IndexInternal(PrefixTopic2L2, u.KeyValue, u.LedgerSeq, u.EventIndex)
	case UpdateL2Topic3:
		bi.addToL2IndexInternal(PrefixTopic3L2, u.KeyValue, u.LedgerSeq, u.EventIndex)
	}
}

// addToIndexInternal adds to L1 index without locks (for writer goroutine).
func (bi *BitmapIndex) addToIndexInternal(prefix byte, keyValue []byte, ledgerSeq uint32) {
	segmentID := SegmentID(ledgerSeq)
	localLedger := ledgerSeq % SegmentSize

	cacheKey := makeL1CacheKey(prefix, keyValue, segmentID)

	bitmap, exists := bi.hotSegments[cacheKey]
	if !exists {
		bitmap = roaring.New()
		bi.hotSegments[cacheKey] = bitmap
	}

	bitmap.Add(localLedger)

	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// addToL2IndexInternal adds to L2 index without locks (for writer goroutine).
func (bi *BitmapIndex) addToL2IndexInternal(prefix byte, keyValue []byte, ledgerSeq uint32, eventIndex uint32) {
	cacheKey := makeL2CacheKey(prefix, keyValue, ledgerSeq)

	bitmap, exists := bi.hotL2Segments[cacheKey]
	if !exists {
		bitmap = roaring.New()
		bi.hotL2Segments[cacheKey] = bitmap
	}

	bitmap.Add(eventIndex)
}

// =============================================================================
// Key Generation (exported for use by storage implementations)
// =============================================================================

// MakeL1Key creates a database key for an L1 bitmap segment.
// Format: <prefix:1><keyValue:32><segmentID:4>
func MakeL1Key(prefix byte, keyValue []byte, segmentID uint32) []byte {
	key := make([]byte, 1+32+4)
	key[0] = prefix

	// Pad or truncate keyValue to 32 bytes
	if len(keyValue) >= 32 {
		copy(key[1:33], keyValue[:32])
	} else {
		copy(key[1:1+len(keyValue)], keyValue)
	}

	// Big-endian segment ID for proper sorting
	binary.BigEndian.PutUint32(key[33:37], segmentID)
	return key
}

// MakeL2Key creates a database key for an L2 bitmap.
// Format: <L2prefix:1><keyValue:32><ledger:4>
func MakeL2Key(prefix byte, keyValue []byte, ledgerSeq uint32) []byte {
	key := make([]byte, 1+32+4)
	key[0] = prefix

	// Pad or truncate keyValue to 32 bytes
	if len(keyValue) >= 32 {
		copy(key[1:33], keyValue[:32])
	} else {
		copy(key[1:1+len(keyValue)], keyValue)
	}

	// Big-endian ledger sequence for proper sorting
	binary.BigEndian.PutUint32(key[33:37], ledgerSeq)
	return key
}

// makeL1CacheKey creates a cache key for the hot segments map.
func makeL1CacheKey(prefix byte, keyValue []byte, segmentID uint32) string {
	return string(MakeL1Key(prefix, keyValue, segmentID))
}

// makeL2CacheKey creates a cache key for the hot L2 segments map.
func makeL2CacheKey(prefix byte, keyValue []byte, ledgerSeq uint32) string {
	return string(MakeL2Key(prefix, keyValue, ledgerSeq))
}

// =============================================================================
// L1 Index Operations (Ledger-level)
// =============================================================================

// AddToIndex adds a ledger to the L1 bitmap index for a given key.
// If the writer goroutine is started, sends through channel (lock-free).
// Otherwise falls back to direct locking (for backwards compatibility).
func (bi *BitmapIndex) AddToIndex(prefix byte, keyValue []byte, ledgerSeq uint32) {
	if bi.started {
		// Use channel-based update (lock-free)
		var updateType UpdateType
		switch prefix {
		case PrefixContractIndex:
			updateType = UpdateL1Contract
		case PrefixTopic0Index:
			updateType = UpdateL1Topic0
		case PrefixTopic1Index:
			updateType = UpdateL1Topic1
		case PrefixTopic2Index:
			updateType = UpdateL1Topic2
		case PrefixTopic3Index:
			updateType = UpdateL1Topic3
		default:
			return // Unknown prefix
		}
		bi.updateCh <- &BitmapUpdate{
			Type:      updateType,
			KeyValue:  keyValue,
			LedgerSeq: ledgerSeq,
		}
		return
	}

	// Fallback: direct locking (when writer not started)
	segmentID := SegmentID(ledgerSeq)
	localLedger := ledgerSeq % SegmentSize // Offset within segment

	cacheKey := makeL1CacheKey(prefix, keyValue, segmentID)

	bi.hotSegmentsMu.Lock()
	defer bi.hotSegmentsMu.Unlock()

	bitmap, exists := bi.hotSegments[cacheKey]
	if !exists {
		bitmap = roaring.New()
		bi.hotSegments[cacheKey] = bitmap
	}

	bitmap.Add(localLedger)

	// Track current segment
	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// AddContractIndex adds a ledger to the contract ID L1 index.
func (bi *BitmapIndex) AddContractIndex(contractID []byte, ledgerSeq uint32) {
	bi.AddToIndex(PrefixContractIndex, contractID, ledgerSeq)
}

// AddTopicIndex adds a ledger to a topic L1 index.
func (bi *BitmapIndex) AddTopicIndex(topicPosition int, topicValue []byte, ledgerSeq uint32) {
	prefix := TopicL1Prefix(topicPosition)
	if prefix == 0 {
		return // Invalid topic position
	}
	bi.AddToIndex(prefix, topicValue, ledgerSeq)
}

// =============================================================================
// L2 Index Operations (Event-level)
// =============================================================================

// AddToL2Index adds an event index to the L2 bitmap for a given key and ledger.
// eventIndex encodes tx:op:event as a single uint32.
// If the writer goroutine is started, sends through channel (lock-free).
// Otherwise falls back to direct locking (for backwards compatibility).
func (bi *BitmapIndex) AddToL2Index(prefix byte, keyValue []byte, ledgerSeq uint32, eventIndex uint32) {
	if bi.started {
		// Use channel-based update (lock-free)
		var updateType UpdateType
		switch prefix {
		case PrefixContractL2:
			updateType = UpdateL2Contract
		case PrefixTopic0L2:
			updateType = UpdateL2Topic0
		case PrefixTopic1L2:
			updateType = UpdateL2Topic1
		case PrefixTopic2L2:
			updateType = UpdateL2Topic2
		case PrefixTopic3L2:
			updateType = UpdateL2Topic3
		default:
			return // Unknown prefix
		}
		bi.updateCh <- &BitmapUpdate{
			Type:       updateType,
			KeyValue:   keyValue,
			LedgerSeq:  ledgerSeq,
			EventIndex: eventIndex,
		}
		return
	}

	// Fallback: direct locking (when writer not started)
	cacheKey := makeL2CacheKey(prefix, keyValue, ledgerSeq)

	bi.hotL2SegmentsMu.Lock()
	defer bi.hotL2SegmentsMu.Unlock()

	bitmap, exists := bi.hotL2Segments[cacheKey]
	if !exists {
		bitmap = roaring.New()
		bi.hotL2Segments[cacheKey] = bitmap
	}

	bitmap.Add(eventIndex)
}

// AddContractL2Index adds an event to the contract ID L2 index.
func (bi *BitmapIndex) AddContractL2Index(contractID []byte, ledgerSeq uint32, txIndex, opIndex, eventIndex uint16) {
	encoded := EncodeEventIndex(txIndex, opIndex, eventIndex)
	bi.AddToL2Index(PrefixContractL2, contractID, ledgerSeq, encoded)
}

// AddTopicL2Index adds an event to a topic L2 index.
func (bi *BitmapIndex) AddTopicL2Index(topicPosition int, topicValue []byte, ledgerSeq uint32, txIndex, opIndex, eventIndex uint16) {
	prefix := TopicL2Prefix(topicPosition)
	if prefix == 0 {
		return // Invalid topic position
	}
	encoded := EncodeEventIndex(txIndex, opIndex, eventIndex)
	bi.AddToL2Index(prefix, topicValue, ledgerSeq, encoded)
}

// =============================================================================
// Query Operations
// =============================================================================

// QueryIndex returns all ledgers matching a key within a range.
// Combines hot segments with persisted segments via the loader.
func (bi *BitmapIndex) QueryIndex(prefix byte, keyValue []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	result := roaring.New()

	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	for segID := startSegment; segID <= endSegment; segID++ {
		bitmap, err := bi.getL1Segment(prefix, keyValue, segID)
		if err != nil {
			return nil, err
		}

		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}

		segmentBase := segID * SegmentSize

		// Apply range mask for partial segments (first and/or last segment)
		isFirstSegment := segID == startSegment
		isLastSegment := segID == endSegment
		needsRangeMask := isFirstSegment || isLastSegment

		if needsRangeMask {
			// Calculate local range within this segment
			localStart := uint64(0)
			localEnd := uint64(SegmentSize)

			if isFirstSegment && startLedger > segmentBase {
				localStart = uint64(startLedger - segmentBase)
			}
			if isLastSegment {
				segmentEnd := segmentBase + SegmentSize - 1
				if endLedger < segmentEnd {
					localEnd = uint64(endLedger - segmentBase + 1)
				}
			}

			// Create range mask and apply with AND
			mask := roaring.New()
			mask.AddRange(localStart, localEnd)
			bitmap = roaring.And(bitmap, mask)
		}

		if bitmap.IsEmpty() {
			continue
		}

		// Convert local offsets to absolute ledger numbers and merge
		localValues := bitmap.ToArray()
		absoluteValues := make([]uint32, len(localValues))
		for i, local := range localValues {
			absoluteValues[i] = segmentBase + local
		}

		segmentResult := roaring.New()
		segmentResult.AddMany(absoluteValues)
		result.Or(segmentResult)
	}

	return result, nil
}

// getL1Segment retrieves an L1 segment from hot cache or storage.
func (bi *BitmapIndex) getL1Segment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error) {
	cacheKey := makeL1CacheKey(prefix, keyValue, segmentID)

	// Check hot cache first
	bi.hotSegmentsMu.RLock()
	bitmap, inCache := bi.hotSegments[cacheKey]
	if inCache {
		bitmap = bitmap.Clone() // Clone to avoid holding lock
	}
	bi.hotSegmentsMu.RUnlock()

	if inCache {
		return bitmap, nil
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		return bi.loader.LoadL1Segment(prefix, keyValue, segmentID)
	}

	return nil, nil
}

// QueryL2Index returns event indices for a specific ledger.
func (bi *BitmapIndex) QueryL2Index(prefix byte, keyValue []byte, ledgerSeq uint32) (*roaring.Bitmap, error) {
	cacheKey := makeL2CacheKey(prefix, keyValue, ledgerSeq)

	// Check hot cache first
	bi.hotL2SegmentsMu.RLock()
	bitmap, inCache := bi.hotL2Segments[cacheKey]
	if inCache {
		bitmap = bitmap.Clone()
	}
	bi.hotL2SegmentsMu.RUnlock()

	if inCache {
		return bitmap, nil
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		return bi.loader.LoadL2Segment(prefix, keyValue, ledgerSeq)
	}

	return nil, nil
}

// QueryContractIndex queries ledgers for a contract ID.
func (bi *BitmapIndex) QueryContractIndex(contractID []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	return bi.QueryIndex(PrefixContractIndex, contractID, startLedger, endLedger)
}

// QueryTopicIndex queries ledgers for a topic value.
func (bi *BitmapIndex) QueryTopicIndex(topicPosition int, topicValue []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	prefix := TopicL1Prefix(topicPosition)
	if prefix == 0 {
		return nil, nil
	}
	return bi.QueryIndex(prefix, topicValue, startLedger, endLedger)
}

// QueryIndexHierarchical performs a two-level query: L1 for ledgers, L2 for events.
// If limit > 0, stops collecting event keys once limit is reached (early termination).
func (bi *BitmapIndex) QueryIndexHierarchical(l1Prefix, l2Prefix byte, keyValue []byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error) {
	// Phase 1: Query L1 to get matching ledgers
	ledgerBitmap, err := bi.QueryIndex(l1Prefix, keyValue, startLedger, endLedger)
	if err != nil {
		return nil, 0, err
	}

	matchingLedgers := int(ledgerBitmap.GetCardinality())
	if ledgerBitmap.IsEmpty() {
		return nil, matchingLedgers, nil
	}

	// Phase 2: For each matching ledger, query L2 to get event indices
	var eventKeys []EventKey
	ledgers := ledgerBitmap.ToArray()

	for _, ledgerSeq := range ledgers {
		// Early termination if we have enough events
		if limit > 0 && len(eventKeys) >= limit {
			break
		}

		l2Bitmap, err := bi.QueryL2Index(l2Prefix, keyValue, ledgerSeq)
		if err != nil {
			return nil, matchingLedgers, err
		}

		if l2Bitmap == nil || l2Bitmap.IsEmpty() {
			continue
		}

		// Convert L2 bitmap to event keys
		eventIndices := l2Bitmap.ToArray()
		for _, encodedIdx := range eventIndices {
			if limit > 0 && len(eventKeys) >= limit {
				break
			}
			eventKeys = append(eventKeys, EventKey{
				LedgerSeq:  ledgerSeq,
				EventIndex: encodedIdx,
			})
		}
	}

	return eventKeys, matchingLedgers, nil
}

// QueryContractIndexHierarchical queries using hierarchical approach for contract ID.
func (bi *BitmapIndex) QueryContractIndexHierarchical(contractID []byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error) {
	return bi.QueryIndexHierarchical(PrefixContractIndex, PrefixContractL2, contractID, startLedger, endLedger, limit)
}

// QueryTopicIndexHierarchical queries using hierarchical approach for topics.
func (bi *BitmapIndex) QueryTopicIndexHierarchical(topicPosition int, topicValue []byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error) {
	l1Prefix := TopicL1Prefix(topicPosition)
	l2Prefix := TopicL2Prefix(topicPosition)
	if l1Prefix == 0 || l2Prefix == 0 {
		return nil, 0, nil
	}
	return bi.QueryIndexHierarchical(l1Prefix, l2Prefix, topicValue, startLedger, endLedger, limit)
}

// =============================================================================
// Serialization (for storage implementations)
// =============================================================================

// Segment represents a serializable bitmap segment.
type Segment struct {
	Key  []byte
	Data []byte
}

// GetDirtyL1Segments returns all hot L1 segments for persistence.
// Each segment is optimized and serialized to bytes.
// The caller is responsible for compression.
// When writer is started, this drains pending updates and pauses the writer
// to ensure safe access to bitmap data during serialization.
// NOTE: Use GetAndClearL1Segments() for flush operations to avoid race conditions.
func (bi *BitmapIndex) GetDirtyL1Segments() ([]Segment, error) {
	// When writer is started, use drain/resume for exclusive access
	if bi.started {
		bi.Drain()
		defer bi.Resume()
	} else {
		// Fallback: use locks when writer is not started
		bi.hotSegmentsMu.RLock()
		defer bi.hotSegmentsMu.RUnlock()
	}

	segments := make([]Segment, 0, len(bi.hotSegments))
	for cacheKey, bitmap := range bi.hotSegments {
		// Optimize bitmap before serialization
		bitmap.RunOptimize()

		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, err
		}

		segments = append(segments, Segment{
			Key:  []byte(cacheKey),
			Data: data,
		})
	}

	return segments, nil
}

// GetAndClearL1Segments atomically gets and clears all hot L1 segments.
// This is the preferred method for flush operations as it keeps the writer
// paused during both get and clear, preventing race conditions.
func (bi *BitmapIndex) GetAndClearL1Segments() ([]Segment, error) {
	// When writer is started, use drain/resume for exclusive access
	if bi.started {
		bi.Drain()
		defer bi.Resume()
	} else {
		bi.hotSegmentsMu.Lock()
		defer bi.hotSegmentsMu.Unlock()
	}

	segments := make([]Segment, 0, len(bi.hotSegments))
	for cacheKey, bitmap := range bi.hotSegments {
		bitmap.RunOptimize()

		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, err
		}

		segments = append(segments, Segment{
			Key:  []byte(cacheKey),
			Data: data,
		})
	}

	// Clear while still holding exclusive access
	bi.hotSegments = make(map[string]*roaring.Bitmap)

	return segments, nil
}

// GetDirtyL2Segments returns all hot L2 segments for persistence.
// When writer is started, this drains pending updates and pauses the writer
// to ensure safe access to bitmap data during serialization.
// NOTE: Use GetAndClearL2Segments() for flush operations to avoid race conditions.
func (bi *BitmapIndex) GetDirtyL2Segments() ([]Segment, error) {
	// When writer is started, use drain/resume for exclusive access
	if bi.started {
		bi.Drain()
		defer bi.Resume()
	} else {
		// Fallback: use locks when writer is not started
		bi.hotL2SegmentsMu.RLock()
		defer bi.hotL2SegmentsMu.RUnlock()
	}

	segments := make([]Segment, 0, len(bi.hotL2Segments))
	for cacheKey, bitmap := range bi.hotL2Segments {
		bitmap.RunOptimize()

		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, err
		}

		segments = append(segments, Segment{
			Key:  []byte(cacheKey),
			Data: data,
		})
	}

	return segments, nil
}

// GetAndClearL2Segments atomically gets and clears all hot L2 segments.
// This is the preferred method for flush operations as it keeps the writer
// paused during both get and clear, preventing race conditions.
func (bi *BitmapIndex) GetAndClearL2Segments() ([]Segment, error) {
	// When writer is started, use drain/resume for exclusive access
	if bi.started {
		bi.Drain()
		defer bi.Resume()
	} else {
		bi.hotL2SegmentsMu.Lock()
		defer bi.hotL2SegmentsMu.Unlock()
	}

	segments := make([]Segment, 0, len(bi.hotL2Segments))
	for cacheKey, bitmap := range bi.hotL2Segments {
		bitmap.RunOptimize()

		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, err
		}

		segments = append(segments, Segment{
			Key:  []byte(cacheKey),
			Data: data,
		})
	}

	// Clear while still holding exclusive access
	bi.hotL2Segments = make(map[string]*roaring.Bitmap)

	return segments, nil
}

// ClearL1Segments clears all hot L1 segments (after successful flush).
func (bi *BitmapIndex) ClearL1Segments() {
	bi.hotSegmentsMu.Lock()
	defer bi.hotSegmentsMu.Unlock()
	bi.hotSegments = make(map[string]*roaring.Bitmap)
}

// ClearL2Segments clears all hot L2 segments (after successful flush).
func (bi *BitmapIndex) ClearL2Segments() {
	bi.hotL2SegmentsMu.Lock()
	defer bi.hotL2SegmentsMu.Unlock()
	bi.hotL2Segments = make(map[string]*roaring.Bitmap)
}

// ClearAll clears all hot segments.
func (bi *BitmapIndex) ClearAll() {
	bi.ClearL1Segments()
	bi.ClearL2Segments()
}

// =============================================================================
// Statistics
// =============================================================================

// GetHotSegmentStats returns statistics about L1 hot segments.
func (bi *BitmapIndex) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	bi.hotSegmentsMu.RLock()
	defer bi.hotSegmentsMu.RUnlock()

	count = len(bi.hotSegments)
	for _, bitmap := range bi.hotSegments {
		totalCards += bitmap.GetCardinality()
		memBytes += bitmap.GetSizeInBytes()
	}
	return
}

// GetL2Stats returns statistics about L2 hot segments.
func (bi *BitmapIndex) GetL2Stats() (count int, totalCards uint64) {
	bi.hotL2SegmentsMu.RLock()
	defer bi.hotL2SegmentsMu.RUnlock()

	count = len(bi.hotL2Segments)
	for _, bm := range bi.hotL2Segments {
		totalCards += bm.GetCardinality()
	}
	return
}

// GetCurrentSegmentID returns the current segment ID being written to.
func (bi *BitmapIndex) GetCurrentSegmentID() uint32 {
	return bi.currentSegmentID
}

// =============================================================================
// Helper Functions
// =============================================================================

// TopicL1Prefix returns the L1 prefix for a topic position.
func TopicL1Prefix(position int) byte {
	switch position {
	case 0:
		return PrefixTopic0Index
	case 1:
		return PrefixTopic1Index
	case 2:
		return PrefixTopic2Index
	case 3:
		return PrefixTopic3Index
	default:
		return 0
	}
}

// TopicL2Prefix returns the L2 prefix for a topic position.
func TopicL2Prefix(position int) byte {
	switch position {
	case 0:
		return PrefixTopic0L2
	case 1:
		return PrefixTopic1L2
	case 2:
		return PrefixTopic2L2
	case 3:
		return PrefixTopic3L2
	default:
		return 0
	}
}
