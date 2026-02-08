package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// Index Key Format (36 bytes):
//   [term_key:32][bucket_id:4]
//   - term_key: SHA-256 hash of the indexed value (contract_id or topic_xdr)
//   - bucket_id: ledger_seq / BucketSize (groups ~14 hours of ledgers)
//
// Used by both posting list and bitmap indexes.
// Column families separate contract vs topic data.
//
// Posting list value: delta-varint encoded local IDs (32-bit)
// Bitmap value: serialized roaring bitmap of local IDs (32-bit)

const (
	// IndexKeySize is the size of an index key in bytes.
	IndexKeySize = 36
)

// EncodeIndexKey creates a 36-byte index key from term key and ledger sequence.
func EncodeIndexKey(termKey [32]byte, ledgerSeq uint32) []byte {
	bucketID := BucketID(ledgerSeq)
	key := make([]byte, IndexKeySize)
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], bucketID)
	return key
}

// EncodeIndexKeyWithBucket creates a 36-byte index key from term key and bucket ID directly.
func EncodeIndexKeyWithBucket(termKey [32]byte, bucketID uint32) []byte {
	key := make([]byte, IndexKeySize)
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], bucketID)
	return key
}

// ContractTermKey computes the term key (SHA-256) for a contract ID.
func ContractTermKey(contractID []byte) [32]byte {
	return sha256.Sum256(contractID)
}

// TopicTermKey computes the term key (SHA-256) for a topic XDR value.
func TopicTermKey(topicXDR []byte) [32]byte {
	return sha256.Sum256(topicXDR)
}

// =============================================================================
// Delta-Varint Encoding Helpers
// =============================================================================

// appendVarint appends a varint-encoded uint64 to buf.
func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

// readVarint reads a varint from data, returns value and bytes consumed.
func readVarint(data []byte) (uint64, int) {
	var v uint64
	var shift uint
	for i, b := range data {
		if i >= 10 { // max 10 bytes for uint64 varint
			return 0, -1
		}
		v |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return v, i + 1
		}
		shift += 7
	}
	return 0, -1 // incomplete varint
}

// =============================================================================
// V2 Posting List Functions (32-bit Local IDs)
// =============================================================================
//
// V2 posting lists store compact 32-bit local IDs instead of 64-bit TOIDs.
// Local ID format: (ledger_offset << 16) | event_seq (same as bitmap32)
// This enables point gets instead of range scans when fetching events.

// FilterLocalIDsByLedgerRange filters local IDs to only include those within the ledger range.
func FilterLocalIDsByLedgerRange(localIDs []uint32, bucketStart, startLedger, endLedger uint32) []uint32 {
	if len(localIDs) == 0 {
		return nil
	}

	// Compute local ID boundaries
	var startOffset, endOffset uint32
	if startLedger > bucketStart {
		startOffset = startLedger - bucketStart
	}
	endOffset = endLedger - bucketStart
	_, bucketEnd := BucketRange(BucketID(bucketStart))
	if endLedger > bucketEnd {
		endOffset = bucketEnd - bucketStart
	}

	startLocalID := startOffset << 16        // min local ID for start ledger
	endLocalID := (endOffset << 16) | 0xFFFF // max local ID for end ledger

	var result []uint32
	for _, id := range localIDs {
		if id >= startLocalID && id <= endLocalID {
			result = append(result, id)
		}
	}
	return result
}

// IntersectLocalIDLists returns the intersection of two sorted local ID lists.
func IntersectLocalIDLists(a, b []uint32) []uint32 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	var result []uint32
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}

	return result
}

// UnionLocalIDLists returns the union of two sorted local ID lists.
func UnionLocalIDLists(a, b []uint32) []uint32 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	result := make([]uint32, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}

	result = append(result, a[i:]...)
	result = append(result, b[j:]...)

	return result
}

// EncodeLocalIDListDeltaVarint encodes local IDs using delta-varint compression.
// IDs must be sorted in ascending order.
// Format: [count:varint][first_id:varint][delta1:varint][delta2:varint]...
func EncodeLocalIDListDeltaVarint(ids []uint32) []byte {
	if len(ids) == 0 {
		return nil
	}

	// Estimate size: count (5) + first id (5) + deltas (avg 2 bytes each)
	buf := make([]byte, 0, 10+len(ids)*2)

	// Write count as varint
	buf = appendVarint(buf, uint64(len(ids)))

	// Write first ID as varint
	buf = appendVarint(buf, uint64(ids[0]))

	// Write deltas as varints
	prev := ids[0]
	for i := 1; i < len(ids); i++ {
		delta := uint64(ids[i] - prev)
		buf = appendVarint(buf, delta)
		prev = ids[i]
	}

	return buf
}

// DecodeLocalIDListDeltaVarint decodes delta-varint encoded local IDs.
func DecodeLocalIDListDeltaVarint(data []byte) []uint32 {
	if len(data) == 0 {
		return nil
	}

	// Read count
	count, n := readVarint(data)
	if n <= 0 || count == 0 {
		return nil
	}
	data = data[n:]

	ids := make([]uint32, count)

	// Read first ID as varint
	firstID, n := readVarint(data)
	if n <= 0 {
		return nil
	}
	ids[0] = uint32(firstID)
	data = data[n:]

	// Read deltas
	prev := ids[0]
	for i := uint64(1); i < count; i++ {
		delta, n := readVarint(data)
		if n <= 0 {
			// Truncated data, return what we have
			return ids[:i]
		}
		data = data[n:]
		ids[i] = prev + uint32(delta)
		prev = ids[i]
	}

	return ids
}

// postingListV2MergeOperator implements a merge operator for delta-varint encoded local ID lists.
// Decodes, merges (union), and re-encodes the posting lists using uint32 local IDs.
type postingListV2MergeOperator struct{}

func (m *postingListV2MergeOperator) Name() string {
	return "posting-list-v2-delta-varint"
}

func (m *postingListV2MergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var result []uint32
	if len(existingValue) > 0 {
		result = DecodeLocalIDListDeltaVarint(existingValue)
	}

	for _, operand := range operands {
		if len(operand) == 0 {
			continue
		}
		newIDs := DecodeLocalIDListDeltaVarint(operand)
		if len(result) == 0 {
			result = newIDs
		} else {
			result = UnionLocalIDLists(result, newIDs)
		}
	}

	if len(result) == 0 {
		return nil, true
	}

	return EncodeLocalIDListDeltaVarint(result), true
}

func (m *postingListV2MergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	left := DecodeLocalIDListDeltaVarint(leftOperand)
	right := DecodeLocalIDListDeltaVarint(rightOperand)

	if len(left) == 0 {
		return rightOperand, true
	}
	if len(right) == 0 {
		return leftOperand, true
	}

	merged := UnionLocalIDLists(left, right)
	return EncodeLocalIDListDeltaVarint(merged), true
}

// =============================================================================
// V2 Posting List Query (32-bit Local IDs)
// =============================================================================

// postingListV2Result holds the result of reading a single V2 posting list.
type postingListV2Result struct {
	localIDs   []uint32
	buckets    int
	bytesRead  int64
	readTime   time.Duration
	decodeTime time.Duration
	err        error
	isContract bool
}

// QueryEventsWithPostingListV2Timing queries events using V2 posting lists (32-bit local IDs).
// Uses point gets with V2 event keys instead of TOID-based range scans.
// Supports parallel reads, guided intersection (smallest-first), and streaming for single-filter queries.
// Requires data ingested with V2Indexes: true (V2 event keys in cfEvents).
func (es *RocksDBEventStore) QueryEventsWithPostingListV2Timing(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) (*PostingListV2QueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &PostingListV2QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	hasContract := len(contractID) > 0
	hasTopics := len(topics) > 0
	multiFilter := (hasContract && hasTopics) || len(topics) > 1

	if !hasContract && !hasTopics {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// For single filter queries with limit, use streaming approach
	if !multiFilter && limit > 0 {
		return es.queryPostingListV2Streaming(contractID, topics, startLedger, endLedger, limit, result, totalStart)
	}

	// Multi-filter: use parallel reads + guided intersection
	buckets := GetBucketsForRange(startLedger, endLedger)

	plStart := time.Now()

	// Parallel read all posting lists
	plResults := es.queryPostingListsV2Parallel(contractID, topics, buckets, startLedger, endLedger)

	// Check for errors and aggregate stats
	for _, plr := range plResults {
		if plr.err != nil {
			return nil, nil, plr.err
		}
		result.BucketsScanned += plr.buckets
		result.PostingListsRead++
		result.PostingListBytes += plr.bytesRead
		result.PostingListReadTime += plr.readTime
		result.PostingListDecodeTime += plr.decodeTime
		result.LocalIDsInPostingList += len(plr.localIDs)
	}

	// Sort by size ascending (smallest first for efficient intersection)
	sort.Slice(plResults, func(a, b int) bool {
		return len(plResults[a].localIDs) < len(plResults[b].localIDs)
	})

	// Intersect progressively (guided by smallest list)
	var resultLocalIDs []uint32
	for i, plr := range plResults {
		if i == 0 {
			resultLocalIDs = plr.localIDs
		} else {
			intersectStart := time.Now()
			resultLocalIDs = IntersectLocalIDLists(resultLocalIDs, plr.localIDs)
			result.IntersectTime += time.Since(intersectStart)
		}
		if len(resultLocalIDs) == 0 {
			result.PostingListTime = time.Since(plStart) - result.IntersectTime
			result.TotalTime = time.Since(totalStart)
			return result, nil, nil
		}
	}

	result.PostingListTime = time.Since(plStart) - result.IntersectTime
	result.LocalIDsAfterIntersect = len(resultLocalIDs)

	// Fetch events using point gets
	events, fetchTime, decodeTime, filterTime, bytesRead, scanned := es.fetchEventsByLocalIDsV2(
		resultLocalIDs, buckets, startLedger, endLedger, limit, contractID, topics)

	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventBytesRead = bytesRead
	result.EventsScanned = scanned
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// queryPostingListsV2Parallel reads all V2 posting lists in parallel.
func (es *RocksDBEventStore) queryPostingListsV2Parallel(contractID []byte, topics [][]byte, buckets []uint32, startLedger, endLedger uint32) []postingListV2Result {
	// Count how many posting lists to read
	numLists := 0
	if len(contractID) > 0 {
		numLists++
	}
	for _, t := range topics {
		if len(t) > 0 {
			numLists++
		}
	}

	results := make([]postingListV2Result, numLists)
	var wg sync.WaitGroup

	idx := 0

	// Read contract posting list
	if len(contractID) > 0 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			termKey := ContractTermKey(contractID)
			ids, bucketsRead, bytesRead, readT, decodeT, err := es.queryPostingListV2WithStats(es.cfContractsPLV2, termKey, buckets, startLedger, endLedger)
			results[i] = postingListV2Result{
				localIDs:   ids,
				buckets:    bucketsRead,
				bytesRead:  bytesRead,
				readTime:   readT,
				decodeTime: decodeT,
				err:        err,
				isContract: true,
			}
		}(idx)
		idx++
	}

	// Read topic posting lists
	for _, topicXDR := range topics {
		if len(topicXDR) == 0 {
			continue
		}
		wg.Add(1)
		go func(i int, topic []byte) {
			defer wg.Done()
			termKey := TopicTermKey(topic)
			ids, bucketsRead, bytesRead, readT, decodeT, err := es.queryPostingListV2WithStats(es.cfTopicsPLV2, termKey, buckets, startLedger, endLedger)
			results[i] = postingListV2Result{
				localIDs:   ids,
				buckets:    bucketsRead,
				bytesRead:  bytesRead,
				readTime:   readT,
				decodeTime: decodeT,
				err:        err,
				isContract: false,
			}
		}(idx, topicXDR)
		idx++
	}

	wg.Wait()
	return results
}

// queryPostingListV2Streaming reads posting lists bucket-by-bucket and fetches events incrementally.
// Stops early when limit is reached.
func (es *RocksDBEventStore) queryPostingListV2Streaming(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int, result *PostingListV2QueryResult, totalStart time.Time) (*PostingListV2QueryResult, []*query.Event, error) {
	buckets := GetBucketsForRange(startLedger, endLedger)

	// Determine which CF and term key to use
	var cf *grocksdb.ColumnFamilyHandle
	var termKey [32]byte

	if len(contractID) > 0 {
		cf = es.cfContractsPLV2
		termKey = ContractTermKey(contractID)
	} else if len(topics) > 0 && len(topics[0]) > 0 {
		cf = es.cfTopicsPLV2
		termKey = TopicTermKey(topics[0])
	} else {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	var allEvents []*query.Event
	var plTime, plReadTime, plDecodeTime time.Duration
	var fetchTime, decodeTime, filterTime time.Duration

	for _, bucketID := range buckets {
		if len(allEvents) >= limit {
			break
		}

		bucketStart := bucketID * BucketSize

		// Read this bucket's posting list
		plStart := time.Now()
		indexKey := EncodeIndexKeyWithBucket(termKey, bucketID)

		t0 := time.Now()
		value, err := es.db.GetCF(es.ro, cf, indexKey)
		plReadTime += time.Since(t0)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read V2 posting list: %w", err)
		}

		if !value.Exists() {
			value.Free()
			result.BucketsScanned++
			continue
		}

		data := value.Data()
		result.PostingListBytes += int64(len(data))
		result.BucketsScanned++
		result.PostingListsRead++

		// Copy data since we need to free value
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		value.Free()

		// Decode local IDs
		t1 := time.Now()
		localIDs := DecodeLocalIDListDeltaVarint(dataCopy)
		plDecodeTime += time.Since(t1)

		// Filter by ledger range
		filtered := FilterLocalIDsByLedgerRange(localIDs, bucketStart, startLedger, endLedger)
		result.LocalIDsInPostingList += len(filtered)
		result.LocalIDsAfterIntersect += len(filtered)
		plTime += time.Since(plStart)

		// Fetch events for this bucket's local IDs
		remaining := limit - len(allEvents)
		events, ft, dt, flt, bytesRead, scanned := es.fetchEventsByLocalIDsV2ForBucket(
			filtered, bucketStart, remaining, contractID, topics)

		fetchTime += ft
		decodeTime += dt
		filterTime += flt
		result.EventBytesRead += bytesRead
		result.EventsScanned += scanned

		allEvents = append(allEvents, events...)
	}

	result.PostingListTime = plTime
	result.PostingListReadTime = plReadTime
	result.PostingListDecodeTime = plDecodeTime
	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventsReturned = len(allEvents)
	result.TotalTime = time.Since(totalStart)

	return result, allEvents, nil
}

// fetchEventsByLocalIDsV2 fetches events by local IDs using point gets with V2 keys.
func (es *RocksDBEventStore) fetchEventsByLocalIDsV2(localIDs []uint32, buckets []uint32, startLedger, endLedger uint32, limit int, contractID []byte, topics [][]byte) ([]*query.Event, time.Duration, time.Duration, time.Duration, int64, int) {
	var fetchTime, decodeTime, filterTime time.Duration
	var bytesRead int64
	var scanned int
	events := make([]*query.Event, 0, min(limit, len(localIDs)))

	// Group local IDs by bucket
	localIDsByBucket := make(map[uint32][]uint32)
	for _, localID := range localIDs {
		ledgerOffset := localID >> 16
		// Find which bucket this belongs to
		for _, bucketID := range buckets {
			bucketStart := bucketID * BucketSize
			ledger := bucketStart + ledgerOffset
			_, bucketEnd := BucketRange(bucketID)
			if ledger >= startLedger && ledger <= endLedger && ledger <= bucketEnd && ledgerOffset < BucketSize {
				localIDsByBucket[bucketID] = append(localIDsByBucket[bucketID], localID)
				break
			}
		}
	}

	// Process buckets in order
	for _, bucketID := range buckets {
		if limit > 0 && len(events) >= limit {
			break
		}

		bucketLocalIDs := localIDsByBucket[bucketID]
		if len(bucketLocalIDs) == 0 {
			continue
		}

		bucketStart := bucketID * BucketSize
		remaining := limit - len(events)
		if limit <= 0 {
			remaining = len(bucketLocalIDs)
		}

		evts, ft, dt, flt, br, sc := es.fetchEventsByLocalIDsV2ForBucket(
			bucketLocalIDs, bucketStart, remaining, contractID, topics)

		fetchTime += ft
		decodeTime += dt
		filterTime += flt
		bytesRead += br
		scanned += sc
		events = append(events, evts...)
	}

	return events, fetchTime, decodeTime, filterTime, bytesRead, scanned
}

// fetchEventsByLocalIDsV2ForBucket fetches events for local IDs within a single bucket.
// Uses parallel iterators to overlap RocksDB lookup latency across multiple goroutines.
func (es *RocksDBEventStore) fetchEventsByLocalIDsV2ForBucket(localIDs []uint32, bucketStart uint32, limit int, contractID []byte, topics [][]byte) ([]*query.Event, time.Duration, time.Duration, time.Duration, int64, int) {
	if len(localIDs) == 0 {
		return nil, 0, 0, 0, 0, 0
	}

	// Cap fetch count at limit
	fetchCount := len(localIDs)
	if limit > 0 && fetchCount > limit {
		fetchCount = limit
	}

	// Determine number of parallel workers
	const maxWorkers = 4
	numWorkers := maxWorkers
	if fetchCount < numWorkers {
		numWorkers = fetchCount
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
	chunkSize := (fetchCount + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > fetchCount {
			end = fetchCount
		}
		if start >= end {
			continue
		}

		wg.Add(1)
		go func(workerID, start, end int) {
			defer wg.Done()
			r := &results[workerID]
			r.events = make([]*query.Event, 0, end-start)

			iter := es.db.NewIteratorCF(es.ro, es.cfEvents)
			defer iter.Close()

			for i := start; i < end; i++ {
				ledger, eventSeq := event.DecodeLocalIDForBucket(localIDs[i], bucketStart)
				key := event.EncodeKeyV2(ledger, eventSeq)

				seekStart := time.Now()
				iter.Seek(key)
				if !iter.Valid() {
					r.fetchTime += time.Since(seekStart)
					break
				}

				iterKey := iter.Key()
				if iterKey == nil || !bytes.Equal(iterKey.Data(), key) {
					r.fetchTime += time.Since(seekStart)
					continue
				}

				iterVal := iter.Value()
				if iterVal == nil {
					r.fetchTime += time.Since(seekStart)
					continue
				}
				valData := iterVal.Data()
				if len(valData) == 0 {
					r.fetchTime += time.Since(seekStart)
					continue
				}

				valueCopy := make([]byte, len(valData))
				copy(valueCopy, valData)
				r.fetchTime += time.Since(seekStart)

				r.bytesRead += int64(len(valueCopy))
				r.scanned++

				// Filter using binary header
				filterStart := time.Now()
				if es.eventFormat == "binary" && (len(contractID) > 0 || len(topics) > 0) {
					header := event.ParseBinaryHeader(valueCopy)
					if header != nil {
						matches := true
						if len(contractID) > 0 && !header.MatchesContractID(contractID) {
							matches = false
						}
						if matches && len(topics) > 0 && !header.MatchesTopicsNonPositional(topics) {
							matches = false
						}
						r.filterTime += time.Since(filterStart)
						if !matches {
							continue
						}
					}
				} else {
					r.filterTime += time.Since(filterStart)
				}

				// Decode to query.Event
				decStart := time.Now()
				var ev *query.Event
				var decErr error
				if es.eventFormat == "binary" {
					ev, decErr = event.DecodeBinaryToQueryEventV2(valueCopy, ledger, eventSeq)
				} else {
					ev, decErr = parseRawXDRToQueryEvent(valueCopy, ledger, 0, 0, eventSeq)
				}
				r.decodeTime += time.Since(decStart)

				if decErr != nil {
					continue
				}

				r.events = append(r.events, ev)
			}
		}(w, start, end)
	}

	wg.Wait()

	// Merge results - use max of per-worker times since workers run in parallel
	var fetchTime, decodeTime, filterTime time.Duration
	var bytesRead int64
	var scanned int
	events := make([]*query.Event, 0, fetchCount)
	for _, r := range results {
		events = append(events, r.events...)
		if r.fetchTime > fetchTime {
			fetchTime = r.fetchTime
		}
		if r.decodeTime > decodeTime {
			decodeTime = r.decodeTime
		}
		if r.filterTime > filterTime {
			filterTime = r.filterTime
		}
		bytesRead += r.bytesRead
		scanned += r.scanned
	}

	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	return events, fetchTime, decodeTime, filterTime, bytesRead, scanned
}

// queryPostingListV2WithStats reads V2 posting lists (32-bit local IDs) and returns stats.
func (es *RocksDBEventStore) queryPostingListV2WithStats(cf *grocksdb.ColumnFamilyHandle, termKey [32]byte, buckets []uint32, startLedger, endLedger uint32) ([]uint32, int, int64, time.Duration, time.Duration, error) {
	var allLocalIDs []uint32
	var bytesRead int64
	var readTime, decodeTime time.Duration

	for _, bucketID := range buckets {
		indexKey := EncodeIndexKeyWithBucket(termKey, bucketID)

		t0 := time.Now()
		value, err := es.db.GetCF(es.ro, cf, indexKey)
		readTime += time.Since(t0)
		if err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("failed to read V2 posting list: %w", err)
		}

		if value.Exists() {
			data := value.Data()
			bytesRead += int64(len(data))
			t1 := time.Now()
			localIDs := DecodeLocalIDListDeltaVarint(data)
			decodeTime += time.Since(t1)

			bucketStart := bucketID * BucketSize
			filtered := FilterLocalIDsByLedgerRange(localIDs, bucketStart, startLedger, endLedger)
			allLocalIDs = append(allLocalIDs, filtered...)
		}
		value.Free()
	}

	return allLocalIDs, len(buckets), bytesRead, readTime, decodeTime, nil
}

// =============================================================================
// Posting List V2 Query Result (32-bit Local IDs)
// =============================================================================

// PostingListV2QueryResult holds detailed results from a V2 posting list query.
// V2 uses 32-bit local IDs (same format as bitmap32) instead of 64-bit TOIDs.
type PostingListV2QueryResult struct {
	// Ledger range
	LedgerRange uint32 // endLedger - startLedger + 1

	// Posting list stats
	BucketsScanned         int   // Number of bucket ranges scanned
	PostingListsRead       int   // Number of posting list keys read
	PostingListBytes       int64 // Total bytes read from posting lists
	LocalIDsInPostingList  int   // Total local IDs in posting lists
	LocalIDsAfterIntersect int   // Local IDs after intersection

	// Event fetch stats
	EventsScanned  int   // Events scanned from storage
	EventsReturned int   // Events returned after filtering
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	PostingListTime       time.Duration // Time reading posting lists (total)
	PostingListReadTime   time.Duration // I/O: time in RocksDB GetCF
	PostingListDecodeTime time.Duration // CPU: time in DecodeLocalIDListDeltaVarint
	IntersectTime         time.Duration // Time intersecting local ID lists
	EventFetchTime        time.Duration // Time fetching events
	DecodeTime            time.Duration // Time decoding events
	FilterTime            time.Duration // Time filtering events
	TotalTime             time.Duration // Total query time
}

// QueryEventsWithPostingListV2MultiFilter queries events using V2 posting lists with multi-value OR filters.
// contractIDs: multiple contract IDs (OR within group)
// topicGroups: per-position topic values (OR within position, AND across positions)
// Semantics: (contract1 OR contract2 OR ...) AND (topic0val1 OR topic0val2 OR ...) AND (topic1val1 OR ...)
func (es *RocksDBEventStore) QueryEventsWithPostingListV2MultiFilter(
	contractIDs [][]byte,
	topicGroups [4][][]byte,
	startLedger, endLedger uint32,
	limit int,
) (*PostingListV2QueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &PostingListV2QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	hasContracts := len(contractIDs) > 0
	hasTopics := false
	for _, tg := range topicGroups {
		if len(tg) > 0 {
			hasTopics = true
			break
		}
	}

	if !hasContracts && !hasTopics {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	buckets := GetBucketsForRange(startLedger, endLedger)
	plStart := time.Now()

	// Count total posting lists to read
	numLists := len(contractIDs)
	for _, tg := range topicGroups {
		numLists += len(tg)
	}

	// Track which group each result belongs to for union/intersect
	type plGroup struct {
		groupIdx int // 0=contracts, 1-4=topic positions
		result   postingListV2Result
	}

	plResults := make([]plGroup, numLists)
	var wg sync.WaitGroup
	idx := 0

	// Read all contract posting lists in parallel
	for ci, cid := range contractIDs {
		wg.Add(1)
		go func(i int, contractID []byte) {
			defer wg.Done()
			termKey := ContractTermKey(contractID)
			ids, bucketsRead, bytesRead, readT, decodeT, err := es.queryPostingListV2WithStats(es.cfContractsPLV2, termKey, buckets, startLedger, endLedger)
			plResults[i] = plGroup{
				groupIdx: 0,
				result: postingListV2Result{
					localIDs:   ids,
					buckets:    bucketsRead,
					bytesRead:  bytesRead,
					readTime:   readT,
					decodeTime: decodeT,
					err:        err,
					isContract: true,
				},
			}
		}(idx, cid)
		idx++
		_ = ci
	}

	// Read all topic posting lists in parallel
	for pos, tg := range topicGroups {
		for _, topicXDR := range tg {
			if len(topicXDR) == 0 {
				continue
			}
			wg.Add(1)
			go func(i int, groupIdx int, topic []byte) {
				defer wg.Done()
				termKey := TopicTermKey(topic)
				ids, bucketsRead, bytesRead, readT, decodeT, err := es.queryPostingListV2WithStats(es.cfTopicsPLV2, termKey, buckets, startLedger, endLedger)
				plResults[i] = plGroup{
					groupIdx: groupIdx,
					result: postingListV2Result{
						localIDs:   ids,
						buckets:    bucketsRead,
						bytesRead:  bytesRead,
						readTime:   readT,
						decodeTime: decodeT,
						err:        err,
						isContract: false,
					},
				}
			}(idx, pos+1, topicXDR)
			idx++
		}
	}

	wg.Wait()

	// Check for errors and aggregate stats
	for _, pg := range plResults[:idx] {
		if pg.result.err != nil {
			return nil, nil, pg.result.err
		}
		result.BucketsScanned += pg.result.buckets
		result.PostingListsRead++
		result.PostingListBytes += pg.result.bytesRead
		result.PostingListReadTime += pg.result.readTime
		result.PostingListDecodeTime += pg.result.decodeTime
		result.LocalIDsInPostingList += len(pg.result.localIDs)
	}

	// Union within each group, then intersect across groups
	groupLists := make(map[int][][]uint32) // groupIdx -> list of localID lists
	for _, pg := range plResults[:idx] {
		groupLists[pg.groupIdx] = append(groupLists[pg.groupIdx], pg.result.localIDs)
	}

	// Union within each group
	var groupUnions [][]uint32
	for _, lists := range groupLists {
		var unioned []uint32
		for _, list := range lists {
			unioned = UnionLocalIDLists(unioned, list)
		}
		groupUnions = append(groupUnions, unioned)
	}

	// Sort by size ascending for efficient intersection
	sort.Slice(groupUnions, func(a, b int) bool {
		return len(groupUnions[a]) < len(groupUnions[b])
	})

	// Intersect across groups
	var resultLocalIDs []uint32
	for i, unioned := range groupUnions {
		if i == 0 {
			resultLocalIDs = unioned
		} else {
			intersectStart := time.Now()
			resultLocalIDs = IntersectLocalIDLists(resultLocalIDs, unioned)
			result.IntersectTime += time.Since(intersectStart)
		}
		if len(resultLocalIDs) == 0 {
			result.PostingListTime = time.Since(plStart) - result.IntersectTime
			result.TotalTime = time.Since(totalStart)
			return result, nil, nil
		}
	}

	result.PostingListTime = time.Since(plStart) - result.IntersectTime
	result.LocalIDsAfterIntersect = len(resultLocalIDs)

	// Flatten contractIDs and topicGroups for post-filter compatibility
	// For post-filtering, we pass all contracts and flatten topics (non-positional)
	var flatTopics [][]byte
	for _, tg := range topicGroups {
		flatTopics = append(flatTopics, tg...)
	}

	// Fetch events using point gets with multi-value post-filter
	events, fetchTime, decodeTime, filterTime, bytesRead, scanned := es.fetchEventsByLocalIDsV2MultiFilter(
		resultLocalIDs, buckets, startLedger, endLedger, limit, contractIDs, topicGroups)

	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventBytesRead = bytesRead
	result.EventsScanned = scanned
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// fetchEventsByLocalIDsV2MultiFilter fetches events by local IDs with multi-value post-filtering.
// Contract match: event matches ANY of the contractIDs
// Topic match: for each topic position with filters, event's topic at that position matches ANY of the values
// Uses parallel iterators to overlap RocksDB lookup latency across multiple goroutines.
func (es *RocksDBEventStore) fetchEventsByLocalIDsV2MultiFilter(localIDs []uint32, buckets []uint32, startLedger, endLedger uint32, limit int, contractIDs [][]byte, topicGroups [4][][]byte) ([]*query.Event, time.Duration, time.Duration, time.Duration, int64, int) {

	// Pre-compute hasTopicFilters once
	hasTopicFilters := false
	for _, tg := range topicGroups {
		if len(tg) > 0 {
			hasTopicFilters = true
			break
		}
	}

	// Group local IDs by bucket and flatten into ordered keys
	type keyMeta struct {
		key      []byte
		localID  uint32
		bucketID uint32
	}

	localIDsByBucket := make(map[uint32][]uint32)
	for _, localID := range localIDs {
		ledgerOffset := localID >> 16
		for _, bucketID := range buckets {
			bucketStart := bucketID * BucketSize
			ledger := bucketStart + ledgerOffset
			_, bucketEnd := BucketRange(bucketID)
			if ledger >= startLedger && ledger <= endLedger && ledger <= bucketEnd && ledgerOffset < BucketSize {
				localIDsByBucket[bucketID] = append(localIDsByBucket[bucketID], localID)
				break
			}
		}
	}

	// Build ordered list of keys, capped at limit
	fetchCap := len(localIDs)
	if limit > 0 && fetchCap > limit {
		fetchCap = limit
	}
	allKeys := make([]keyMeta, 0, fetchCap)
	for _, bucketID := range buckets {
		if len(allKeys) >= fetchCap {
			break
		}
		bucketLocalIDs := localIDsByBucket[bucketID]
		bucketStart := bucketID * BucketSize
		for _, localID := range bucketLocalIDs {
			if len(allKeys) >= fetchCap {
				break
			}
			ledger, eventSeq := event.DecodeLocalIDForBucket(localID, bucketStart)
			allKeys = append(allKeys, keyMeta{
				key:      event.EncodeKeyV2(ledger, eventSeq),
				localID:  localID,
				bucketID: bucketID,
			})
		}
	}

	if len(allKeys) == 0 {
		return nil, 0, 0, 0, 0, 0
	}

	// Parallel fetch using iterators
	const maxWorkers = 4
	numWorkers := maxWorkers
	if len(allKeys) < numWorkers {
		numWorkers = len(allKeys)
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
			r := &results[workerID]
			r.events = make([]*query.Event, 0, end-start)

			iter := es.db.NewIteratorCF(es.ro, es.cfEvents)
			defer iter.Close()

			for i := start; i < end; i++ {
				km := allKeys[i]

				seekStart := time.Now()
				iter.Seek(km.key)
				if !iter.Valid() {
					r.fetchTime += time.Since(seekStart)
					break
				}

				iterKey := iter.Key()
				if iterKey == nil || !bytes.Equal(iterKey.Data(), km.key) {
					r.fetchTime += time.Since(seekStart)
					continue
				}

				iterVal := iter.Value()
				if iterVal == nil {
					r.fetchTime += time.Since(seekStart)
					continue
				}
				valData := iterVal.Data()
				if len(valData) == 0 {
					r.fetchTime += time.Since(seekStart)
					continue
				}

				valueCopy := make([]byte, len(valData))
				copy(valueCopy, valData)
				r.fetchTime += time.Since(seekStart)

				r.bytesRead += int64(len(valueCopy))
				r.scanned++

				// Multi-value post-filter using binary header
				filterStart := time.Now()
				if es.eventFormat == "binary" && (len(contractIDs) > 0 || hasTopicFilters) {
					header := event.ParseBinaryHeader(valueCopy)
					if header != nil {
						matches := true
						// Contract: event must match ANY of the contractIDs
						if len(contractIDs) > 0 {
							contractMatch := false
							for _, cid := range contractIDs {
								if header.MatchesContractID(cid) {
									contractMatch = true
									break
								}
							}
							if !contractMatch {
								matches = false
							}
						}
						// Topics: OR within each position, AND across positions
						if matches && hasTopicFilters && !header.MatchesTopicsPositionalMulti(topicGroups) {
							matches = false
						}
						r.filterTime += time.Since(filterStart)
						if !matches {
							continue
						}
					}
				} else {
					r.filterTime += time.Since(filterStart)
				}

				// Decode to query.Event
				bucketStart := km.bucketID * BucketSize
				ledger, eventSeq := event.DecodeLocalIDForBucket(km.localID, bucketStart)

				decStart := time.Now()
				var ev *query.Event
				var decErr error
				if es.eventFormat == "binary" {
					ev, decErr = event.DecodeBinaryToQueryEventV2(valueCopy, ledger, eventSeq)
				} else {
					ev, decErr = parseRawXDRToQueryEvent(valueCopy, ledger, 0, 0, eventSeq)
				}
				r.decodeTime += time.Since(decStart)

				if decErr != nil {
					continue
				}

				r.events = append(r.events, ev)
			}
		}(w, start, end)
	}

	wg.Wait()

	// Merge results - use max of per-worker times since workers run in parallel
	var fetchTime, decodeTime, filterTime time.Duration
	var bytesRead int64
	var scanned int
	events := make([]*query.Event, 0, fetchCap)
	for _, r := range results {
		events = append(events, r.events...)
		if r.fetchTime > fetchTime {
			fetchTime = r.fetchTime
		}
		if r.decodeTime > decodeTime {
			decodeTime = r.decodeTime
		}
		if r.filterTime > filterTime {
			filterTime = r.filterTime
		}
		bytesRead += r.bytesRead
		scanned += r.scanned
	}

	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	return events, fetchTime, decodeTime, filterTime, bytesRead, scanned
}

// ToUnified converts PostingListV2QueryResult to UnifiedQueryResult
func (r *PostingListV2QueryResult) ToUnified() *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:       "posting-v2",
		LedgerRange:     r.LedgerRange,
		IndexMatches:    r.LocalIDsAfterIntersect,
		MatchUnitName:   "local IDs",
		EventsScanned:   r.EventsScanned,
		EventsReturned:  r.EventsReturned,
		IndexBytesRead:  r.PostingListBytes,
		EventBytesRead:  r.EventBytesRead,
		IndexLookupTime: r.PostingListTime + r.IntersectTime,
		EventFetchTime:  r.EventFetchTime,
		DecodeTime:      r.DecodeTime,
		FilterTime:      r.FilterTime,
		TotalTime:       r.TotalTime,
	}
}
