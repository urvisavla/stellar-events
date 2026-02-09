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

// TopicTermKey computes the term key (SHA-256) for a topic XDR value at a given position.
// The position byte is prepended before hashing to produce distinct keys per topic position.
func TopicTermKey(pos int, topicXDR []byte) [32]byte {
	buf := make([]byte, 1+len(topicXDR))
	buf[0] = byte(pos)
	copy(buf[1:], topicXDR)
	return sha256.Sum256(buf)
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

// FilterLocalIDsByLedgerRange filters dense local IDs to only include those within the ledger range.
// Uses the BucketLedgerMap to convert ledger range to dense ID range for filtering.
func FilterLocalIDsByLedgerRange(localIDs []uint32, lm *BucketLedgerMap, bucketStart, startLedger, endLedger uint32) []uint32 {
	if len(localIDs) == 0 {
		return nil
	}

	if lm == nil {
		// No ledger map — can't filter by range, return all
		return localIDs
	}

	// Compute ledger offsets within the bucket
	var startOff uint16
	if startLedger > bucketStart {
		startOff = uint16(startLedger - bucketStart)
	}
	endOff := uint16(BucketSize - 1)
	_, bucketEnd := BucketRange(BucketID(bucketStart))
	if endLedger < bucketEnd {
		endOff = uint16(endLedger - bucketStart)
	}

	startLocalID, endLocalID := lm.LedgerRangeToIDRange(startOff, endOff)

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
	localIDs         []uint32            // flat list (for stats/compat)
	localIDsByBucket map[uint32][]uint32 // per-bucket dense IDs
	buckets          int
	bytesRead        int64
	readTime         time.Duration
	decodeTime       time.Duration
	filterTime       time.Duration
	err              error
	isContract       bool
}

// QueryEventsWithPostingListV2Timing queries events using V2 posting lists (32-bit local IDs).
// Uses point gets with V2 event keys instead of TOID-based range scans.
// Supports parallel reads, guided intersection (smallest-first), and streaming for single-filter queries.
// Requires data ingested with V2Indexes: true (V2 event keys in cfEvents).
func (es *RocksDBEventStore) QueryEventsWithPostingListV2Timing(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*PostingListV2QueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &PostingListV2QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	hasContract := len(contractID) > 0
	hasTopics := false
	topicTermCount := 0
	for _, tg := range topicGroups {
		if len(tg) > 0 {
			hasTopics = true
			topicTermCount += len(tg)
		}
	}
	multiFilter := (hasContract && hasTopics) || topicTermCount > 1

	if !hasContract && !hasTopics {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// For single filter queries with limit, use streaming approach
	if !multiFilter && limit > 0 {
		return es.queryPostingListV2Streaming(contractID, topicGroups, startLedger, endLedger, limit, result, totalStart)
	}

	// Multi-filter: use parallel reads + guided intersection
	buckets := GetBucketsForRange(startLedger, endLedger)

	plStart := time.Now()

	// Parallel read all posting lists
	plResults := es.queryPostingListsV2Parallel(contractID, topicGroups, buckets, startLedger, endLedger)

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
		result.PostingListFilterTime += plr.filterTime
		result.LocalIDsInPostingList += len(plr.localIDs)
	}

	// Intersect per-bucket (dense IDs are only unique within a bucket)
	intersectStart := time.Now()
	resultByBucket := make(map[uint32][]uint32)
	totalAfterIntersect := 0

	for _, bucketID := range buckets {
		// Collect per-bucket lists from all posting list results
		var bucketLists [][]uint32
		for _, plr := range plResults {
			if ids, ok := plr.localIDsByBucket[bucketID]; ok {
				bucketLists = append(bucketLists, ids)
			}
		}
		// All posting lists must have data for this bucket
		if len(bucketLists) < len(plResults) {
			continue
		}

		// Sort by size for efficient intersection
		sort.Slice(bucketLists, func(a, b int) bool {
			return len(bucketLists[a]) < len(bucketLists[b])
		})

		ids := bucketLists[0]
		for i := 1; i < len(bucketLists); i++ {
			ids = IntersectLocalIDLists(ids, bucketLists[i])
			if len(ids) == 0 {
				break
			}
		}
		if len(ids) > 0 {
			resultByBucket[bucketID] = ids
			totalAfterIntersect += len(ids)
		}
	}
	result.IntersectTime = time.Since(intersectStart)

	if totalAfterIntersect == 0 {
		result.PostingListTime = time.Since(plStart) - result.IntersectTime
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	result.PostingListTime = time.Since(plStart) - result.IntersectTime
	result.LocalIDsAfterIntersect = totalAfterIntersect

	// Fetch events per bucket using point gets
	events, fetchTime, decodeTime, filterTime, bytesRead, scanned := es.fetchEventsByLocalIDsV2PerBucket(
		resultByBucket, buckets, limit, contractID)

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
func (es *RocksDBEventStore) queryPostingListsV2Parallel(contractID []byte, topicGroups [4][][]byte, buckets []uint32, startLedger, endLedger uint32) []postingListV2Result {
	// Count how many posting lists to read
	numLists := 0
	if len(contractID) > 0 {
		numLists++
	}
	for _, tg := range topicGroups {
		for _, t := range tg {
			if len(t) > 0 {
				numLists++
			}
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
			ids, perBucket, bucketsRead, bytesRead, readT, decodeT, filterT, err := es.queryPostingListV2WithStats(es.cfContractsPLV2, termKey, buckets, startLedger, endLedger)
			results[i] = postingListV2Result{
				localIDs:         ids,
				localIDsByBucket: perBucket,
				buckets:          bucketsRead,
				bytesRead:        bytesRead,
				readTime:         readT,
				decodeTime:       decodeT,
				filterTime:       filterT,
				err:              err,
				isContract:       true,
			}
		}(idx)
		idx++
	}

	// Read topic posting lists (positional)
	for pos, tg := range topicGroups {
		for _, topicXDR := range tg {
			if len(topicXDR) == 0 {
				continue
			}
			wg.Add(1)
			go func(i, p int, topic []byte) {
				defer wg.Done()
				termKey := TopicTermKey(p, topic)
				ids, perBucket, bucketsRead, bytesRead, readT, decodeT, filterT, err := es.queryPostingListV2WithStats(es.cfTopicsPLV2, termKey, buckets, startLedger, endLedger)
				results[i] = postingListV2Result{
					localIDs:         ids,
					localIDsByBucket: perBucket,
					buckets:          bucketsRead,
					bytesRead:        bytesRead,
					readTime:         readT,
					decodeTime:       decodeT,
					filterTime:       filterT,
					err:              err,
					isContract:       false,
				}
			}(idx, pos, topicXDR)
			idx++
		}
	}

	wg.Wait()
	return results
}

// queryPostingListV2Streaming reads posting lists bucket-by-bucket and fetches events incrementally.
// Stops early when limit is reached.
func (es *RocksDBEventStore) queryPostingListV2Streaming(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int, result *PostingListV2QueryResult, totalStart time.Time) (*PostingListV2QueryResult, []*query.Event, error) {
	buckets := GetBucketsForRange(startLedger, endLedger)

	// Determine which CF and term key to use
	var cf *grocksdb.ColumnFamilyHandle
	var termKey [32]byte

	if len(contractID) > 0 {
		cf = es.cfContractsPLV2
		termKey = ContractTermKey(contractID)
	} else {
		found := false
		for pos, tg := range topicGroups {
			if len(tg) > 0 && len(tg[0]) > 0 {
				termKey = TopicTermKey(pos, tg[0])
				cf = es.cfTopicsPLV2
				found = true
				break
			}
		}
		if !found {
			result.TotalTime = time.Since(totalStart)
			return result, nil, nil
		}
	}

	var allEvents []*query.Event
	var plTime, plReadTime, plDecodeTime, plFilterTime time.Duration
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

		// Load ledger map for this bucket for range filtering and ID resolution
		lm, lmErr := es.LoadBucketLedgerMap(bucketID)
		if lmErr != nil {
			return nil, nil, fmt.Errorf("failed to load ledger map for bucket %d: %w", bucketID, lmErr)
		}

		// Filter by ledger range using dense IDs
		t2 := time.Now()
		filtered := FilterLocalIDsByLedgerRange(localIDs, lm, bucketStart, startLedger, endLedger)
		plFilterTime += time.Since(t2)
		result.LocalIDsInPostingList += len(filtered)
		result.LocalIDsAfterIntersect += len(filtered)
		plTime += time.Since(plStart)

		// Fetch events for this bucket's local IDs
		remaining := limit - len(allEvents)
		events, ft, dt, flt, bytesRead, scanned := es.fetchEventsByLocalIDsV2ForBucket(
			filtered, lm, remaining, contractID)

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
	result.PostingListFilterTime = plFilterTime
	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventsReturned = len(allEvents)
	result.TotalTime = time.Since(totalStart)

	return result, allEvents, nil
}

// fetchEventsByLocalIDsV2PerBucket fetches events by per-bucket dense local IDs using point gets with V2 keys.
// Uses the BucketLedgerMap to convert dense IDs to (ledger, eventSeq) pairs.
func (es *RocksDBEventStore) fetchEventsByLocalIDsV2PerBucket(localIDsByBucket map[uint32][]uint32, buckets []uint32, limit int, contractID []byte) ([]*query.Event, time.Duration, time.Duration, time.Duration, int64, int) {
	var fetchTime, decodeTime, filterTime time.Duration
	var bytesRead int64
	var scanned int
	events := make([]*query.Event, 0)

	// Process buckets in order
	for _, bucketID := range buckets {
		if limit > 0 && len(events) >= limit {
			break
		}

		bucketLocalIDs := localIDsByBucket[bucketID]
		if len(bucketLocalIDs) == 0 {
			continue
		}

		lm, err := es.LoadBucketLedgerMap(bucketID)
		if err != nil || lm == nil {
			continue
		}

		remaining := limit - len(events)
		if limit <= 0 {
			remaining = len(bucketLocalIDs)
		}

		evts, ft, dt, flt, br, sc := es.fetchEventsByLocalIDsV2ForBucket(
			bucketLocalIDs, lm, remaining, contractID)

		fetchTime += ft
		decodeTime += dt
		filterTime += flt
		bytesRead += br
		scanned += sc
		events = append(events, evts...)
	}

	return events, fetchTime, decodeTime, filterTime, bytesRead, scanned
}

// fetchEventsByLocalIDsV2ForBucket fetches events for dense local IDs within a single bucket.
// Uses the BucketLedgerMap to convert dense IDs to (ledger, eventSeq) pairs.
// Uses parallel iterators to overlap RocksDB lookup latency across multiple goroutines.
func (es *RocksDBEventStore) fetchEventsByLocalIDsV2ForBucket(localIDs []uint32, lm *BucketLedgerMap, limit int, contractID []byte) ([]*query.Event, time.Duration, time.Duration, time.Duration, int64, int) {
	if len(localIDs) == 0 || lm == nil {
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
				ledger, eventSeq := lm.DenseIDToLedgerAndSeq(localIDs[i])
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

				// Filter using binary header (contract ID only; topic filtering is handled by positional index)
				filterStart := time.Now()
				if es.eventFormat == "binary" && len(contractID) > 0 {
					header := event.ParseBinaryHeader(valueCopy)
					if header != nil {
						if !header.MatchesContractID(contractID) {
							r.filterTime += time.Since(filterStart)
							continue
						}
					}
				}
				r.filterTime += time.Since(filterStart)

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

// queryPostingListV2WithStats reads V2 posting lists (32-bit dense local IDs) and returns stats.
// Returns both a flat list (for stats) and per-bucket map (for correct per-bucket intersection with dense IDs).
func (es *RocksDBEventStore) queryPostingListV2WithStats(cf *grocksdb.ColumnFamilyHandle, termKey [32]byte, buckets []uint32, startLedger, endLedger uint32) ([]uint32, map[uint32][]uint32, int, int64, time.Duration, time.Duration, time.Duration, error) {
	var allLocalIDs []uint32
	perBucket := make(map[uint32][]uint32)
	var bytesRead int64
	var readTime, decodeTime, filterTime time.Duration

	for _, bucketID := range buckets {
		indexKey := EncodeIndexKeyWithBucket(termKey, bucketID)

		t0 := time.Now()
		value, err := es.db.GetCF(es.ro, cf, indexKey)
		readTime += time.Since(t0)
		if err != nil {
			return nil, nil, 0, 0, 0, 0, 0, fmt.Errorf("failed to read V2 posting list: %w", err)
		}

		if value.Exists() {
			data := value.Data()
			bytesRead += int64(len(data))
			t1 := time.Now()
			localIDs := DecodeLocalIDListDeltaVarint(data)
			decodeTime += time.Since(t1)

			t2 := time.Now()
			bucketStart := bucketID * BucketSize
			lm, lmErr := es.LoadBucketLedgerMap(bucketID)
			if lmErr != nil {
				value.Free()
				return nil, nil, 0, 0, 0, 0, 0, fmt.Errorf("failed to load ledger map for bucket %d: %w", bucketID, lmErr)
			}
			filtered := FilterLocalIDsByLedgerRange(localIDs, lm, bucketStart, startLedger, endLedger)
			allLocalIDs = append(allLocalIDs, filtered...)
			if len(filtered) > 0 {
				perBucket[bucketID] = filtered
			}
			filterTime += time.Since(t2)
		}
		value.Free()
	}

	return allLocalIDs, perBucket, len(buckets), bytesRead, readTime, decodeTime, filterTime, nil
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
	PostingListTime        time.Duration // Time reading posting lists (total wall-clock)
	PostingListReadTime    time.Duration // I/O: time in RocksDB GetCF
	PostingListDecodeTime  time.Duration // CPU: time in DecodeLocalIDListDeltaVarint
	PostingListFilterTime  time.Duration // CPU: time in FilterLocalIDsByLedgerRange
	IntersectTime          time.Duration // Time intersecting local ID lists
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
			ids, perBucket, bucketsRead, bytesRead, readT, decodeT, filterT, err := es.queryPostingListV2WithStats(es.cfContractsPLV2, termKey, buckets, startLedger, endLedger)
			plResults[i] = plGroup{
				groupIdx: 0,
				result: postingListV2Result{
					localIDs:         ids,
					localIDsByBucket: perBucket,
					buckets:          bucketsRead,
					bytesRead:        bytesRead,
					readTime:         readT,
					decodeTime:       decodeT,
					filterTime:       filterT,
					err:              err,
					isContract:       true,
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
			go func(i int, groupIdx int, p int, topic []byte) {
				defer wg.Done()
				termKey := TopicTermKey(p, topic)
				ids, perBucket, bucketsRead, bytesRead, readT, decodeT, filterT, err := es.queryPostingListV2WithStats(es.cfTopicsPLV2, termKey, buckets, startLedger, endLedger)
				plResults[i] = plGroup{
					groupIdx: groupIdx,
					result: postingListV2Result{
						localIDs:         ids,
						localIDsByBucket: perBucket,
						buckets:          bucketsRead,
						bytesRead:        bytesRead,
						readTime:         readT,
						decodeTime:       decodeT,
						filterTime:       filterT,
						err:              err,
						isContract:       false,
					},
				}
			}(idx, pos+1, pos, topicXDR)
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
		result.PostingListFilterTime += pg.result.filterTime
		result.LocalIDsInPostingList += len(pg.result.localIDs)
	}

	// Per-bucket: union within each group, then intersect across groups
	// groupBucketLists: groupIdx -> bucketID -> list of localID slices
	type groupBucketData struct {
		lists map[uint32][][]uint32 // bucketID -> list of ID slices to union
	}
	groupData := make(map[int]*groupBucketData)
	for _, pg := range plResults[:idx] {
		gd, ok := groupData[pg.groupIdx]
		if !ok {
			gd = &groupBucketData{lists: make(map[uint32][][]uint32)}
			groupData[pg.groupIdx] = gd
		}
		for bucketID, ids := range pg.result.localIDsByBucket {
			gd.lists[bucketID] = append(gd.lists[bucketID], ids)
		}
	}

	// For each bucket: union within each group, then intersect across groups
	intersectStart := time.Now()
	resultByBucket := make(map[uint32][]uint32)
	totalAfterIntersect := 0
	numGroups := len(groupData)

	for _, bucketID := range buckets {
		// Union within each group for this bucket
		var groupUnions [][]uint32
		for _, gd := range groupData {
			lists := gd.lists[bucketID]
			if len(lists) == 0 {
				// This group has no data for this bucket -> AND produces empty
				groupUnions = nil
				break
			}
			var unioned []uint32
			for _, list := range lists {
				unioned = UnionLocalIDLists(unioned, list)
			}
			groupUnions = append(groupUnions, unioned)
		}

		if len(groupUnions) < numGroups {
			continue
		}

		// Sort by size for efficient intersection
		sort.Slice(groupUnions, func(a, b int) bool {
			return len(groupUnions[a]) < len(groupUnions[b])
		})

		ids := groupUnions[0]
		for i := 1; i < len(groupUnions); i++ {
			ids = IntersectLocalIDLists(ids, groupUnions[i])
			if len(ids) == 0 {
				break
			}
		}
		if len(ids) > 0 {
			resultByBucket[bucketID] = ids
			totalAfterIntersect += len(ids)
		}
	}
	result.IntersectTime = time.Since(intersectStart)

	if totalAfterIntersect == 0 {
		result.PostingListTime = time.Since(plStart) - result.IntersectTime
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	result.PostingListTime = time.Since(plStart) - result.IntersectTime
	result.LocalIDsAfterIntersect = totalAfterIntersect

	// Fetch events per bucket using point gets with contract post-filter
	events, fetchTime, decodeTime, filterTime, bytesRead, scanned := es.fetchEventsByLocalIDsV2MultiFilterPerBucket(
		resultByBucket, buckets, limit, contractIDs)

	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventBytesRead = bytesRead
	result.EventsScanned = scanned
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// fetchEventsByLocalIDsV2MultiFilterPerBucket fetches events by per-bucket dense local IDs
// with multi-value post-filtering. Uses BucketLedgerMap for ID resolution.
func (es *RocksDBEventStore) fetchEventsByLocalIDsV2MultiFilterPerBucket(localIDsByBucket map[uint32][]uint32, buckets []uint32, limit int, contractIDs [][]byte) ([]*query.Event, time.Duration, time.Duration, time.Duration, int64, int) {

	// Build ordered list of keys using ledger maps, capped at limit
	type keyMeta struct {
		key      []byte
		ledger   uint32
		eventSeq uint16
	}

	totalIDs := 0
	for _, ids := range localIDsByBucket {
		totalIDs += len(ids)
	}
	fetchCap := totalIDs
	if limit > 0 && fetchCap > limit {
		fetchCap = limit
	}
	allKeys := make([]keyMeta, 0, fetchCap)
	for _, bucketID := range buckets {
		if len(allKeys) >= fetchCap {
			break
		}
		bucketLocalIDs := localIDsByBucket[bucketID]
		if len(bucketLocalIDs) == 0 {
			continue
		}
		lm, err := es.LoadBucketLedgerMap(bucketID)
		if err != nil || lm == nil {
			continue
		}
		for _, localID := range bucketLocalIDs {
			if len(allKeys) >= fetchCap {
				break
			}
			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(localID)
			allKeys = append(allKeys, keyMeta{
				key:      event.EncodeKeyV2(ledger, eventSeq),
				ledger:   ledger,
				eventSeq: eventSeq,
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

				// Post-filter: contract ID only (topic filtering is handled by positional index)
				filterStart := time.Now()
				if es.eventFormat == "binary" && len(contractIDs) > 0 {
					header := event.ParseBinaryHeader(valueCopy)
					if header != nil {
						contractMatch := false
						for _, cid := range contractIDs {
							if header.MatchesContractID(cid) {
								contractMatch = true
								break
							}
						}
						if !contractMatch {
							r.filterTime += time.Since(filterStart)
							continue
						}
					}
				}
				r.filterTime += time.Since(filterStart)

				// Decode to query.Event
				decStart := time.Now()
				var ev *query.Event
				var decErr error
				if es.eventFormat == "binary" {
					ev, decErr = event.DecodeBinaryToQueryEventV2(valueCopy, km.ledger, km.eventSeq)
				} else {
					ev, decErr = parseRawXDRToQueryEvent(valueCopy, km.ledger, 0, 0, km.eventSeq)
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
