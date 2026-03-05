package store

import (
	"container/heap"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/urvisavla/stellar-events/internal/event"
)

// =============================================================================
// Statistics Types
// =============================================================================

// DBStats holds statistics about the event database.
type DBStats struct {
	TotalEvents     int64  `json:"total_events"`
	MinLedger       uint32 `json:"min_ledger"`
	MaxLedger       uint32 `json:"max_ledger"`
	LastProcessed   uint32 `json:"last_processed_ledger"`
	UniqueContracts int    `json:"unique_contracts"`
}

// ColumnFamilyStats holds storage stats for a single column family.
type ColumnFamilyStats struct {
	Name           string `json:"name"`
	EstimatedKeys  uint64 `json:"estimated_keys"`
	SSTFilesBytes  uint64 `json:"sst_files_bytes"`
	MemtableBytes  uint64 `json:"memtable_bytes"`
	PendingCompact uint64 `json:"pending_compact_bytes"`
	NumFiles       int    `json:"num_files"`
}

// StorageSnapshot holds storage stats for all column families at a point in time.
type StorageSnapshot struct {
	Timestamp      time.Time                     `json:"timestamp"`
	ColumnFamilies map[string]*ColumnFamilyStats `json:"column_families"`
	TotalSST       uint64                        `json:"total_sst_bytes"`
	TotalMemtable  uint64                        `json:"total_memtable_bytes"`
	TotalFiles     int                           `json:"total_files"`
}

// CFCompactionResult holds compaction results for one column family.
type CFCompactionResult struct {
	Name           string  `json:"name"`
	BeforeBytes    uint64  `json:"before_bytes"`
	AfterBytes     uint64  `json:"after_bytes"`
	Reclaimed      uint64  `json:"reclaimed_bytes"`
	SavingsPercent float64 `json:"savings_percent"`
}

// CompactionSummary holds complete compaction results with per-CF breakdown.
type CompactionSummary struct {
	Before         *StorageSnapshot               `json:"before"`
	After          *StorageSnapshot               `json:"after"`
	Duration       time.Duration                  `json:"duration"`
	PerCF          map[string]*CFCompactionResult `json:"per_cf"`
	TotalReclaimed uint64                         `json:"total_reclaimed"`
	SavingsPercent float64                        `json:"savings_percent"`
}

// =============================================================================
// Index Statistics Types
// =============================================================================

// UniqueIndexCounts holds counts from unique indexes.
type UniqueIndexCounts struct {
	UniqueContracts int64 `json:"unique_contracts"`
	UniqueTopic0    int64 `json:"unique_topic0"`
	UniqueTopic1    int64 `json:"unique_topic1"`
	UniqueTopic2    int64 `json:"unique_topic2"`
	UniqueTopic3    int64 `json:"unique_topic3"`

	TotalContractEvents int64 `json:"total_contract_events"`
	TotalTopic0Events   int64 `json:"total_topic0_events"`
	TotalTopic1Events   int64 `json:"total_topic1_events"`
	TotalTopic2Events   int64 `json:"total_topic2_events"`
	TotalTopic3Events   int64 `json:"total_topic3_events"`
}

// DistributionStats holds percentile statistics for event counts.
type DistributionStats struct {
	Count int64      `json:"count"`
	Min   int64      `json:"min"`
	Max   int64      `json:"max"`
	Mean  float64    `json:"mean"`
	P50   int64      `json:"p50"`
	P75   int64      `json:"p75"`
	P90   int64      `json:"p90"`
	P99   int64      `json:"p99"`
	Total int64      `json:"total"`
	TopN    []TopEntry `json:"top_n,omitempty"`
	BottomN []TopEntry `json:"bottom_n,omitempty"`

	// Key size metrics (for bitmap index truncation analysis)
	Over32Bytes int64 `json:"over_32_bytes"` // Count of unique values > 32 bytes
}

// TopEntry represents a top item by event count.
type TopEntry struct {
	Value      string `json:"value"`
	EventCount int64  `json:"event_count"`
}

// IndexDistribution holds distribution stats for all index types.
type IndexDistribution struct {
	Contracts *DistributionStats `json:"contracts"`
	Topic0    *DistributionStats `json:"topic0"`
	Topic1    *DistributionStats `json:"topic1"`
	Topic2    *DistributionStats `json:"topic2"`
	Topic3    *DistributionStats `json:"topic3"`
}

// EventStats holds computed statistics from scanning all events.
type EventStats struct {
	TotalEvents      int64 `json:"total_events"`
	UniqueContracts  int   `json:"unique_contracts"`
	UniqueTopic0     int   `json:"unique_topic0"`
	UniqueTopic1     int   `json:"unique_topic1"`
	UniqueTopic2     int   `json:"unique_topic2"`
	UniqueTopic3     int   `json:"unique_topic3"`
	ContractEvents   int64 `json:"contract_events"`
	SystemEvents     int64 `json:"system_events"`
	DiagnosticEvents int64 `json:"diagnostic_events"`
}

// BitmapStats holds statistics about bitmap indexes.
type BitmapStats struct {
	CurrentSegmentID   uint32 `json:"current_segment_id"`
	HotSegmentCount    int    `json:"hot_segment_count"`
	HotSegmentCards    uint64 `json:"hot_segment_cards"`
	HotSegmentMemBytes uint64 `json:"hot_segment_mem_bytes"`
	ContractIndexCount int64  `json:"contract_index_count"`
	TopicIndexCount    int64  `json:"topic_index_count"` // All topics (non-positional)
}

// LedgerTxStats holds statistics about transactions per ledger.
type LedgerTxStats struct {
	TotalLedgers     int64 `json:"total_ledgers"`
	LedgersOver10kTx int64 `json:"ledgers_over_10k_tx"`
}

// =============================================================================
// Heap Helpers (for top-N / bottom-N tracking)
// =============================================================================

// topNHeap is a min-heap for tracking top N entries by count
type topNHeap struct {
	entries   []TopEntry
	maxSize   int
	indexType byte // Used to determine encoding format (strkey for contracts)
}

func (h *topNHeap) Len() int           { return len(h.entries) }
func (h *topNHeap) Less(i, j int) bool { return h.entries[i].EventCount < h.entries[j].EventCount }
func (h *topNHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }

func (h *topNHeap) Push(x interface{}) {
	h.entries = append(h.entries, x.(TopEntry))
}

func (h *topNHeap) Pop() interface{} {
	old := h.entries
	n := len(old)
	x := old[n-1]
	h.entries = old[0 : n-1]
	return x
}

// tryAdd adds an entry if it belongs in top N (min-heap, so smallest is at top)
func (h *topNHeap) tryAdd(value []byte, count int64) {
	if h.maxSize <= 0 {
		return
	}

	// Encode value based on index type
	var encoded string
	if h.indexType == UniqueTypeContract && len(value) == 32 {
		// Contract IDs: encode as strkey (C...)
		if s, err := strkey.Encode(strkey.VersionByteContract, value); err == nil {
			encoded = s
		} else {
			encoded = base64.StdEncoding.EncodeToString(value)
		}
	} else {
		// Topics and other values: use base64
		encoded = base64.StdEncoding.EncodeToString(value)
	}

	if len(h.entries) < h.maxSize {
		heap.Push(h, TopEntry{
			Value:      encoded,
			EventCount: count,
		})
	} else if count > h.entries[0].EventCount {
		// Replace smallest if this is larger
		h.entries[0] = TopEntry{
			Value:      encoded,
			EventCount: count,
		}
		heap.Fix(h, 0)
	}
}

// getSorted returns entries sorted by count descending
func (h *topNHeap) getSorted() []TopEntry {
	if len(h.entries) == 0 {
		return nil
	}
	// Sort descending by count
	result := make([]TopEntry, len(h.entries))
	copy(result, h.entries)
	sort.Slice(result, func(i, j int) bool {
		return result[i].EventCount > result[j].EventCount
	})
	return result
}

// bottomNHeap is a max-heap for tracking bottom N entries by count (smallest event counts).
// It keeps the N smallest entries by evicting the largest when full.
type bottomNHeap struct {
	entries   []TopEntry
	maxSize   int
	indexType byte
}

func (h *bottomNHeap) Len() int           { return len(h.entries) }
func (h *bottomNHeap) Less(i, j int) bool { return h.entries[i].EventCount > h.entries[j].EventCount }
func (h *bottomNHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }

func (h *bottomNHeap) Push(x interface{}) {
	h.entries = append(h.entries, x.(TopEntry))
}

func (h *bottomNHeap) Pop() interface{} {
	old := h.entries
	n := len(old)
	x := old[n-1]
	h.entries = old[0 : n-1]
	return x
}

// tryAdd adds an entry if it belongs in bottom N (max-heap, so largest is at top)
func (h *bottomNHeap) tryAdd(value []byte, count int64) {
	if h.maxSize <= 0 {
		return
	}

	var encoded string
	if h.indexType == UniqueTypeContract && len(value) == 32 {
		if s, err := strkey.Encode(strkey.VersionByteContract, value); err == nil {
			encoded = s
		} else {
			encoded = base64.StdEncoding.EncodeToString(value)
		}
	} else {
		encoded = base64.StdEncoding.EncodeToString(value)
	}

	if len(h.entries) < h.maxSize {
		heap.Push(h, TopEntry{
			Value:      encoded,
			EventCount: count,
		})
	} else if count < h.entries[0].EventCount {
		h.entries[0] = TopEntry{
			Value:      encoded,
			EventCount: count,
		}
		heap.Fix(h, 0)
	}
}

// getSorted returns entries sorted by count ascending (smallest first)
func (h *bottomNHeap) getSorted() []TopEntry {
	if len(h.entries) == 0 {
		return nil
	}
	result := make([]TopEntry, len(h.entries))
	copy(result, h.entries)
	sort.Slice(result, func(i, j int) bool {
		return result[i].EventCount < result[j].EventCount
	})
	return result
}

// percentile calculates the p-th percentile from a sorted slice
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := int(float64(len(sorted)-1) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// =============================================================================
// Statistics Methods
// =============================================================================

// GetStorageSnapshot returns per-column-family storage statistics.
func (rb *RocksDBBackend) GetStorageSnapshot() (*StorageSnapshot, error) {
	snapshot := &StorageSnapshot{
		Timestamp:      time.Now(),
		ColumnFamilies: make(map[string]*ColumnFamilyStats),
	}

	cfNames := []string{
		CFDefault, CFEvents, CFUnique,
		CFContractsBM32, CFTopicsBM32,
	}

	// Helper to parse uint64 from RocksDB property string
	parseUint64 := func(val string) uint64 {
		var n uint64
		fmt.Sscanf(val, "%d", &n)
		return n
	}

	// Helper to count files across all levels for a CF
	countFiles := func(cf *grocksdb.ColumnFamilyHandle) int {
		total := 0
		for level := 0; level <= 6; level++ {
			prop := fmt.Sprintf("rocksdb.num-files-at-level%d", level)
			total += int(parseUint64(rb.db.GetPropertyCF(prop, cf)))
		}
		return total
	}

	for i, name := range cfNames {
		cf := rb.cfHandles[i]
		cfStats := &ColumnFamilyStats{
			Name:           name,
			EstimatedKeys:  parseUint64(rb.db.GetPropertyCF("rocksdb.estimate-num-keys", cf)),
			SSTFilesBytes:  parseUint64(rb.db.GetPropertyCF("rocksdb.total-sst-files-size", cf)),
			MemtableBytes:  parseUint64(rb.db.GetPropertyCF("rocksdb.cur-size-all-mem-tables", cf)),
			PendingCompact: parseUint64(rb.db.GetPropertyCF("rocksdb.estimate-pending-compaction-bytes", cf)),
			NumFiles:       countFiles(cf),
		}

		snapshot.ColumnFamilies[name] = cfStats
		snapshot.TotalSST += cfStats.SSTFilesBytes
		snapshot.TotalMemtable += cfStats.MemtableBytes
		snapshot.TotalFiles += cfStats.NumFiles
	}

	return snapshot, nil
}

// GetStats returns statistics about the event database (O(1) - no full scan)
func (rb *RocksDBBackend) GetStats() (*DBStats, error) {
	stats := &DBStats{}

	// Get min/max ledger using seek (O(1))
	it := rb.db.NewIteratorCF(rb.ro, rb.cfEvents)
	defer it.Close()

	// Min ledger from first key
	it.SeekToFirst()
	if it.Valid() {
		key := it.Key().Data()
		if len(key) >= 4 {
			stats.MinLedger = binary.BigEndian.Uint32(key[0:4])
		}
	}

	// Max ledger from last key
	it.SeekToLast()
	if it.Valid() {
		key := it.Key().Data()
		if len(key) >= 4 {
			stats.MaxLedger = binary.BigEndian.Uint32(key[0:4])
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Get estimated event count from events column family (instant, approximate)
	estKeys := rb.db.GetPropertyCF("rocksdb.estimate-num-keys", rb.cfEvents)
	var estCount int64
	fmt.Sscanf(estKeys, "%d", &estCount)
	stats.TotalEvents = estCount

	// Get unique contracts from unique index (instant if index exists)
	counts, _ := rb.CountUniqueIndexes()
	if counts != nil {
		stats.UniqueContracts = int(counts.UniqueContracts)
	}

	stats.LastProcessed, _ = rb.GetLastProcessedLedger()

	return stats, nil
}

// GetLedgerTxStats scans events to count ledgers with high transaction counts.
func (rb *RocksDBBackend) GetLedgerTxStats() (*LedgerTxStats, error) {
	stats := &LedgerTxStats{}

	it := rb.db.NewIteratorCF(rb.ro, rb.cfEvents)
	defer it.Close()

	var currentLedger uint32
	var maxTxIdx uint16
	var first = true

	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 6 {
			continue
		}

		ledger := binary.BigEndian.Uint32(key[0:4])
		txIdx := binary.BigEndian.Uint16(key[4:6])

		if first || ledger != currentLedger {
			// Process previous ledger
			if !first && maxTxIdx >= 10000 {
				stats.LedgersOver10kTx++
			}
			if !first {
				stats.TotalLedgers++
			}

			currentLedger = ledger
			maxTxIdx = txIdx
			first = false
		} else if txIdx > maxTxIdx {
			maxTxIdx = txIdx
		}
	}

	// Process last ledger
	if !first {
		stats.TotalLedgers++
		if maxTxIdx >= 10000 {
			stats.LedgersOver10kTx++
		}
	}

	return stats, it.Err()
}

// CompactAllWithStats runs manual compaction and returns before/after stats per column family.
func (rb *RocksDBBackend) CompactAllWithStats() (*CompactionSummary, error) {
	before, err := rb.GetStorageSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to get pre-compaction stats: %w", err)
	}

	start := time.Now()

	// Create compaction options for full compaction
	compactOpts := grocksdb.NewCompactRangeOptions()
	defer compactOpts.Destroy()

	// Force compaction to bottommost level for maximum compression
	compactOpts.SetBottommostLevelCompaction(grocksdb.KForceOptimized)
	compactOpts.SetExclusiveManualCompaction(true)

	// Compact ALL column families
	fullRange := grocksdb.Range{Start: nil, Limit: nil}
	for _, cf := range rb.cfHandles {
		rb.db.CompactRangeCFOpt(cf, fullRange, compactOpts)
	}

	duration := time.Since(start)

	after, err := rb.GetStorageSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to get post-compaction stats: %w", err)
	}

	// Build per-CF compaction results
	perCF := make(map[string]*CFCompactionResult)
	for name, beforeCF := range before.ColumnFamilies {
		afterCF := after.ColumnFamilies[name]
		var reclaimed uint64
		if beforeCF.SSTFilesBytes > afterCF.SSTFilesBytes {
			reclaimed = beforeCF.SSTFilesBytes - afterCF.SSTFilesBytes
		}
		pct := 0.0
		if beforeCF.SSTFilesBytes > 0 {
			pct = float64(reclaimed) / float64(beforeCF.SSTFilesBytes) * 100
		}
		perCF[name] = &CFCompactionResult{
			Name:           name,
			BeforeBytes:    beforeCF.SSTFilesBytes,
			AfterBytes:     afterCF.SSTFilesBytes,
			Reclaimed:      reclaimed,
			SavingsPercent: pct,
		}
	}

	var totalReclaimed uint64
	if before.TotalSST > after.TotalSST {
		totalReclaimed = before.TotalSST - after.TotalSST
	}
	totalSavings := 0.0
	if before.TotalSST > 0 {
		totalSavings = float64(totalReclaimed) / float64(before.TotalSST) * 100
	}

	return &CompactionSummary{
		Before:         before,
		After:          after,
		Duration:       duration,
		PerCF:          perCF,
		TotalReclaimed: totalReclaimed,
		SavingsPercent: totalSavings,
	}, nil
}

// CountUniqueIndexes counts entries in unique indexes and sums their event counts (parallel)
func (rb *RocksDBBackend) CountUniqueIndexes() (*UniqueIndexCounts, error) {
	// Use 16 partitions per index type (total 80 goroutines for 5 index types)
	const partitions = 16

	type result struct {
		indexType   byte
		uniqueCount int64
		totalEvents int64
		err         error
	}

	results := make(chan result, 5*partitions)
	var wg sync.WaitGroup

	// For each index type, partition the key space and count in parallel
	for _, indexType := range []byte{UniqueTypeContract, UniqueTypeTopic0, UniqueTypeTopic1, UniqueTypeTopic2, UniqueTypeTopic3} {
		for p := 0; p < partitions; p++ {
			wg.Add(1)
			go func(idxType byte, partition int) {
				defer wg.Done()
				unique, total, err := rb.countIndexTypePartition(idxType, partition, partitions)
				results <- result{idxType, unique, total, err}
			}(indexType, p)
		}
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	counts := &UniqueIndexCounts{}
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		switch r.indexType {
		case UniqueTypeContract:
			counts.UniqueContracts += r.uniqueCount
			counts.TotalContractEvents += r.totalEvents
		case UniqueTypeTopic0:
			counts.UniqueTopic0 += r.uniqueCount
			counts.TotalTopic0Events += r.totalEvents
		case UniqueTypeTopic1:
			counts.UniqueTopic1 += r.uniqueCount
			counts.TotalTopic1Events += r.totalEvents
		case UniqueTypeTopic2:
			counts.UniqueTopic2 += r.uniqueCount
			counts.TotalTopic2Events += r.totalEvents
		case UniqueTypeTopic3:
			counts.UniqueTopic3 += r.uniqueCount
			counts.TotalTopic3Events += r.totalEvents
		}
	}

	return counts, nil
}

// countIndexTypePartition counts entries for a partition of an index type
// Partitions are based on the first byte of the value (after the type prefix)
func (rb *RocksDBBackend) countIndexTypePartition(indexType byte, partition, totalPartitions int) (uniqueCount, totalEvents int64, err error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := rb.db.NewIteratorCF(ro, rb.cfUnique)
	defer it.Close()

	// Calculate byte range for this partition
	bytesPerPartition := 256 / totalPartitions
	startByte := byte(partition * bytesPerPartition)
	endByte := byte((partition + 1) * bytesPerPartition)
	if partition == totalPartitions-1 {
		endByte = 0 // Will wrap, handled in comparison
	}

	// Start key: [indexType][startByte]
	startKey := []byte{indexType, startByte}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 2 || key[0] != indexType {
			break
		}

		// Check if we're still in our partition
		valueByte := key[1]
		if partition < totalPartitions-1 && valueByte >= endByte {
			break
		}

		uniqueCount++
		if it.Value().Size() == 8 {
			totalEvents += int64(binary.BigEndian.Uint64(it.Value().Data()))
		}
	}

	if err := it.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterator error for type %d partition %d: %w", indexType, partition, err)
	}

	return uniqueCount, totalEvents, nil
}

// GetIndexDistribution computes percentile statistics for each index type in parallel
// topN specifies how many top entries to include (0 for none)
func (rb *RocksDBBackend) GetIndexDistribution(topN int, bottomN ...int) (*IndexDistribution, error) {
	botN := 0
	if len(bottomN) > 0 {
		botN = bottomN[0]
	}

	// Scan each type prefix in parallel
	type result struct {
		indexType byte
		stats     *DistributionStats
		err       error
	}

	results := make(chan result, 5)
	var wg sync.WaitGroup

	// Launch a goroutine for each index type
	for _, indexType := range []byte{UniqueTypeContract, UniqueTypeTopic0, UniqueTypeTopic1, UniqueTypeTopic2, UniqueTypeTopic3} {
		wg.Add(1)
		go func(idxType byte) {
			defer wg.Done()
			stats, err := rb.computeDistributionForType(idxType, topN, botN)
			results <- result{idxType, stats, err}
		}(indexType)
	}

	// Close results channel when all goroutines done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	dist := &IndexDistribution{}
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		switch r.indexType {
		case UniqueTypeContract:
			dist.Contracts = r.stats
		case UniqueTypeTopic0:
			dist.Topic0 = r.stats
		case UniqueTypeTopic1:
			dist.Topic1 = r.stats
		case UniqueTypeTopic2:
			dist.Topic2 = r.stats
		case UniqueTypeTopic3:
			dist.Topic3 = r.stats
		}
	}

	return dist, nil
}

// computeDistributionForType computes distribution for a single index type using parallel partitions
func (rb *RocksDBBackend) computeDistributionForType(indexType byte, topN, bottomN int) (*DistributionStats, error) {
	const partitions = 16

	type partitionResult struct {
		counts      []int64
		total       int64
		over32Bytes int64
		topN        []TopEntry
		bottomN     []TopEntry
		err         error
	}

	results := make(chan partitionResult, partitions)
	var wg sync.WaitGroup

	// Scan each partition in parallel
	for p := 0; p < partitions; p++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			counts, total, over32, top, bot, err := rb.scanDistributionPartition(indexType, partition, partitions, topN, bottomN)
			results <- partitionResult{counts, total, over32, top, bot, err}
		}(p)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Merge results from all partitions
	var allCounts []int64
	var totalSum int64
	var over32Sum int64
	mergedTopN := &topNHeap{maxSize: topN, entries: make([]TopEntry, 0, topN)}
	mergedBottomN := &bottomNHeap{maxSize: bottomN, entries: make([]TopEntry, 0, bottomN)}

	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		allCounts = append(allCounts, r.counts...)
		totalSum += r.total
		over32Sum += r.over32Bytes

		// Merge top-N entries
		for _, entry := range r.topN {
			if len(mergedTopN.entries) < topN {
				heap.Push(mergedTopN, entry)
			} else if entry.EventCount > mergedTopN.entries[0].EventCount {
				mergedTopN.entries[0] = entry
				heap.Fix(mergedTopN, 0)
			}
		}

		// Merge bottom-N entries
		for _, entry := range r.bottomN {
			if len(mergedBottomN.entries) < bottomN {
				heap.Push(mergedBottomN, entry)
			} else if entry.EventCount < mergedBottomN.entries[0].EventCount {
				mergedBottomN.entries[0] = entry
				heap.Fix(mergedBottomN, 0)
			}
		}
	}

	if len(allCounts) == 0 {
		return &DistributionStats{}, nil
	}

	// Sort for percentiles
	sort.Slice(allCounts, func(i, j int) bool {
		return allCounts[i] < allCounts[j]
	})

	return &DistributionStats{
		Count:       int64(len(allCounts)),
		Min:         allCounts[0],
		Max:         allCounts[len(allCounts)-1],
		Mean:        float64(totalSum) / float64(len(allCounts)),
		Total:       totalSum,
		P50:         percentile(allCounts, 50),
		P75:         percentile(allCounts, 75),
		P90:         percentile(allCounts, 90),
		P99:         percentile(allCounts, 99),
		TopN:        mergedTopN.getSorted(),
		BottomN:     mergedBottomN.getSorted(),
		Over32Bytes: over32Sum,
	}, nil
}

// scanDistributionPartition scans a partition and returns counts for distribution
// Returns: counts, total, over32Bytes, topN entries, bottomN entries, error
func (rb *RocksDBBackend) scanDistributionPartition(indexType byte, partition, totalPartitions, topN, bottomN int) ([]int64, int64, int64, []TopEntry, []TopEntry, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := rb.db.NewIteratorCF(ro, rb.cfUnique)
	defer it.Close()

	// Calculate byte range for this partition
	bytesPerPartition := 256 / totalPartitions
	startByte := byte(partition * bytesPerPartition)
	endByte := byte((partition + 1) * bytesPerPartition)
	if partition == totalPartitions-1 {
		endByte = 0
	}

	startKey := []byte{indexType, startByte}
	var counts []int64
	var total int64
	var over32Bytes int64
	topHeap := &topNHeap{maxSize: topN, entries: make([]TopEntry, 0, topN), indexType: indexType}
	botHeap := &bottomNHeap{maxSize: bottomN, entries: make([]TopEntry, 0, bottomN), indexType: indexType}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 2 || key[0] != indexType {
			break
		}

		valueByte := key[1]
		if partition < totalPartitions-1 && valueByte >= endByte {
			break
		}

		// Count values > 32 bytes (key[1:] is the value portion)
		valueLen := len(key) - 1
		if valueLen > 32 {
			over32Bytes++
		}

		var eventCount int64
		if it.Value().Size() == 8 {
			eventCount = int64(binary.BigEndian.Uint64(it.Value().Data()))
		}

		counts = append(counts, eventCount)
		total += eventCount
		topHeap.tryAdd(key[1:], eventCount)
		botHeap.tryAdd(key[1:], eventCount)
	}

	if err := it.Err(); err != nil {
		return nil, 0, 0, nil, nil, fmt.Errorf("iterator error for type %d partition %d: %w", indexType, partition, err)
	}

	return counts, total, over32Bytes, topHeap.getSorted(), botHeap.getSorted(), nil
}

// getLedgerRange finds the min and max ledger sequences in the database
func (rb *RocksDBBackend) getLedgerRange() (uint32, uint32, error) {
	var minLedger, maxLedger uint32

	it := rb.db.NewIteratorCF(rb.ro, rb.cfEvents)
	defer it.Close()

	// Find first key
	it.SeekToFirst()
	if it.Valid() {
		key := it.Key().Data()
		if len(key) >= 4 {
			minLedger = binary.BigEndian.Uint32(key[0:4])
		}
	}

	// Find last key
	it.SeekToLast()
	if it.Valid() {
		key := it.Key().Data()
		if len(key) >= 4 {
			maxLedger = binary.BigEndian.Uint32(key[0:4])
		}
	}

	return minLedger, maxLedger, it.Err()
}

// =============================================================================
// Compute Event Stats (Full Scan)
// =============================================================================

// ComputeEventStats scans all events and computes unique counts.
// Workers controls parallelism: 0 uses NumCPU, 1 for single-threaded, >1 for parallel.
func (rb *RocksDBBackend) ComputeEventStats(workers int) (*EventStats, error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// For single worker, use simple sequential scan (more memory efficient)
	if workers == 1 {
		return rb.computeEventStatsSingleThread()
	}

	// Parallel implementation
	minLedger, maxLedger, err := rb.getLedgerRange()
	if err != nil {
		return nil, fmt.Errorf("failed to get ledger range: %w", err)
	}

	if minLedger == 0 && maxLedger == 0 {
		return &EventStats{}, nil
	}

	totalLedgers := maxLedger - minLedger + 1
	ledgersPerWorker := totalLedgers / uint32(workers)
	if ledgersPerWorker == 0 {
		ledgersPerWorker = 1
		workers = int(totalLedgers)
	}

	type workerResult struct {
		stats     *EventStats
		contracts map[string]struct{}
		topic0s   map[string]struct{}
		topic1s   map[string]struct{}
		topic2s   map[string]struct{}
		topic3s   map[string]struct{}
		err       error
	}
	results := make(chan workerResult, workers)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		startLedger := minLedger + uint32(i)*ledgersPerWorker
		endLedger := startLedger + ledgersPerWorker - 1
		if i == workers-1 {
			endLedger = maxLedger
		}

		wg.Add(1)
		go func(start, end uint32) {
			defer wg.Done()
			result := rb.computeStatsForRange(start, end)
			results <- result
		}(startLedger, endLedger)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	finalStats := &EventStats{}
	contracts := make(map[string]struct{})
	topic0s := make(map[string]struct{})
	topic1s := make(map[string]struct{})
	topic2s := make(map[string]struct{})
	topic3s := make(map[string]struct{})

	for r := range results {
		if r.err != nil {
			return nil, r.err
		}

		finalStats.TotalEvents += r.stats.TotalEvents
		finalStats.ContractEvents += r.stats.ContractEvents
		finalStats.SystemEvents += r.stats.SystemEvents
		finalStats.DiagnosticEvents += r.stats.DiagnosticEvents

		for k := range r.contracts {
			contracts[k] = struct{}{}
		}
		for k := range r.topic0s {
			topic0s[k] = struct{}{}
		}
		for k := range r.topic1s {
			topic1s[k] = struct{}{}
		}
		for k := range r.topic2s {
			topic2s[k] = struct{}{}
		}
		for k := range r.topic3s {
			topic3s[k] = struct{}{}
		}
	}

	finalStats.UniqueContracts = len(contracts)
	finalStats.UniqueTopic0 = len(topic0s)
	finalStats.UniqueTopic1 = len(topic1s)
	finalStats.UniqueTopic2 = len(topic2s)
	finalStats.UniqueTopic3 = len(topic3s)

	return finalStats, nil
}

// computeEventStatsSingleThread is the single-threaded implementation
func (rb *RocksDBBackend) computeEventStatsSingleThread() (*EventStats, error) {
	stats := &EventStats{}

	contracts := make(map[string]struct{})
	topic0s := make(map[string]struct{})
	topic1s := make(map[string]struct{})
	topic2s := make(map[string]struct{})
	topic3s := make(map[string]struct{})

	it := rb.db.NewIteratorCF(rb.ro, rb.cfEvents)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		stats.TotalEvents++

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		switch xdrEvent.Type {
		case xdr.ContractEventTypeContract:
			stats.ContractEvents++
		case xdr.ContractEventTypeSystem:
			stats.SystemEvents++
		case xdr.ContractEventTypeDiagnostic:
			stats.DiagnosticEvents++
		}

		if xdrEvent.ContractId != nil {
			contractID := base64.StdEncoding.EncodeToString(xdrEvent.ContractId[:])
			contracts[contractID] = struct{}{}
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			for i, topic := range body.Topics {
				topicBytes, _ := topic.MarshalBinary()
				topicStr := base64.StdEncoding.EncodeToString(topicBytes)
				switch i {
				case 0:
					topic0s[topicStr] = struct{}{}
				case 1:
					topic1s[topicStr] = struct{}{}
				case 2:
					topic2s[topicStr] = struct{}{}
				case 3:
					topic3s[topicStr] = struct{}{}
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	stats.UniqueContracts = len(contracts)
	stats.UniqueTopic0 = len(topic0s)
	stats.UniqueTopic1 = len(topic1s)
	stats.UniqueTopic2 = len(topic2s)
	stats.UniqueTopic3 = len(topic3s)

	return stats, nil
}

// computeStatsForRange computes stats for a specific ledger range
func (rb *RocksDBBackend) computeStatsForRange(startLedger, endLedger uint32) struct {
	stats     *EventStats
	contracts map[string]struct{}
	topic0s   map[string]struct{}
	topic1s   map[string]struct{}
	topic2s   map[string]struct{}
	topic3s   map[string]struct{}
	err       error
} {
	result := struct {
		stats     *EventStats
		contracts map[string]struct{}
		topic0s   map[string]struct{}
		topic1s   map[string]struct{}
		topic2s   map[string]struct{}
		topic3s   map[string]struct{}
		err       error
	}{
		stats:     &EventStats{},
		contracts: make(map[string]struct{}),
		topic0s:   make(map[string]struct{}),
		topic1s:   make(map[string]struct{}),
		topic2s:   make(map[string]struct{}),
		topic3s:   make(map[string]struct{}),
	}

	startKey := EncodeKey(SegmentID(startLedger), 0)
	it := rb.db.NewIteratorCF(rb.ro, rb.cfEvents)
	defer it.Close()

	var currentSegID uint32 = ^uint32(0)
	var lm *SegmentLedgerOffsets

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 8 {
			break
		}

		segID, denseID := DecodeKey(key)
		if segID > SegmentID(endLedger) {
			break
		}

		// Load ledger offsets for new segment
		if segID != currentSegID {
			currentSegID = segID
			lm, _ = rb.bitmapStore.LoadSegmentLedgerOffsets(segID)
		}
		if lm == nil {
			continue
		}

		ledger, _ := lm.DenseIDToLedgerAndSeq(denseID)
		if ledger < startLedger {
			continue
		}
		if ledger > endLedger {
			break
		}

		result.stats.TotalEvents++

		// Strip header, unmarshal DiagnosticEvent XDR
		valueData := it.Value().Data()
		var xdrEvent xdr.ContractEvent
		if len(valueData) >= event.BinaryHeaderSize && valueData[0] == event.BinaryFormatVersion {
			var diagEvent xdr.DiagnosticEvent
			if err := diagEvent.UnmarshalBinary(valueData[event.BinaryHeaderSize:]); err != nil {
				continue
			}
			xdrEvent = diagEvent.Event
		} else {
			var diagEvent xdr.DiagnosticEvent
			if err := diagEvent.UnmarshalBinary(valueData); err != nil {
				continue
			}
			xdrEvent = diagEvent.Event
		}

		switch xdrEvent.Type {
		case xdr.ContractEventTypeContract:
			result.stats.ContractEvents++
		case xdr.ContractEventTypeSystem:
			result.stats.SystemEvents++
		case xdr.ContractEventTypeDiagnostic:
			result.stats.DiagnosticEvents++
		}

		if xdrEvent.ContractId != nil {
			contractID := base64.StdEncoding.EncodeToString(xdrEvent.ContractId[:])
			result.contracts[contractID] = struct{}{}
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			for i, topic := range body.Topics {
				topicBytes, _ := topic.MarshalBinary()
				topicStr := base64.StdEncoding.EncodeToString(topicBytes)
				switch i {
				case 0:
					result.topic0s[topicStr] = struct{}{}
				case 1:
					result.topic1s[topicStr] = struct{}{}
				case 2:
					result.topic2s[topicStr] = struct{}{}
				case 3:
					result.topic3s[topicStr] = struct{}{}
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		result.err = fmt.Errorf("iterator error: %w", err)
	}

	return result
}

// =============================================================================
// Bitmap Stats and Metadata
// =============================================================================

func (es *Store) GetBitmapStats() *BitmapStats {
	if es.indexStore == nil {
		return nil
	}
	count, cards, memBytes := es.indexStore.GetHotSegmentStats()
	return &BitmapStats{
		CurrentSegmentID:    es.indexStore.GetCurrentSegmentID(),
		HotSegmentCount:    count,
		HotSegmentCards:    cards,
		HotSegmentMemBytes: memBytes,
	}
}
