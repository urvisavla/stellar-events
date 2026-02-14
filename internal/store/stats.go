// Package store provides event storage interfaces and implementations.
package store

import (
	"container/heap"
	"encoding/base64"
	"sort"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
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
	CurrentBucketID    uint32 `json:"current_bucket_id"`
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

// ============================================================================
// Unified Query Result (for all index types)
// =============================================================================

// UnifiedQueryResult holds query results in a common format for all index types.
// This enables consistent output and comparison across posting-v2, bitmap32, and bitmap64.
type UnifiedQueryResult struct {
	// Index identification
	IndexType string // "posting-v2", "bitmap32", or "bitmap64"

	// Ledger range
	LedgerRange    uint32 // endLedger - startLedger + 1
	BucketsTouched int    // Number of buckets touched by the query
	IndexMatches   int    // Matches from index (ledgers, keys, or TOIDs depending on type)
	MatchUnitName  string // "ledgers", "event keys", or "TOIDs" - describes what IndexMatches counts

	// Event stats
	EventsScanned  int // Events scanned from storage
	EventsReturned int // Events returned after filtering

	// I/O stats
	IndexBytesRead int64 // Bytes read from index
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	IndexLookupTime   time.Duration // Time querying index
	EventFetchTime    time.Duration // Time fetching events from storage
	DecompressTime    time.Duration // Time decompressing event blobs
	EventDiskReadTime time.Duration // Time on event disk I/O (separate from decompress)
	DecodeTime        time.Duration // Time decoding/unmarshalling events
	FilterTime        time.Duration // Time filtering events (post-fetch)
	TotalTime         time.Duration // Total query time
}
