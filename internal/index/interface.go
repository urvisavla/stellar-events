package index

import "github.com/RoaringBitmap/roaring"

// =============================================================================
// IndexStore Interface
// =============================================================================

// Store provides indexed lookups for contract events.
// Implementations can be backed by RocksDB, Redis, memory, etc.
//
// The index supports two levels:
//   - L1 (Ledger-level): Maps contract/topic -> bitmap of ledger sequences
//   - L2 (Event-level): Maps contract/topic + ledger -> bitmap of event indices
type Store interface {
	// =========================================================================
	// Write Operations
	// =========================================================================

	// AddContractLedger adds a ledger to the L1 contract index.
	AddContractLedger(contractID []byte, ledger uint32) error

	// AddTopicLedger adds a ledger to the L1 topic index.
	AddTopicLedger(position int, topic []byte, ledger uint32) error

	// AddContractEvent adds an event to the L2 contract index.
	AddContractEvent(contractID []byte, ledger uint32, txIdx, opIdx, eventIdx uint16) error

	// AddTopicEvent adds an event to the L2 topic index.
	AddTopicEvent(position int, topic []byte, ledger uint32, txIdx, opIdx, eventIdx uint16) error

	// =========================================================================
	// Query Operations
	// =========================================================================

	// QueryLedgers returns a bitmap of ledgers matching the filter criteria.
	// All non-nil filter fields are ANDed together.
	// This is an L1 (ledger-level) query.
	QueryLedgers(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*roaring.Bitmap, error)

	// QueryEvents returns precise event keys matching the filter criteria.
	// This uses hierarchical L1+L2 lookup for efficient precise matching.
	// Returns event keys and the number of matching ledgers.
	QueryEvents(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error)

	// =========================================================================
	// Statistics
	// =========================================================================

	// GetStats returns statistics about the index.
	GetStats() *BitmapIndexStats

	// =========================================================================
	// Lifecycle
	// =========================================================================

	// Flush persists any buffered index data to storage.
	Flush() error

	// Close releases resources held by the index store.
	Close() error
}

// =============================================================================
// Write-Only Interface (for ingestion)
// =============================================================================

// Writer is a subset of Store for write-only operations during ingestion.
// This allows the ingestion pipeline to update indexes without query capability.
type Writer interface {
	// L1 index updates
	AddContractLedger(contractID []byte, ledger uint32) error
	AddTopicLedger(position int, topic []byte, ledger uint32) error

	// L2 index updates
	AddContractEvent(contractID []byte, ledger uint32, txIdx, opIdx, eventIdx uint16) error
	AddTopicEvent(position int, topic []byte, ledger uint32, txIdx, opIdx, eventIdx uint16) error

	// Lifecycle
	Flush() error
}

// =============================================================================
// Read-Only Interface (for queries)
// =============================================================================

// Reader is a subset of Store for read-only operations during queries.
type Reader interface {
	// QueryLedgers returns a bitmap of ledgers matching the filter criteria.
	QueryLedgers(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*roaring.Bitmap, error)

	// QueryEvents returns precise event keys matching the filter criteria.
	QueryEvents(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error)

	// GetStats returns statistics about the index.
	GetStats() *BitmapIndexStats
}
