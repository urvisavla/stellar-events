package index

import "github.com/RoaringBitmap/roaring"

// =============================================================================
// IndexStore Interface
// =============================================================================

// Store provides indexed lookups for contract events.
// Implementations can be backed by RocksDB, Redis, memory, etc.
//
// The index maps contract/topic -> bitmap of ledger sequences.
// Event-level filtering is done by scanning events in matching ledgers.
type Store interface {
	// =========================================================================
	// Write Operations
	// =========================================================================

	// AddContractLedger adds a ledger to the contract index.
	AddContractLedger(contractID []byte, ledger uint32) error

	// AddTopicLedger adds a ledger to the topic index.
	AddTopicLedger(position int, topic []byte, ledger uint32) error

	// =========================================================================
	// Query Operations
	// =========================================================================

	// QueryLedgers returns a bitmap of ledgers matching the filter criteria.
	// All non-nil filter fields are ANDed together.
	QueryLedgers(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*roaring.Bitmap, error)

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
	// Index updates
	AddContractLedger(contractID []byte, ledger uint32) error
	AddTopicLedger(position int, topic []byte, ledger uint32) error

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

	// GetStats returns statistics about the index.
	GetStats() *BitmapIndexStats
}
