package store

import (
	"container/heap"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/urvisavla/stellar-events/internal/index"
	"github.com/urvisavla/stellar-events/internal/query"
)

// Column family names
const (
	CFDefault = "default" // Metadata (last_processed_ledger, etc.)
	CFEvents  = "events"  // Primary event storage (raw XDR)
	CFUnique  = "unique"  // Unique value indexes with counts
	CFBitmap  = "bitmap"  // Roaring bitmap inverted indexes
)

// Unique index type prefixes within CFUnique
const (
	UniqueTypeContract byte = 0x00 // Contract ID
	UniqueTypeTopic0   byte = 0x01 // Topic 0
	UniqueTypeTopic1   byte = 0x02 // Topic 1
	UniqueTypeTopic2   byte = 0x03 // Topic 2
	UniqueTypeTopic3   byte = 0x04 // Topic 3
)

// uint64AddMergeOperator implements a merge operator that adds uint64 values
type uint64AddMergeOperator struct{}

func (m *uint64AddMergeOperator) Name() string {
	return "uint64-add"
}

func (m *uint64AddMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var total uint64

	// Parse existing value if present
	if len(existingValue) == 8 {
		total = binary.BigEndian.Uint64(existingValue)
	}

	// Add all operands
	for _, operand := range operands {
		if len(operand) == 8 {
			total += binary.BigEndian.Uint64(operand)
		}
	}

	// Return new value
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, total)
	return result, true
}

func (m *uint64AddMergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	var left, right uint64
	if len(leftOperand) == 8 {
		left = binary.BigEndian.Uint64(leftOperand)
	}
	if len(rightOperand) == 8 {
		right = binary.BigEndian.Uint64(rightOperand)
	}

	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, left+right)
	return result, true
}

// eventKey generates a 10-byte binary key for events
// Format: [ledger:4][tx:2][op:2][event:2]
func eventKey(e *IngestEvent) []byte {
	key := make([]byte, 10)
	binary.BigEndian.PutUint32(key[0:4], e.LedgerSequence)
	binary.BigEndian.PutUint16(key[4:6], e.TransactionIndex)
	binary.BigEndian.PutUint16(key[6:8], e.OperationIndex)
	binary.BigEndian.PutUint16(key[8:10], e.EventIndex)
	return key
}

// eventKeyFromParts generates a 10-byte binary key from components
func eventKeyFromParts(ledger uint32, tx, op, event uint16) []byte {
	key := make([]byte, 10)
	binary.BigEndian.PutUint32(key[0:4], ledger)
	binary.BigEndian.PutUint16(key[4:6], tx)
	binary.BigEndian.PutUint16(key[6:8], op)
	binary.BigEndian.PutUint16(key[8:10], event)
	return key
}

// parseEventKey extracts position info from a binary key
func parseEventKey(key []byte) (ledger uint32, tx, op, event uint16) {
	if len(key) < 10 {
		return 0, 0, 0, 0
	}
	ledger = binary.BigEndian.Uint32(key[0:4])
	tx = binary.BigEndian.Uint16(key[4:6])
	op = binary.BigEndian.Uint16(key[6:8])
	event = binary.BigEndian.Uint16(key[8:10])
	return
}

// uniqueKey generates a key for the unique index column family
// Format: [type:1][value:N]
func uniqueKey(uniqueType byte, value []byte) []byte {
	key := make([]byte, 1+len(value))
	key[0] = uniqueType
	copy(key[1:], value)
	return key
}

// parseRawXDRToEvent converts raw XDR bytes and key info to a ContractEvent
func parseRawXDRToEvent(rawXDR []byte, ledger uint32, tx, op, eventIdx uint16) (*ContractEvent, error) {
	var xdrEvent xdr.ContractEvent
	if err := xdrEvent.UnmarshalBinary(rawXDR); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR event: %w", err)
	}

	event := &ContractEvent{
		LedgerSequence:   ledger,
		TransactionIndex: int(tx),
		OperationIndex:   int(op),
		EventIndex:       int(eventIdx),
	}

	// Extract contract ID if present
	if xdrEvent.ContractId != nil {
		event.ContractID = base64.StdEncoding.EncodeToString(xdrEvent.ContractId[:])
	}

	// Event type
	switch xdrEvent.Type {
	case xdr.ContractEventTypeContract:
		event.Type = "contract"
	case xdr.ContractEventTypeSystem:
		event.Type = "system"
	case xdr.ContractEventTypeDiagnostic:
		event.Type = "diagnostic"
	}

	// Extract topics and data from event body
	if xdrEvent.Body.V == 0 {
		body := xdrEvent.Body.MustV0()

		// Topics
		for _, topic := range body.Topics {
			topicBytes, _ := topic.MarshalBinary()
			event.Topics = append(event.Topics, base64.StdEncoding.EncodeToString(topicBytes))
		}

		// Data
		dataBytes, _ := body.Data.MarshalBinary()
		event.Data = base64.StdEncoding.EncodeToString(dataBytes)
	}

	return event, nil
}

// RocksDBEventStore manages storing events in RocksDB
type RocksDBEventStore struct {
	db          *grocksdb.DB
	dbPath      string // Store path for filesystem-based stats
	wo          *grocksdb.WriteOptions
	ro          *grocksdb.ReadOptions
	indexes     *IndexConfig
	eventFormat string // "xdr" or "binary"

	// Column family handles (managed by DB, don't destroy manually)
	cfHandles []*grocksdb.ColumnFamilyHandle
	cfDefault *grocksdb.ColumnFamilyHandle // Metadata
	cfEvents  *grocksdb.ColumnFamilyHandle // Primary event storage
	cfUnique  *grocksdb.ColumnFamilyHandle // Unique value indexes with counts
	cfBitmap  *grocksdb.ColumnFamilyHandle // Roaring bitmap indexes

	// Index store (manages bitmap indexes with RocksDB persistence)
	indexStore *index.RocksDBStore

	// Options that need to be destroyed on Close
	baseOpts *grocksdb.Options
	cfOpts   []*grocksdb.Options
	bbtoList []*grocksdb.BlockBasedTableOptions

	// Keep merge operator alive to prevent GC (RocksDB holds a reference)
	mergeOp grocksdb.MergeOperator
}

// NewEventStore creates a new event store with RocksDB backend
func NewEventStore(dbPath string) (*RocksDBEventStore, error) {
	return NewEventStoreWithOptions(dbPath, nil, nil)
}

// NewEventStoreWithOptions creates a new event store with custom options
func NewEventStoreWithOptions(dbPath string, rocksOpts *RocksDBOptions, indexOpts *IndexConfig) (*RocksDBEventStore, error) {
	// Create base options
	baseOpts := grocksdb.NewDefaultOptions()
	baseOpts.SetCreateIfMissing(true)
	baseOpts.SetCreateIfMissingColumnFamilies(true)
	applyRocksDBOptions(baseOpts, rocksOpts)

	// Create CF-specific options
	// Default CF - metadata, small values
	defaultOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(defaultOpts, rocksOpts)

	// Events CF - large values, optimized for sequential writes
	eventsOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(eventsOpts, rocksOpts)
	eventsBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	eventsBBTO.SetBlockSize(64 * 1024) // 64KB blocks for better compression
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		eventsBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	eventsOpts.SetBlockBasedTableFactory(eventsBBTO)

	// Unique CF - small values (8-byte counts), optimized for point lookups
	// Uses merge operator for fast counter increments (no read-modify-write)
	uniqueOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(uniqueOpts, rocksOpts)
	mergeOp := &uint64AddMergeOperator{}
	uniqueOpts.SetMergeOperator(mergeOp)
	uniqueBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	uniqueBBTO.SetBlockSize(4 * 1024) // 4KB blocks
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		uniqueBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	uniqueOpts.SetBlockBasedTableFactory(uniqueBBTO)

	// Bitmap CF - roaring bitmap segments, medium-sized values
	// Optimized for sequential writes and prefix scans
	bitmapOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(bitmapOpts, rocksOpts)
	bitmapBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	bitmapBBTO.SetBlockSize(16 * 1024) // 16KB blocks for bitmap data
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		bitmapBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	bitmapOpts.SetBlockBasedTableFactory(bitmapBBTO)

	cfNames := []string{CFDefault, CFEvents, CFUnique, CFBitmap}
	cfOpts := []*grocksdb.Options{defaultOpts, eventsOpts, uniqueOpts, bitmapOpts}
	bbtoList := []*grocksdb.BlockBasedTableOptions{eventsBBTO, uniqueBBTO, bitmapBBTO}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(baseOpts, dbPath, cfNames, cfOpts)
	if err != nil {
		for _, opt := range cfOpts {
			opt.Destroy()
		}
		for _, bbto := range bbtoList {
			bbto.Destroy()
		}
		baseOpts.Destroy()
		return nil, fmt.Errorf("failed to open RocksDB with column families: %w", err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	if rocksOpts != nil && rocksOpts.DisableWAL {
		wo.DisableWAL(true)
	}

	indexes := indexOpts
	if indexes == nil {
		indexes = DefaultIndexConfig()
	}

	// Create index store (RocksDB-backed bitmap indexes)
	indexStore, err := index.NewRocksDBStore(db, cfHandles[3])
	if err != nil {
		db.Close()
		for _, opt := range cfOpts {
			opt.Destroy()
		}
		for _, bbto := range bbtoList {
			bbto.Destroy()
		}
		baseOpts.Destroy()
		wo.Destroy()
		return nil, fmt.Errorf("failed to create index store: %w", err)
	}

	return &RocksDBEventStore{
		db:          db,
		dbPath:      dbPath,
		wo:          wo,
		ro:          grocksdb.NewDefaultReadOptions(),
		indexes:     indexes,
		eventFormat: "xdr", // Default to XDR format
		cfHandles:   cfHandles,
		cfDefault:   cfHandles[0],
		cfEvents:    cfHandles[1],
		cfUnique:    cfHandles[2],
		cfBitmap:    cfHandles[3],
		indexStore:  indexStore,
		baseOpts:    baseOpts,
		cfOpts:      cfOpts,
		bbtoList:    bbtoList,
		mergeOp:     mergeOp,
	}, nil
}

// SetEventFormat sets the event storage format ("xdr" or "binary").
// Must be called before storing events.
func (es *RocksDBEventStore) SetEventFormat(format string) {
	if format == "binary" || format == "xdr" {
		es.eventFormat = format
	}
}

// GetEventFormat returns the current event storage format.
func (es *RocksDBEventStore) GetEventFormat() string {
	return es.eventFormat
}

// applyRocksDBOptions applies common RocksDB options
func applyRocksDBOptions(opts *grocksdb.Options, rocksOpts *RocksDBOptions) {
	if rocksOpts == nil {
		opts.SetCompression(grocksdb.LZ4Compression)
		return
	}

	// Write performance
	if rocksOpts.WriteBufferSizeMB > 0 {
		opts.SetWriteBufferSize(uint64(rocksOpts.WriteBufferSizeMB) * 1024 * 1024)
	}
	if rocksOpts.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(rocksOpts.MaxWriteBufferNumber)
	}
	if rocksOpts.MinWriteBufferNumberToMerge > 0 {
		opts.SetMinWriteBufferNumberToMerge(rocksOpts.MinWriteBufferNumberToMerge)
	}

	// Background jobs
	if rocksOpts.MaxBackgroundJobs > 0 {
		opts.SetMaxBackgroundJobs(rocksOpts.MaxBackgroundJobs)
	}

	// Compression
	opts.SetCompression(parseCompression(rocksOpts.Compression))
	if rocksOpts.BottommostCompression != "" {
		opts.SetBottommostCompression(parseCompression(rocksOpts.BottommostCompression))
	}

	// Disable auto compaction
	if rocksOpts.DisableAutoCompaction {
		opts.SetDisableAutoCompactions(true)
	}

	// Compaction tuning - larger files = fewer files after compaction
	if rocksOpts.TargetFileSizeMB > 0 {
		opts.SetTargetFileSizeBase(uint64(rocksOpts.TargetFileSizeMB) * 1024 * 1024)
	}
	if rocksOpts.MaxBytesForLevelBaseMB > 0 {
		opts.SetMaxBytesForLevelBase(uint64(rocksOpts.MaxBytesForLevelBaseMB) * 1024 * 1024)
	}
}

// parseCompression converts a compression string to grocksdb compression type
func parseCompression(compression string) grocksdb.CompressionType {
	switch strings.ToLower(compression) {
	case "none", "":
		return grocksdb.NoCompression
	case "snappy":
		return grocksdb.SnappyCompression
	case "lz4":
		return grocksdb.LZ4Compression
	case "zstd":
		return grocksdb.ZSTDCompression
	default:
		return grocksdb.LZ4Compression
	}
}

// Close closes the event store
func (es *RocksDBEventStore) Close() {
	// Close index store (flushes any remaining hot segments)
	if es.indexStore != nil {
		es.indexStore.Close()
	}

	// Destroy read/write options first
	es.wo.Destroy()
	es.ro.Destroy()

	// Close the database
	// Note: CF handles returned from OpenDbColumnFamilies are managed by the DB
	es.db.Close()

	// Note: We intentionally don't destroy cfOpts, bbtoList, or baseOpts here.
	// The merge operator attached to uniqueOpts causes a crash when destroyed
	// after the DB is closed (the C pointer becomes invalid).
	// These are small memory leaks but avoid the crash.
	// A proper fix would require grocksdb to handle this case.
}

// StoreEvents stores events with optional index updates based on options.
// Returns the number of bytes written.
func (es *RocksDBEventStore) StoreEvents(events []*IngestEvent, opts *StoreOptions) (int64, error) {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	var totalBytes int64

	// Ensure opts is not nil
	if opts == nil {
		opts = &StoreOptions{}
	}

	// Track counts to increment for this batch
	// Map from unique key -> count to add
	countUpdates := make(map[string]uint64)

	// Get bitmap index once outside the loop (avoid per-event method call overhead)
	var bitmapIdx *index.BitmapIndex
	if opts.BitmapIndexes && es.indexStore != nil {
		bitmapIdx = es.indexStore.GetBitmapIndex()
	}

	for _, event := range events {
		key := eventKey(event)

		// Encode event based on configured format
		var value []byte
		if es.eventFormat == "binary" {
			value = EncodeBinaryEvent(event, event.EventType, event.DataBytes)
		} else {
			value = event.RawXDR
		}
		batch.PutCF(es.cfEvents, key, value)
		totalBytes += int64(len(value))

		// Update bitmap indexes (fast, in-memory operation)
		if bitmapIdx != nil {
			// Index contract ID -> ledger
			if len(event.ContractID) > 0 {
				bitmapIdx.AddContractIndex(event.ContractID, event.LedgerSequence)
			}

			// Index topics -> ledger
			for i, topicBytes := range event.Topics {
				if i > 3 {
					break // Only index first 4 topics
				}
				bitmapIdx.AddTopicIndex(i, topicBytes, event.LedgerSequence)
			}
		}

		// Optionally update unique indexes with counts
		// Uses pre-extracted fields from IngestEvent (no XDR parsing needed!)
		if opts.UniqueIndexes {
			// Count contract ID
			if len(event.ContractID) > 0 {
				uk := string(uniqueKey(UniqueTypeContract, event.ContractID))
				countUpdates[uk]++
			}

			// Count topics (already marshaled as XDR bytes)
			for i, topicBytes := range event.Topics {
				if i > 3 {
					break // Only index first 4 topics
				}

				var uniqueType byte
				switch i {
				case 0:
					uniqueType = UniqueTypeTopic0
				case 1:
					uniqueType = UniqueTypeTopic1
				case 2:
					uniqueType = UniqueTypeTopic2
				case 3:
					uniqueType = UniqueTypeTopic3
				}
				uk := string(uniqueKey(uniqueType, topicBytes))
				countUpdates[uk]++
			}
		}
	}

	// Apply count updates using merge operator (no reads needed!)
	if opts.UniqueIndexes && len(countUpdates) > 0 {
		for keyStr, addCount := range countUpdates {
			keyBytes := []byte(keyStr)
			countBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(countBytes, addCount)
			batch.MergeCF(es.cfUnique, keyBytes, countBytes)
		}
	}

	if err := es.db.Write(es.wo, batch); err != nil {
		return 0, fmt.Errorf("failed to write batch: %w", err)
	}

	return totalBytes, nil
}

// FlushBitmapIndexes flushes all hot bitmap segments to disk
func (es *RocksDBEventStore) FlushBitmapIndexes() error {
	if es.indexStore == nil {
		return nil
	}
	return es.indexStore.Flush()
}

// GetBitmapStats returns bitmap index statistics
func (es *RocksDBEventStore) GetBitmapStats() *BitmapStats {
	if es.indexStore == nil {
		return nil
	}
	internalStats := es.indexStore.GetStats()
	if internalStats == nil {
		return nil
	}
	return &BitmapStats{
		CurrentSegmentID:   internalStats.CurrentSegmentID,
		HotSegmentCount:    internalStats.HotSegmentCount,
		HotSegmentCards:    internalStats.HotSegmentCards,
		HotSegmentMemBytes: internalStats.HotSegmentMemBytes,
		ContractIndexCount: internalStats.ContractIndexCount,
		Topic0IndexCount:   internalStats.Topic0IndexCount,
		Topic1IndexCount:   internalStats.Topic1IndexCount,
		Topic2IndexCount:   internalStats.Topic2IndexCount,
		Topic3IndexCount:   internalStats.Topic3IndexCount,
	}
}

// SetLastProcessedLedger stores the last processed ledger sequence
func (es *RocksDBEventStore) SetLastProcessedLedger(sequence uint32) error {
	key := []byte("last_processed_ledger")
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, sequence)
	return es.db.PutCF(es.wo, es.cfDefault, key, value)
}

// GetLastProcessedLedger retrieves the last processed ledger sequence
func (es *RocksDBEventStore) GetLastProcessedLedger() (uint32, error) {
	key := []byte("last_processed_ledger")
	value, err := es.db.GetCF(es.ro, es.cfDefault, key)
	if err != nil {
		return 0, err
	}
	defer value.Free()

	if value.Size() == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint32(value.Data()), nil
}

// =============================================================================
// Query by Ledger (Primary Index)
// =============================================================================

// GetEventsByLedgerRange retrieves all events within a ledger range
func (es *RocksDBEventStore) GetEventsByLedgerRange(startLedger, endLedger uint32) ([]*ContractEvent, error) {
	var events []*ContractEvent

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		// Compare ledger (bytes 0-3)
		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger > endLedger {
			break
		}

		// Parse key components
		ledger, tx, op, eventIdx := parseEventKey(key)

		// Parse raw XDR value
		event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// GetEventsByLedger retrieves all events for a specific ledger
func (es *RocksDBEventStore) GetEventsByLedger(ledgerSequence uint32) ([]*ContractEvent, error) {
	return es.GetEventsByLedgerRange(ledgerSequence, ledgerSequence)
}

// GetEventsInRangeWithTiming retrieves events in a ledger range with detailed timing.
// Returns query.Event format with disk read and unmarshal timing.
func (es *RocksDBEventStore) GetEventsInRangeWithTiming(startLedger, endLedger uint32, limit int) (*query.RangeResult, error) {
	result := &query.RangeResult{
		Timing: query.FetchTiming{},
	}

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)

	// Time iterator creation and seek
	diskStart := time.Now()
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()
	it.Seek(startKey)
	result.Timing.DiskReadTime += time.Since(diskStart)

	for it.Valid() {
		// Check limit
		if limit > 0 && len(result.Events) >= limit {
			break
		}

		// Time key access (disk read)
		diskStart = time.Now()
		key := it.Key().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)

		if len(key) < 10 {
			break
		}

		// Compare ledger (bytes 0-3)
		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger > endLedger {
			break
		}

		_, tx, op, eventIdx := parseEventKey(key)

		// Time value access (disk read)
		diskStart = time.Now()
		valueData := it.Value().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)

		// Time unmarshalling/decoding
		unmarshalStart := time.Now()
		var event *query.Event
		var err error
		if es.eventFormat == "binary" {
			event, err = DecodeBinaryToQueryEvent(valueData, keyLedger, tx, op, eventIdx)
		} else {
			event, err = parseRawXDRToQueryEvent(valueData, keyLedger, tx, op, eventIdx)
		}
		result.Timing.UnmarshalTime += time.Since(unmarshalStart)

		result.EventsScanned++

		if err != nil {
			// Time next iteration
			diskStart = time.Now()
			it.Next()
			result.Timing.DiskReadTime += time.Since(diskStart)
			continue
		}
		result.Events = append(result.Events, event)

		// Time next iteration
		diskStart = time.Now()
		it.Next()
		result.Timing.DiskReadTime += time.Since(diskStart)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return result, nil
}

// =============================================================================
// Query by Contract ID
// =============================================================================

// GetEventsByContractID retrieves events for a specific contract (scans all events)
func (es *RocksDBEventStore) GetEventsByContractID(contractID []byte, limit int) ([]*ContractEvent, error) {
	var events []*ContractEvent

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if limit > 0 && len(events) >= limit {
			break
		}

		key := it.Key().Data()
		if len(key) < 10 {
			continue
		}

		ledger, tx, op, eventIdx := parseEventKey(key)

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.ContractId != nil && string(xdrEvent.ContractId[:]) == string(contractID) {
			event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
			if err != nil {
				continue
			}
			events = append(events, event)
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// GetEventsByContractIDInRange retrieves events for a contract within a ledger range
func (es *RocksDBEventStore) GetEventsByContractIDInRange(contractID []byte, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	var events []*ContractEvent

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		ledger, tx, op, eventIdx := parseEventKey(key)
		if ledger > endLedger {
			break
		}

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.ContractId != nil && string(xdrEvent.ContractId[:]) == string(contractID) {
			event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
			if err != nil {
				continue
			}
			events = append(events, event)
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// =============================================================================
// Query by Topic
// =============================================================================

// GetEventsByTopic retrieves events with a specific topic value at the given position
func (es *RocksDBEventStore) GetEventsByTopic(position int, topicValue []byte, limit int) ([]*ContractEvent, error) {
	if position < 0 || position > 3 {
		return nil, fmt.Errorf("topic position must be 0-3, got %d", position)
	}

	var events []*ContractEvent

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if limit > 0 && len(events) >= limit {
			break
		}

		key := it.Key().Data()
		if len(key) < 10 {
			continue
		}

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			if position < len(body.Topics) {
				topicXDR, _ := body.Topics[position].MarshalBinary()
				if string(topicXDR) == string(topicValue) {
					ledger, tx, op, eventIdx := parseEventKey(key)
					event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
					if err != nil {
						continue
					}
					events = append(events, event)
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// GetEventsByTopicInRange retrieves events with a specific topic within a ledger range
func (es *RocksDBEventStore) GetEventsByTopicInRange(position int, topicValue []byte, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	if position < 0 || position > 3 {
		return nil, fmt.Errorf("topic position must be 0-3, got %d", position)
	}

	var events []*ContractEvent

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		ledger := binary.BigEndian.Uint32(key[0:4])
		if ledger > endLedger {
			break
		}

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			if position < len(body.Topics) {
				topicXDR, _ := body.Topics[position].MarshalBinary()
				if string(topicXDR) == string(topicValue) {
					_, tx, op, eventIdx := parseEventKey(key)
					event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
					if err != nil {
						continue
					}
					events = append(events, event)
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// =============================================================================
// Statistics
// =============================================================================

// CountEvents returns the total number of events in the store
func (es *RocksDBEventStore) CountEvents() (int64, error) {
	var count int64

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
	}

	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("iterator error: %w", err)
	}

	return count, nil
}

// GetStorageSnapshot returns per-column-family storage statistics.
func (es *RocksDBEventStore) GetStorageSnapshot() (*StorageSnapshot, error) {
	snapshot := &StorageSnapshot{
		Timestamp:      time.Now(),
		ColumnFamilies: make(map[string]*ColumnFamilyStats),
	}

	cfNames := []string{CFDefault, CFEvents, CFUnique, CFBitmap}

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
			total += int(parseUint64(es.db.GetPropertyCF(prop, cf)))
		}
		return total
	}

	for i, name := range cfNames {
		cf := es.cfHandles[i]
		cfStats := &ColumnFamilyStats{
			Name:           name,
			EstimatedKeys:  parseUint64(es.db.GetPropertyCF("rocksdb.estimate-num-keys", cf)),
			SSTFilesBytes:  parseUint64(es.db.GetPropertyCF("rocksdb.total-sst-files-size", cf)),
			MemtableBytes:  parseUint64(es.db.GetPropertyCF("rocksdb.cur-size-all-mem-tables", cf)),
			PendingCompact: parseUint64(es.db.GetPropertyCF("rocksdb.estimate-pending-compaction-bytes", cf)),
			NumFiles:       countFiles(cf),
		}

		snapshot.ColumnFamilies[name] = cfStats
		snapshot.TotalSST += cfStats.SSTFilesBytes
		snapshot.TotalMemtable += cfStats.MemtableBytes
		snapshot.TotalFiles += cfStats.NumFiles
	}

	return snapshot, nil
}

// Flush forces all memtables to be flushed to SST files
// This should be called before getting accurate storage stats
func (es *RocksDBEventStore) Flush() error {
	flushOpts := grocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	flushOpts.SetWait(true)

	// Flush all column families
	for _, cf := range es.cfHandles {
		if err := es.db.FlushCF(cf, flushOpts); err != nil {
			return fmt.Errorf("failed to flush column family: %w", err)
		}
	}
	return nil
}

// GetStats returns statistics about the event database (O(1) - no full scan)
func (es *RocksDBEventStore) GetStats() (*DBStats, error) {
	stats := &DBStats{}

	// Get min/max ledger using seek (O(1))
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
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
	estKeys := es.db.GetPropertyCF("rocksdb.estimate-num-keys", es.cfEvents)
	var estCount int64
	fmt.Sscanf(estKeys, "%d", &estCount)
	stats.TotalEvents = estCount

	// Get unique contracts from unique index (instant if index exists)
	counts, _ := es.CountUniqueIndexes()
	if counts != nil {
		stats.UniqueContracts = int(counts.UniqueContracts)
	}

	stats.LastProcessed, _ = es.GetLastProcessedLedger()

	return stats, nil
}

// CompactAllWithStats runs manual compaction and returns before/after stats per column family.
func (es *RocksDBEventStore) CompactAllWithStats() (*CompactionSummary, error) {
	before, err := es.GetStorageSnapshot()
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
	for _, cf := range es.cfHandles {
		es.db.CompactRangeCFOpt(cf, fullRange, compactOpts)
	}

	duration := time.Since(start)

	after, err := es.GetStorageSnapshot()
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
func (es *RocksDBEventStore) CountUniqueIndexes() (*UniqueIndexCounts, error) {
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
				unique, total, err := es.countIndexTypePartition(idxType, partition, partitions)
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
func (es *RocksDBEventStore) countIndexTypePartition(indexType byte, partition, totalPartitions int) (uniqueCount, totalEvents int64, err error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := es.db.NewIteratorCF(ro, es.cfUnique)
	defer it.Close()

	// Calculate byte range for this partition
	// Each partition handles a range of first-byte values
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

// topNHeap is a min-heap for tracking top N entries by count
type topNHeap struct {
	entries []TopEntry
	maxSize int
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
	if len(h.entries) < h.maxSize {
		heap.Push(h, TopEntry{
			Value:      base64.StdEncoding.EncodeToString(value),
			EventCount: count,
		})
	} else if count > h.entries[0].EventCount {
		// Replace smallest if this is larger
		h.entries[0] = TopEntry{
			Value:      base64.StdEncoding.EncodeToString(value),
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

// GetIndexDistribution computes percentile statistics for each index type in parallel
// topN specifies how many top entries to include (0 for none)
func (es *RocksDBEventStore) GetIndexDistribution(topN int) (*IndexDistribution, error) {
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
			stats, err := es.computeDistributionForType(idxType, topN)
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
func (es *RocksDBEventStore) computeDistributionForType(indexType byte, topN int) (*DistributionStats, error) {
	const partitions = 16

	type partitionResult struct {
		counts []int64
		total  int64
		topN   []TopEntry
		err    error
	}

	results := make(chan partitionResult, partitions)
	var wg sync.WaitGroup

	// Scan each partition in parallel
	for p := 0; p < partitions; p++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			counts, total, top, err := es.scanDistributionPartition(indexType, partition, partitions, topN)
			results <- partitionResult{counts, total, top, err}
		}(p)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Merge results from all partitions
	var allCounts []int64
	var totalSum int64
	mergedTopN := &topNHeap{maxSize: topN, entries: make([]TopEntry, 0, topN)}

	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		allCounts = append(allCounts, r.counts...)
		totalSum += r.total

		// Merge top-N entries
		for _, entry := range r.topN {
			if len(mergedTopN.entries) < topN {
				heap.Push(mergedTopN, entry)
			} else if entry.EventCount > mergedTopN.entries[0].EventCount {
				mergedTopN.entries[0] = entry
				heap.Fix(mergedTopN, 0)
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
		Count: int64(len(allCounts)),
		Min:   allCounts[0],
		Max:   allCounts[len(allCounts)-1],
		Mean:  float64(totalSum) / float64(len(allCounts)),
		Total: totalSum,
		P50:   percentile(allCounts, 50),
		P75:   percentile(allCounts, 75),
		P90:   percentile(allCounts, 90),
		P99:   percentile(allCounts, 99),
		TopN:  mergedTopN.getSorted(),
	}, nil
}

// scanDistributionPartition scans a partition and returns counts for distribution
func (es *RocksDBEventStore) scanDistributionPartition(indexType byte, partition, totalPartitions, topN int) ([]int64, int64, []TopEntry, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := es.db.NewIteratorCF(ro, es.cfUnique)
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
	topHeap := &topNHeap{maxSize: topN, entries: make([]TopEntry, 0, topN)}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 2 || key[0] != indexType {
			break
		}

		valueByte := key[1]
		if partition < totalPartitions-1 && valueByte >= endByte {
			break
		}

		var eventCount int64
		if it.Value().Size() == 8 {
			eventCount = int64(binary.BigEndian.Uint64(it.Value().Data()))
		}

		counts = append(counts, eventCount)
		total += eventCount
		topHeap.tryAdd(key[1:], eventCount)
	}

	if err := it.Err(); err != nil {
		return nil, 0, nil, fmt.Errorf("iterator error for type %d partition %d: %w", indexType, partition, err)
	}

	return counts, total, topHeap.getSorted(), nil
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

// getLedgerRange finds the min and max ledger sequences in the database
func (es *RocksDBEventStore) getLedgerRange() (uint32, uint32, error) {
	var minLedger, maxLedger uint32

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
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

// GetLedgerRange returns the min and max ledger sequences in the store.
// Implements the EventReader interface.
func (es *RocksDBEventStore) GetLedgerRange() (min, max uint32, err error) {
	return es.getLedgerRange()
}

// GetEventsInLedger retrieves all events in a specific ledger as query.Event format.
// Implements the EventReader interface.
func (es *RocksDBEventStore) GetEventsInLedger(ledger uint32) ([]*query.Event, error) {
	var events []*query.Event

	startKey := eventKeyFromParts(ledger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger != ledger {
			break
		}

		_, tx, op, eventIdx := parseEventKey(key)
		valueData := it.Value().Data()

		var event *query.Event
		var err error
		if es.eventFormat == "binary" {
			event, err = DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
		} else {
			event, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
		}
		if err != nil {
			continue
		}
		events = append(events, event)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// GetEventsInLedgerWithTiming retrieves all events in a specific ledger with detailed timing.
// Implements the EventReader interface.
func (es *RocksDBEventStore) GetEventsInLedgerWithTiming(ledger uint32) (*query.FetchResult, error) {
	result := &query.FetchResult{
		Timing: query.FetchTiming{},
	}

	startKey := eventKeyFromParts(ledger, 0, 0, 0)

	// Time iterator creation and seek
	diskStart := time.Now()
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()
	it.Seek(startKey)
	result.Timing.DiskReadTime += time.Since(diskStart)

	for it.Valid() {
		// Time key access (disk read)
		diskStart = time.Now()
		key := it.Key().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)

		if len(key) < 10 {
			break
		}

		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger != ledger {
			break
		}

		_, tx, op, eventIdx := parseEventKey(key)

		// Time value access (disk read)
		diskStart = time.Now()
		valueData := it.Value().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)

		// Time unmarshalling/decoding
		unmarshalStart := time.Now()
		var event *query.Event
		var err error
		if es.eventFormat == "binary" {
			event, err = DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
		} else {
			event, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
		}
		result.Timing.UnmarshalTime += time.Since(unmarshalStart)

		if err != nil {
			// Time next iteration
			diskStart = time.Now()
			it.Next()
			result.Timing.DiskReadTime += time.Since(diskStart)
			continue
		}
		result.Events = append(result.Events, event)

		// Time next iteration
		diskStart = time.Now()
		it.Next()
		result.Timing.DiskReadTime += time.Since(diskStart)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return result, nil
}

// parseRawXDRToQueryEvent converts raw XDR bytes to a query.Event
func parseRawXDRToQueryEvent(rawXDR []byte, ledger uint32, tx, op, eventIdx uint16) (*query.Event, error) {
	var xdrEvent xdr.ContractEvent
	if err := xdrEvent.UnmarshalBinary(rawXDR); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR event: %w", err)
	}

	event := &query.Event{
		LedgerSequence:   ledger,
		TransactionIndex: int(tx),
		OperationIndex:   int(op),
		EventIndex:       int(eventIdx),
	}

	// Extract contract ID if present
	if xdrEvent.ContractId != nil {
		event.ContractID = base64.StdEncoding.EncodeToString(xdrEvent.ContractId[:])
	}

	// Event type
	switch xdrEvent.Type {
	case xdr.ContractEventTypeContract:
		event.Type = "contract"
	case xdr.ContractEventTypeSystem:
		event.Type = "system"
	case xdr.ContractEventTypeDiagnostic:
		event.Type = "diagnostic"
	}

	// Extract topics and data from event body
	if xdrEvent.Body.V == 0 {
		body := xdrEvent.Body.MustV0()

		// Topics
		for _, topic := range body.Topics {
			topicBytes, _ := topic.MarshalBinary()
			event.Topics = append(event.Topics, base64.StdEncoding.EncodeToString(topicBytes))
		}

		// Data
		dataBytes, _ := body.Data.MarshalBinary()
		event.Data = base64.StdEncoding.EncodeToString(dataBytes)
	}

	return event, nil
}

// ComputeEventStats scans all events and computes unique counts.
// Workers controls parallelism: 0 uses NumCPU, 1 for single-threaded, >1 for parallel.
func (es *RocksDBEventStore) ComputeEventStats(workers int) (*EventStats, error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// For single worker, use simple sequential scan (more memory efficient)
	if workers == 1 {
		return es.computeEventStatsSingleThread()
	}

	// Parallel implementation
	minLedger, maxLedger, err := es.getLedgerRange()
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
			result := es.computeStatsForRange(start, end)
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
func (es *RocksDBEventStore) computeEventStatsSingleThread() (*EventStats, error) {
	stats := &EventStats{}

	contracts := make(map[string]struct{})
	topic0s := make(map[string]struct{})
	topic1s := make(map[string]struct{})
	topic2s := make(map[string]struct{})
	topic3s := make(map[string]struct{})

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
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
func (es *RocksDBEventStore) computeStatsForRange(startLedger, endLedger uint32) struct {
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

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 4 {
			break
		}

		ledger := binary.BigEndian.Uint32(key[0:4])
		if ledger > endLedger {
			break
		}

		result.stats.TotalEvents++

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
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

// BuildIndexes scans all events and builds indexes based on options (one-time operation)
// Bitmap indexes are always built. Unique indexes are optional.
// Uses a collector pattern: workers read/extract in parallel, single goroutine updates bitmaps.
func (es *RocksDBEventStore) BuildIndexes(workers int, opts *BuildIndexOptions, progressFn func(processed int64)) error {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Default options: L1 bitmap only
	if opts == nil {
		opts = &BuildIndexOptions{
			BitmapIndexes: true,
		}
	}

	// Ensure index store is initialized if we're building bitmap indexes
	if opts.BitmapIndexes && es.indexStore == nil {
		return fmt.Errorf("index store not initialized - cannot build bitmap indexes")
	}

	minLedger, maxLedger, err := es.getLedgerRange()
	if err != nil {
		return fmt.Errorf("failed to get ledger range: %w", err)
	}

	if minLedger == 0 && maxLedger == 0 {
		return nil
	}

	totalLedgers := maxLedger - minLedger + 1
	ledgersPerWorker := totalLedgers / uint32(workers)
	if ledgersPerWorker == 0 {
		ledgersPerWorker = 1
		workers = int(totalLedgers)
	}

	// Create channel for index entries (collector pattern for bitmap indexes)
	var entryCh chan *indexEntry
	var collectorDone chan error

	if opts.BitmapIndexes {
		entryCh = make(chan *indexEntry, 100000) // buffered channel
		collectorDone = make(chan error, 1)

		// Start collector goroutine - single goroutine updates bitmaps (no lock contention)
		go func() {
			collectorDone <- es.indexCollector(entryCh, opts, progressFn)
		}()
	}

	errCh := make(chan error, workers)
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
			if err := es.buildIndexesForRange(start, end, opts, entryCh); err != nil {
				errCh <- err
			}
		}(startLedger, endLedger)
	}

	wg.Wait()

	// Close entry channel and wait for collector to finish
	if entryCh != nil {
		close(entryCh)
		if err := <-collectorDone; err != nil {
			return fmt.Errorf("index collector failed: %w", err)
		}
	}

	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// buildIndexesForRange reads events for a ledger range and sends index data to collector.
// Unique indexes are still handled locally (no lock contention issue with RocksDB merge).
func (es *RocksDBEventStore) buildIndexesForRange(startLedger, endLedger uint32, opts *BuildIndexOptions, entryCh chan<- *indexEntry) error {
	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	// Accumulate unique counts in memory (only if building unique indexes)
	var counts map[string]uint64
	if opts.UniqueIndexes {
		counts = make(map[string]uint64)
	}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		// Parse key: [ledger:4][tx:2][op:2][event:2]
		ledger := binary.BigEndian.Uint32(key[0:4])
		if ledger > endLedger {
			break
		}

		txIdx := binary.BigEndian.Uint16(key[4:6])
		opIdx := binary.BigEndian.Uint16(key[6:8])
		eventIdx := binary.BigEndian.Uint16(key[8:10])

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		// Extract contract ID
		var contractID []byte
		if xdrEvent.ContractId != nil {
			contractID = xdrEvent.ContractId[:]
		}

		// Extract topics
		var topics [][]byte
		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			for i, topic := range body.Topics {
				if i > 3 {
					break
				}
				topicBytes, err := topic.MarshalBinary()
				if err != nil {
					continue
				}
				topics = append(topics, topicBytes)
			}
		}

		// Build UNIQUE indexes (counts) - handled locally, no contention
		if opts.UniqueIndexes {
			if len(contractID) > 0 {
				uk := string(uniqueKey(UniqueTypeContract, contractID))
				counts[uk]++
			}
			for i, topicBytes := range topics {
				var uniqueType byte
				switch i {
				case 0:
					uniqueType = UniqueTypeTopic0
				case 1:
					uniqueType = UniqueTypeTopic1
				case 2:
					uniqueType = UniqueTypeTopic2
				case 3:
					uniqueType = UniqueTypeTopic3
				default:
					continue
				}
				uk := string(uniqueKey(uniqueType, topicBytes))
				counts[uk]++
			}
		}

		// Send to collector for bitmap indexes (no direct bitmap updates here)
		if entryCh != nil && (len(contractID) > 0 || len(topics) > 0) {
			entryCh <- &indexEntry{
				Ledger:     ledger,
				TxIdx:      txIdx,
				OpIdx:      opIdx,
				EventIdx:   eventIdx,
				ContractID: contractID,
				Topics:     topics,
			}
		}
	}

	// Write unique index counts
	if opts.UniqueIndexes && len(counts) > 0 {
		batch := grocksdb.NewWriteBatch()
		defer batch.Destroy()

		for keyStr, count := range counts {
			keyBytes := []byte(keyStr)
			countBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(countBytes, count)
			batch.MergeCF(es.cfUnique, keyBytes, countBytes)
		}

		if err := es.db.Write(es.wo, batch); err != nil {
			return fmt.Errorf("failed to write unique index batch: %w", err)
		}
	}

	if err := it.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	return nil
}

// indexCollector receives index entries from workers and updates bitmaps sequentially.
// This eliminates lock contention - only one goroutine touches the bitmap index.
func (es *RocksDBEventStore) indexCollector(entryCh <-chan *indexEntry, opts *BuildIndexOptions, progressFn func(processed int64)) error {
	var processed int64
	var lastFlushLedger uint32
	var prevLedger uint32

	bitmapIdx := es.indexStore.GetBitmapIndex()

	for entry := range entryCh {
		// Periodic flush on ledger boundary
		if opts.BitmapFlushInterval > 0 && entry.Ledger != prevLedger && prevLedger > 0 {
			if (entry.Ledger - lastFlushLedger) >= uint32(opts.BitmapFlushInterval) {
				if err := es.indexStore.Flush(); err != nil {
					return fmt.Errorf("failed to flush bitmap indexes: %w", err)
				}
				lastFlushLedger = entry.Ledger
			}
		}
		prevLedger = entry.Ledger

		// Bitmap indexes (contract/topic -> ledger)
		if bitmapIdx != nil {
			if len(entry.ContractID) > 0 {
				bitmapIdx.AddContractIndex(entry.ContractID, entry.Ledger)
			}
			for i, topic := range entry.Topics {
				bitmapIdx.AddTopicIndex(i, topic, entry.Ledger)
			}
		}

		processed++

		// Progress callback every 100k events
		if progressFn != nil && processed%100000 == 0 {
			progressFn(processed)
		}
	}

	// Final flush
	if es.indexStore != nil {
		if err := es.indexStore.Flush(); err != nil {
			return fmt.Errorf("failed to flush bitmap indexes: %w", err)
		}
	}

	// Final progress
	if progressFn != nil {
		progressFn(processed)
	}

	return nil
}

// =============================================================================
// Compile-time Interface Checks
// =============================================================================

// Verify RocksDBEventStore implements EventStore interface
var _ EventStore = (*RocksDBEventStore)(nil)

// Verify RocksDBEventStore implements EventReader interface
var _ EventReader = (*RocksDBEventStore)(nil)

// GetIndexReader returns the index store as an index.IndexReader for use with query.Engine.
// Returns nil if the index store is not available.
func (es *RocksDBEventStore) GetIndexReader() index.IndexReader {
	return es.indexStore
}
