package ingest

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/urvisavla/stellar-events/internal/reader"
	"github.com/urvisavla/stellar-events/internal/store"
)

// PipelineConfig configures the parallel ingestion pipeline
type PipelineConfig struct {
	Workers             int    // Number of parallel workers
	BatchSize           int    // Ledgers to batch before writing
	QueueSize           int    // Channel buffer size
	DataDir             string // Ledger data directory
	NetworkPassphrase   string // Network passphrase for XDR parsing
	MaintainUniqueIdx   bool   // Maintain unique indexes during ingestion
	MaintainBitmapIdx   bool   // Maintain roaring bitmap indexes during ingestion
	MaintainL2Idx       bool   // Maintain L2 hierarchical indexes during ingestion
	BitmapFlushInterval int    // Ledgers between bitmap index flushes (0 = only at end)
}

// PipelineStats tracks pipeline performance
type PipelineStats struct {
	LedgersProcessed int64
	EventsExtracted  int64
	RawBytesTotal    int64 // Total raw event data bytes
	DiskReadTimeNs   int64 // Time reading from disk
	DecompressTimeNs int64 // Time decompressing zstd
	UnmarshalTimeNs  int64 // Time unmarshalling XDR
	WriteTimeNs      int64
}

// LedgerResult represents the result of processing a single ledger
type LedgerResult struct {
	Sequence       uint32
	Events         []*store.IngestEvent
	RawBytes       int64
	Stats          *LedgerStats
	DiskReadTime   time.Duration // Time reading from disk
	DecompressTime time.Duration // Time decompressing zstd
	UnmarshalTime  time.Duration // Time unmarshalling XDR
	Error          error
}

// Pipeline is a parallel ingestion pipeline
type Pipeline struct {
	config PipelineConfig
	stats  PipelineStats
	store  *store.RocksDBEventStore

	// Channels
	jobs    chan uint32        // Ledger sequences to process
	results chan *LedgerResult // Processed ledgers

	// Control
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopCh   chan struct{}

	// Callbacks
	onProgress func(ledger uint32, ledgersProcessed, eventsTotal int, stats *LedgerStats)
	onError    func(ledger uint32, err error)
}

// NewPipeline creates a new parallel ingestion pipeline
func NewPipeline(config PipelineConfig, store *store.RocksDBEventStore) *Pipeline {
	if config.Workers <= 0 {
		config.Workers = runtime.NumCPU()
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.QueueSize <= 0 {
		config.QueueSize = config.Workers * 2
	}

	return &Pipeline{
		config:  config,
		store:   store,
		jobs:    make(chan uint32, config.QueueSize),
		results: make(chan *LedgerResult, config.QueueSize),
		stopCh:  make(chan struct{}),
	}
}

// SetProgressCallback sets the progress callback
func (p *Pipeline) SetProgressCallback(fn func(ledger uint32, ledgersProcessed, eventsTotal int, stats *LedgerStats)) {
	p.onProgress = fn
}

// SetErrorCallback sets the error callback
func (p *Pipeline) SetErrorCallback(fn func(ledger uint32, err error)) {
	p.onError = fn
}

// Run executes the pipeline for the given ledger range
func (p *Pipeline) Run(startLedger, endLedger uint32) error {
	totalLedgers := int(endLedger - startLedger + 1)

	// Start worker goroutines
	for i := 0; i < p.config.Workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	// Start result collector
	collectDone := make(chan error, 1)
	go func() {
		collectDone <- p.collector(startLedger, endLedger, totalLedgers)
	}()

	// Feed jobs to workers
	go func() {
		defer close(p.jobs)
		for seq := startLedger; seq <= endLedger; seq++ {
			select {
			case p.jobs <- seq:
			case <-p.stopCh:
				return
			}
		}
	}()

	// Wait for workers to finish
	p.wg.Wait()
	close(p.results)

	// Wait for collector to finish
	return <-collectDone
}

// worker processes ledgers from the jobs channel
func (p *Pipeline) worker(id int) {
	defer p.wg.Done()

	// Each worker has its own ledger reader (with its own zstd decoder)
	ledgerReader, err := reader.NewLedgerReader(p.config.DataDir)
	if err != nil {
		// Send error result
		p.results <- &LedgerResult{Error: fmt.Errorf("worker %d: failed to create reader: %w", id, err)}
		return
	}
	defer ledgerReader.Close()

	// Process ledgers
	for seq := range p.jobs {
		result := p.processLedger(ledgerReader, seq)
		select {
		case p.results <- result:
		case <-p.stopCh:
			return
		}
	}
}

// processLedger reads and parses a single ledger
func (p *Pipeline) processLedger(ledgerReader *reader.LedgerReader, seq uint32) *LedgerResult {
	result := &LedgerResult{
		Sequence: seq,
		Stats:    NewLedgerStats(),
	}

	// Read ledger from disk (with timing breakdown)
	xdrBytes, timing, err := ledgerReader.GetLedgerWithTiming(seq)
	if timing != nil {
		result.DiskReadTime = timing.DiskRead
		result.DecompressTime = timing.Decompress
	}

	if err != nil {
		result.Error = fmt.Errorf("failed to read ledger %d: %w", seq, err)
		return result
	}

	// Parse XDR and extract events
	unmarshalStart := time.Now()
	events, err := ExtractEvents(xdrBytes, p.config.NetworkPassphrase, result.Stats)
	result.UnmarshalTime = time.Since(unmarshalStart)

	if err != nil {
		result.Error = fmt.Errorf("failed to extract events from ledger %d: %w", seq, err)
		return result
	}

	result.Events = events

	// Calculate raw bytes
	for _, e := range events {
		result.RawBytes += int64(len(e.RawXDR))
	}

	return result
}

// collector receives results and writes them to the store in order
func (p *Pipeline) collector(startLedger, endLedger uint32, _ int) error {
	// Buffer for out-of-order results
	pending := make(map[uint32]*LedgerResult)
	nextSeq := startLedger

	// Aggregated stats
	var ledgersProcessed, totalEvents int
	aggStats := NewLedgerStats()

	// Time-based progress tracking
	lastProgressTime := time.Now()

	// Event batch for writing
	var eventBatch []*store.IngestEvent
	var batchRawBytes int64
	batchStartSeq := startLedger

	for result := range p.results {
		if result.Error != nil {
			if p.onError != nil {
				p.onError(result.Sequence, result.Error)
			}
			return result.Error
		}

		// Buffer result
		pending[result.Sequence] = result

		// Update timing stats
		atomic.AddInt64(&p.stats.DiskReadTimeNs, result.DiskReadTime.Nanoseconds())
		atomic.AddInt64(&p.stats.DecompressTimeNs, result.DecompressTime.Nanoseconds())
		atomic.AddInt64(&p.stats.UnmarshalTimeNs, result.UnmarshalTime.Nanoseconds())

		// Process results in order
		for {
			r, ok := pending[nextSeq]
			if !ok {
				break
			}
			delete(pending, nextSeq)

			// Aggregate events
			eventBatch = append(eventBatch, r.Events...)
			batchRawBytes += r.RawBytes
			atomic.AddInt64(&p.stats.RawBytesTotal, r.RawBytes)

			// Aggregate stats
			aggStats.TotalLedgers += r.Stats.TotalLedgers
			aggStats.TotalTransactions += r.Stats.TotalTransactions
			aggStats.TotalEvents += r.Stats.TotalEvents
			aggStats.OperationEvents += r.Stats.OperationEvents
			aggStats.TransactionEvents += r.Stats.TransactionEvents

			ledgersProcessed++
			totalEvents += len(r.Events)
			atomic.AddInt64(&p.stats.LedgersProcessed, 1)
			atomic.AddInt64(&p.stats.EventsExtracted, int64(len(r.Events)))

			nextSeq++

			// Write batch when full or at end
			batchFull := ledgersProcessed%p.config.BatchSize == 0
			atEnd := nextSeq > endLedger

			if (batchFull || atEnd) && len(eventBatch) > 0 {
				writeStart := time.Now()
				_, err := p.store.StoreEvents(eventBatch, &store.StoreOptions{
					UniqueIndexes: p.config.MaintainUniqueIdx,
					BitmapIndexes: p.config.MaintainBitmapIdx,
					L2Indexes:     p.config.MaintainL2Idx,
				})
				atomic.AddInt64(&p.stats.WriteTimeNs, time.Since(writeStart).Nanoseconds())

				if err != nil {
					return fmt.Errorf("failed to store events for ledgers %d-%d: %w", batchStartSeq, nextSeq-1, err)
				}

				// Update last processed ledger
				if err := p.store.SetLastProcessedLedger(nextSeq - 1); err != nil {
					return fmt.Errorf("failed to update last processed ledger: %w", err)
				}

				// Fresh allocation to release underlying array memory
				eventBatch = make([]*store.IngestEvent, 0, p.config.BatchSize*100)
				batchRawBytes = 0
				batchStartSeq = nextSeq

				// Periodic bitmap flush to prevent hot segment memory growth
				if p.config.MaintainBitmapIdx && p.config.BitmapFlushInterval > 0 &&
					ledgersProcessed%p.config.BitmapFlushInterval == 0 {
					if err := p.store.FlushBitmapIndexes(); err != nil {
						return fmt.Errorf("failed to flush bitmap indexes: %w", err)
					}
				}
			}

			// Progress callback - time-based or every 1000 ledgers
			shouldReportProgress := ledgersProcessed%1000 == 0 ||
				time.Since(lastProgressTime) > 5*time.Second
			if p.onProgress != nil && shouldReportProgress {
				p.onProgress(nextSeq-1, ledgersProcessed, totalEvents, aggStats)
				lastProgressTime = time.Now()
			}
		}
	}

	// Flush bitmap indexes if enabled
	if p.config.MaintainBitmapIdx {
		if err := p.store.FlushBitmapIndexes(); err != nil {
			return fmt.Errorf("failed to flush bitmap indexes: %w", err)
		}
	}

	// Final progress callback
	if p.onProgress != nil {
		p.onProgress(nextSeq-1, ledgersProcessed, totalEvents, aggStats)
	}

	return nil
}

// Stop stops the pipeline
func (p *Pipeline) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
}

// GetStats returns the current pipeline stats
func (p *Pipeline) GetStats() PipelineStats {
	return PipelineStats{
		LedgersProcessed: atomic.LoadInt64(&p.stats.LedgersProcessed),
		EventsExtracted:  atomic.LoadInt64(&p.stats.EventsExtracted),
		RawBytesTotal:    atomic.LoadInt64(&p.stats.RawBytesTotal),
		DiskReadTimeNs:   atomic.LoadInt64(&p.stats.DiskReadTimeNs),
		DecompressTimeNs: atomic.LoadInt64(&p.stats.DecompressTimeNs),
		UnmarshalTimeNs:  atomic.LoadInt64(&p.stats.UnmarshalTimeNs),
		WriteTimeNs:      atomic.LoadInt64(&p.stats.WriteTimeNs),
	}
}

// GetRawBytesTotal returns the total raw event data bytes
func (p *Pipeline) GetRawBytesTotal() int64 {
	return atomic.LoadInt64(&p.stats.RawBytesTotal)
}

// GetDiskReadTime returns the total disk read time
func (p *Pipeline) GetDiskReadTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&p.stats.DiskReadTimeNs))
}

// GetDecompressTime returns the total decompression time
func (p *Pipeline) GetDecompressTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&p.stats.DecompressTimeNs))
}

// GetUnmarshalTime returns the total XDR unmarshal time
func (p *Pipeline) GetUnmarshalTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&p.stats.UnmarshalTimeNs))
}

// GetWriteTime returns the total write time
func (p *Pipeline) GetWriteTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&p.stats.WriteTimeNs))
}
