package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/urvisavla/stellar-events/internal/config"
	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/ingest"
	"github.com/urvisavla/stellar-events/internal/progress"
	"github.com/urvisavla/stellar-events/internal/store"
)

// =============================================================================
// Ingest Command — hot segment path (write per-ledger, convert to cold)
// =============================================================================

func runIngest(cfg *config.Config, args []string) {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	start := fs.Uint("start", 0, "Start ledger (0 = default/source min)")
	end := fs.Uint("end", 0, "End ledger (0 = auto-detect max)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: ingest [options]\n\n")
		fmt.Fprintf(os.Stderr, "Ingests events via hot append-only files, converting to cold on segment boundaries.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --start <ledger>   Start ledger (default: %d)\n", ingest.FirstLedgerSequence)
		fmt.Fprintf(os.Stderr, "  --end <ledger>     End ledger (0 = auto-detect max)\n\n")
		fmt.Fprintf(os.Stderr, "Parallelism configured in config.toml [ingestion]\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	cmdIngest(cfg, uint32(*start), uint32(*end))
}

func cmdIngest(cfg *config.Config, startLedger, endLedger uint32) {
	fmt := message.NewPrinter(language.English)

	// Resolve ledger range
	if startLedger == 0 {
		startLedger = ingest.FirstLedgerSequence
	}

	if endLedger == 0 {
		minLedger, maxLedger, _, err := ingest.GetLedgerDataStats(cfg.Source.LedgerDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get source stats: %v\n", err)
			os.Exit(1)
		}
		endLedger = maxLedger
		fmt.Fprintf(os.Stderr, "Source ledger range: %d - %d\n", minLedger, maxLedger)
	}

	// Validate
	if startLedger < ingest.FirstLedgerSequence {
		fmt.Fprintf(os.Stderr, "Error: start ledger must be >= %d\n", ingest.FirstLedgerSequence)
		os.Exit(2)
	}
	if endLedger < startLedger {
		fmt.Fprintf(os.Stderr, "Error: end ledger must be >= start ledger\n")
		os.Exit(2)
	}

	segmentPath := cfg.Storage.SegmentPath
	if segmentPath == "" {
		fmt.Fprintf(os.Stderr, "Error: storage.segment_path is required for ingest command\n")
		os.Exit(2)
	}

	// Create an IndexStore for in-memory bitmap tracking (segment-file-only mode, no RocksDB).
	flusher := &store.SegmentIndexFlusher{}
	indexStore := store.NewIndexStore(flusher, nil, nil)
	coldPath := filepath.Join(segmentPath, "cold")
	indexStore.SetWriteConfig(coldPath, false)
	defer indexStore.Close()

	// Create a SegmentDataWriter for cold output (events.pack)
	sdw := store.NewSegmentDataWriter(coldPath, cfg.Storage.CompressData, cfg.Storage.BlockSize)

	networkPassphrase := cfg.GetNetworkPassphrase()

	// Config-only parallelism settings (with safe defaults)
	workers := cfg.Ingestion.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	batchSize := cfg.Ingestion.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	queueSize := cfg.Ingestion.QueueSize
	if queueSize <= 0 {
		queueSize = workers * 2
	}

	// Start pprof HTTP endpoint for heap profiling during ingestion
	go func() {
		fmt.Fprintf(os.Stderr, "pprof listening on localhost:6060\n")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			fmt.Fprintf(os.Stderr, "pprof server failed: %v\n", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "Ingesting events (hot→cold) from ledgers %d to %d...\n", startLedger, endLedger)
	fmt.Fprintf(os.Stderr, "Parallel mode: %d workers, batch size %d, queue size %d\n", workers, batchSize, queueSize)
	fmt.Fprintf(os.Stderr, "Segment path: %s (hot/ → cold/)\n", segmentPath)

	pipelineConfig := ingest.PipelineConfig{
		Workers:           workers,
		BatchSize:         batchSize,
		QueueSize:         queueSize,
		DataDir:           cfg.Source.LedgerDir,
		NetworkPassphrase: networkPassphrase,
	}

	// We use a custom collector instead of the pipeline's built-in Store path.
	// Create pipeline with a nil store — we'll handle writing in a custom collector.
	// Actually, we need to use the pipeline's parallel reader but handle the writes ourselves.
	// So we replicate the pipeline pattern with direct control over the hot writer.

	startTime := time.Now()

	var totalLedgers, totalEvents int64
	var diskReadTimeNs, decompressTimeNs, unmarshalTimeNs, writeTimeNs int64
	var fsyncTimeNs int64
	var rawBytesTotal int64

	// Use the pipeline's worker pool for parallel reading, with a custom collector
	// that writes to hot segments instead of Store.StoreEvents().
	jobs := make(chan uint32, queueSize)
	results := make(chan *ingest.LedgerResult, queueSize)

	// Start worker goroutines
	var workersDone int32
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer func() {
				if atomic.AddInt32(&workersDone, 1) == int32(workers) {
					close(results)
				}
			}()

			ledgerReader, err := ingest.NewLedgerReader(pipelineConfig.DataDir)
			if err != nil {
				results <- &ingest.LedgerResult{Error: fmtErr("worker %d: failed to create reader: %v", id, err)}
				return
			}
			defer ledgerReader.Close()

			for seq := range jobs {
				result := processLedger(ledgerReader, seq, networkPassphrase)
				results <- result
			}
		}(i)
	}

	// Feed jobs
	go func() {
		defer close(jobs)
		for seq := startLedger; seq <= endLedger; seq++ {
			jobs <- seq
		}
	}()

	// Collect results in order and write to hot segment
	pending := make(map[uint32]*ingest.LedgerResult)
	nextSeq := startLedger

	var hotWriter *store.HotSegmentWriter
	var currentSegID uint32
	var hasCurrentSeg bool

	var eventBatch []*event.IngestEvent
	batchStartSeq := startLedger
	var ledgersProcessed int

	// Progress tracking
	var progressWriter *progress.Writer
	if cfg.Ingestion.ProgressFile != "" {
		progressWriter = progress.NewWriter(cfg.Ingestion.ProgressFile, startLedger, endLedger)
		fmt.Fprintf(os.Stderr, "Progress file: %s\n", cfg.Ingestion.ProgressFile)
	}

	var pipelineErr error

	for result := range results {
		if result.Error != nil {
			fmt.Fprintf(os.Stderr, "Error processing ledger %d: %v\n", result.Sequence, result.Error)
			pipelineErr = result.Error
			break
		}

		pending[result.Sequence] = result

		atomic.AddInt64(&diskReadTimeNs, result.DiskReadTime.Nanoseconds())
		atomic.AddInt64(&decompressTimeNs, result.DecompressTime.Nanoseconds())
		atomic.AddInt64(&unmarshalTimeNs, result.UnmarshalTime.Nanoseconds())

		for {
			r, ok := pending[nextSeq]
			if !ok {
				break
			}
			delete(pending, nextSeq)

			segID := store.SegmentID(r.Sequence)

			// Segment boundary: flush pending batch to current hot writer, then convert to cold
			if hasCurrentSeg && segID != currentSegID {
				if hotWriter != nil {
					// Flush any pending events from the completed segment before conversion
					if len(eventBatch) > 0 {
						ledgerEvents := groupEventsByLedger(eventBatch)
						for _, le := range ledgerEvents {
							writeStart := time.Now()
							if err := hotWriter.WriteLedger(le, indexStore); err != nil {
								pipelineErr = fmtErr("write ledger to hot segment: %v", err)
								break
							}
							atomic.AddInt64(&writeTimeNs, time.Since(writeStart).Nanoseconds())
							fsyncStart := time.Now()
							if err := hotWriter.Fsync(); err != nil {
								pipelineErr = fmtErr("fsync hot segment: %v", err)
								break
							}
							atomic.AddInt64(&fsyncTimeNs, time.Since(fsyncStart).Nanoseconds())
						}
						if pipelineErr != nil {
							break
						}
						eventBatch = eventBatch[:0]
					}
					freezeStats, err := hotWriter.ConvertToCold(indexStore, sdw)
					if err != nil {
						pipelineErr = fmtErr("convert segment %d to cold: %v", currentSegID, err)
						break
					}
					if freezeStats != nil && progressWriter != nil {
						progressWriter.RecordFreeze(*freezeStats)
					}
					hotWriter = nil
				}
			}

			// Open new hot writer if needed
			if hotWriter == nil || segID != currentSegID {
				var err error
				hotWriter, err = store.NewHotSegmentWriter(segmentPath, segID)
				if err != nil {
					pipelineErr = fmtErr("open hot segment %d: %v", segID, err)
					break
				}
				currentSegID = segID
				hasCurrentSeg = true
			}

			eventBatch = append(eventBatch, r.Events...)
			atomic.AddInt64(&rawBytesTotal, r.RawBytes)

			ledgersProcessed++
			atomic.AddInt64(&totalLedgers, 1)
			atomic.AddInt64(&totalEvents, int64(len(r.Events)))

			nextSeq++

			// Write batch when full or at end
			batchFull := ledgersProcessed%batchSize == 0
			atEnd := nextSeq > endLedger

			if (batchFull || atEnd) && len(eventBatch) > 0 {
				// Group events by ledger and write each ledger to hot segment
				ledgerEvents := groupEventsByLedger(eventBatch)
				for _, le := range ledgerEvents {
					writeStart := time.Now()
					if err := hotWriter.WriteLedger(le, indexStore); err != nil {
						pipelineErr = fmtErr("write ledger to hot segment: %v", err)
						break
					}
					atomic.AddInt64(&writeTimeNs, time.Since(writeStart).Nanoseconds())
					fsyncStart := time.Now()
					if err := hotWriter.Fsync(); err != nil {
						pipelineErr = fmtErr("fsync hot segment: %v", err)
						break
					}
					atomic.AddInt64(&fsyncTimeNs, time.Since(fsyncStart).Nanoseconds())
				}
				if pipelineErr != nil {
					break
				}

				eventBatch = eventBatch[:0]
				batchStartSeq = nextSeq
			}
			_ = batchStartSeq

			// Progress callback every 1000 ledgers
			if ledgersProcessed%1000 == 0 {
				fmt.Fprintf(os.Stderr, "Processed %d ledgers, %d events (ledger %d)...\n",
					ledgersProcessed, atomic.LoadInt64(&totalEvents), nextSeq-1)
				if progressWriter != nil {
					_ = progressWriter.Update(nextSeq-1, ledgersProcessed, int(atomic.LoadInt64(&totalEvents)))
				}
			}
		}

		if pipelineErr != nil {
			break
		}
	}

	if pipelineErr != nil {
		if hotWriter != nil {
			hotWriter.Close()
		}
		if progressWriter != nil {
			_ = progressWriter.Failed(nextSeq-1, ledgersProcessed, int(atomic.LoadInt64(&totalEvents)), pipelineErr)
		}
		fmt.Fprintf(os.Stderr, "Pipeline failed: %v\n", pipelineErr)
		os.Exit(1)
	}

	// Finalize the last hot segment
	if hotWriter != nil {
		if err := hotWriter.Fsync(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to fsync last hot segment: %v\n", err)
		}
		freezeStats, err := hotWriter.ConvertToCold(indexStore, sdw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to convert last segment to cold: %v\n", err)
		}
		if freezeStats != nil && progressWriter != nil {
			progressWriter.RecordFreeze(*freezeStats)
		}
		hotWriter = nil
	}

	// Final progress
	fmt.Fprintf(os.Stderr, "Processed %d ledgers, %d events (ledger %d)...\n",
		ledgersProcessed, atomic.LoadInt64(&totalEvents), nextSeq-1)

	if progressWriter != nil {
		if err := progressWriter.Complete(ledgersProcessed, int(atomic.LoadInt64(&totalEvents))); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to write final progress: %v\n", err)
		}
	}

	ingestionElapsed := time.Since(startTime)

	// =========================================================================
	// Ingestion Summary
	// =========================================================================

	ledgerCount := int(atomic.LoadInt64(&totalLedgers))
	eventCount := int(atomic.LoadInt64(&totalEvents))
	rawBytes := atomic.LoadInt64(&rawBytesTotal)

	diskReadTime := time.Duration(atomic.LoadInt64(&diskReadTimeNs))
	decompressTime := time.Duration(atomic.LoadInt64(&decompressTimeNs))
	unmarshalTime := time.Duration(atomic.LoadInt64(&unmarshalTimeNs))
	writeTime := time.Duration(atomic.LoadInt64(&writeTimeNs))
	fsyncTime := time.Duration(atomic.LoadInt64(&fsyncTimeNs))

	var summary strings.Builder
	rawDataMB := float64(rawBytes) / (1024 * 1024)

	summary.WriteString("\n")
	summary.WriteString("=== Ingestion Complete (hot→cold) ===\n")
	summary.WriteString("\n")
	summary.WriteString(fmt.Sprintf("  Ledgers processed:       %d\n", ledgerCount))
	summary.WriteString(fmt.Sprintf("  Events ingested:         %d\n", eventCount))

	if ingestionElapsed.Seconds() > 0 {
		ledgersPerSec := float64(ledgerCount) / ingestionElapsed.Seconds()
		eventsPerSec := float64(eventCount) / ingestionElapsed.Seconds()
		summary.WriteString(fmt.Sprintf("  Avg ledgers/sec:         %.0f\n", ledgersPerSec))
		summary.WriteString(fmt.Sprintf("  Avg events/sec:          %.0f\n", eventsPerSec))
	}

	totalWorkerTime := diskReadTime + decompressTime + unmarshalTime + writeTime + fsyncTime
	summary.WriteString("\n")
	summary.WriteString("Ingestion Time Breakdown:\n")
	summary.WriteString(fmt.Sprintf("  Wall clock time:         %s\n", formatElapsed(ingestionElapsed)))
	if totalWorkerTime > 0 {
		summary.WriteString(fmt.Sprintf("  Disk read:               %s (%.1f%%)\n", formatElapsed(diskReadTime), float64(diskReadTime)/float64(totalWorkerTime)*100))
		summary.WriteString(fmt.Sprintf("  Decompress (zstd):       %s (%.1f%%)\n", formatElapsed(decompressTime), float64(decompressTime)/float64(totalWorkerTime)*100))
		summary.WriteString(fmt.Sprintf("  XDR unmarshal:           %s (%.1f%%)\n", formatElapsed(unmarshalTime), float64(unmarshalTime)/float64(totalWorkerTime)*100))
		summary.WriteString(fmt.Sprintf("  Write (hot):             %s (%.1f%%)\n", formatElapsed(writeTime), float64(writeTime)/float64(totalWorkerTime)*100))
		summary.WriteString(fmt.Sprintf("  Fsync (per-ledger):      %s (%.1f%%)\n", formatElapsed(fsyncTime), float64(fsyncTime)/float64(totalWorkerTime)*100))
	}

	summary.WriteString("\n")
	summary.WriteString("Raw Event Data (XDR):\n")
	summary.WriteString(fmt.Sprintf("  Total size:              %.2f MB\n", rawDataMB))
	summary.WriteString(fmt.Sprintf("  Event count:             %d\n", eventCount))
	if eventCount > 0 && rawDataMB > 0 {
		rawBytesPerEvent := (rawDataMB * 1024 * 1024) / float64(eventCount)
		summary.WriteString(fmt.Sprintf("  Avg bytes/event:         %.f\n", rawBytesPerEvent))
	}

	fmt.Fprint(os.Stderr, summary.String())

	// Write ingestion summary to file (timestamped)
	filetime := time.Now().Format("20060102T150405")
	summaryFile := fmt.Sprintf("summary_%s.txt", filetime)
	if err := os.WriteFile(summaryFile, []byte(summary.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write summary file: %v\n", err)
	}
}

// processLedger reads and parses a single ledger (used by hot ingest workers).
func processLedger(reader *ingest.LedgerReader, seq uint32, networkPassphrase string) *ingest.LedgerResult {
	result := &ingest.LedgerResult{
		Sequence: seq,
		Stats:    ingest.NewLedgerStats(),
	}

	xdrBytes, timing, err := reader.GetLedgerWithTiming(seq)
	if timing != nil {
		result.DiskReadTime = timing.DiskRead
		result.DecompressTime = timing.Decompress
	}

	if err != nil {
		result.Error = fmtErr("failed to read ledger %d: %v", seq, err)
		return result
	}

	unmarshalStart := time.Now()
	events, err := ingest.ExtractEventsFast(xdrBytes, networkPassphrase, result.Stats)
	result.UnmarshalTime = time.Since(unmarshalStart)

	if err != nil {
		result.Error = fmtErr("failed to extract events from ledger %d: %v", seq, err)
		return result
	}

	result.Events = events

	for _, e := range events {
		result.RawBytes += int64(len(e.RawXDR))
	}

	return result
}

// groupEventsByLedger groups events into per-ledger slices, preserving order.
func groupEventsByLedger(events []*event.IngestEvent) [][]*event.IngestEvent {
	if len(events) == 0 {
		return nil
	}

	var result [][]*event.IngestEvent
	var current []*event.IngestEvent
	var currentLedger uint32

	for _, ev := range events {
		if len(current) > 0 && ev.LedgerSequence != currentLedger {
			result = append(result, current)
			current = nil
		}
		currentLedger = ev.LedgerSequence
		current = append(current, ev)
	}
	if len(current) > 0 {
		result = append(result, current)
	}

	return result
}

// fmtErr creates a formatted error.
func fmtErr(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
