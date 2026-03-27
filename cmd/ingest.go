package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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
	noFreeze := fs.Bool("no-freeze", false, "Keep hot segments without converting to cold")
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

	cmdIngest(cfg, uint32(*start), uint32(*end), *noFreeze)
}

func cmdIngest(cfg *config.Config, startLedger, endLedger uint32, noFreeze bool) {
	fmt := message.NewPrinter(language.English)

	// Tee all stderr output to a timestamped log file
	logFile := fmt.Sprintf("ingest_%s.log", time.Now().Format("20060102T150405"))
	cleanupLog := setupStderrTee(logFile)
	defer cleanupLog()

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

	// Open RocksDB backend if configured
	var rocksBackend *store.RocksDBBackend
	if cfg.Storage.RocksDB {
		var err error
		rocksBackend, err = store.NewRocksDBBackend(cfg.Storage.DBPath, configToRocksDBOptions(&cfg.Storage))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to open RocksDB: %v\n", err)
			os.Exit(1)
		}
		defer rocksBackend.Close()
		fmt.Fprintf(os.Stderr, "RocksDB hot segments enabled (db_path=%s)\n", cfg.Storage.DBPath)
	}

	// segment_path is required unless using RocksDB with --no-freeze
	// (in that case data stays in RocksDB, no cold output needed)
	needsColdPath := !noFreeze || rocksBackend == nil
	if segmentPath == "" && needsColdPath {
		fmt.Fprintf(os.Stderr, "Error: storage.segment_path is required (needed for cold segment output)\n")
		os.Exit(2)
	}

	// Create an IndexStore for in-memory bitmap tracking.
	flusher := &store.SegmentIndexFlusher{}
	indexStore := store.NewIndexStore(flusher, nil, nil)
	var coldPath string
	if segmentPath != "" {
		coldPath = filepath.Join(segmentPath, "cold")
		indexStore.SetWriteConfig(coldPath, false)
	}
	defer indexStore.Close()

	// Create a SegmentDataWriter for cold output (events.pack)
	var sdw *store.SegmentDataWriter
	if coldPath != "" {
		sdw = store.NewSegmentDataWriter(coldPath, cfg.Storage.CompressData, cfg.Storage.BlockSize)
	}

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

	disableFsync := cfg.Ingestion.DisableFsync

	fmt.Fprintf(os.Stderr, "Ingesting events (hot→cold) from ledgers %d to %d...\n", startLedger, endLedger)
	fmt.Fprintf(os.Stderr, "Parallel mode: %d workers, batch size %d, queue size %d\n", workers, batchSize, queueSize)
	fmt.Fprintf(os.Stderr, "Segment path: %s (hot/ → cold/)\n", segmentPath)
	if disableFsync {
		fmt.Fprintf(os.Stderr, "Per-ledger fsync: DISABLED (faster, less durable)\n")
	}

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

	// Per-ledger write latency tracking (non-empty ledgers only)
	var writeDurations []time.Duration

	// Collect results in order and write to hot segment
	pending := make(map[uint32]*ingest.LedgerResult)
	nextSeq := startLedger

	var hotWriter store.HotWriter
	var currentSegID uint32
	var hasCurrentSeg bool

	var ledgersProcessed int

	// Per-segment accumulators
	var segStartTime time.Time
	var segEvents int
	var segLedgers int
	var allSegmentMetrics []progress.SegmentStats

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

			// Segment boundary: finalize current hot writer, then convert to cold
			if hasCurrentSeg && segID != currentSegID {
				if hotWriter != nil {
					// All ledgers already written per-ledger; just ensure durability
					if err := hotWriter.Fsync(); err != nil {
						pipelineErr = fmtErr("fsync hot segment before freeze: %v", err)
						break
					}

					// Log per-segment stats before conversion
					meta := hotWriter.CommittedLengths()
					var ms runtime.MemStats
					runtime.ReadMemStats(&ms)
					wallMs := float64(time.Since(segStartTime).Milliseconds())
					wallSec := wallMs / 1000.0
					var eventsPerSec, avgEventBytes float64
					if wallSec > 0 {
						eventsPerSec = float64(segEvents) / wallSec
					}
					if segEvents > 0 {
						avgEventBytes = float64(meta.EventsDatLen) / float64(segEvents)
					}
					heapMB := int64(ms.HeapInuse / (1024 * 1024))

					fmt.Fprintf(os.Stderr, "[segment %06d] %d ledgers, %d events, %s raw, avg %.0f bytes/event\n",
						currentSegID, segLedgers, segEvents,
						formatBytes(meta.EventsDatLen), avgEventBytes)
					fmt.Fprintf(os.Stderr, "[segment %06d] %.0f events/s, heap: %d MB, wall: %.0fms\n",
						currentSegID, eventsPerSec, heapMB, wallMs)

					segStats := progress.SegmentStats{
						SegmentID:     currentSegID,
						Ledgers:       segLedgers,
						Events:        segEvents,
						HotEventBytes: meta.EventsDatLen,
						AvgEventBytes: avgEventBytes,
						EventsPerSec: eventsPerSec,
						HeapInUseMB:   heapMB,
						IngestWallMs:  wallMs,
					}

					if noFreeze {
						// Fsync already called above; just close
						hotWriter.Close()
						// Flush bitmaps and populate term counts (normally done inside ConvertToCold)
						indexStore.Flush()
						// TODO: enable bitmap snapshot writes once validated
						// if rw, ok := hotWriter.(*store.RocksDBHotSegmentWriter); ok {
						// 	if err := rw.WriteBitmapSnapshots(indexStore); err != nil {
						// 		fmt.Fprintf(os.Stderr, "[segment %06d] warning: failed to write bitmap snapshots: %v\n", currentSegID, err)
						// 	}
						// }
						c, t0, t1, t2, t3 := indexStore.SegmentTermCounts(currentSegID)
						segStats.ContractTerms = c
						segStats.Topic0Terms = t0
						segStats.Topic1Terms = t1
						segStats.Topic2Terms = t2
						segStats.Topic3Terms = t3
						segStats.IndexTerms = c + t0 + t1 + t2 + t3
						fmt.Fprintf(os.Stderr, "[segment %06d] no-freeze: hot segment kept on disk, %d terms (c:%d t0:%d t1:%d t2:%d t3:%d)\n",
							currentSegID, segStats.IndexTerms, c, t0, t1, t2, t3)
					} else {
						if err := hotWriter.ConvertToCold(indexStore, sdw, &segStats); err != nil {
							pipelineErr = fmtErr("convert segment %d to cold: %v", currentSegID, err)
							break
						}
						fmt.Fprintf(os.Stderr, "[segment %06d] freeze: %.0fms (events.pack %.0fms, mphf %.0fms), %d terms (c:%d t0:%d t1:%d t2:%d t3:%d), cold: %s events, %s index, heap freed %d MB\n",
							currentSegID, segStats.FreezeWallMs, segStats.EventsPackMs, segStats.MphfMs, segStats.IndexTerms,
							segStats.ContractTerms, segStats.Topic0Terms, segStats.Topic1Terms, segStats.Topic2Terms, segStats.Topic3Terms,
							formatBytes(segStats.ColdEventBytes), formatBytes(segStats.ColdIndexBytes), segStats.HeapFreedMB)
					}

					allSegmentMetrics = append(allSegmentMetrics, segStats)
					if progressWriter != nil {
						progressWriter.RecordSegmentStats(segStats)
					}
					hotWriter = nil
				}
			}

			// Open new hot writer if needed
			if hotWriter == nil || segID != currentSegID {
				var err error
				if rocksBackend != nil {
					hotWriter = store.NewRocksDBHotSegmentWriter(rocksBackend, segmentPath, segID)
				} else {
					hotWriter, err = store.NewHotSegmentWriter(segmentPath, segID)
				}
				if err != nil {
					pipelineErr = fmtErr("open hot segment %d: %v", segID, err)
					break
				}
				currentSegID = segID
				hasCurrentSeg = true
				segStartTime = time.Now()
				segEvents = 0
				segLedgers = 0
			}

			// Write this ledger directly (including empty ledgers for correct
			// cumulative offset tracking in the ledger offsets array).
			ledgerWriteStart := time.Now()
			if err := hotWriter.WriteLedger(r.Events, indexStore); err != nil {
				pipelineErr = fmtErr("write ledger to hot segment: %v", err)
				break
			}
			atomic.AddInt64(&writeTimeNs, time.Since(ledgerWriteStart).Nanoseconds())
			if !disableFsync && len(r.Events) > 0 {
				fsyncStart := time.Now()
				if err := hotWriter.Fsync(); err != nil {
					pipelineErr = fmtErr("fsync hot segment: %v", err)
					break
				}
				atomic.AddInt64(&fsyncTimeNs, time.Since(fsyncStart).Nanoseconds())
			}
			if len(r.Events) > 0 {
				writeDurations = append(writeDurations, time.Since(ledgerWriteStart))
			}

			atomic.AddInt64(&rawBytesTotal, r.RawBytes)

			segLedgers++
			segEvents += len(r.Events)

			ledgersProcessed++
			atomic.AddInt64(&totalLedgers, 1)
			atomic.AddInt64(&totalEvents, int64(len(r.Events)))

			nextSeq++

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
		// Full flush before freeze
		if err := hotWriter.Fsync(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to fsync last hot segment: %v\n", err)
		}

		// Log per-segment stats for the final segment
		meta := hotWriter.CommittedLengths()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		wallMs := float64(time.Since(segStartTime).Milliseconds())
		wallSec := wallMs / 1000.0
		var eventsPerSec, avgEventBytes float64
		if wallSec > 0 {
			eventsPerSec = float64(segEvents) / wallSec
		}
		if segEvents > 0 {
			avgEventBytes = float64(meta.EventsDatLen) / float64(segEvents)
		}
		heapMB := int64(ms.HeapInuse / (1024 * 1024))

		fmt.Fprintf(os.Stderr, "[segment %06d] %d ledgers, %d events, %s raw, avg %.0f bytes/event\n",
			currentSegID, segLedgers, segEvents,
			formatBytes(meta.EventsDatLen), avgEventBytes)
		fmt.Fprintf(os.Stderr, "[segment %06d] %.0f events/s, heap: %d MB, wall: %.0fms\n",
			currentSegID, eventsPerSec, heapMB, wallMs)

		segStats := progress.SegmentStats{
			SegmentID:     currentSegID,
			Ledgers:       segLedgers,
			Events:        segEvents,
			HotEventBytes: meta.EventsDatLen,
			AvgEventBytes: avgEventBytes,
			EventsPerSec:  eventsPerSec,
			HeapInUseMB:   heapMB,
			IngestWallMs:  wallMs,
		}

		if noFreeze {
			// Fsync already called above; just close without converting to cold
			hotWriter.Close()
			// Flush bitmaps and populate term counts (normally done inside ConvertToCold)
			indexStore.Flush()
			// TODO: enable bitmap snapshot writes once validated
			// if rw, ok := hotWriter.(*store.RocksDBHotSegmentWriter); ok {
			// 	if err := rw.WriteBitmapSnapshots(indexStore); err != nil {
			// 		fmt.Fprintf(os.Stderr, "[segment %06d] warning: failed to write bitmap snapshots: %v\n", currentSegID, err)
			// 	}
			// }
			c, t0, t1, t2, t3 := indexStore.SegmentTermCounts(currentSegID)
			segStats.ContractTerms = c
			segStats.Topic0Terms = t0
			segStats.Topic1Terms = t1
			segStats.Topic2Terms = t2
			segStats.Topic3Terms = t3
			segStats.IndexTerms = c + t0 + t1 + t2 + t3
			fmt.Fprintf(os.Stderr, "[segment %06d] no-freeze: hot segment kept on disk, %d terms (c:%d t0:%d t1:%d t2:%d t3:%d)\n",
				currentSegID, segStats.IndexTerms, c, t0, t1, t2, t3)
		} else {
			if err := hotWriter.ConvertToCold(indexStore, sdw, &segStats); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to convert last segment to cold: %v\n", err)
			}
			fmt.Fprintf(os.Stderr, "[segment %06d] freeze: %.0fms (events.pack %.0fms, mphf %.0fms), %d terms (c:%d t0:%d t1:%d t2:%d t3:%d), cold: %s events, %s index, heap freed %d MB\n",
				currentSegID, segStats.FreezeWallMs, segStats.EventsPackMs, segStats.MphfMs, segStats.IndexTerms,
				segStats.ContractTerms, segStats.Topic0Terms, segStats.Topic1Terms, segStats.Topic2Terms, segStats.Topic3Terms,
				formatBytes(segStats.ColdEventBytes), formatBytes(segStats.ColdIndexBytes), segStats.HeapFreedMB)
		}

		allSegmentMetrics = append(allSegmentMetrics, segStats)
		if progressWriter != nil {
			progressWriter.RecordSegmentStats(segStats)
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
		if disableFsync {
			summary.WriteString("  Fsync (per-ledger):      DISABLED\n")
		} else {
			summary.WriteString(fmt.Sprintf("  Fsync (per-ledger):      %s (%.1f%%)\n", formatElapsed(fsyncTime), float64(fsyncTime)/float64(totalWorkerTime)*100))
		}
	}

	summary.WriteString("\n")
	summary.WriteString("Raw Event Data (XDR):\n")
	summary.WriteString(fmt.Sprintf("  Total size:              %.2f MB\n", rawDataMB))
	summary.WriteString(fmt.Sprintf("  Event count:             %d\n", eventCount))
	if eventCount > 0 && rawDataMB > 0 {
		rawBytesPerEvent := (rawDataMB * 1024 * 1024) / float64(eventCount)
		summary.WriteString(fmt.Sprintf("  Avg bytes/event:         %.f\n", rawBytesPerEvent))
	}

	if len(writeDurations) > 0 {
		wMin, wAvg, wP50, wP99, wMax := writeLatencyStats(writeDurations)
		summary.WriteString("\n")
		summary.WriteString("Per-Ledger Write Latency (non-empty ledgers):\n")
		summary.WriteString(fmt.Sprintf("  Samples:                 %d\n", len(writeDurations)))
		summary.WriteString(fmt.Sprintf("  Min:                     %v\n", wMin))
		summary.WriteString(fmt.Sprintf("  Avg:                     %v\n", wAvg))
		summary.WriteString(fmt.Sprintf("  P50:                     %v\n", wP50))
		summary.WriteString(fmt.Sprintf("  P99:                     %v\n", wP99))
		summary.WriteString(fmt.Sprintf("  Max:                     %v\n", wMax))
	}

	writeSegmentMetricsSummary(&summary, allSegmentMetrics)

	fmt.Fprint(os.Stderr, summary.String())
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

// writeLatencyStats computes min/avg/p50/p99/max from a slice of durations.
func writeLatencyStats(durations []time.Duration) (min, avg, p50, p99, max time.Duration) {
	if len(durations) == 0 {
		return
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	min = durations[0]
	max = durations[len(durations)-1]
	p50 = durations[len(durations)*50/100]
	p99 = durations[len(durations)*99/100]
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	avg = total / time.Duration(len(durations))
	return
}

// fmtErr creates a formatted error.
func fmtErr(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
