package progress

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// =============================================================================
// Types
// =============================================================================

// ProgressFile represents the JSON structure written to the progress file
type ProgressFile struct {
	Range       RangeInfo       `json:"range"`
	Progress    ProgressInfo    `json:"progress"`
	Events      EventsInfo      `json:"events"`
	Performance PerformanceInfo `json:"performance"`
	Status      string          `json:"status"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// RangeInfo tracks the ledger range being ingested
type RangeInfo struct {
	Start   uint32 `json:"start"`
	End     uint32 `json:"end"`
	Current uint32 `json:"current"`
}

// ProgressInfo tracks overall progress
type ProgressInfo struct {
	LedgersProcessed int     `json:"ledgers_processed"`
	PercentComplete  float64 `json:"percent_complete"`
}

// EventsInfo tracks event statistics
type EventsInfo struct {
	Total        int     `json:"total"`
	AvgPerLedger float64 `json:"avg_per_ledger"`
}

// PerformanceInfo tracks timing and rates
type PerformanceInfo struct {
	LedgersPerSec float64 `json:"ledgers_per_sec"`
	EventsPerSec  float64 `json:"events_per_sec"`
	Elapsed       string  `json:"elapsed"`
	Remaining     string  `json:"remaining"`
}

// =============================================================================
// Writer
// =============================================================================

// Writer writes progress updates to a file
type Writer struct {
	filePath  string
	startTime time.Time
	start     uint32
	end       uint32
}

// NewWriter creates a new progress writer
func NewWriter(filePath string, start, end uint32) *Writer {
	return &Writer{
		filePath:  filePath,
		startTime: time.Now(),
		start:     start,
		end:       end,
	}
}

// Update writes the current progress to the file
func (w *Writer) Update(current uint32, ledgersProcessed, eventsTotal int) error {
	elapsed := time.Since(w.startTime)
	elapsedSecs := elapsed.Seconds()

	// Calculate rates
	var ledgersPerSec, eventsPerSec float64
	if elapsedSecs > 0 {
		ledgersPerSec = float64(ledgersProcessed) / elapsedSecs
		eventsPerSec = float64(eventsTotal) / elapsedSecs
	}

	// Calculate progress
	totalLedgers := int(w.end - w.start + 1)
	var percentComplete float64
	if totalLedgers > 0 {
		percentComplete = float64(ledgersProcessed) / float64(totalLedgers) * 100
	}

	// Calculate ETA
	var remaining string
	if ledgersPerSec > 0 {
		ledgersRemaining := totalLedgers - ledgersProcessed
		secsRemaining := float64(ledgersRemaining) / ledgersPerSec
		remaining = formatDuration(time.Duration(secsRemaining * float64(time.Second)))
	} else {
		remaining = "calculating..."
	}

	// Calculate avg events per ledger
	var avgPerLedger float64
	if ledgersProcessed > 0 {
		avgPerLedger = float64(eventsTotal) / float64(ledgersProcessed)
	}

	progress := &ProgressFile{
		Range: RangeInfo{
			Start:   w.start,
			End:     w.end,
			Current: current,
		},
		Progress: ProgressInfo{
			LedgersProcessed: ledgersProcessed,
			PercentComplete:  percentComplete,
		},
		Events: EventsInfo{
			Total:        eventsTotal,
			AvgPerLedger: avgPerLedger,
		},
		Performance: PerformanceInfo{
			LedgersPerSec: ledgersPerSec,
			EventsPerSec:  eventsPerSec,
			Elapsed:       formatDuration(elapsed),
			Remaining:     remaining,
		},
		Status:    "running",
		UpdatedAt: time.Now(),
	}

	return w.write(progress)
}

// Complete writes the final progress with status "completed"
func (w *Writer) Complete(ledgersProcessed, eventsTotal int) error {
	elapsed := time.Since(w.startTime)
	elapsedSecs := elapsed.Seconds()

	var ledgersPerSec, eventsPerSec float64
	if elapsedSecs > 0 {
		ledgersPerSec = float64(ledgersProcessed) / elapsedSecs
		eventsPerSec = float64(eventsTotal) / elapsedSecs
	}

	var avgPerLedger float64
	if ledgersProcessed > 0 {
		avgPerLedger = float64(eventsTotal) / float64(ledgersProcessed)
	}

	progress := &ProgressFile{
		Range: RangeInfo{
			Start:   w.start,
			End:     w.end,
			Current: w.end,
		},
		Progress: ProgressInfo{
			LedgersProcessed: ledgersProcessed,
			PercentComplete:  100.0,
		},
		Events: EventsInfo{
			Total:        eventsTotal,
			AvgPerLedger: avgPerLedger,
		},
		Performance: PerformanceInfo{
			LedgersPerSec: ledgersPerSec,
			EventsPerSec:  eventsPerSec,
			Elapsed:       formatDuration(elapsed),
			Remaining:     "0s",
		},
		Status:    "completed",
		UpdatedAt: time.Now(),
	}

	return w.write(progress)
}

// Failed writes the final progress with status "failed"
func (w *Writer) Failed(current uint32, ledgersProcessed, eventsTotal int, err error) error {
	elapsed := time.Since(w.startTime)
	elapsedSecs := elapsed.Seconds()

	var ledgersPerSec, eventsPerSec float64
	if elapsedSecs > 0 {
		ledgersPerSec = float64(ledgersProcessed) / elapsedSecs
		eventsPerSec = float64(eventsTotal) / elapsedSecs
	}

	var avgPerLedger float64
	if ledgersProcessed > 0 {
		avgPerLedger = float64(eventsTotal) / float64(ledgersProcessed)
	}

	totalLedgers := int(w.end - w.start + 1)
	var percentComplete float64
	if totalLedgers > 0 {
		percentComplete = float64(ledgersProcessed) / float64(totalLedgers) * 100
	}

	progress := &ProgressFile{
		Range: RangeInfo{
			Start:   w.start,
			End:     w.end,
			Current: current,
		},
		Progress: ProgressInfo{
			LedgersProcessed: ledgersProcessed,
			PercentComplete:  percentComplete,
		},
		Events: EventsInfo{
			Total:        eventsTotal,
			AvgPerLedger: avgPerLedger,
		},
		Performance: PerformanceInfo{
			LedgersPerSec: ledgersPerSec,
			EventsPerSec:  eventsPerSec,
			Elapsed:       formatDuration(elapsed),
			Remaining:     "-",
		},
		Status:    fmt.Sprintf("failed: %v", err),
		UpdatedAt: time.Now(),
	}

	return w.write(progress)
}

// write marshals and writes the progress to file
func (w *Writer) write(p *ProgressFile) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal progress: %w", err)
	}

	if err := os.WriteFile(w.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write progress file: %w", err)
	}

	return nil
}

// formatDuration formats a duration as a human-readable string
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)

	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
