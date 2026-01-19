package progress

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/urvisavla/stellar-events/internal/ingest"
)

// StatsProvider interface for getting RocksDB stats
type StatsProvider interface {
	GetSnapshotStats() *SnapshotStats
}

// SnapshotStats holds numeric RocksDB stats (matches store.SnapshotStats)
type SnapshotStats struct {
	// Size metrics (in bytes)
	SSTFilesSizeBytes   int64 `json:"sst_files_size_bytes"`
	MemtableSizeBytes   int64 `json:"memtable_size_bytes"`
	EstimatedNumKeys    int64 `json:"estimated_num_keys"`
	PendingCompactBytes int64 `json:"pending_compact_bytes"`

	// Level metrics
	L0Files int `json:"l0_files"`
	L1Files int `json:"l1_files"`
	L2Files int `json:"l2_files"`
	L3Files int `json:"l3_files"`
	L4Files int `json:"l4_files"`
	L5Files int `json:"l5_files"`
	L6Files int `json:"l6_files"`

	// Compaction state
	RunningCompactions int  `json:"running_compactions"`
	CompactionPending  bool `json:"compaction_pending"`
}

// Default snapshot interval
const DefaultSnapshotInterval = 1000000 // ledgers

// IngestionProgress tracks the progress of an ingestion job
type IngestionProgress struct {
	// Configuration
	StartLedger uint32 `json:"start_ledger"`
	EndLedger   uint32 `json:"end_ledger"`

	// Progress
	CurrentLedger    uint32  `json:"current_ledger"`
	LedgersProcessed int     `json:"ledgers_processed"`
	ProgressPercent  float64 `json:"progress_percent"`

	// Events
	EventsIngested    int `json:"events_ingested"`
	ContractEvents    int `json:"contract_events"`
	SystemEvents      int `json:"system_events"`
	DiagnosticEvents  int `json:"diagnostic_events"`
	OperationEvents   int `json:"operation_events"`
	TransactionEvents int `json:"transaction_events"`
	TransactionsTotal int `json:"transactions_total"`

	// Storage
	RawDataBytes int64 `json:"raw_data_bytes"` // Total raw event data size before RocksDB compression

	// Timing
	StartedAt              time.Time `json:"started_at"`
	UpdatedAt              time.Time `json:"updated_at"`
	LedgersPerSec          float64   `json:"ledgers_per_sec"`
	EventsPerSec           float64   `json:"events_per_sec"`
	EstimatedTimeRemaining string    `json:"estimated_time_remaining"`

	// Errors
	Errors []string `json:"errors,omitempty"`

	// Status
	Status string `json:"status"` // "running", "completed", "failed", "paused"
}

// ProgressSnapshot captures metrics at a point in time for trend analysis
type ProgressSnapshot struct {
	Timestamp        time.Time `json:"timestamp"`
	CurrentLedger    uint32    `json:"current_ledger"`
	LedgersProcessed int       `json:"ledgers_processed"`
	EventsIngested   int       `json:"events_ingested"`

	// Metrics for this snapshot period (since last snapshot)
	Ledgers       int     `json:"ledgers"`
	Events        int     `json:"events"`
	Seconds       float64 `json:"seconds"`
	LedgersPerSec float64 `json:"ledgers_per_sec"`
	EventsPerSec  float64 `json:"events_per_sec"`

	// Timing breakdown for this period (milliseconds)
	ReadTimeMs  int64 `json:"read_time_ms"`
	WriteTimeMs int64 `json:"write_time_ms"`

	// Storage metrics
	RawDataMB       float64 `json:"raw_data_mb"`       // cumulative in MB
	SSTFilesSizeMB  float64 `json:"sst_files_size_mb"` // RocksDB disk size
	StorageOverhead float64 `json:"storage_overhead"`  // percentage: (sst - raw) / raw * 100

	// RocksDB stats at snapshot time
	MemtableSizeMB     float64 `json:"memtable_size_mb"`
	EstimatedKeys      int64   `json:"estimated_keys"`
	PendingCompactMB   float64 `json:"pending_compact_mb"`
	L0Files            int     `json:"l0_files"`
	RunningCompactions int     `json:"running_compactions"`
	CompactionPending  bool    `json:"compaction_pending"`
}

// Tracker manages progress tracking during ingestion
type Tracker struct {
	mu       sync.Mutex
	progress *IngestionProgress
	filePath string
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}

	// Snapshot tracking
	snapshotInterval    int // ledgers between snapshots
	lastSnapshotLedgers int
	lastSnapshotEvents  int
	lastSnapshotTime    time.Time
	historyFile         string

	// Timing tracking for current period
	periodReadTime  time.Duration
	periodWriteTime time.Duration

	// Raw data tracking
	totalRawBytes int64

	// Stats provider (RocksDB)
	statsProvider StatsProvider
}

// NewTracker creates a new progress tracker
func NewTracker(filePath string, startLedger, endLedger uint32) *Tracker {
	// Derive history file from progress file
	historyFile := strings.TrimSuffix(filePath, ".json") + "_history.jsonl"

	return &Tracker{
		progress: &IngestionProgress{
			StartLedger: startLedger,
			EndLedger:   endLedger,
			StartedAt:   time.Now(),
			UpdatedAt:   time.Now(),
			Status:      "running",
		},
		filePath:         filePath,
		interval:         5 * time.Second, // Write progress every 5 seconds
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
		snapshotInterval: DefaultSnapshotInterval,
		lastSnapshotTime: time.Now(),
		historyFile:      historyFile,
	}
}

// SetSnapshotInterval sets the number of ledgers between snapshots
func (t *Tracker) SetSnapshotInterval(interval int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snapshotInterval = interval
}

// SetStatsProvider sets the RocksDB stats provider
func (t *Tracker) SetStatsProvider(provider StatsProvider) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.statsProvider = provider
}

// AddReadTime adds to the read time for the current period
func (t *Tracker) AddReadTime(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.periodReadTime += d
}

// AddWriteTime adds to the write time for the current period
func (t *Tracker) AddWriteTime(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.periodWriteTime += d
}

// AddRawBytes adds to the total raw data bytes
func (t *Tracker) AddRawBytes(bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.totalRawBytes += bytes
	t.progress.RawDataBytes = t.totalRawBytes
}

// LoadProgress loads progress from a checkpoint file
func LoadProgress(filePath string) (*IngestionProgress, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read progress file: %w", err)
	}

	var progress IngestionProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, fmt.Errorf("failed to parse progress file: %w", err)
	}

	return &progress, nil
}

// Start begins periodic progress file updates
func (t *Tracker) Start() {
	go func() {
		defer close(t.doneCh)
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				t.writeProgress()
			case <-t.stopCh:
				t.writeProgress() // Final write
				t.writeSnapshot() // Final snapshot
				return
			}
		}
	}()
}

// Stop stops the progress tracker
func (t *Tracker) Stop() {
	close(t.stopCh)
	<-t.doneCh
}

// Update updates the progress with new values
func (t *Tracker) Update(currentLedger uint32, ledgersProcessed int, eventsIngested int, stats *ingest.LedgerStats) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(t.progress.StartedAt).Seconds()

	t.progress.CurrentLedger = currentLedger
	t.progress.LedgersProcessed = ledgersProcessed
	t.progress.EventsIngested = eventsIngested
	t.progress.UpdatedAt = now

	// Calculate progress percentage
	totalLedgers := t.progress.EndLedger - t.progress.StartLedger + 1
	if totalLedgers > 0 {
		t.progress.ProgressPercent = float64(ledgersProcessed) / float64(totalLedgers) * 100
	}

	// Calculate rates
	if elapsed > 0 {
		t.progress.LedgersPerSec = float64(ledgersProcessed) / elapsed
		t.progress.EventsPerSec = float64(eventsIngested) / elapsed

		// Estimate remaining time
		if t.progress.LedgersPerSec > 0 {
			remainingLedgers := int(totalLedgers) - ledgersProcessed
			remainingSecs := float64(remainingLedgers) / t.progress.LedgersPerSec
			t.progress.EstimatedTimeRemaining = formatDuration(time.Duration(remainingSecs) * time.Second)
		}
	}

	// Update stats from LedgerStats if provided
	if stats != nil {
		t.progress.TransactionsTotal = stats.TotalTransactions
		t.progress.OperationEvents = stats.OperationEvents
		t.progress.TransactionEvents = stats.TransactionEvents
	}

	// Check if we should write a snapshot
	ledgersSinceSnapshot := ledgersProcessed - t.lastSnapshotLedgers
	if ledgersSinceSnapshot >= t.snapshotInterval {
		t.writeSnapshotLocked(now, ledgersProcessed, eventsIngested)
	}
}

// writeSnapshotLocked writes a snapshot (must be called with lock held)
func (t *Tracker) writeSnapshotLocked(now time.Time, ledgersProcessed, eventsIngested int) {
	intervalSeconds := now.Sub(t.lastSnapshotTime).Seconds()
	intervalLedgers := ledgersProcessed - t.lastSnapshotLedgers
	intervalEvents := eventsIngested - t.lastSnapshotEvents

	var intervalLedgersPerSec, intervalEventsPerSec float64
	if intervalSeconds > 0 {
		intervalLedgersPerSec = float64(intervalLedgers) / intervalSeconds
		intervalEventsPerSec = float64(intervalEvents) / intervalSeconds
	}

	snapshot := ProgressSnapshot{
		Timestamp:        now,
		CurrentLedger:    t.progress.CurrentLedger,
		LedgersProcessed: ledgersProcessed,
		EventsIngested:   eventsIngested,
		Ledgers:          intervalLedgers,
		Events:           intervalEvents,
		Seconds:          intervalSeconds,
		LedgersPerSec:    intervalLedgersPerSec,
		EventsPerSec:     intervalEventsPerSec,
		ReadTimeMs:       t.periodReadTime.Milliseconds(),
		WriteTimeMs:      t.periodWriteTime.Milliseconds(),
		RawDataMB:        float64(t.totalRawBytes) / (1024 * 1024),
	}

	// Add RocksDB stats if provider is set
	if t.statsProvider != nil {
		stats := t.statsProvider.GetSnapshotStats()
		snapshot.SSTFilesSizeMB = float64(stats.SSTFilesSizeBytes) / (1024 * 1024)
		snapshot.MemtableSizeMB = float64(stats.MemtableSizeBytes) / (1024 * 1024)
		snapshot.EstimatedKeys = stats.EstimatedNumKeys
		snapshot.PendingCompactMB = float64(stats.PendingCompactBytes) / (1024 * 1024)
		snapshot.L0Files = stats.L0Files
		snapshot.RunningCompactions = stats.RunningCompactions
		snapshot.CompactionPending = stats.CompactionPending

		// Calculate storage overhead (negative means compression savings)
		if t.totalRawBytes > 0 {
			snapshot.StorageOverhead = (snapshot.SSTFilesSizeMB - snapshot.RawDataMB) / snapshot.RawDataMB * 100
		}
	}

	// Reset period timing
	t.periodReadTime = 0
	t.periodWriteTime = 0

	// Append to history file
	data, err := json.Marshal(snapshot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to marshal snapshot: %v\n", err)
		return
	}

	f, err := os.OpenFile(t.historyFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to open history file: %v\n", err)
		return
	}
	defer f.Close()

	if _, err := f.WriteString(string(data) + "\n"); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write snapshot: %v\n", err)
		return
	}

	// Update last snapshot state
	t.lastSnapshotLedgers = ledgersProcessed
	t.lastSnapshotEvents = eventsIngested
	t.lastSnapshotTime = now
}

// writeSnapshot writes a final snapshot
func (t *Tracker) writeSnapshot() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.writeSnapshotLocked(time.Now(), t.progress.LedgersProcessed, t.progress.EventsIngested)
}

// UpdateEventCounts updates the event type counts
func (t *Tracker) UpdateEventCounts(contractEvents, systemEvents, diagnosticEvents int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.progress.ContractEvents = contractEvents
	t.progress.SystemEvents = systemEvents
	t.progress.DiagnosticEvents = diagnosticEvents
}

// AddError adds an error to the progress
func (t *Tracker) AddError(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.progress.Errors = append(t.progress.Errors, err.Error())
}

// SetStatus sets the status of the ingestion
func (t *Tracker) SetStatus(status string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.progress.Status = status
	t.progress.UpdatedAt = time.Now()
}

// GetProgress returns the current progress
func (t *Tracker) GetProgress() *IngestionProgress {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Return a copy
	copy := *t.progress
	return &copy
}

// GetHistoryFile returns the path to the history file
func (t *Tracker) GetHistoryFile() string {
	return t.historyFile
}

// writeProgress writes progress to file
func (t *Tracker) writeProgress() {
	t.mu.Lock()
	progress := *t.progress
	t.mu.Unlock()

	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to marshal progress: %v\n", err)
		return
	}

	if err := os.WriteFile(t.filePath, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write progress file: %v\n", err)
	}
}

// formatDuration formats a duration as a human-readable string
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	if hours >= 24 {
		days := hours / 24
		hours = hours % 24
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}
	return fmt.Sprintf("%dh %dm", hours, minutes)
}
