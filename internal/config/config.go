package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Network passphrases
const (
	TestnetPassphrase = "Test SDF Network ; September 2015"
	MainnetPassphrase = "Public Global Stellar Network ; September 2015"
)

// =============================================================================
// Main Config Structure
// =============================================================================

// Config represents the application configuration
type Config struct {
	Source    SourceConfig    `toml:"source"`
	Storage   StorageConfig   `toml:"storage"`
	Ingestion IngestionConfig `toml:"ingestion"`
	Query     QueryConfig     `toml:"query"`
}

// =============================================================================
// Source Config
// =============================================================================

// SourceConfig contains ledger source settings
type SourceConfig struct {
	LedgerDir string `toml:"ledger_dir"` // Path to ledger chunk files
	Network   string `toml:"network"`    // "mainnet", "testnet", or custom passphrase
}

// =============================================================================
// Storage Config
// =============================================================================

// StorageConfig contains storage settings.
// At least one backend (RocksDB or flat files) must be enabled.
type StorageConfig struct {
	// --- Backend selection ---
	RocksDB      bool   `toml:"rocksdb"`       // Enable RocksDB backend (default: true)
	SegmentFiles bool   `toml:"segment_files"` // Enable flat file segment backend (default: false)
	HotWriter    string `toml:"hot_writer"`    // Hot segment writer: "flatfile" (default), "rocksdb", "live"

	// --- RocksDB options (ignored when rocksdb = false) ---
	DBPath string `toml:"db_path"` // Path to RocksDB database directory

	// Write performance
	WriteBufferSizeMB           int `toml:"write_buffer_size_mb"`             // Memtable size (default: 64)
	MaxWriteBufferNumber        int `toml:"max_write_buffer_number"`          // Number of memtables (default: 2)
	MinWriteBufferNumberToMerge int `toml:"min_write_buffer_number_to_merge"` // Memtables to merge before flush (default: 1)

	// Read performance
	BlockCacheSizeMB          int  `toml:"block_cache_size_mb"`           // LRU cache size (default: 64)
	BloomFilterBitsPerKey     int  `toml:"bloom_filter_bits_per_key"`     // Bloom filter bits (default: 10, 0 to disable)
	CacheIndexAndFilterBlocks bool `toml:"cache_index_and_filter_blocks"` // Cache indexes in block cache (default: true)

	// Background jobs
	MaxBackgroundJobs int `toml:"max_background_jobs"` // Parallel background threads (default: 4)

	// Compression
	Compression           string `toml:"compression"`            // "none", "snappy", "lz4", "zstd" (default: "zstd")
	BottommostCompression string `toml:"bottommost_compression"` // Compression for oldest data (default: "zstd")

	// WAL
	DisableWAL bool `toml:"disable_wal"` // Disable write-ahead log for faster bulk ingestion

	// Compaction
	DisableAutoCompaction  bool `toml:"disable_auto_compaction"`     // Disable background compaction during ingestion
	TargetFileSizeMB       int  `toml:"target_file_size_mb"`         // Target SST file size (default: 256)
	MaxBytesForLevelBaseMB int  `toml:"max_bytes_for_level_base_mb"` // Max bytes for L1 (default: 1024)

	// --- Flat file segment options (ignored when segment_files = false) ---
	SegmentPath  string `toml:"segment_path"`  // Base directory for segment flat files (required if segment_files = true)
	CompressData bool   `toml:"compress_data"` // Zstd compress event data blocks (default: false)
	BlockSize    int    `toml:"block_size"`    // Events per compression block (default: 128)
}

// =============================================================================
// Ingestion Config
// =============================================================================

// IngestionConfig contains ingestion settings
type IngestionConfig struct {
	// Progress tracking
	ProgressFile string `toml:"progress_file"` // Progress file path (empty = disabled)

	// Post-processing
	FinalCompaction bool `toml:"final_compaction"` // Run compaction after ingestion (default: true)
	ComputeStats    bool `toml:"compute_stats"`    // Compute event stats after ingestion (default: false)

	// Index maintenance during ingestion
	UniqueIndexes bool `toml:"unique_indexes"` // Maintain unique value counts (default: false)

	// Durability
	DisableFsync bool `toml:"disable_fsync"` // Skip per-ledger fsync in hot ingest (faster, less durable)

	// Parallelism
	Workers   int `toml:"workers"`    // Parallel workers (0 = NumCPU)
	BatchSize int `toml:"batch_size"` // Ledgers per batch (default: 100)
	QueueSize int `toml:"queue_size"` // Pipeline buffer (0 = workers * 2)

}

// =============================================================================
// Query Config
// =============================================================================

// QueryConfig contains query command settings
type QueryConfig struct {
	MaxLedgerRange int `toml:"max_ledger_range"` // Max ledgers if end not specified (default: 100000)
	DefaultLimit   int `toml:"default_limit"`    // Default max events to return (default: 100)
}

// =============================================================================
// Defaults
// =============================================================================

// DefaultConfig returns a config with default values
func DefaultConfig() *Config {
	return &Config{
		Source: SourceConfig{
			LedgerDir: "./data/ledgers",
			Network:   "mainnet",
		},
		Storage: StorageConfig{
			// Backend selection
			RocksDB:      true,
			SegmentFiles: false,
			// RocksDB options
			DBPath:                      "./events.db",
			WriteBufferSizeMB:           64,
			MaxWriteBufferNumber:        2,
			MinWriteBufferNumberToMerge: 1,
			BlockCacheSizeMB:            64,
			BloomFilterBitsPerKey:       10,
			CacheIndexAndFilterBlocks:   true,
			MaxBackgroundJobs:           4,
			Compression:                 "zstd",
			BottommostCompression:       "zstd",
			DisableWAL:                  false,
			DisableAutoCompaction:       false,
			TargetFileSizeMB:            256,
			MaxBytesForLevelBaseMB:      1024,
			// Flat file segment options
			SegmentPath:  "",
			CompressData: false,
			BlockSize:    128,
		},
		Ingestion: IngestionConfig{
			ProgressFile:       "", // Empty = disabled
			FinalCompaction:    true,
			ComputeStats:       false,
			UniqueIndexes: false,
			Workers:       0, // 0 = NumCPU
			BatchSize:          100,
			QueueSize:          0, // 0 = workers * 2
		},
		Query: QueryConfig{
			MaxLedgerRange: 100000,
			DefaultLimit:   100,
		},
	}
}

// =============================================================================
// Loading and Validation
// =============================================================================

// LoadConfig loads configuration from a TOML file
func LoadConfig(path string) (*Config, error) {
	config := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if _, err := toml.Decode(string(data), config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Source.LedgerDir == "" {
		return fmt.Errorf("source.ledger_dir is required")
	}

	if c.Storage.RocksDB && c.Storage.DBPath == "" {
		return fmt.Errorf("storage.db_path is required when storage.rocksdb is true")
	}

	if c.Source.Network == "" {
		return fmt.Errorf("source.network is required")
	}

	// At least one storage destination must be enabled
	if !c.Storage.RocksDB && !c.Storage.SegmentFiles {
		return fmt.Errorf("at least one of storage.rocksdb or storage.segment_files must be true")
	}

	// Segment files require a path
	if c.Storage.SegmentFiles && c.Storage.SegmentPath == "" {
		return fmt.Errorf("storage.segment_path is required when storage.segment_files is true")
	}

	return nil
}

// GetNetworkPassphrase returns the network passphrase based on config
func (c *Config) GetNetworkPassphrase() string {
	switch c.Source.Network {
	case "mainnet":
		return MainnetPassphrase
	case "testnet":
		return TestnetPassphrase
	default:
		return c.Source.Network // Custom passphrase
	}
}

// FindConfigFile looks for config.toml in the current directory
func FindConfigFile() (string, error) {
	candidates := []string{
		"config.toml",
	}

	for _, name := range candidates {
		if _, err := os.Stat(name); err == nil {
			return name, nil
		}
	}

	return "", fmt.Errorf("config file not found. Create config.toml")
}
