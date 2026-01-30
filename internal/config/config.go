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
	Indexes   IndexConfig     `toml:"indexes"`
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
// Storage Config (flattened - RocksDB only)
// =============================================================================

// StorageConfig contains storage settings
type StorageConfig struct {
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
	BitmapIndexes bool `toml:"bitmap_indexes"` // Maintain bitmap indexes (default: true)
	UniqueIndexes bool `toml:"unique_indexes"` // Maintain unique value counts (default: false)

	// Bitmap flush interval
	BitmapFlushInterval int `toml:"bitmap_flush_interval"` // Ledgers between bitmap index flushes (default: 10000)

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
// Index Config
// =============================================================================

// IndexConfig contains index settings
type IndexConfig struct {
	ContractID bool `toml:"contract_id"` // Index by contract ID
	Topics     bool `toml:"topics"`      // Index by topic0-3
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
			DBPath: "./events.db",
			// Write performance
			WriteBufferSizeMB:           64,
			MaxWriteBufferNumber:        2,
			MinWriteBufferNumberToMerge: 1,
			// Read performance
			BlockCacheSizeMB:          64,
			BloomFilterBitsPerKey:     10,
			CacheIndexAndFilterBlocks: true,
			// Background jobs
			MaxBackgroundJobs: 4,
			// Compression
			Compression:           "zstd",
			BottommostCompression: "zstd",
			// WAL
			DisableWAL: false,
			// Compaction
			DisableAutoCompaction:  false,
			TargetFileSizeMB:       256,
			MaxBytesForLevelBaseMB: 1024,
		},
		Ingestion: IngestionConfig{
			ProgressFile:        "", // Empty = disabled
			FinalCompaction:     true,
			ComputeStats:        false,
			BitmapIndexes:       true,
			UniqueIndexes:       false,
			BitmapFlushInterval: 10000, // Flush hot segments every 10K ledgers
			Workers:             0,     // 0 = NumCPU
			BatchSize:           100,
			QueueSize:           0, // 0 = workers * 2
		},
		Query: QueryConfig{
			MaxLedgerRange: 100000,
			DefaultLimit:   100,
		},
		Indexes: IndexConfig{
			ContractID: true,
			Topics:     true,
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

	if c.Storage.DBPath == "" {
		return fmt.Errorf("storage.db_path is required")
	}

	if c.Source.Network == "" {
		return fmt.Errorf("source.network is required")
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
