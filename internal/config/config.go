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

// Config represents the application configuration
type Config struct {
	Source    SourceConfig    `toml:"source"`
	Storage   StorageConfig   `toml:"storage"`
	Ingestion IngestionConfig `toml:"ingestion"`
	Indexes   IndexConfig     `toml:"indexes"`
}

// SourceConfig contains ledger source settings
type SourceConfig struct {
	LedgerDir string `toml:"ledger_dir"`
	Network   string `toml:"network"` // mainnet, testnet, or custom passphrase
}

// StorageConfig contains RocksDB settings
type StorageConfig struct {
	DBPath  string        `toml:"db_path"`
	RocksDB RocksDBConfig `toml:"rocksdb"`
}

// RocksDBConfig contains RocksDB tuning parameters
type RocksDBConfig struct {
	// Write performance
	WriteBufferSizeMB           int `toml:"write_buffer_size_mb"`             // Memtable size (default: 64)
	MaxWriteBufferNumber        int `toml:"max_write_buffer_number"`          // Number of memtables (default: 2)
	MinWriteBufferNumberToMerge int `toml:"min_write_buffer_number_to_merge"` // Memtables to merge before flush (default: 1)

	// Read performance
	BlockCacheSizeMB          int  `toml:"block_cache_size_mb"`           // LRU cache size (default: 64)
	BloomFilterBitsPerKey     int  `toml:"bloom_filter_bits_per_key"`     // Bloom filter bits (default: 10, 0 to disable)
	CacheIndexAndFilterBlocks bool `toml:"cache_index_and_filter_blocks"` // Cache indexes in block cache (default: true)

	// Background jobs
	MaxBackgroundJobs int `toml:"max_background_jobs"` // Parallel background threads for flushing (default: 4)

	// Compression
	Compression           string `toml:"compression"`            // Compression: "none", "snappy", "lz4", "zstd" (default: "lz4")
	BottommostCompression string `toml:"bottommost_compression"` // Compression for oldest data (default: "zstd")

	// WAL
	DisableWAL bool `toml:"disable_wal"` // Disable write-ahead log for faster bulk ingestion (default: false)

	// Auto compaction
	DisableAutoCompaction bool `toml:"disable_auto_compaction"` // Disable background compaction during ingestion (default: false)
}

// IngestionConfig contains ingestion settings
type IngestionConfig struct {
	ProgressFile      string `toml:"progress_file"`
	FinalCompaction   bool   `toml:"final_compaction"`    // Run manual compaction after ingestion (default: true)
	ComputeStats      bool   `toml:"compute_stats"`       // Compute event stats after ingestion (default: false)
	MaintainUniqueIdx bool   `toml:"maintain_unique_idx"` // Maintain unique indexes during ingestion (default: false)
	SnapshotInterval  int    `toml:"snapshot_interval"`   // Ledgers between progress snapshots (default: 1000000)

	// Parallelism settings
	Workers   int `toml:"workers"`    // Number of parallel workers (default: number of CPUs)
	BatchSize int `toml:"batch_size"` // Ledgers to batch before writing (default: 100)
	QueueSize int `toml:"queue_size"` // Channel buffer size for pipeline (default: workers * 2)
}

// IndexConfig contains index settings
type IndexConfig struct {
	ContractID      bool `toml:"contract_id"`
	TransactionHash bool `toml:"transaction_hash"`
	EventType       bool `toml:"event_type"`
	Topics          bool `toml:"topics"` // enables topic0-3
}

// DefaultConfig returns a config with default values
func DefaultConfig() *Config {
	return &Config{
		Source: SourceConfig{
			LedgerDir: "./data/ledgers",
			Network:   "mainnet",
		},
		Storage: StorageConfig{
			DBPath: "./events.db",
			RocksDB: RocksDBConfig{
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
				// Auto compaction
				DisableAutoCompaction: false,
			},
		},
		Ingestion: IngestionConfig{
			ProgressFile:      "",
			FinalCompaction:   true,
			ComputeStats:      false, // Disabled by default (slow operation)
			MaintainUniqueIdx: false, // Disabled by default (adds XDR parsing overhead)
			SnapshotInterval:  1000000,
			Workers:           0, // 0 means use runtime.NumCPU()
			BatchSize:         100,
			QueueSize:         0, // 0 means workers * 2
		},
		Indexes: IndexConfig{
			ContractID:      true,
			TransactionHash: true,
			EventType:       true,
			Topics:          true,
		},
	}
}

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

	// Validate config
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
