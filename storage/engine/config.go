package engine

import (
	"encoding/json"
	"io/ioutil"
	"time"
)

const (
	defaultRootDirectory   = "/tmp/mos"
	defaultDataFileMaxSize = 1 << 32
	defaultMergeRatio      = 0.5
	defaultMergeSpace      = 1 << 32
	defaultMergeInterval   = time.Hour
)

type Config struct {
	RootDirectory       string        `json:"root_directory"`
	DataFileMaxSize     int64         `json:"data_file_max_size"`
	AutoMerging         bool          `json:"auto_merging"`
	SyncWrite           bool          `json:"sync_write"`
	MergeRatioThreshold float64       `json:"merge_ratio_threshold"`
	MergeSpaceThreshold int64         `json:"merge_space_threshold"`
	MergeInterval       time.Duration `json:"merge_interval"`
}

func DefaultConfig() *Config {
	return &Config{
		RootDirectory:       defaultRootDirectory,
		DataFileMaxSize:     defaultDataFileMaxSize,
		AutoMerging:         false,
		SyncWrite:           false,
		MergeRatioThreshold: defaultMergeRatio,
		MergeSpaceThreshold: defaultMergeSpace,
		MergeInterval:       defaultMergeInterval,
	}
}

func LoadConfig(path string) (*Config, error) {
	var cfg *Config
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

type Option func(config *Config)

func WithRootDirectory(dir string) Option {
	return func(config *Config) {
		config.RootDirectory = dir
	}
}
