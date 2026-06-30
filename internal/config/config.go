// Package config loads and validates application configuration from environment
// variables and an optional YAML/TOML config file via Viper.
//
// Configuration hierarchy (lowest to highest priority):
//  1. Built-in defaults
//  2. Configuration file (YAML/TOML)
//  3. Environment variables (RTMDS_ prefix)
//
// Environment variable mapping:
//
//	RTMDS_SERVER_PORT=9090
//	RTMDS_REDIS_ADDR=localhost:6379
//	RTMDS_DATABASE_PASSWORD=secret
//
// All configuration is validated at startup. Invalid configuration causes
// immediate startup failure with a clear error message.
package config

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/sumit/rtmds/internal/exchange"
)

// Config is the root application configuration.
type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Redis     RedisConfig     `mapstructure:"redis"`
	Feed      FeedConfig      `mapstructure:"feed"`
	Log       LogConfig       `mapstructure:"log"`
	Metrics   MetricsConfig   `mapstructure:"metrics"`
	Discovery DiscoveryConfig `mapstructure:"discovery"`
	Database  DatabaseConfig  `mapstructure:"database"`
	Snapshot  SnapshotConfig  `mapstructure:"snapshot"`
	Tracing   TracingConfig   `mapstructure:"tracing"`
	Exchange  ExchangeConfig  `mapstructure:"exchange"`
	Profiling ProfilingConfig `mapstructure:"profiling"`
}

// ServerConfig holds HTTP / WebSocket listener settings.
type ServerConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	GatewayID       string        `mapstructure:"gateway_id"` // unique identifier for sticky session routing
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// RedisConfig holds connection details for the Redis pub/sub broker.
type RedisConfig struct {
	Enabled  bool          `mapstructure:"enabled"`
	Addr     string        `mapstructure:"addr"`
	Password SecretString  `mapstructure:"password"`
	DB       int           `mapstructure:"db"`
}

// FeedConfig holds settings for upstream market-data feed providers.
type FeedConfig struct {
	// Enabled controls whether this instance runs the feed generator.
	// Disable on subscriber-only gateways in multi-instance deployments.
	Enabled bool `mapstructure:"enabled"`
	// Symbols is the list of ticker symbols to subscribe to on startup.
	Symbols []string `mapstructure:"symbols"`
	// ReconnectDelay is the back-off wait between feed reconnect attempts.
	ReconnectDelay time.Duration `mapstructure:"reconnect_delay"`
	// TickInterval overrides the simulator tick interval. Lower values
	// produce higher message rates for benchmarking. 0 uses default (500ms).
	TickInterval time.Duration `mapstructure:"tick_interval"`
	// BenchmarkMode enables high-throughput feed generation (~50K msg/sec).
	BenchmarkMode bool `mapstructure:"benchmark_mode"`
}

// ExchangeConfig holds settings for the Exchange Adapter Framework.
type ExchangeConfig struct {
	Adapters []exchange.AdapterConfig `mapstructure:"adapters"`
}

// LogConfig controls structured logging behaviour.
type LogConfig struct {
	Level  string `mapstructure:"level"`  // debug | info | warn | error
	Format string `mapstructure:"format"` // json | text
}

// MetricsConfig exposes Prometheus scrape endpoint settings.
type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

// DatabaseConfig holds PostgreSQL connection settings for the event log.
type DatabaseConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	User         string        `mapstructure:"user"`
	Password     SecretString  `mapstructure:"password"`
	DBName       string        `mapstructure:"dbname"`
	SSLMode      string        `mapstructure:"sslmode"`
	MaxOpenConns int           `mapstructure:"max_open_conns"`
	MaxIdleConns int           `mapstructure:"max_idle_conns"`
	MaxLifetime  time.Duration `mapstructure:"max_lifetime"`
}

// DSN returns the PostgreSQL connection string.
func (c DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password.Value(), c.DBName, c.SSLMode,
	)
}

// DiscoveryConfig controls Redis-based service discovery for gateway
// registration and health-aware routing.
type DiscoveryConfig struct {
	// Enabled controls whether this gateway registers with the service
	// discovery registry. Requires Redis to be enabled.
	Enabled bool `mapstructure:"enabled"`
	// TTL is the time-to-live for a gateway registration. If a gateway
	// fails to heartbeat within this window, its entry is removed.
	TTL time.Duration `mapstructure:"ttl"`
	// HeartbeatInterval is how often the gateway refreshes its TTL.
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
}

// SnapshotConfig controls snapshot persistence and recovery.
type SnapshotConfig struct {
	// Enabled controls whether snapshots are checkpointed to disk.
	Enabled bool `mapstructure:"enabled"`
	// CheckpointPath is the file path for the snapshot checkpoint.
	CheckpointPath string `mapstructure:"checkpoint_path"`
	// CheckpointInterval is how often to persist snapshots to disk.
	CheckpointInterval time.Duration `mapstructure:"checkpoint_interval"`
	// MaxAge is the TTL for inactive symbols in the snapshot cache.
	// Symbols not updated within this duration are evicted.
	MaxAge time.Duration `mapstructure:"max_age"`
}

// TracingConfig controls OpenTelemetry distributed tracing.
type TracingConfig struct {
	// Enabled controls whether tracing is active. When false, a noop
	// provider is installed and all span operations are no-ops.
	Enabled bool `mapstructure:"enabled"`
	// Endpoint is the OTLP collector URL (e.g., "localhost:4318").
	Endpoint string `mapstructure:"endpoint"`
	// SampleRatio is the fraction of traces to sample (0.0–1.0).
	// Default 0.01 = 1% of traces.
	SampleRatio float64 `mapstructure:"sample_ratio"`
}

// ProfilingConfig controls runtime profiling rates (Mutex and Block profiling).
type ProfilingConfig struct {
	// Enabled controls whether runtime profiling overrides are applied on startup.
	Enabled bool `mapstructure:"enabled"`
	// MutexFraction controls the fraction of mutex contention events that are reported in the mutex profile.
	// 0 disables mutex profiling. 1 profiles everything. Higher values profile less.
	MutexFraction int `mapstructure:"mutex_fraction"`
	// BlockRate controls the fraction of goroutine blocking events that are reported in the blocking profile.
	// 0 disables block profiling.
	BlockRate int `mapstructure:"block_rate"`
}

// knownKeys is the complete set of valid configuration keys.
// Any key in a config file not in this list causes a startup failure.
// This prevents silent typos (e.g., REDIS_POTR=6380 → wrong port).
var knownKeys = map[string]bool{
	"server.host": true, "server.port": true, "server.gateway_id": true,
	"server.read_timeout": true, "server.write_timeout": true, "server.shutdown_timeout": true,
	"redis.enabled": true, "redis.addr": true, "redis.password": true, "redis.db": true,
	"feed.enabled": true, "feed.symbols": true, "feed.reconnect_delay": true,
	"feed.tick_interval": true, "feed.benchmark_mode": true,
	"log.level": true, "log.format": true,
	"metrics.enabled": true, "metrics.path": true,
	"database.enabled": true, "database.host": true, "database.port": true,
	"database.user": true, "database.password": true, "database.dbname": true,
	"database.sslmode": true, "database.max_open_conns": true,
	"database.max_idle_conns": true, "database.max_lifetime": true,
	"snapshot.enabled": true, "snapshot.checkpoint_path": true,
	"snapshot.checkpoint_interval": true, "snapshot.max_age": true,
	"discovery.enabled": true, "discovery.ttl": true, "discovery.heartbeat_interval": true,
	"tracing.enabled": true, "tracing.endpoint": true, "tracing.sample_ratio": true,
	"exchange.adapters": true,
	"profiling.enabled": true, "profiling.mutex_fraction": true, "profiling.block_rate": true,
}

// Load reads configuration from environment variables (prefix RTMDS_) and, if
// present, a config file at the given path. Returns an error on validation
// failure.
func Load(cfgFile string) (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 9090)
	v.SetDefault("server.gateway_id", "") // empty = auto-generate from port
	v.SetDefault("server.read_timeout", "10s")
	v.SetDefault("server.write_timeout", "10s")
	v.SetDefault("server.shutdown_timeout", "15s")
	v.SetDefault("redis.enabled", false)
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.db", 0)
	v.SetDefault("feed.enabled", true)
	v.SetDefault("feed.symbols", []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"})
	v.SetDefault("feed.reconnect_delay", "5s")
	v.SetDefault("feed.tick_interval", "0s") // 0 = use simulator default
	v.SetDefault("feed.benchmark_mode", false)
	
	v.SetDefault("exchange.adapters", []map[string]interface{}{
		{
			"name": "simulator",
			"enabled": true,
			"symbols": []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"},
		},
		{
			"name": "nasdaq",
			"enabled": true,
			"symbols": []string{"META", "NFLX"},
		},
		{
			"name": "nyse",
			"enabled": true,
			"symbols": []string{"IBM", "JPM"},
		},
		{
			"name": "crypto",
			"enabled": true,
			"symbols": []string{"BTC-USD", "ETH-USD"},
		},
	})

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("discovery.enabled", false)
	v.SetDefault("discovery.ttl", "30s")
	v.SetDefault("discovery.heartbeat_interval", "10s")
	v.SetDefault("database.enabled", false)
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "postgres")
	v.SetDefault("database.password", "")
	v.SetDefault("database.dbname", "rtmds")
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("database.max_open_conns", 10)
	v.SetDefault("database.max_idle_conns", 5)
	v.SetDefault("database.max_lifetime", "5m")
	v.SetDefault("snapshot.enabled", true)
	v.SetDefault("snapshot.checkpoint_path", "data/snapshot.json")
	v.SetDefault("snapshot.checkpoint_interval", "30s")
	v.SetDefault("snapshot.max_age", "24h")
	v.SetDefault("tracing.enabled", false)
	v.SetDefault("tracing.endpoint", "localhost:4318")
	v.SetDefault("tracing.sample_ratio", 0.01)
	v.SetDefault("profiling.enabled", false)
	v.SetDefault("profiling.mutex_fraction", 0)
	v.SetDefault("profiling.block_rate", 0)

	// Environment variables
	v.SetEnvPrefix("RTMDS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Load secrets from files if _FILE environment variables are present
	if file := os.Getenv("RTMDS_DATABASE_PASSWORD_FILE"); file != "" {
		if b, err := os.ReadFile(file); err == nil {
			os.Setenv("RTMDS_DATABASE_PASSWORD", strings.TrimSpace(string(b)))
		}
	}
	if file := os.Getenv("RTMDS_REDIS_PASSWORD_FILE"); file != "" {
		if b, err := os.ReadFile(file); err == nil {
			os.Setenv("RTMDS_REDIS_PASSWORD", strings.TrimSpace(string(b)))
		}
	}

	v.AutomaticEnv()

	// Config file (optional)
	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("config: read file %q: %w", cfgFile, err)
		}

		// Strict key validation: fail on unknown keys to catch typos.
		// Without this, REDIS_POTR=6380 would silently use the default port.
		if err := validateUnknownKeys(v); err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			// Decode strings into SecretString type
			decodeSecretString(),
			// Decode comma-separated strings into []string slices
			decodeStringToSlice(),
			// Decode duration strings (Viper handles this, but we need the hook)
			mapstructure.StringToTimeDurationHookFunc(),
		),
	)); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: validation: %w", err)
	}

	return &cfg, nil
}

// decodeSecretString returns a mapstructure decode hook that converts
// strings to SecretString types during config unmarshalling.
func decodeSecretString() mapstructure.DecodeHookFuncType {
	return func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
		if to != reflect.TypeOf(SecretString{}) {
			return data, nil
		}
		if from.Kind() != reflect.String {
			return data, nil
		}
		return NewSecretString(data.(string)), nil
	}
}

// decodeStringToSlice returns a mapstructure decode hook that converts
// comma-separated strings to []string slices. This allows environment
// variables like RTMDS_FEED_SYMBOLS=AAPL,TSLA,MSFT to work correctly.
func decodeStringToSlice() mapstructure.DecodeHookFuncType {
	return func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
		if to != reflect.TypeOf([]string{}) {
			return data, nil
		}
		if from.Kind() != reflect.String {
			return data, nil
		}
		s := data.(string)
		if s == "" {
			return []string{}, nil
		}
		parts := strings.Split(s, ",")
		result := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				result = append(result, p)
			}
		}
		return result, nil
	}
}

// validateUnknownKeys checks that all keys in the config file are known.
// This prevents silent failures from typos (e.g., REDIS_POTR=6380).
func validateUnknownKeys(v *viper.Viper) error {
	// Get all keys from all sources (defaults + file)
	allKeys := v.AllKeys()

	var unknown []string
	for _, key := range allKeys {
		// Environment variables are not checked — they use RTMDS_ prefix
		// and are handled by Viper's AutomaticEnv. Only check file keys.
		if v.IsSet(key) && !knownKeys[key] {
			unknown = append(unknown, key)
		}
	}

	if len(unknown) > 0 {
		return fmt.Errorf("unknown configuration keys: %s (check for typos — valid keys: %s)",
			strings.Join(unknown, ", "), formatKnownKeys())
	}
	return nil
}

// formatKnownKeys returns a comma-separated list of valid config keys.
func formatKnownKeys() string {
	keys := make([]string, 0, len(knownKeys))
	for k := range knownKeys {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}

// validate checks all configuration values at startup.
// Fails fast on any invalid configuration.
func (c *Config) validate() error {
	var errs []string

	// ── Server Validation ───────────────────────────────────────────
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		errs = append(errs, fmt.Sprintf("server.port %d is out of range [1, 65535]", c.Server.Port))
	}
	if c.Server.ReadTimeout <= 0 {
		errs = append(errs, "server.read_timeout must be positive")
	}
	if c.Server.WriteTimeout <= 0 {
		errs = append(errs, "server.write_timeout must be positive")
	}
	if c.Server.ShutdownTimeout <= 0 {
		errs = append(errs, "server.shutdown_timeout must be positive")
	}
	if c.Server.ShutdownTimeout < c.Server.ReadTimeout {
		errs = append(errs, "server.shutdown_timeout should be >= read_timeout for graceful shutdown")
	}

	// ── Redis Validation ────────────────────────────────────────────
	if c.Redis.Enabled {
		if c.Redis.Addr == "" {
			errs = append(errs, "redis.addr must not be empty when redis is enabled")
		}
		if _, _, err := net.SplitHostPort(c.Redis.Addr); err != nil {
			errs = append(errs, fmt.Sprintf("redis.addr %q is not a valid host:port: %v", c.Redis.Addr, err))
		}
		if c.Redis.DB < 0 || c.Redis.DB > 15 {
			errs = append(errs, fmt.Sprintf("redis.db %d is out of range [0, 15]", c.Redis.DB))
		}
	}

	// ── Feed Validation ─────────────────────────────────────────────
	if c.Feed.Enabled {
		if len(c.Feed.Symbols) == 0 {
			errs = append(errs, "feed.symbols must not be empty when feed is enabled")
		}
		for i, sym := range c.Feed.Symbols {
			if sym == "" {
				errs = append(errs, fmt.Sprintf("feed.symbols[%d] must not be empty", i))
			}
			if len(sym) > 10 {
				errs = append(errs, fmt.Sprintf("feed.symbols[%d] %q exceeds max length 10", i, sym))
			}
		}
		if c.Feed.ReconnectDelay <= 0 {
			errs = append(errs, "feed.reconnect_delay must be positive")
		}
	}

	// ── Log Validation ──────────────────────────────────────────────
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[c.Log.Level] {
		errs = append(errs, fmt.Sprintf("log.level %q must be one of: debug, info, warn, error", c.Log.Level))
	}
	validLogFormats := map[string]bool{"json": true, "text": true}
	if !validLogFormats[c.Log.Format] {
		errs = append(errs, fmt.Sprintf("log.format %q must be one of: json, text", c.Log.Format))
	}

	// ── Database Validation ─────────────────────────────────────────
	if c.Database.Enabled {
		if c.Database.Host == "" {
			errs = append(errs, "database.host must not be empty when database is enabled")
		}
		if c.Database.Port < 1 || c.Database.Port > 65535 {
			errs = append(errs, fmt.Sprintf("database.port %d is out of range [1, 65535]", c.Database.Port))
		}
		if c.Database.User == "" {
			errs = append(errs, "database.user must not be empty when database is enabled")
		}
		if c.Database.DBName == "" {
			errs = append(errs, "database.dbname must not be empty when database is enabled")
		}
		validSSLModes := map[string]bool{"disable": true, "require": true, "verify-ca": true, "verify-full": true}
		if !validSSLModes[c.Database.SSLMode] {
			errs = append(errs, fmt.Sprintf("database.sslmode %q must be one of: disable, require, verify-ca, verify-full", c.Database.SSLMode))
		}
		if c.Database.MaxOpenConns < 0 {
			errs = append(errs, "database.max_open_conns must be non-negative")
		}
		if c.Database.MaxIdleConns < 0 {
			errs = append(errs, "database.max_idle_conns must be non-negative")
		}
		if c.Database.MaxIdleConns > c.Database.MaxOpenConns && c.Database.MaxOpenConns > 0 {
			errs = append(errs, "database.max_idle_conns should be <= max_open_conns")
		}
		if c.Database.MaxLifetime <= 0 {
			errs = append(errs, "database.max_lifetime must be positive")
		}
	}

	// ── Snapshot Validation ─────────────────────────────────────────
	if c.Snapshot.Enabled {
		if c.Snapshot.CheckpointPath == "" {
			errs = append(errs, "snapshot.checkpoint_path must not be empty when snapshot is enabled")
		}
		if c.Snapshot.CheckpointInterval <= 0 {
			errs = append(errs, "snapshot.checkpoint_interval must be positive")
		}
		if c.Snapshot.MaxAge <= 0 {
			errs = append(errs, "snapshot.max_age must be positive")
		}
	}

	// ── Discovery Validation ────────────────────────────────────────
	if c.Discovery.Enabled && !c.Redis.Enabled {
		errs = append(errs, "discovery.enabled requires redis.enabled to be true")
	}
	if c.Discovery.Enabled {
		if c.Discovery.TTL <= 0 {
			errs = append(errs, "discovery.ttl must be positive")
		}
		if c.Discovery.HeartbeatInterval <= 0 {
			errs = append(errs, "discovery.heartbeat_interval must be positive")
		}
		if c.Discovery.HeartbeatInterval >= c.Discovery.TTL {
			errs = append(errs, "discovery.heartbeat_interval should be < ttl for reliable registration")
		}
	}

	// ── Metrics Validation ──────────────────────────────────────────
	if c.Metrics.Enabled && c.Metrics.Path == "" {
		errs = append(errs, "metrics.path must not be empty when metrics is enabled")
	}

	// ── Tracing Validation ──────────────────────────────────────────
	if c.Tracing.Enabled {
		if c.Tracing.Endpoint == "" {
			errs = append(errs, "tracing.endpoint must not be empty when tracing is enabled")
		}
		if c.Tracing.SampleRatio < 0 || c.Tracing.SampleRatio > 1.0 {
			errs = append(errs, fmt.Sprintf("tracing.sample_ratio %f must be in range [0.0, 1.0]", c.Tracing.SampleRatio))
		}
	}

	// ── Profiling Validation ────────────────────────────────────────
	if c.Profiling.Enabled {
		if c.Profiling.MutexFraction < 0 {
			errs = append(errs, "profiling.mutex_fraction must be non-negative")
		}
		if c.Profiling.BlockRate < 0 {
			errs = append(errs, "profiling.block_rate must be non-negative")
		}
	}

	// ── Cross-Field Validation ──────────────────────────────────────
	// Replay requires database
	if c.Database.Enabled && !c.Redis.Enabled {
		// This is valid for single-instance mode
	}

	// Return combined errors
	if len(errs) > 0 {
		return fmt.Errorf("configuration validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return nil
}

// Addr returns the combined host:port listen address.
func (s ServerConfig) Addr() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

// GetGatewayID returns the gateway identifier, generating one from the port if not set.
func (s ServerConfig) GetGatewayID() string {
	if s.GatewayID != "" {
		return s.GatewayID
	}
	return fmt.Sprintf("gateway-%d", s.Port)
}

// Summary returns a safe-to-log summary of the configuration.
// Secrets (passwords) are redacted.
func (c *Config) Summary() map[string]interface{} {
	return map[string]interface{}{
		"server": map[string]interface{}{
			"host":            c.Server.Host,
			"port":            c.Server.Port,
			"read_timeout":    c.Server.ReadTimeout,
			"write_timeout":   c.Server.WriteTimeout,
			"shutdown_timeout": c.Server.ShutdownTimeout,
		},
		"redis": map[string]interface{}{
			"enabled": c.Redis.Enabled,
			"addr":    c.Redis.Addr,
			"db":      c.Redis.DB,
		},
		"feed": map[string]interface{}{
			"enabled":         c.Feed.Enabled,
			"symbols":         c.Feed.Symbols,
			"benchmark_mode":  c.Feed.BenchmarkMode,
			"tick_interval":   c.Feed.TickInterval,
		},
		"log": map[string]interface{}{
			"level":  c.Log.Level,
			"format": c.Log.Format,
		},
		"database": map[string]interface{}{
			"enabled":         c.Database.Enabled,
			"host":            c.Database.Host,
			"port":            c.Database.Port,
			"user":            c.Database.User,
			"dbname":          c.Database.DBName,
			"sslmode":         c.Database.SSLMode,
			"max_open_conns":  c.Database.MaxOpenConns,
			"max_idle_conns":  c.Database.MaxIdleConns,
		},
		"snapshot": map[string]interface{}{
			"enabled":           c.Snapshot.Enabled,
			"checkpoint_path":   c.Snapshot.CheckpointPath,
			"checkpoint_interval": c.Snapshot.CheckpointInterval,
			"max_age":           c.Snapshot.MaxAge,
		},
		"discovery": map[string]interface{}{
			"enabled":            c.Discovery.Enabled,
			"ttl":                c.Discovery.TTL,
			"heartbeat_interval": c.Discovery.HeartbeatInterval,
		},
		"tracing": map[string]interface{}{
			"enabled":      c.Tracing.Enabled,
			"endpoint":     c.Tracing.Endpoint,
			"sample_ratio": c.Tracing.SampleRatio,
		},
		"profiling": map[string]interface{}{
			"enabled":        c.Profiling.Enabled,
			"mutex_fraction": c.Profiling.MutexFraction,
			"block_rate":     c.Profiling.BlockRate,
		},
	}
}

// LoadFile is a convenience wrapper that loads configuration from a file path.
// Returns the config or an error if loading/validation fails.
func LoadFile(path string) (*Config, error) {
	return Load(path)
}

// LoadEnv loads configuration from environment variables only (no config file).
func LoadEnv() (*Config, error) {
	return Load("")
}
