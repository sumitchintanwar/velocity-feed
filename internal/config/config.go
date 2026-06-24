// Package config loads and validates application configuration from environment
// variables and an optional YAML/TOML config file via Viper.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config is the root application configuration.
type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Redis     RedisConfig     `mapstructure:"redis"`
	Feed      FeedConfig      `mapstructure:"feed"`
	Log       LogConfig       `mapstructure:"log"`
	Metrics   MetricsConfig   `mapstructure:"metrics"`
	Discovery DiscoveryConfig `mapstructure:"discovery"`
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
	Enabled  bool   `mapstructure:"enabled"`
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
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
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("discovery.enabled", false)
	v.SetDefault("discovery.ttl", "30s")
	v.SetDefault("discovery.heartbeat_interval", "10s")

	// Environment variables
	v.SetEnvPrefix("RTMDS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Config file (optional)
	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("config: read file %q: %w", cfgFile, err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: validation: %w", err)
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port %d is out of range [1, 65535]", c.Server.Port)
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("redis.addr must not be empty")
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
