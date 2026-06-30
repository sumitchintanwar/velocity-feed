package exchange

import "time"

// Purpose: Provides uniform configuration structures for all exchange adapters.
// Architecture: Configuration-driven design allows enabling/disabling adapters dynamically at runtime without code changes.
// Design Decisions: A generic Custom map is used for adapter-specific settings (like API keys) to allow flexibility without breaking the schema.

// AdapterConfig holds configuration for a specific exchange adapter.
type AdapterConfig struct {
	// Name identifies the adapter factory to use (e.g., "simulator", "nasdaq_mock").
	Name string `json:"name" yaml:"name" mapstructure:"name"`
	// Enabled determines if this adapter should be instantiated and started.
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	// Endpoint is the primary URL or address to connect to.
	Endpoint string `json:"endpoint" yaml:"endpoint" mapstructure:"endpoint"`
	// Symbols is the initial list of symbols to subscribe to upon connection.
	Symbols []string `json:"symbols" yaml:"symbols" mapstructure:"symbols"`
	// ReconnectDelay is the base backoff duration when a connection drops.
	ReconnectDelay time.Duration `json:"reconnect_delay" yaml:"reconnect_delay" mapstructure:"reconnect_delay"`
	// Custom holds adapter-specific configuration (auth tokens, protocol modes, etc.).
	Custom map[string]interface{} `json:"custom" yaml:"custom" mapstructure:"custom"`
}
