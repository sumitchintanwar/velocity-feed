// Package config provides configuration structures for the metrics framework.
package config

// Config holds the configuration required to initialize the metrics framework.
type Config struct {
	// Enabled determines if metrics collection and the exporter endpoint are active.
	Enabled bool
	
	// Namespace is used as a global prefix for all metrics (e.g., "marketdata").
	Namespace string

	// Path is the HTTP path where metrics are exposed (default: "/metrics").
	Path string
}

// DefaultConfig returns a sane default configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:   true,
		Namespace: "marketdata",
		Path:      "/metrics",
	}
}
