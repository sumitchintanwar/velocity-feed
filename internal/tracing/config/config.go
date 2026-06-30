// Package config provides OpenTelemetry configuration structures.
package config

// Config encapsulates settings for the OpenTelemetry TracerProvider.
type Config struct {
	ServiceName     string  // e.g. "marketdata-gateway"
	Environment     string  // e.g. "production", "staging"
	OTLPEndpoint    string  // e.g. "localhost:4318"
	Insecure        bool    // Set to true for local testing without TLS
	SamplingRate    float64 // 0.0 to 1.0
}

// DefaultConfig returns a secure default configuration for production.
func DefaultConfig() *Config {
	return &Config{
		ServiceName:  "rtmds",
		Environment:  "production",
		OTLPEndpoint: "localhost:4318",
		Insecure:     false,
		SamplingRate: 0.01, // 1% sampling default for production
	}
}
