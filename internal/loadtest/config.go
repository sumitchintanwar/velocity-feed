// Package loadtest provides a WebSocket load testing tool for the
// market data gateway. It creates configurable numbers of clients,
// measures end-to-end latency, and reports throughput metrics.
package loadtest

import "time"

// Config controls the load test behavior.
type Config struct {
	// ServerURL is the WebSocket gateway URL (e.g. "ws://localhost:8080/ws").
	ServerURL string

	// Connections is the number of WebSocket clients to create.
	Connections int

	// Symbols is the list of symbols to subscribe to.
	// Each client subscribes to all symbols.
	Symbols []string

	// Duration is how long to run the load test.
	Duration time.Duration

	// RampUp is the time to stagger connection establishment.
	// 0 = all connections simultaneous.
	RampUp time.Duration

	// ReadDelay is an artificial delay applied to slow consumers.
	// 0 = fast consumer (reads immediately).
	ReadDelay time.Duration

	// ReportInterval is how often to print progress.
	ReportInterval time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		ServerURL:       "ws://localhost:8080/ws",
		Connections:     100,
		Symbols:         []string{"AAPL", "MSFT", "GOOG", "TSLA", "NVDA"},
		Duration:        30 * time.Second,
		RampUp:          0,
		ReadDelay:       0,
		ReportInterval:  5 * time.Second,
	}
}

// Result holds the final load test results.
type Result struct {
	// Connection stats.
	ConnectionsAttempted int
	ConnectionsSucceeded int
	ConnectionsFailed    int
	ConnectionTime       time.Duration // total time to establish all connections

	// Throughput.
	MessagesSent     int64
	MessagesReceived int64
	MsgPerSec        float64

	// Latency (end-to-end: server timestamp → client receive).
	Latency LatencyStats

	// Errors.
	Errors []string
}

// LatencyStats holds latency percentile data.
type LatencyStats struct {
	Min   time.Duration
	Max   time.Duration
	Mean  time.Duration
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	P999  time.Duration
	Count int64
}
