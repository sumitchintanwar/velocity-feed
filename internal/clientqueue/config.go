package clientqueue

import (
	"time"

	"github.com/sumit/rtmds/internal/backpressure"
)

// Config controls the behavior of a per-client queue.
type Config struct {
	// QueueSize is the maximum number of events the queue can hold.
	// Recommended range: 64–128 for real-time market data.
	// Must be > 0.
	QueueSize int

	// Policy determines how the queue behaves when full.
	// Defaults to backpressure.PolicyDropOldest.
	Policy backpressure.Policy

	// MaxConsecutiveDrops triggers a disconnect after this many
	// consecutive drops. Only used with PolicyDisconnect.
	// Set to 0 to disable (default).
	MaxConsecutiveDrops int

	// DropWindow is the time window for measuring sustained drop rate.
	// Only used with PolicyDisconnect.
	DropWindow time.Duration

	// DropThreshold is the minimum drop rate (0.0–1.0) within DropWindow
	// to trigger a disconnect. Only used with PolicyDisconnect.
	DropThreshold float64

	// MaxAge, if > 0, drops events older than this duration.
	// Recommended: 100ms for real-time market data.
	// At 10k msgs/sec, even 100ms of buffering is too old.
	MaxAge time.Duration
}

// DefaultConfig returns a Config with sensible defaults for market data.
// Queue size is 64 (reduced from 256) to enforce tighter backpressure.
// MaxAge is 100ms — events older than this are dropped to prevent stale data.
func DefaultConfig() Config {
	return Config{
		QueueSize: 64,
		Policy:    backpressure.PolicyDropOldest,
		MaxAge:    100 * time.Millisecond,
	}
}

// backpressureConfig converts a clientqueue Config to a backpressure Config.
func (c Config) backpressureConfig() backpressure.Config {
	return backpressure.Config{
		Policy:              c.Policy,
		BufferSize:          c.QueueSize,
		MaxConsecutiveDrops: c.MaxConsecutiveDrops,
		DropWindow:          c.DropWindow,
		DropThreshold:       c.DropThreshold,
		MaxAge:              c.MaxAge,
	}
}
