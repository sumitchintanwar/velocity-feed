// Package backpressure provides configurable backpressure policies for
// bounded event delivery in a pub/sub system.
//
// Three strategies are supported:
//
//   - DropOldest: evicts the oldest event in the ring buffer when full.
//     The consumer always sees the most recent events.
//   - DropNewest: silently discards incoming events when the buffer is full.
//     The consumer retains whatever it already had, even if stale.
//   - Disconnect: tracks consecutive drop count and signals a callback
//     when a consumer should be forcibly removed.
//
// All policies are safe for concurrent use.
package backpressure

import (
	"fmt"
	"time"
)

// Policy controls what happens when a subscriber's buffer is full.
type Policy int

const (
	// PolicyDropOldest evicts the oldest event to make room for the new one.
	// Best for market data — latest price always available.
	PolicyDropOldest Policy = iota

	// PolicyDropNewest discards the incoming event. The buffer retains
	// whatever it already holds, which may be stale.
	PolicyDropNewest

	// PolicyDisconnect triggers a callback after sustained drops.
	// Does not manage a buffer itself — use with PolicyDropOldest or
	// PolicyDropNewest for actual buffering.
	PolicyDisconnect
)

func (p Policy) String() string {
	switch p {
	case PolicyDropOldest:
		return "drop_oldest"
	case PolicyDropNewest:
		return "drop_newest"
	case PolicyDisconnect:
		return "disconnect"
	default:
		return fmt.Sprintf("Policy(%d)", int(p))
	}
}

// ParsePolicy converts a string to a Policy.
func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "drop_oldest", "drop-oldest", "oldest":
		return PolicyDropOldest, nil
	case "drop_newest", "drop-newest", "newest":
		return PolicyDropNewest, nil
	case "disconnect":
		return PolicyDisconnect, nil
	default:
		return 0, fmt.Errorf("unknown backpressure policy: %q", s)
	}
}

// Config controls the backpressure behavior for a subscriber channel.
type Config struct {
	// Policy determines the behavior when the buffer is full.
	Policy Policy

	// BufferSize is the maximum number of events the subscriber can hold
	// before backpressure activates. Must be > 0.
	BufferSize int

	// MaxConsecutiveDrops is the maximum number of consecutive drops
	// before the Disconnect policy triggers. Only meaningful when
	// Policy is PolicyDisconnect. Set to 0 to disable disconnect.
	MaxConsecutiveDrops int

	// DropWindow is the time window over which sustained drop rate is
	// measured. When the drop rate exceeds DropThreshold for this
	// duration, the consumer is flagged. Only meaningful for
	// PolicyDisconnect. Zero disables windowed detection.
	DropWindow time.Duration

	// DropThreshold is the fraction of events dropped (0.0–1.0) that
	// triggers disconnection within DropWindow. Only meaningful for
	// PolicyDisconnect.
	DropThreshold float64

	// MaxAge, if > 0, drops events older than this duration.
	// Enforced during Push to prevent stale data delivery.
	// At 10k msgs/sec, even 100ms of buffering is too old for
	// real-time market data.
	MaxAge time.Duration
}

// DefaultConfig returns a Config tuned for market data workloads:
// drop-oldest with a 256-event ring buffer.
func DefaultConfig() Config {
	return Config{
		Policy:              PolicyDropOldest,
		BufferSize:          256,
		MaxConsecutiveDrops: 100,
		DropWindow:          10 * time.Second,
		DropThreshold:       0.9,
	}
}

// Validate checks the config for invariant violations.
func (c Config) Validate() error {
	if c.BufferSize <= 0 {
		return fmt.Errorf("backpressure: BufferSize must be > 0, got %d", c.BufferSize)
	}
	if c.Policy == PolicyDisconnect && c.MaxConsecutiveDrops <= 0 {
		return fmt.Errorf("backpressure: MaxConsecutiveDrops must be > 0 for disconnect policy")
	}
	if c.DropThreshold < 0 || c.DropThreshold > 1 {
		return fmt.Errorf("backpressure: DropThreshold must be in [0,1], got %f", c.DropThreshold)
	}
	return nil
}

// DisconnectFunc is called when a consumer should be disconnected due to
// sustained backpressure. The caller is responsible for closing the
// connection and freeing resources.
type DisconnectFunc func(reason string)
