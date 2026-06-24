package ratelimit

import (
	"sync/atomic"
	"time"
)

// Config defines rate limits for a single client.
type Config struct {
	// Connect is the max new connection requests per second.
	Connect RateLimit

	// Subscribe is the max subscribe requests per second.
	Subscribe RateLimit

	// Unsubscribe is the max unsubscribe requests per second.
	Unsubscribe RateLimit

	// MaxSubscriptions is the maximum number of active subscriptions
	// per client. 0 = unlimited.
	MaxSubscriptions int

	// ClientTTL is how long a client entry is kept after last access.
	// Stale entries are evicted by a background goroutine.
	// 0 = no eviction (not recommended in production).
	ClientTTL time.Duration
}

// RateLimit defines a token bucket for a single operation type.
type RateLimit struct {
	// Rate is the sustained tokens per second (sustained request rate).
	Rate float64

	// Burst is the maximum number of tokens that can accumulate.
	// Controls how many requests can be sent in a short burst.
	Burst int
}

// DefaultConfig returns a Config with sensible defaults for market data.
func DefaultConfig() Config {
	return Config{
		Connect: RateLimit{
			Rate:  10, // 10 new connections/sec per client identity
			Burst: 20,
		},
		Subscribe: RateLimit{
			Rate:  50,  // 50 subscribe requests/sec sustained
			Burst: 100, // burst up to 100 on reconnect
		},
		Unsubscribe: RateLimit{
			Rate:  20,
			Burst: 50,
		},
		MaxSubscriptions: 1000,
		ClientTTL:        5 * time.Minute,
	}
}

func (c Config) connectBucket() *Bucket {
	rate := c.Connect.Rate
	burst := c.Connect.Burst
	if rate <= 0 {
		rate = 10
	}
	if burst <= 0 {
		burst = 20
	}
	return NewBucket(rate, burst)
}

func (c Config) subscribeBucket() *Bucket {
	rate := c.Subscribe.Rate
	burst := c.Subscribe.Burst
	if rate <= 0 {
		rate = 50
	}
	if burst <= 0 {
		burst = 100
	}
	return NewBucket(rate, burst)
}

func (c Config) unsubscribeBucket() *Bucket {
	rate := c.Unsubscribe.Rate
	burst := c.Unsubscribe.Burst
	if rate <= 0 {
		rate = 20
	}
	if burst <= 0 {
		burst = 50
	}
	return NewBucket(rate, burst)
}

// NoLimits returns a Config with effectively unlimited rates.
// Useful for tests or internal clients.
func NoLimits() Config {
	return Config{
		Connect:   RateLimit{Rate: 1e6, Burst: 1e6},
		Subscribe: RateLimit{Rate: 1e6, Burst: 1e6},
		Unsubscribe: RateLimit{Rate: 1e6, Burst: 1e6},
	}
}

// clientLimits holds the buckets and state for a single client.
type clientLimits struct {
	connect     *Bucket
	subscribe   *Bucket
	unsubscribe *Bucket
	activeSubs  atomic.Int64 // B1 fix: atomic for race-free check+increment
	lastAccess  atomic.Int64 // MG1 fix: Unix nano for TTL eviction, atomic for concurrent writes
}

func newClientLimits(cfg Config) *clientLimits {
	cl := &clientLimits{
		connect:     cfg.connectBucket(),
		subscribe:   cfg.subscribeBucket(),
		unsubscribe: cfg.unsubscribeBucket(),
	}
	cl.lastAccess.Store(time.Now().UnixNano())
	return cl
}

// touch updates the last access time atomically.
func (cl *clientLimits) touch() {
	cl.lastAccess.Store(time.Now().UnixNano())
}

// accessedAt returns the last access time.
func (cl *clientLimits) accessedAt() time.Time {
	return time.Unix(0, cl.lastAccess.Load())
}
