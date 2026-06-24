// Package ratelimit provides token-bucket rate limiting for per-client
// request throttling in the market data gateway.
//
// Algorithm: Token Bucket
//   - Tokens accumulate at a fixed rate (tokensPerSec).
//   - Bucket has a maximum capacity (burstSize).
//   - Each request consumes one token.
//   - If no tokens available, the request is rejected.
//
// Thread-safe for concurrent use by multiple goroutines.
package ratelimit

import (
	"sync"
	"time"
)

// Bucket is a token bucket rate limiter.
// Tokens accumulate at tokensPerSec up to burstSize.
// Each Allow() call consumes one token. Returns false when empty.
//
// Thread-safe for concurrent use.
type Bucket struct {
	mu sync.Mutex

	tokens     float64
	burstSize  float64
	tokensPerSec float64
	lastRefill time.Time
}

// NewBucket creates a token bucket with the given rate and burst capacity.
// tokensPerSec: sustained rate (e.g. 10 = 10 requests/sec).
// burstSize: maximum tokens that can accumulate (e.g. 50 = allow burst of 50).
// Both must be > 0.
func NewBucket(tokensPerSec float64, burstSize int) *Bucket {
	if tokensPerSec <= 0 {
		tokensPerSec = 1
	}
	if burstSize <= 0 {
		burstSize = 1
	}
	return &Bucket{
		tokens:       float64(burstSize),
		burstSize:    float64(burstSize),
		tokensPerSec: tokensPerSec,
		lastRefill:   time.Now(),
	}
}

// Allow attempts to consume one token. Returns true if allowed.
// Refills tokens based on elapsed time since last call.
func (b *Bucket) Allow() bool {
	return b.AllowN(1)
}

// AllowN attempts to consume n tokens. Returns true if all were available.
func (b *Bucket) AllowN(n int) bool {
	if n <= 0 {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill()

	if b.tokens >= float64(n) {
		b.tokens -= float64(n)
		return true
	}
	return false
}

// refill adds tokens based on elapsed time. Must be called under lock.
func (b *Bucket) refill() {
	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}

	b.tokens += elapsed * b.tokensPerSec
	if b.tokens > b.burstSize {
		b.tokens = b.burstSize
	}
	b.lastRefill = now
}

// Tokens returns the current number of available tokens.
// Useful for metrics and debugging.
func (b *Bucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return b.tokens
}

// BurstSize returns the maximum token capacity.
func (b *Bucket) BurstSize() int {
	return int(b.burstSize)
}

// TokensPerSec returns the sustained refill rate.
func (b *Bucket) TokensPerSec() float64 {
	return b.tokensPerSec
}

// Refund returns n tokens to the bucket (up to burst capacity).
// Used by AllowAndSubscribe to undo a token consumption when the
// subscription cap rejects the request.
func (b *Bucket) Refund(n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens += float64(n)
	if b.tokens > b.burstSize {
		b.tokens = b.burstSize
	}
}
