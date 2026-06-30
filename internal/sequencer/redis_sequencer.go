package sequencer

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisSequencer is a distributed Generator backed by Redis INCR.
// It provides consistent sequence numbers across multiple instances.
type RedisSequencer struct {
	client *redis.Client
	prefix string // Redis key prefix, e.g. "seq:"
}

// NewRedisSequencer creates a Redis-backed sequencer. The prefix
// namespace isolates sequence keys from other Redis data.
func NewRedisSequencer(client *redis.Client, prefix string) *RedisSequencer {
	if prefix == "" {
		prefix = "seq:"
	}
	return &RedisSequencer{
		client: client,
		prefix: prefix,
	}
}

// Next atomically increments and returns the next sequence for the symbol.
// The first call for a symbol returns 1 (Redis INCR starts from 0).
func (r *RedisSequencer) Next(symbol string) int64 {
	key := r.prefix + symbol
	val, err := r.client.Incr(context.Background(), key).Result()
	if err != nil {
		// Fallback: log and return 0. In production, metrics/alerting
		// should fire here. Returning 0 matches the "unseen" contract.
		return 0
	}
	return val
}

// Current returns the current sequence for a symbol, or 0 if unseen.
func (r *RedisSequencer) Current(symbol string) int64 {
	key := r.prefix + symbol
	val, err := r.client.Get(context.Background(), key).Int64()
	if err != nil {
		return 0
	}
	return val
}

// Reset clears all sequence state for the given symbols.
// If no symbols are provided, it clears all keys matching the prefix.
// This satisfies the Generator interface (no-arg Reset).
func (r *RedisSequencer) Reset() {
	r.resetAll()
}

// ResetSymbols clears sequence state for specific symbols.
func (r *RedisSequencer) ResetSymbols(symbols ...string) {
	ctx := context.Background()
	if len(symbols) == 0 {
		r.resetAll()
		return
	}
	for _, sym := range symbols {
		r.client.Del(ctx, r.prefix+sym)
	}
}

// resetAll scans and deletes all keys matching the prefix.
func (r *RedisSequencer) resetAll() {
	ctx := context.Background()
	var cursor uint64
	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, r.prefix+"*", 100).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			r.client.Del(ctx, keys...)
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}

// SetTTL sets an expiration on a symbol's sequence key. Useful for
// reclaiming Redis memory for inactive symbols.
func (r *RedisSequencer) SetTTL(symbol string, ttl time.Duration) error {
	key := r.prefix + symbol
	return r.client.Expire(context.Background(), key, ttl).Err()
}

// Key returns the full Redis key for a symbol (for testing/debugging).
func (r *RedisSequencer) Key(symbol string) string {
	return fmt.Sprintf("%s%s", r.prefix, symbol)
}
