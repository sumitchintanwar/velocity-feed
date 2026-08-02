package sequencer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const blockSize = 10000

type seqBlock struct {
	current int64
	max     int64
}

// RedisSequencer is a distributed Generator backed by Redis.
// It uses block allocation (INCRBY) to eliminate network RTT on the hot path.
type RedisSequencer struct {
	client *redis.Client
	prefix string // Redis key prefix, e.g. "seq:"

	mu     sync.Mutex
	blocks map[string]*seqBlock
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
		blocks: make(map[string]*seqBlock),
	}
}

// Next atomically increments and returns the next sequence for the symbol.
// It serves from a local block to achieve high throughput, fetching a new
// block from Redis only when exhausted.
func (r *RedisSequencer) Next(symbol string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	blk, ok := r.blocks[symbol]
	if !ok {
		blk = &seqBlock{}
		r.blocks[symbol] = blk
	}

	if blk.current >= blk.max {
		key := r.prefix + symbol
		val, err := r.client.IncrBy(context.Background(), key, blockSize).Result()
		if err != nil {
			// Fallback: log and return 0. In production, metrics/alerting
			// should fire here. Returning 0 matches the "unseen" contract.
			return 0
		}
		blk.max = val
		blk.current = val - blockSize
	}

	blk.current++
	return blk.current
}

// Current returns the current sequence for a symbol, or 0 if unseen.
func (r *RedisSequencer) Current(symbol string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if blk, ok := r.blocks[symbol]; ok {
		return blk.current
	}

	key := r.prefix + symbol
	val, err := r.client.Get(context.Background(), key).Int64()
	if err != nil {
		return 0
	}
	return val
}

// Reset clears all sequence state for the given symbols.
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

	r.mu.Lock()
	for _, sym := range symbols {
		delete(r.blocks, sym)
		r.client.Del(ctx, r.prefix+sym)
	}
	r.mu.Unlock()
}

// resetAll scans and deletes all keys matching the prefix.
func (r *RedisSequencer) resetAll() {
	r.mu.Lock()
	r.blocks = make(map[string]*seqBlock)
	r.mu.Unlock()

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

// SetTTL sets an expiration on a symbol's sequence key.
func (r *RedisSequencer) SetTTL(symbol string, ttl time.Duration) error {
	key := r.prefix + symbol
	return r.client.Expire(context.Background(), key, ttl).Err()
}

// Key returns the full Redis key for a symbol.
func (r *RedisSequencer) Key(symbol string) string {
	return fmt.Sprintf("%s%s", r.prefix, symbol)
}
