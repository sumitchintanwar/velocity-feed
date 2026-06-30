package sequencer

import (
	"hash/fnv"
	"sync"
	"time"
)

const numShards = 16

// ValidationResult describes the outcome of validating a single message.
type ValidationResult int

const (
	// Ok means the message arrived in the expected position (no disorder).
	Ok ValidationResult = iota
	// Gap means one or more sequences were skipped.
	Gap
	// OutOfOrder means the message arrived before its predecessor.
	OutOfOrder
	// Duplicate means the message was already seen.
	Duplicate
)

// String returns a human-readable label for the result.
func (r ValidationResult) String() string {
	switch r {
	case Ok:
		return "ok"
	case Gap:
		return "gap"
	case OutOfOrder:
		return "out_of_order"
	case Duplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}

// GapInfo describes a detected gap between two sequence numbers.
type GapInfo struct {
	Symbol   string
	Expected int64
	Received int64
	Missing  int64 // number of missing sequences (Received - Expected)
}

// Stats tracks aggregate ordering metrics for a single symbol.
type Stats struct {
	TotalReceived   int64
	GapsDetected    int64
	OutOfOrderCount int64
	Duplicates      int64
	LatestSeq       int64
	LastSeen        time.Time // for TTL eviction
}

// shard is a partition of the validator state protected by its own mutex.
type shard struct {
	mu    sync.RWMutex
	stats map[string]*Stats
}

// Validator tracks per-symbol last-seen sequence numbers and validates
// that incoming messages arrive in order. Safe for concurrent use.
// Uses sharded mutexes to reduce lock contention under high concurrency.
type Validator struct {
	shards    [numShards]shard
	ttl       time.Duration
	lastEvict time.Time
	evictInt  time.Duration
}

// ValidatorOption configures the Validator.
type ValidatorOption func(*Validator)

// WithValidatorTTL sets the TTL for per-symbol stats entries. After
// this duration of inactivity, the entry is evicted. Default is 5 minutes.
func WithValidatorTTL(d time.Duration) ValidatorOption {
	return func(v *Validator) { v.ttl = d }
}

// WithValidatorEvictInterval sets how often the eviction sweep runs.
// Default is 1 minute.
func WithValidatorEvictInterval(d time.Duration) ValidatorOption {
	return func(v *Validator) { v.evictInt = d }
}

// NewValidator creates a Validator ready for use.
func NewValidator(opts ...ValidatorOption) *Validator {
	v := &Validator{
		ttl:      5 * time.Minute,
		evictInt: 1 * time.Minute,
	}
	for _, o := range opts {
		o(v)
	}
	for i := range v.shards {
		v.shards[i].stats = make(map[string]*Stats)
	}
	v.lastEvict = time.Now()
	return v
}

// shardIndex returns the shard for a given symbol using FNV-1a hashing.
func shardIndex(symbol string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(symbol))
	return h.Sum32() % numShards
}

// Validate checks the ordering of a message for the given symbol.
// It returns the validation result and, when a gap is detected, the
// missing sequence range. Automatically triggers periodic eviction.
func (v *Validator) Validate(symbol string, seq int64) (ValidationResult, *GapInfo) {
	v.maybeEvict()

	si := shardIndex(symbol)
	sh := &v.shards[si]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	st, exists := sh.stats[symbol]
	if !exists {
		st = &Stats{}
		sh.stats[symbol] = st
	}

	st.TotalReceived++
	st.LastSeen = time.Now()
	prevLast := st.LatestSeq

	// First message for this symbol.
	if prevLast == 0 {
		st.LatestSeq = seq
		return Ok, nil
	}

	// Duplicate: already seen.
	if seq == prevLast {
		st.Duplicates++
		return Duplicate, nil
	}

	// Out of order: arrived before its predecessor.
	if seq < prevLast {
		st.OutOfOrderCount++
		return OutOfOrder, nil
	}

	// In order: seq > prevLast.
	expected := prevLast + 1
	if seq == expected {
		// Perfect in-order delivery.
		st.LatestSeq = seq
		return Ok, nil
	}

	// seq > expected: gap detected.
	st.GapsDetected++
	st.LatestSeq = seq
	return Ok, &GapInfo{
		Symbol:   symbol,
		Expected: expected,
		Received: seq,
		Missing:  seq - expected,
	}
}

// StatsFor returns a copy of the stats for a symbol.
func (v *Validator) StatsFor(symbol string) Stats {
	si := shardIndex(symbol)
	sh := &v.shards[si]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if st, ok := sh.stats[symbol]; ok {
		return *st
	}
	return Stats{}
}

// Reset clears all validation state.
func (v *Validator) Reset() {
	for i := range v.shards {
		v.shards[i].mu.Lock()
		v.shards[i].stats = make(map[string]*Stats)
		v.shards[i].mu.Unlock()
	}
}

// Evict removes entries older than the configured TTL. Called automatically
// by Validate at the configured interval. Can also be called explicitly.
func (v *Validator) Evict() {
	v.maybeEvict()
}

// maybeEvict removes entries older than the configured TTL if enough
// time has elapsed since the last sweep. Must be called without locks held.
func (v *Validator) maybeEvict() {
	now := time.Now()
	if now.Sub(v.lastEvict) < v.evictInt {
		return
	}
	v.lastEvict = now
	for i := range v.shards {
		sh := &v.shards[i]
		sh.mu.Lock()
		for sym, st := range sh.stats {
			if now.Sub(st.LastSeen) > v.ttl {
				delete(sh.stats, sym)
			}
		}
		sh.mu.Unlock()
	}
}
