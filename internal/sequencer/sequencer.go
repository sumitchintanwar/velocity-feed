// Package sequencer provides per-symbol sequence number generation and
// ordering validation for the market data pipeline.
//
// Design guarantees (per MESSAGE_ORDERING_GUARANTEES_DESIGN.md):
//   - Each symbol maintains an independent monotonically increasing sequence.
//   - Sequence numbers are assigned at the publisher, before Redis.
//   - Consumers validate ordering and detect gaps per symbol.
package sequencer

import (
	"sync"
	"time"
)

// Generator assigns monotonically increasing sequence numbers per symbol.
type Generator interface {
	// Next returns the next sequence number for the given symbol.
	Next(symbol string) int64
	// Current returns the current (last assigned) sequence for a symbol,
	// or 0 if the symbol has not been seen.
	Current(symbol string) int64
	// Reset clears all sequence state. Primarily useful for tests.
	Reset()
}

// entry tracks the last sequence for a symbol along with a TTL timestamp.
type entry struct {
	seq     int64
	expires time.Time
}

// Sequencer is an in-memory Generator with TTL-based eviction.
// Safe for concurrent use. Sequences start at 1 by default.
type Sequencer struct {
	mu       sync.Mutex
	seqs     map[string]entry // symbol → entry
	start    int64            // initial sequence value (default 1)
	ttl      time.Duration    // how long a symbol's entry lives after last access
	lastEvict time.Time       // last time eviction ran
	evictInt time.Duration   // interval between eviction sweeps
}

// Option configures the in-memory Sequencer.
type Option func(*Sequencer)

// WithTTL sets the TTL for per-symbol sequence entries. After this
// duration of inactivity, the entry is evicted. Default is 5 minutes.
func WithTTL(d time.Duration) Option {
	return func(s *Sequencer) { s.ttl = d }
}

// WithEvictInterval sets how often the eviction sweep runs. Default is 1 minute.
func WithEvictInterval(d time.Duration) Option {
	return func(s *Sequencer) { s.evictInt = d }
}

// New creates an in-memory Sequencer. Sequences start at 1 by default.
func New(opts ...Option) *Sequencer {
	s := &Sequencer{
		seqs:     make(map[string]entry),
		start:    1,
		ttl:      5 * time.Minute,
		evictInt: 1 * time.Minute,
	}
	for _, o := range opts {
		o(s)
	}
	s.lastEvict = time.Now()
	return s
}

// Next returns the next sequence number for the given symbol.
// The first call for a symbol returns the start value (1).
func (s *Sequencer) Next(symbol string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.seqs[symbol]
	if e.seq == 0 {
		e.seq = s.start
	} else {
		e.seq++
	}
	e.expires = time.Now().Add(s.ttl)
	s.seqs[symbol] = e

	s.maybeEvict()

	return e.seq
}

// Current returns the current (last assigned) sequence for a symbol,
// or 0 if the symbol has not been seen.
func (s *Sequencer) Current(symbol string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seqs[symbol].seq
}

// Reset clears all sequence state. Primarily useful for tests.
func (s *Sequencer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seqs = make(map[string]entry)
}

// maybeEvict removes expired entries if enough time has elapsed.
// Must be called with s.mu held.
func (s *Sequencer) maybeEvict() {
	now := time.Now()
	if now.Sub(s.lastEvict) < s.evictInt {
		return
	}
	s.lastEvict = now
	for sym, e := range s.seqs {
		if now.After(e.expires) {
			delete(s.seqs, sym)
		}
	}
}
