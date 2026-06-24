package loadtest

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const _shardCount = 16

// LatencyCollector collects latency samples and computes percentiles.
// Thread-safe for concurrent use by multiple client goroutines.
// Uses sharded storage to minimize lock contention at high connection counts.
type LatencyCollector struct {
	// Aggregate stats via atomics (no lock needed for Record).
	min    atomic.Int64 // nanoseconds, initialized to MaxInt64
	max    atomic.Int64 // nanoseconds
	sum    atomic.Int64 // nanoseconds
	total  atomic.Int64

	// Sharded sample storage for percentile computation.
	shards [_shardCount]struct {
		mu      sync.Mutex
		samples []time.Duration
	}
}

// NewLatencyCollector creates a collector with pre-allocated capacity.
func NewLatencyCollector(estimate int) *LatencyCollector {
	lc := &LatencyCollector{}
	lc.min.Store(math.MaxInt64)
	perShard := estimate / _shardCount
	if perShard < 64 {
		perShard = 64
	}
	for i := range lc.shards {
		lc.shards[i].samples = make([]time.Duration, 0, perShard)
	}
	return lc
}

// Record adds a latency sample. Safe for concurrent use.
// Uses atomic ops for aggregates and sharded appends for samples.
func (lc *LatencyCollector) Record(d time.Duration) {
	ns := int64(d)

	// Atomic min.
	for {
		old := lc.min.Load()
		if ns >= old || lc.min.CompareAndSwap(old, ns) {
			break
		}
	}

	// Atomic max.
	for {
		old := lc.max.Load()
		if ns <= old || lc.max.CompareAndSwap(old, ns) {
			break
		}
	}

	lc.sum.Add(ns)
	lc.total.Add(1)

	// Sharded append: use goroutine ID or time-based hash to pick shard.
	shard := int(uintptr(unsafe.Pointer(&d)) % _shardCount)
	lc.shards[shard].mu.Lock()
	lc.shards[shard].samples = append(lc.shards[shard].samples, d)
	lc.shards[shard].mu.Unlock()
}

// Stats computes percentile statistics from collected samples.
// Must be called after the load test completes (not during).
func (lc *LatencyCollector) Stats() LatencyStats {
	total := lc.total.Load()
	if total == 0 {
		return LatencyStats{}
	}

	// Merge all shards into a single sorted slice.
	all := make([]time.Duration, 0, total)
	for i := range lc.shards {
		lc.shards[i].mu.Lock()
		all = append(all, lc.shards[i].samples...)
		lc.shards[i].mu.Unlock()
	}

	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	n := len(all)
	minVal := time.Duration(lc.min.Load())
	maxVal := time.Duration(lc.max.Load())
	sumVal := lc.sum.Load()

	if n == 0 {
		return LatencyStats{
			Min:   minVal,
			Max:   maxVal,
			Count: total,
		}
	}

	return LatencyStats{
		Min:   minVal,
		Max:   maxVal,
		Mean:  time.Duration(sumVal / int64(n)),
		P50:   all[percentileIndex(n, 0.50)],
		P95:   all[percentileIndex(n, 0.95)],
		P99:   all[percentileIndex(n, 0.99)],
		P999:  all[percentileIndex(n, 0.999)],
		Count: total,
	}
}

// Count returns the number of samples collected.
func (lc *LatencyCollector) Count() int64 {
	return lc.total.Load()
}

func percentileIndex(n int, pct float64) int {
	idx := int(math.Ceil(pct*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

// ThroughputCounter tracks messages over time using atomic operations.
type ThroughputCounter struct {
	messages atomic.Int64
}

// Inc increments the message count by 1.
func (tc *ThroughputCounter) Inc() {
	tc.messages.Add(1)
}

// IncN increments the message count by n.
func (tc *ThroughputCounter) IncN(n int64) {
	tc.messages.Add(n)
}

// Load returns the current total count and resets to 0.
// Returns the count since the last Load.
func (tc *ThroughputCounter) Load() int64 {
	return tc.messages.Swap(0)
}

// Total returns the total count without resetting.
func (tc *ThroughputCounter) Total() int64 {
	return tc.messages.Load()
}
