// Package clientqueue provides per-client bounded event queues with
// integrated backpressure policies, metrics, and slow consumer detection.
//
// Architecture:
//
//	Publisher → Queue.Send() → backpressure.Channel → Queue.C() → Consumer
//
// Each Queue is independent — a slow consumer affects only its own queue.
// The backpressure policy determines behavior when the queue is full:
//   - DropOldest: evicts oldest event, keeps freshest
//   - DropNewest: drops incoming event
//   - Disconnect: drops and triggers disconnect after threshold
package clientqueue

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/backpressure"
	"github.com/sumit/rtmds/internal/marketdata"
)

// Queue is a per-client bounded event queue backed by a backpressure.Channel.
// It adds per-client metrics and lifecycle management on top of the core
// backpressure mechanism.
type Queue struct {
	id      string
	cfg     Config
	ch      *backpressure.Channel
	log     zerolog.Logger
	metrics *QueueMetrics

	// Lifecycle.
	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once

	// Stats.
	enqueued atomic.Uint64
	sent     atomic.Uint64
}

// New creates a new per-client queue with the given configuration.
// The onDisconnect callback is invoked (in a goroutine) when the
// disconnect policy triggers. Pass nil if not using PolicyDisconnect.
// metrics can be nil — in that case, a per-queue registry is used.
func New(id string, cfg Config, log zerolog.Logger, reg prometheus.Registerer, onDisconnect func(reason string)) *Queue {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Policy == 0 {
		cfg.Policy = backpressure.PolicyDropOldest
	}

	if reg == nil {
		reg = prometheus.NewPedanticRegistry()
	}

	qm := newQueueMetrics(reg)
	return newWithMetrics(id, cfg, log, qm, onDisconnect)
}

// newWithMetrics creates a queue using pre-existing shared metrics.
func newWithMetrics(id string, cfg Config, log zerolog.Logger, qm *QueueMetrics, onDisconnect func(reason string)) *Queue {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Policy == 0 {
		cfg.Policy = backpressure.PolicyDropOldest
	}

	q := &Queue{
		id:      id,
		cfg:     cfg,
		log:     log.With().Str("queue_id", id).Logger(),
		metrics: qm,
		closeCh: make(chan struct{}),
	}

	bpCfg := cfg.backpressureConfig()
	q.ch = backpressure.NewChannel(bpCfg, q.log, qm.bpMetrics, onDisconnect)

	return q
}

// Send enqueues an event into the queue. Returns true if accepted.
// This is non-blocking — if the queue is full, the backpressure
// policy determines whether the event is dropped or the consumer
// is disconnected.
func (q *Queue) Send(ev marketdata.MarketEvent) bool {
	if q.closed.Load() {
		return false
	}
	q.enqueued.Add(1)
	q.metrics.enqueuedTotal.Inc()

	accepted := q.ch.Send(ev)
	if accepted {
		q.sent.Add(1)
		q.metrics.sentTotal.Inc()
	} else {
		q.metrics.droppedTotal.Inc()
	}

	return accepted
}

// C returns the receive-only channel for the consumer.
// It is never closed. Use Done() to detect queue shutdown.
func (q *Queue) C() <-chan marketdata.MarketEvent {
	return q.ch.C()
}

// Done returns a channel that is closed when the queue is shut down.
func (q *Queue) Done() <-chan struct{} {
	return q.closeCh
}

// Len returns the current number of buffered events.
func (q *Queue) Len() int {
	return q.ch.Len()
}

// Cap returns the queue capacity.
func (q *Queue) Cap() int {
	return q.ch.Cap()
}

// TotalDropped returns the total number of events dropped.
func (q *Queue) TotalDropped() uint64 {
	return q.ch.TotalDropped()
}

// ConsecutiveDrops returns the current consecutive drop count.
func (q *Queue) ConsecutiveDrops() int64 {
	return q.ch.ConsecutiveDrops()
}

// ID returns the queue's identifier (typically the client ID).
func (q *Queue) ID() string {
	return q.id
}

// Config returns the queue's configuration.
func (q *Queue) Config() Config {
	return q.cfg
}

// Close shuts down the queue. Safe for concurrent use and idempotent.
// Ordering: closeCh is closed first (signals Done()), then the backpressure
// channel is closed. This gives the consumer a chance to drain C() after
// seeing Done() before the channel is closed.
func (q *Queue) Close() {
	q.closeOnce.Do(func() {
		q.closed.Store(true)
		close(q.closeCh)
		q.ch.Close()
		q.metrics.closedTotal.Inc()
	})
}

// ResetDrops resets the consecutive drop counter and clears the drop window.
func (q *Queue) ResetDrops() {
	q.ch.ResetDrops()
}

// ---------- Aggregate Metrics ----------

// QueueMetrics holds aggregate Prometheus instruments shared across all queues.
// No per-client labels — avoids unbounded cardinality at 5,000+ clients.
// Per-client stats are available via Manager.PerClientStats().
type QueueMetrics struct {
	enqueuedTotal prometheus.Counter
	sentTotal     prometheus.Counter
	droppedTotal  prometheus.Counter
	closedTotal   prometheus.Counter
	queueDepth    prometheus.Gauge
	bpMetrics     *backpressure.Metrics
}

func newQueueMetrics(reg prometheus.Registerer) *QueueMetrics {
	f := promauto.With(reg)

	return &QueueMetrics{
		enqueuedTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "client_queue",
			Name:      "enqueued_total",
			Help:      "Total events enqueued across all client queues.",
		}),
		sentTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "client_queue",
			Name:      "sent_total",
			Help:      "Total events successfully accepted by all client queues.",
		}),
		droppedTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "client_queue",
			Name:      "dropped_total",
			Help:      "Total events dropped across all client queues.",
		}),
		closedTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "client_queue",
			Name:      "closed_total",
			Help:      "Total number of client queues closed.",
		}),
		queueDepth: f.NewGauge(prometheus.GaugeOpts{
			Namespace: "rtmds",
			Subsystem: "client_queue",
			Name:      "total_depth",
			Help:      "Total number of buffered events across all client queues.",
		}),
		bpMetrics: backpressure.NewMetrics(reg),
	}
}

// ---------- Manager ----------

const defaultManagerShards = 16

// managerShard protects a disjoint subset of client queues.
// Reduces contention on the global map during market-open storms.
type managerShard struct {
	mu    sync.RWMutex
	queues map[string]*Queue
}

// Manager tracks all active client queues and provides aggregate metrics.
// Uses sharded locks to reduce contention — same pattern as TopicManager.
type Manager struct {
	shards    []managerShard
	shardMask uint32
	metrics   *QueueMetrics
	log       zerolog.Logger
	cfg       Config
}

// NewManager creates a queue Manager that creates queues with the
// given default configuration. All queues share a single set of
// aggregate Prometheus metrics (no per-client labels).
func NewManager(cfg Config, log zerolog.Logger, reg prometheus.Registerer) *Manager {
	if reg == nil {
		reg = prometheus.NewPedanticRegistry()
	}
	n := uint32(defaultManagerShards)
	m := &Manager{
		shards:    make([]managerShard, n),
		shardMask: n - 1,
		metrics:   newQueueMetrics(reg),
		log:       log,
		cfg:       cfg,
	}
	for i := range m.shards {
		m.shards[i].queues = make(map[string]*Queue)
	}
	return m
}

func (m *Manager) shard(id string) *managerShard {
	h := fnv1a32(id)
	return &m.shards[h&m.shardMask]
}

// fnv1a32 returns a zero-allocation FNV-1a hash for shard selection.
func fnv1a32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// Create creates a new queue for the given client ID.
// If a queue already exists for this ID, it is closed first.
func (m *Manager) Create(clientID string, onDisconnect func(reason string)) *Queue {
	sh := m.shard(clientID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	// Close existing queue if any.
	if old, ok := sh.queues[clientID]; ok {
		old.Close()
	}

	q := newWithMetrics(clientID, m.cfg, m.log, m.metrics, onDisconnect)
	sh.queues[clientID] = q
	return q
}

// Get returns the queue for the given client ID, or nil if not found.
func (m *Manager) Get(clientID string) *Queue {
	sh := m.shard(clientID)
	sh.mu.RLock()
	q := sh.queues[clientID]
	sh.mu.RUnlock()
	return q
}

// Remove closes and removes the queue for the given client ID.
func (m *Manager) Remove(clientID string) {
	sh := m.shard(clientID)
	sh.mu.Lock()
	q, ok := sh.queues[clientID]
	if ok {
		delete(sh.queues, clientID)
	}
	sh.mu.Unlock()

	if ok {
		q.Close()
	}
}

// Count returns the number of active queues.
func (m *Manager) Count() int {
	n := 0
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.RLock()
		n += len(sh.queues)
		sh.mu.RUnlock()
	}
	return n
}

// Snapshot returns a copy of all active queue IDs.
func (m *Manager) Snapshot() []string {
	var ids []string
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.RLock()
		for id := range sh.queues {
			ids = append(ids, id)
		}
		sh.mu.RUnlock()
	}
	return ids
}

// ClientStats holds per-client statistics without Prometheus labels.
type ClientStats struct {
	ID           string
	Enqueued     uint64
	Sent         uint64
	Dropped      uint64
	Depth        int
	Consecutive  int64
}

// PerClientStats returns per-client statistics for all active queues.
// This is the safe alternative to per-client Prometheus labels — it returns
// a snapshot without creating unbounded time series.
func (m *Manager) PerClientStats() []ClientStats {
	var stats []ClientStats
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.RLock()
		for id, q := range sh.queues {
			stats = append(stats, ClientStats{
				ID:          id,
				Enqueued:    q.enqueued.Load(),
				Sent:        q.sent.Load(),
				Dropped:     q.TotalDropped(),
				Depth:       q.Len(),
				Consecutive: q.ConsecutiveDrops(),
			})
		}
		sh.mu.RUnlock()
	}
	return stats
}

// AggregateStats returns aggregate statistics across all queues.
type AggregateStats struct {
	ActiveQueues  int
	TotalEnqueued uint64
	TotalSent     uint64
	TotalDropped  uint64
	TotalDepth    int
	AvgDepth      float64
}

func (m *Manager) AggregateStats() AggregateStats {
	stats := AggregateStats{}
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.RLock()
		for _, q := range sh.queues {
			stats.ActiveQueues++
			stats.TotalEnqueued += q.enqueued.Load()
			stats.TotalSent += q.sent.Load()
			stats.TotalDropped += q.TotalDropped()
			stats.TotalDepth += q.Len()
		}
		sh.mu.RUnlock()
	}
	if stats.ActiveQueues > 0 {
		stats.AvgDepth = float64(stats.TotalDepth) / float64(stats.ActiveQueues)
	}
	return stats
}

// CloseAll closes all active queues concurrently with a timeout.
func (m *Manager) CloseAll() {
	// Collect all queues under shard locks.
	var queues []*Queue
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		for _, q := range sh.queues {
			queues = append(queues, q)
		}
		sh.queues = make(map[string]*Queue)
		sh.mu.Unlock()
	}

	// Close concurrently with timeout.
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(len(queues))
		for _, q := range queues {
			go func(q *Queue) {
				defer wg.Done()
				q.Close()
			}(q)
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Timeout — some queues may be stuck.
	}
}

// ---------- CachedQueue (pre-encoded JSON support) ----------

// CachedQueue is a per-client bounded event queue for *CachedEvent.
// It mirrors Queue but carries pre-encoded JSON bytes, eliminating
// redundant serialization in the Gateway fan-out path.
type CachedQueue struct {
	id      string
	cfg     Config
	ch      *backpressure.CachedChannel
	log     zerolog.Logger
	metrics *QueueMetrics

	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once

	enqueued atomic.Uint64
	sent     atomic.Uint64
}

// NewCachedQueue creates a CachedQueue with the given configuration.
func NewCachedQueue(id string, cfg Config, log zerolog.Logger, reg prometheus.Registerer, onDisconnect func(reason string)) *CachedQueue {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Policy == 0 {
		cfg.Policy = backpressure.PolicyDropOldest
	}
	if reg == nil {
		reg = prometheus.NewPedanticRegistry()
	}
	qm := newQueueMetrics(reg)
	return newCachedQueueWithMetrics(id, cfg, log, qm, onDisconnect)
}

func newCachedQueueWithMetrics(id string, cfg Config, log zerolog.Logger, qm *QueueMetrics, onDisconnect func(reason string)) *CachedQueue {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Policy == 0 {
		cfg.Policy = backpressure.PolicyDropOldest
	}
	q := &CachedQueue{
		id:      id,
		cfg:     cfg,
		log:     log.With().Str("queue_id", id).Logger(),
		metrics: qm,
		closeCh: make(chan struct{}),
	}
	bpCfg := cfg.backpressureConfig()
	q.ch = backpressure.NewCachedChannel(bpCfg, q.log, qm.bpMetrics, onDisconnect)
	return q
}

// Send enqueues a CachedEvent. Returns true if accepted.
func (q *CachedQueue) Send(ev *marketdata.CachedEvent) bool {
	if q.closed.Load() {
		return false
	}
	q.enqueued.Add(1)
	q.metrics.enqueuedTotal.Inc()

	accepted := q.ch.Send(ev)
	if accepted {
		q.sent.Add(1)
		q.metrics.sentTotal.Inc()
	} else {
		q.metrics.droppedTotal.Inc()
	}
	return accepted
}

// C returns the receive-only channel for CachedEvents.
func (q *CachedQueue) C() <-chan *marketdata.CachedEvent {
	return q.ch.C()
}

// Done returns a channel closed when the queue shuts down.
func (q *CachedQueue) Done() <-chan struct{} {
	return q.closeCh
}

// Len returns current buffered event count.
func (q *CachedQueue) Len() int {
	return q.ch.Len()
}

// Cap returns the queue capacity.
func (q *CachedQueue) Cap() int {
	return q.ch.Cap()
}

// TotalDropped returns total dropped events.
func (q *CachedQueue) TotalDropped() uint64 {
	return q.ch.TotalDropped()
}

// ConsecutiveDrops returns current consecutive drop count.
func (q *CachedQueue) ConsecutiveDrops() int64 {
	return q.ch.ConsecutiveDrops()
}

// ID returns the queue identifier.
func (q *CachedQueue) ID() string {
	return q.id
}

// Close shuts down the queue. Safe for concurrent use and idempotent.
func (q *CachedQueue) Close() {
	q.closeOnce.Do(func() {
		q.closed.Store(true)
		close(q.closeCh)
		q.ch.Close()
		q.metrics.closedTotal.Inc()
	})
}

// ResetDrops resets the consecutive drop counter.
func (q *CachedQueue) ResetDrops() {
	q.ch.ResetDrops()
}
