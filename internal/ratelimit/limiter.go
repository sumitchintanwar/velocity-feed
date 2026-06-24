package ratelimit

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const defaultShards = 16

// limiterShard protects a disjoint subset of client entries.
type limiterShard struct {
	mu      sync.RWMutex
	clients map[string]*clientLimits
}

// Limiter manages per-client token buckets with configurable limits.
// It supports four rate-limited operations: Connect, Subscribe, Unsubscribe,
// and MaxSubscriptions (cap on active subscriptions).
//
// Uses sharded locks (16 shards) to reduce contention during market-open.
// Stale entries are evicted by a background goroutine based on ClientTTL.
//
// Thread-safe for concurrent use.
type Limiter struct {
	cfg      Config
	shards   []limiterShard
	shardMask uint32
	metrics  *PrometheusMetrics
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewLimiter creates a per-client rate limiter with the given config.
// All clients share the same rate limits. Pass nil for metrics to disable.
// Starts a background evictor if ClientTTL > 0.
func NewLimiter(cfg Config, metrics *PrometheusMetrics) *Limiter {
	n := uint32(defaultShards)
	l := &Limiter{
		cfg:      cfg,
		shards:   make([]limiterShard, n),
		shardMask: n - 1,
		metrics:  metrics,
		stopCh:   make(chan struct{}),
	}
	for i := range l.shards {
		l.shards[i].clients = make(map[string]*clientLimits)
	}

	// Start background evictor if TTL is configured.
	if cfg.ClientTTL > 0 {
		l.wg.Add(1)
		go l.evictor()
	}

	return l
}

// Stop shuts down the background evictor. Call before discarding the Limiter.
func (l *Limiter) Stop() {
	close(l.stopCh)
	l.wg.Wait()
}

// shard returns the shard for a given client ID.
func (l *Limiter) shard(clientID string) *limiterShard {
	h := fnv1a32(clientID)
	return &l.shards[h&l.shardMask]
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

// AllowConnect checks if the client can make a new connection request.
func (l *Limiter) AllowConnect(clientID string) bool {
	cl := l.getOrCreate(clientID)
	cl.touch()
	allowed := cl.connect.Allow()
	if l.metrics != nil {
		if allowed {
			l.metrics.ConnectAllowed.Inc()
		} else {
			l.metrics.ConnectRejected.Inc()
			l.metrics.LimiterHits.WithLabelValues("connect").Inc()
		}
	}
	return allowed
}

// AllowSubscribe checks if the client can make a subscribe request.
func (l *Limiter) AllowSubscribe(clientID string) bool {
	cl := l.getOrCreate(clientID)
	cl.touch()
	allowed := cl.subscribe.Allow()
	if l.metrics != nil {
		if allowed {
			l.metrics.SubscribeAllowed.Inc()
		} else {
			l.metrics.SubscribeRejected.Inc()
			l.metrics.LimiterHits.WithLabelValues("subscribe").Inc()
		}
	}
	return allowed
}

// AllowUnsubscribe checks if the client can make an unsubscribe request.
func (l *Limiter) AllowUnsubscribe(clientID string) bool {
	cl := l.getOrCreate(clientID)
	cl.touch()
	allowed := cl.unsubscribe.Allow()
	if l.metrics != nil {
		if allowed {
			l.metrics.UnsubscribeAllowed.Inc()
		} else {
			l.metrics.UnsubscribeRejected.Inc()
			l.metrics.LimiterHits.WithLabelValues("unsubscribe").Inc()
		}
	}
	return allowed
}

// AllowSubscribeN checks if the client can make n subscribe requests.
// All-or-nothing: if n tokens aren't available, none are consumed.
func (l *Limiter) AllowSubscribeN(clientID string, n int) bool {
	cl := l.getOrCreate(clientID)
	cl.touch()
	allowed := cl.subscribe.AllowN(n)
	if l.metrics != nil && !allowed {
		l.metrics.LimiterHits.WithLabelValues("subscribe").Inc()
	}
	return allowed
}

// AllowSubscription checks if the client is within the max subscription
// limit and atomically increments the counter. Returns false if at limit.
// B1 fix: uses atomic.Int64 with CAS to prevent race conditions.
func (l *Limiter) AllowSubscription(clientID string) bool {
	if l.cfg.MaxSubscriptions <= 0 {
		return true
	}
	cl := l.getOrCreate(clientID)
	cl.touch()

	for {
		cur := cl.activeSubs.Load()
		if cur >= int64(l.cfg.MaxSubscriptions) {
			if l.metrics != nil {
				l.metrics.LimiterHits.WithLabelValues("max_subscriptions").Inc()
			}
			return false
		}
		if cl.activeSubs.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// ReleaseSubscription atomically decrements the active subscription count.
// B1 fix: uses atomic operations to prevent race conditions.
func (l *Limiter) ReleaseSubscription(clientID string) {
	sh := l.shard(clientID)
	sh.mu.RLock()
	cl, ok := sh.clients[clientID]
	sh.mu.RUnlock()
	if ok {
		for {
			cur := cl.activeSubs.Load()
			if cur <= 0 {
				return
			}
			if cl.activeSubs.CompareAndSwap(cur, cur-1) {
				return
			}
		}
	}
}

// AllowAndSubscribe atomically checks both the rate limit and the
// subscription cap. Only consumes rate tokens if both checks pass.
// B2 fix: prevents rate tokens from being wasted when the cap rejects.
func (l *Limiter) AllowAndSubscribe(clientID string) bool {
	if l.cfg.MaxSubscriptions <= 0 {
		return l.AllowSubscribe(clientID)
	}

	cl := l.getOrCreate(clientID)
	cl.touch()

	// Check rate limit first (consumes a token).
	if !cl.subscribe.Allow() {
		if l.metrics != nil {
			l.metrics.SubscribeRejected.Inc()
			l.metrics.LimiterHits.WithLabelValues("subscribe").Inc()
		}
		return false
	}

	// Check subscription cap (atomic CAS).
	for {
		cur := cl.activeSubs.Load()
		if cur >= int64(l.cfg.MaxSubscriptions) {
			// Cap exceeded — refund the token.
			cl.subscribe.Refund(1)

			if l.metrics != nil {
				l.metrics.SubscribeRejected.Inc()
				l.metrics.LimiterHits.WithLabelValues("max_subscriptions").Inc()
			}
			return false
		}
		if cl.activeSubs.CompareAndSwap(cur, cur+1) {
			break
		}
	}

	if l.metrics != nil {
		l.metrics.SubscribeAllowed.Inc()
	}
	return true
}

// RemoveClient removes a client's rate limiter state.
// Call when a client disconnects to free memory.
func (l *Limiter) RemoveClient(clientID string) {
	sh := l.shard(clientID)
	sh.mu.Lock()
	delete(sh.clients, clientID)
	sh.mu.Unlock()
}

// ClientCount returns the number of tracked clients.
func (l *Limiter) ClientCount() int {
	n := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.RLock()
		n += len(sh.clients)
		sh.mu.RUnlock()
	}
	return n
}

// getOrCreate returns the client's limits, creating if needed.
func (l *Limiter) getOrCreate(clientID string) *clientLimits {
	sh := l.shard(clientID)

	sh.mu.RLock()
	cl, ok := sh.clients[clientID]
	sh.mu.RUnlock()
	if ok {
		return cl
	}

	sh.mu.Lock()
	// Double-check after acquiring write lock.
	cl, ok = sh.clients[clientID]
	if !ok {
		cl = newClientLimits(l.cfg)
		sh.clients[clientID] = cl
	}
	sh.mu.Unlock()
	return cl
}

// evictor periodically scans all shards and removes entries that haven't
// been accessed within the configured ClientTTL.
func (l *Limiter) evictor() {
	defer l.wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.evictStale()
		}
	}
}

// evictStale removes clients not accessed within ClientTTL.
func (l *Limiter) evictStale() {
	now := time.Now()
	ttl := l.cfg.ClientTTL

	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for id, cl := range sh.clients {
			if now.Sub(cl.accessedAt()) > ttl {
				delete(sh.clients, id)
			}
		}
		sh.mu.Unlock()
	}
}

// ---------- Prometheus Metrics ----------

// PrometheusMetrics holds Prometheus instruments for the rate limiter.
// Uses aggregate counters (no per-client labels) to avoid cardinality explosion.
type PrometheusMetrics struct {
	ConnectAllowed    prometheus.Counter
	ConnectRejected   prometheus.Counter
	SubscribeAllowed  prometheus.Counter
	SubscribeRejected prometheus.Counter
	UnsubscribeAllowed  prometheus.Counter
	UnsubscribeRejected prometheus.Counter
	LimiterHits         *prometheus.CounterVec
}

// NewPrometheusMetrics creates and registers rate limiter metrics.
func NewPrometheusMetrics(reg prometheus.Registerer) *PrometheusMetrics {
	f := promauto.With(reg)

	return &PrometheusMetrics{
		ConnectAllowed: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "ratelimit",
			Name:      "connect_allowed_total",
			Help:      "Total connection requests allowed.",
		}),
		ConnectRejected: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "ratelimit",
			Name:      "connect_rejected_total",
			Help:      "Total connection requests rejected by rate limiter.",
		}),
		SubscribeAllowed: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "ratelimit",
			Name:      "subscribe_allowed_total",
			Help:      "Total subscribe requests allowed.",
		}),
		SubscribeRejected: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "ratelimit",
			Name:      "subscribe_rejected_total",
			Help:      "Total subscribe requests rejected by rate limiter.",
		}),
		UnsubscribeAllowed: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "ratelimit",
			Name:      "unsubscribe_allowed_total",
			Help:      "Total unsubscribe requests allowed.",
		}),
		UnsubscribeRejected: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "ratelimit",
			Name:      "unsubscribe_rejected_total",
			Help:      "Total unsubscribe requests rejected by rate limiter.",
		}),
		LimiterHits: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "ratelimit",
			Name:      "hits_total",
			Help:      "Total rate limit hits by type.",
		}, []string{"type"}),
	}
}
