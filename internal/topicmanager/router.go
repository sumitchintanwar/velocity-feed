// Package topicmanager implements topic-based pub/sub routing.
// DistributedRouter extends this with Redis-backed distributed routing,
// dynamically subscribing to per-symbol Redis channels based on local
// client demand (gateway independence).
package topicmanager

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
)

const (
	// defaultRouterShards is the number of shards for the symbolSubs map.
	// Must be a power of 2 for efficient bitwise modulo.
	defaultRouterShards = 16
)

// RedisSubscriber is the interface for managing Redis channel subscriptions.
// Implemented by redisbus.Subscriber.
type RedisSubscriber interface {
	Subscribe(symbol string)
	Unsubscribe(symbol string)
}

// SubscriptionChange is the type of subscription change event.
type SubscriptionChange int

const (
	// SubscribeRequested means a Redis channel should be subscribed.
	SubscribeRequested SubscriptionChange = iota
	// UnsubscribeRequested means a Redis channel should be unsubscribed.
	UnsubscribeRequested
)

// OnSubscriptionChange is called when the router determines that a Redis
// channel subscription should be added or removed. The app wires this
// callback to call redisbus.Subscriber.Subscribe/Unsubscribe.
type OnSubscriptionChange func(symbol string, change SubscriptionChange)

// symbolShard protects a disjoint subset of symbolState entries.
// Multiple shards allow concurrent Subscribe/Unsubscribe for different
// symbols without a global lock bottleneck.
type symbolShard struct {
	mu    sync.Mutex
	subs  map[string]*symbolState
	count int64 // active Redis subscriptions in this shard (atomic)
}

// symbolState tracks local subscriber count and Redis subscription state.
type symbolState struct {
	localCount int32       // number of local subscribers
	redisSub   atomic.Bool // true if Redis channel is subscribed
}

// DistributedRouter wraps a local TopicManager and adds dynamic Redis
// subscription management. When the first local client subscribes to a
// symbol, the router subscribes to the corresponding Redis channel.
// When the last client unsubscribes, the Redis channel is unsubscribed.
//
// This implements gateway independence: each gateway only subscribes to
// Redis channels for symbols that its local clients actually need.
//
// Message flow:
//
//	Redis → RedisSubscriber.listen() → DistributedRouter.local.Publish()
//	  → TopicManager → local Client queues
//
// Concurrency: symbolSubs is sharded (defaultRouterShards) to minimize
// lock contention during thundering-herd subscribe/unsubscribe storms.
// Each shard protects a disjoint subset of symbols via FNV-1a hashing.
type DistributedRouter struct {
	local     Manager  // local topic manager for fan-out
	prefix    string   // Redis channel prefix (default "market:")
	log       zerolog.Logger
	gatewayID string

	// Sharded per-symbol local subscriber tracking.
	shards    []symbolShard
	shardMask uint64

	// Subscriber topic tracking (merged from TrackedRouter to eliminate
	// the double-lock: subsMu + symbolMu).
	subs   map[ID][]Topic
	subsMu sync.RWMutex

	// Callback for Redis subscription changes.
	onChange OnSubscriptionChange

	// Metrics.
	activeRedisSubs atomic.Int64
	eventsRouted    atomic.Uint64
}

// RouterOption configures the DistributedRouter.
type RouterOption func(*DistributedRouter)

// WithChannelPrefix overrides the default Redis channel prefix ("market:").
func WithChannelPrefix(prefix string) RouterOption {
	return func(r *DistributedRouter) { r.prefix = prefix }
}

// WithSubscriptionChangeCallback sets the callback invoked when a Redis
// channel subscription should be added or removed.
func WithSubscriptionChangeCallback(fn OnSubscriptionChange) RouterOption {
	return func(r *DistributedRouter) { r.onChange = fn }
}

// routerHash returns a zero-allocation FNV-1a hash for shard selection.
func routerHash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// NewDistributedRouter creates a router that wraps the local TopicManager
// with dynamic Redis subscription management.
func NewDistributedRouter(local Manager, prefix string, log zerolog.Logger, gatewayID string, opts ...RouterOption) *DistributedRouter {
	n := defaultRouterShards
	r := &DistributedRouter{
		local:      local,
		prefix:     prefix,
		log:        log,
		gatewayID:  gatewayID,
		shards:     make([]symbolShard, n),
		shardMask:  uint64(n - 1),
		subs:       make(map[ID][]Topic),
	}
	for i := range r.shards {
		r.shards[i].subs = make(map[string]*symbolState)
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// shardFor returns the shard index for a symbol.
func (r *DistributedRouter) shardFor(symbol string) uint64 {
	return routerHash(symbol) & r.shardMask
}

// getShard returns the shard for a symbol (caller must hold s.mu).
func (r *DistributedRouter) getShard(symbol string) *symbolShard {
	return &r.shards[r.shardFor(symbol)]
}

// getOrCreateState returns the symbolState for a symbol, creating if needed.
// Caller must hold shard.mu.
func (s *symbolShard) getOrCreateState(symbol string) *symbolState {
	st, ok := s.subs[symbol]
	if !ok {
		st = &symbolState{}
		s.subs[symbol] = st
	}
	return st
}

// Subscribe registers interest in the given topics. The router tracks
// per-symbol local subscriber counts and subscribes to Redis channels
// on demand. Thread-safe: shards are selected per-symbol via FNV-1a hash.
func (r *DistributedRouter) Subscribe(id ID, topics ...Topic) Handle {
	// Delegate to local topic manager for client delivery.
	h := r.local.Subscribe(id, topics...)

	// Track subscriber topics.
	r.subsMu.Lock()
	r.subs[id] = topics
	r.subsMu.Unlock()

	// Track local subscriber counts and manage Redis subscriptions.
	// Each symbol is handled under its own shard lock — no global mutex.
	var toSubscribe []string
	for _, topic := range topics {
		shard := r.getShard(topic)
		shard.mu.Lock()
		st := shard.getOrCreateState(topic)
		st.localCount++

		// First local subscriber for this symbol → subscribe to Redis.
		if st.localCount == 1 && !st.redisSub.Load() {
			st.redisSub.Store(true)
			shard.count++
			r.activeRedisSubs.Add(1)
			r.log.Info().Str("symbol", topic).Str("gateway", r.gatewayID).
				Msg("distributed: first local subscriber → subscribing to Redis")
			toSubscribe = append(toSubscribe, topic)
		}
		shard.mu.Unlock()
	}

	// Fire onChange callbacks OUTSIDE shard locks to avoid holding locks
	// during potentially slow I/O (Redis network calls).
	for _, sym := range toSubscribe {
		r.fireOnChange(sym, SubscribeRequested)
	}

	return h
}

// fireOnChange invokes the onChange callback. Logs but does not block on
// failures — the Reconcile loop will fix any inconsistencies.
func (r *DistributedRouter) fireOnChange(symbol string, change SubscriptionChange) {
	if r.onChange == nil {
		return
	}
	r.onChange(symbol, change)
}

// Publish receives a market event and routes it to local subscribers.
// This is called by the Redis subscriber when a message arrives from Redis.
func (r *DistributedRouter) Publish(ctx context.Context, event marketdata.MarketEvent) {
	r.eventsRouted.Add(1)
	r.local.Publish(ctx, event)
}

// SubscriberCount returns the number of local subscribers for a topic.
func (r *DistributedRouter) SubscriberCount(topic Topic) int {
	return r.local.SubscriberCount(topic)
}

// TopicCount returns the number of topics with active subscribers.
func (r *DistributedRouter) TopicCount() int {
	return r.local.TopicCount()
}

// Topics returns all topics with active subscribers.
func (r *DistributedRouter) Topics() []Topic {
	return r.local.Topics()
}

// NeedsSymbol returns true if any local client is subscribed to the symbol.
func (r *DistributedRouter) NeedsSymbol(symbol string) bool {
	shard := r.getShard(symbol)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	st, ok := shard.subs[symbol]
	return ok && st.localCount > 0
}

// SymbolLocalCount returns the number of local subscribers for a symbol.
func (r *DistributedRouter) SymbolLocalCount(symbol string) int {
	shard := r.getShard(symbol)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	st, ok := shard.subs[symbol]
	if !ok {
		return 0
	}
	return int(st.localCount)
}

// ActiveRedisSubscriptions returns the number of active Redis subscriptions.
func (r *DistributedRouter) ActiveRedisSubscriptions() int {
	return int(r.activeRedisSubs.Load())
}

// EventsRouted returns the total number of events routed to local subscribers.
func (r *DistributedRouter) EventsRouted() uint64 {
	return r.eventsRouted.Load()
}

// SymbolsWithLocalSubscribers returns the number of symbols that have
// at least one local subscriber.
func (r *DistributedRouter) SymbolsWithLocalSubscribers() int {
	count := 0
	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.Lock()
		for _, st := range shard.subs {
			if st.localCount > 0 {
				count++
			}
		}
		shard.mu.Unlock()
	}
	return count
}

// Unsubscribe removes the subscriber and decrements local counts.
// When the last subscriber for a symbol leaves, the Redis channel
// is unsubscribed via the onChange callback.
func (r *DistributedRouter) Unsubscribe(id ID) {
	r.subsMu.Lock()
	topics, ok := r.subs[id]
	if ok {
		delete(r.subs, id)
	}
	r.subsMu.Unlock()

	if ok {
		var toUnsubscribe []string
		for _, topic := range topics {
			shard := r.getShard(topic)
			shard.mu.Lock()
			st, found := shard.subs[topic]
			if !found {
				shard.mu.Unlock()
				continue
			}
			st.localCount--
			if st.localCount < 0 {
				st.localCount = 0
			}

			// Last subscriber left → unsubscribe from Redis.
			if st.localCount == 0 && st.redisSub.Load() {
				st.redisSub.Store(false)
				shard.count--
				r.activeRedisSubs.Add(-1)
				r.log.Info().Str("symbol", topic).Str("gateway", r.gatewayID).
					Msg("distributed: last subscriber left → unsubscribing from Redis")
				toUnsubscribe = append(toUnsubscribe, topic)
			}
			shard.mu.Unlock()
		}

		// Fire onChange callbacks OUTSIDE shard locks.
		for _, sym := range toUnsubscribe {
			r.fireOnChange(sym, UnsubscribeRequested)
		}
	}

	// Delegate to local topic manager.
	r.local.Unsubscribe(id)
}

// Subscribers returns a snapshot of all tracked subscribers and their topics.
func (r *DistributedRouter) Subscribers() map[ID][]Topic {
	r.subsMu.RLock()
	defer r.subsMu.RUnlock()

	result := make(map[ID][]Topic, len(r.subs))
	for id, topics := range r.subs {
		cp := make([]Topic, len(topics))
		copy(cp, topics)
		result[id] = cp
	}
	return result
}

// SymbolsNeedingUnsubscribe returns symbols where local count hit zero
// but Redis subscription is still active.
func (r *DistributedRouter) SymbolsNeedingUnsubscribe() []string {
	var result []string
	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.Lock()
		for sym, st := range shard.subs {
			if st.localCount <= 0 && st.redisSub.Load() {
				result = append(result, sym)
			}
		}
		shard.mu.Unlock()
	}
	return result
}

// Reconcile checks for inconsistencies between the desired state (local
// subscriber counts) and the actual Redis subscription state. It retries
// any failed onChange calls. Run this periodically in a background goroutine
// to recover from transient failures (e.g., Redis network timeouts).
func (r *DistributedRouter) Reconcile() {
	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.Lock()
		for sym, st := range shard.subs {
			// Local subscribers exist but Redis not subscribed → retry subscribe.
			if st.localCount > 0 && !st.redisSub.Load() {
				st.redisSub.Store(true)
				shard.count++
				r.activeRedisSubs.Add(1)
				r.log.Warn().Str("symbol", sym).Str("gateway", r.gatewayID).
					Msg("distributed: reconciliation — re-subscribing to Redis")
				shard.mu.Unlock()
				r.fireOnChange(sym, SubscribeRequested)
				shard.mu.Lock()
			}
			// No local subscribers but Redis still subscribed → retry unsubscribe.
			if st.localCount <= 0 && st.redisSub.Load() {
				st.redisSub.Store(false)
				shard.count--
				r.activeRedisSubs.Add(-1)
				r.log.Warn().Str("symbol", sym).Str("gateway", r.gatewayID).
					Msg("distributed: reconciliation — re-unsubscribing from Redis")
				shard.mu.Unlock()
				r.fireOnChange(sym, UnsubscribeRequested)
				shard.mu.Lock()
			}
		}
		shard.mu.Unlock()
	}
}

// StartReconciler starts a background goroutine that periodically runs
// Reconcile() to fix any subscription state inconsistencies.
// Call this after the router is fully initialized and the onChange callback
// is set. The goroutine stops when ctx is cancelled.
func (r *DistributedRouter) StartReconciler(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.Reconcile()
			}
		}
	}()
}

// SymbolToRedisChannel maps a client topic to the Redis channel name.
// This is the single source of truth for the topic→channel translation,
// preventing accidental overlapping subscriptions.
func (r *DistributedRouter) SymbolToRedisChannel(topic Topic) string {
	group := SymbolToGroup(topic)
	if group == "" {
		// Unknown symbol: use per-symbol channel (no group mapping).
		return r.prefix + topic
	}
	// Known symbol: use group channel.
	return r.prefix + group
}
