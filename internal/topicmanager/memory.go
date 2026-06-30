package topicmanager

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/clientqueue"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
)

const (
	defaultShardCount = 16
	defaultBuffer     = 64 // reduced from 256 to enforce tighter backpressure
)

// subList is an immutable snapshot of subscribers for a topic.
// It is never modified after creation — writes build a new list and
// atomically swap the pointer. This eliminates per-publish allocation.
type subList struct {
	subs []*subscriber
}

// shard protects the topics map for a disjoint subset of topics.
// Uses sync.Map for lock-free reads on the publish hot path (Alt A1).
// Updates are handled via lock-free CompareAndSwap on atomic.Pointer.
type shard struct {
	topics sync.Map // map[Topic]*atomic.Pointer[subList]
}

// subscriber tracks one client's delivery queue and topic membership.
// When using clientqueue integration, the queue field is non-nil and
// ch/done are derived from it. When using legacy mode (New without queue),
// ch/done are used directly.
type subscriber struct {
	ch     chan *marketdata.CachedEvent
	done   chan struct{} // closed on unsubscribe
	topics map[Topic]struct{}
	closed atomic.Bool
	handle Handle // back-pointer to the handle returned by Subscribe

	// queue is the per-client backpressure queue. When non-nil, it
	// replaces ch/done for event delivery. Set via NewWithQueue.
	queue *clientqueue.CachedQueue

	// legacyDropped counts events silently dropped on the legacy
	// raw-channel path (when queue is nil). Provides observability
	// for the non-clientqueue code path.
	legacyDropped atomic.Uint64
}

// subShard protects a subset of subscribers for a disjoint subset of subscriber IDs.
// This reduces contention on the global subscriber registry during market-open storms.
type subShard struct {
	mu   sync.RWMutex
	subs map[ID]*subscriber
}

// MemoryManager is the in-memory Manager implementation. It uses sharded
// locks and copy-on-write subscriber lists to minimize contention on the
// publish hot path.
type MemoryManager struct {
	shards    []shard
	shardMask uint64

	subShards    []subShard
	subShardMask uint64

	// queueCfg enables per-subscriber clientqueue integration.
	// When nil, legacy raw-channel mode is used.
	queueCfg *clientqueue.Config
	log      *log.Logger
	reg      prometheus.Registerer

	// appMetrics is optional application-level metrics. When non-nil,
	// Subscribe / Unsubscribe / Publish keep the platform.Metrics counters
	// and gauges accurate (broadcasts, drops, subscribers, subscriptions).
	appMetrics *platform.Metrics

	// Prometheus instruments for topic-level observability.
	publishLatency    prometheus.Histogram
	publishOperations prometheus.Counter
}

// New creates a MemoryManager with legacy raw-channel mode.
// If shards <= 0, defaultShardCount is used.
func New(shards int) *MemoryManager {
	n := nextPowerOf2(shards)
	if n <= 0 {
		n = defaultShardCount
	}

	tm := &MemoryManager{
		shards:       make([]shard, n),
		shardMask:    uint64(n - 1),
		subShards:    make([]subShard, n),
		subShardMask: uint64(n - 1),
	}
	for i := range tm.subShards {
		tm.subShards[i].subs = make(map[ID]*subscriber)
	}
	return tm
}

// NewWithQueue creates a MemoryManager with per-subscriber clientqueue
// integration. Each subscriber gets an independent bounded queue with
// the specified backpressure policy. If queueCfg is nil, defaults are used.
// The appMetrics parameter is optional: when non-nil, the manager increments
// the application-level Prometheus instruments (broadcasts, drops, subscribers,
// subscription events).
func NewWithQueue(shards int, queueCfg *clientqueue.Config, l *log.Logger, reg prometheus.Registerer, appMetrics *platform.Metrics) *MemoryManager {
	tm := New(shards)
	if queueCfg == nil {
		defaults := clientqueue.DefaultConfig()
		queueCfg = &defaults
	}
	tm.queueCfg = queueCfg
	tm.log = l
	tm.reg = reg
	tm.appMetrics = appMetrics

	// Register topic-level Prometheus instruments.
	if reg != nil {
		f := promauto.With(reg)
		tm.publishLatency = f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "rtmds",
			Subsystem: "topic",
			Name:      "publish_latency_seconds",
			Help:      "Time in seconds to publish an event to all subscribers.",
			Buckets:   []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		})
		tm.publishOperations = f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "topic",
			Name:      "publish_operations_total",
			Help:      "Total number of publish operations performed.",
		})
	}

	return tm
}

// fnv1a returns a zero-allocation FNV-1a hash. Replaces maphash.Hash
// which allocates 112B on every call — the single alloc on the publish hot path.
func fnv1a(s string) uint64 {
	h := uint64(14695981039346656037) // FNV offset basis
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211 // FNV prime
	}
	return h
}

func (tm *MemoryManager) hashTopic(t Topic) uint64 {
	return fnv1a(t) & tm.shardMask
}

func (tm *MemoryManager) hashSubscriber(id ID) uint64 {
	return fnv1a(id) & tm.subShardMask
}

// ---------- Manager interface ----------

func (tm *MemoryManager) Subscribe(id ID, topics ...Topic) Handle {
	if len(topics) == 0 {
		return nil
	}

	si := tm.hashSubscriber(id)
	ss := &tm.subShards[si]

	// Fast path: check if subscriber exists using RLock
	ss.mu.RLock()
	sub, exists := ss.subs[id]
	ss.mu.RUnlock()

	var newQueue *clientqueue.CachedQueue
	if !exists {
		// Prepare new subscriber outside the lock to minimize critical section
		if tm.queueCfg != nil {
			cfg := *tm.queueCfg
			zl := tm.log.Underlying()
			queueLog := *zl
			if zl.GetLevel() == zerolog.Disabled {
				queueLog = zerolog.Nop()
			}
			newQueue = clientqueue.NewCachedQueue(id, cfg, queueLog, tm.reg, func(reason string) {
				tm.Unsubscribe(id)
			})
		}

		// Double-checked locking for insertion
		ss.mu.Lock()
		sub, exists = ss.subs[id]
		if !exists {
			sub = &subscriber{
				done:   make(chan struct{}),
				topics: make(map[Topic]struct{}, len(topics)),
			}
			if newQueue != nil {
				sub.queue = newQueue
			} else {
				sub.ch = make(chan *marketdata.CachedEvent, defaultBuffer)
			}
			ss.subs[id] = sub
		} else {
			// Lost the race, clean up the unused queue
			if newQueue != nil {
				newQueue.Close()
			}
		}
		ss.mu.Unlock()
	}

	for _, t := range topics {
		if _, ok := sub.topics[t]; ok {
			continue
		}
		si := tm.hashTopic(t)
		s := &tm.shards[si]

		// Lock-free RCU: load current list, create new list with sub appended, and CAS.
		for {
			v, ok := s.topics.Load(t)
			if !ok {
				ap := &atomic.Pointer[subList]{}
				newSubs := []*subscriber{sub}
				ap.Store(&subList{subs: newSubs})
				_, loaded := s.topics.LoadOrStore(t, ap)
				if loaded {
					// Another goroutine created it first, retry loop
					continue
				}
				break
			}
			ap := v.(*atomic.Pointer[subList])
			oldList := ap.Load()
			var oldSubs []*subscriber
			if oldList != nil {
				oldSubs = oldList.subs
			}
			newSubs := make([]*subscriber, 0, len(oldSubs)+1)
			newSubs = append(newSubs, oldSubs...)
			newSubs = append(newSubs, sub)
			
			// CompareAndSwap ensures we only commit if the list hasn't changed
			if ap.CompareAndSwap(oldList, &subList{subs: newSubs}) {
				break
			}
		}

		sub.topics[t] = struct{}{}
	}

	if sub.handle == nil {
		sub.handle = &handle{sub: sub, id: id, mgr: tm}
	}

	// First-time subscriptions bump the active subscribers gauge and the
	// subscribe counter; re-subscribes only count as events.
	if tm.appMetrics != nil {
		if !exists {
			tm.appMetrics.SubscribersActive.Inc()
		}
		tm.appMetrics.SubscriptionEvents.WithLabelValues("subscribe").Inc()
	}

	return sub.handle
}

func (tm *MemoryManager) Unsubscribe(id ID) {
	si := tm.hashSubscriber(id)
	ss := &tm.subShards[si]
	ss.mu.Lock()
	sub, ok := ss.subs[id]
	if !ok {
		ss.mu.Unlock()
		return
	}
	delete(ss.subs, id)
	ss.mu.Unlock()

	// Remove from every topic shard via COW.
	for t := range sub.topics {
		si := tm.hashTopic(t)
		s := &tm.shards[si]

		// Lock-free RCU: load current list, create new list omitting sub, and CAS.
		for {
			v, ok := s.topics.Load(t)
			if !ok {
				break
			}
			ap := v.(*atomic.Pointer[subList])
			oldList := ap.Load()
			var oldSubs []*subscriber
			if oldList != nil {
				oldSubs = oldList.subs
			}
			newSubs := make([]*subscriber, 0, len(oldSubs))
			for _, s := range oldSubs {
				if s != sub {
					newSubs = append(newSubs, s)
				}
			}
			if len(newSubs) == 0 {
				if ap.CompareAndSwap(oldList, nil) {
					s.topics.Delete(t)
					break
				}
			} else {
				if ap.CompareAndSwap(oldList, &subList{subs: newSubs}) {
					break
				}
			}
		}
	}

	// Signal done. The event channel is NOT closed — this avoids the
	// send-on-closed-channel race entirely.
	if sub.closed.CompareAndSwap(false, true) {
		close(sub.done)
	}
	// Close the per-client queue if using clientqueue integration.
	if sub.queue != nil {
		sub.queue.Close()
	}

	// Subscription counter and active-subscribers gauge.
	if tm.appMetrics != nil {
		tm.appMetrics.SubscribersActive.Dec()
		tm.appMetrics.SubscriptionEvents.WithLabelValues("unsubscribe").Inc()
	}
}

// Publish delivers an event to all subscribers of the event's topic.
// Encodes JSON ONCE, then fans out the pre-encoded bytes to all subscribers.
// This eliminates O(N) JSON serialization in the Gateway writePump path.
func (tm *MemoryManager) Publish(_ context.Context, event marketdata.MarketEvent) {
	start := time.Now()

	topic := event.EventSymbol()
	si := tm.hashTopic(topic)
	s := &tm.shards[si]

	v, ok := s.topics.Load(topic)
	if !ok {
		return
	}
	ap := v.(*atomic.Pointer[subList])
	list := ap.Load()
	if list == nil {
		return
	}

	// Encode JSON ONCE for all subscribers.
	cached := marketdata.NewCachedEvent(event)

	var sent, dropped int
	for _, sub := range list.subs {
		if sub.closed.Load() {
			continue
		}
		if sub.queue != nil {
			if sub.queue.Send(cached) {
				sent++
			} else {
				dropped++
			}
		} else {
			select {
			case sub.ch <- cached:
				sent++
			default:
				sub.legacyDropped.Add(1)
				dropped++
			}
		}
	}

	// Record publish latency and operation count.
	if tm.publishLatency != nil {
		tm.publishLatency.Observe(time.Since(start).Seconds())
	}
	if tm.publishOperations != nil {
		tm.publishOperations.Inc()
	}

	// Application-level instruments — aggregate broadcast counter and
	// dropped-event counter. Cardinality stays bounded (no per-symbol or
	// per-subscriber labels).
	if tm.appMetrics != nil {
		if sent > 0 {
			tm.appMetrics.BroadcastsTotal.Add(float64(sent))
		}
		if dropped > 0 {
			tm.appMetrics.EventsDroppedTotal.Add(float64(dropped))
		}
	}
}

func (tm *MemoryManager) SubscriberCount(topic Topic) int {
	si := tm.hashTopic(topic)
	s := &tm.shards[si]

	v, ok := s.topics.Load(topic)
	if !ok {
		return 0
	}
	ap := v.(*atomic.Pointer[subList])
	if list := ap.Load(); list != nil {
		return len(list.subs)
	}
	return 0
}

func (tm *MemoryManager) TopicCount() int {
	count := 0
	for i := range tm.shards {
		s := &tm.shards[i]
		s.topics.Range(func(_, _ any) bool {
			count++
			return true
		})
	}
	return count
}

func (tm *MemoryManager) Topics() []Topic {
	var result []Topic
	for i := range tm.shards {
		s := &tm.shards[i]
		s.topics.Range(func(key, _ any) bool {
			result = append(result, key.(Topic))
			return true
		})
	}
	return result
}

// ---------- Handle ----------

type handle struct {
	sub *subscriber
	id  ID
	mgr *MemoryManager
}

// C returns the event channel. It is NEVER closed. Use Done() to detect
// cancellation instead of relying on channel close.
func (h *handle) C() <-chan *marketdata.CachedEvent {
	if h.sub.queue != nil {
		return h.sub.queue.C()
	}
	return h.sub.ch
}

// Done returns a channel that is closed when the subscription is cancelled.
// Select on this alongside C() to detect termination.
func (h *handle) Done() <-chan struct{} {
	if h.sub.queue != nil {
		return h.sub.queue.Done()
	}
	return h.sub.done
}

func (h *handle) Cancel() { h.mgr.Unsubscribe(h.id) }
func (h *handle) ID() ID  { return h.id }

// ---------- Helpers ----------

func nextPowerOf2(n int) int {
	if n <= 0 {
		return 0
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
