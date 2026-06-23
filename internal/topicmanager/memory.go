package topicmanager

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/sumit/rtmds/internal/marketdata"
)

const (
	defaultShardCount = 16
	defaultBuffer     = 256
)

// subList is an immutable snapshot of subscribers for a topic.
// It is never modified after creation — writes build a new list and
// atomically swap the pointer. This eliminates per-publish allocation.
type subList struct {
	subs []*subscriber
}

// shard protects the topics map for a disjoint subset of topics.
// Subscriber list reads are lock-free via atomic pointer loads.
type shard struct {
	mu     sync.RWMutex
	topics map[Topic]*atomic.Pointer[subList]
}

// subscriber tracks one client's delivery queue and topic membership.
// The event channel is NEVER closed — cancellation is signaled via done.
type subscriber struct {
	ch     chan marketdata.MarketEvent
	done   chan struct{} // closed on unsubscribe
	topics map[Topic]struct{}
	closed atomic.Bool
	handle Handle // back-pointer to the handle returned by Subscribe
}

// MemoryManager is the in-memory Manager implementation. It uses sharded
// locks and copy-on-write subscriber lists to minimize contention on the
// publish hot path.
type MemoryManager struct {
	shards    []shard
	shardMask uint64

	subMu sync.RWMutex
	subs  map[ID]*subscriber
}

// New creates a MemoryManager. If shards <= 0, defaultShardCount is used.
func New(shards int) *MemoryManager {
	n := nextPowerOf2(shards)
	if n <= 0 {
		n = defaultShardCount
	}

	tm := &MemoryManager{
		shards:    make([]shard, n),
		shardMask: uint64(n - 1),
		subs:      make(map[ID]*subscriber),
	}
	for i := range tm.shards {
		tm.shards[i].topics = make(map[Topic]*atomic.Pointer[subList])
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

// ---------- Manager interface ----------

func (tm *MemoryManager) Subscribe(id ID, topics ...Topic) Handle {
	if len(topics) == 0 {
		return nil
	}

	tm.subMu.Lock()
	sub, exists := tm.subs[id]
	if !exists {
		sub = &subscriber{
			ch:     make(chan marketdata.MarketEvent, defaultBuffer),
			done:   make(chan struct{}),
			topics: make(map[Topic]struct{}, len(topics)),
		}
		tm.subs[id] = sub
	}
	tm.subMu.Unlock()

	for _, t := range topics {
		if _, ok := sub.topics[t]; ok {
			continue
		}
		si := tm.hashTopic(t)
		s := &tm.shards[si]

		// COW: load current list, create new list with sub appended, swap.
		s.mu.Lock()
		ap := s.topics[t]
		if ap == nil {
			ap = &atomic.Pointer[subList]{}
			s.topics[t] = ap
		}
		old := ap.Load()
		var oldSubs []*subscriber
		if old != nil {
			oldSubs = old.subs
		}
		newSubs := make([]*subscriber, 0, len(oldSubs)+1)
		newSubs = append(newSubs, oldSubs...)
		newSubs = append(newSubs, sub)
		ap.Store(&subList{subs: newSubs})
		s.mu.Unlock()

		sub.topics[t] = struct{}{}
	}

	if sub.handle == nil {
		sub.handle = &handle{sub: sub, id: id, mgr: tm}
	}
	return sub.handle
}

func (tm *MemoryManager) Unsubscribe(id ID) {
	tm.subMu.Lock()
	sub, ok := tm.subs[id]
	if !ok {
		tm.subMu.Unlock()
		return
	}
	delete(tm.subs, id)
	tm.subMu.Unlock()

	// Remove from every topic shard via COW.
	for t := range sub.topics {
		si := tm.hashTopic(t)
		s := &tm.shards[si]

		s.mu.Lock()
		if ap, ok := s.topics[t]; ok {
			old := ap.Load()
			var oldSubs []*subscriber
			if old != nil {
				oldSubs = old.subs
			}
			newSubs := make([]*subscriber, 0, len(oldSubs))
			for _, s := range oldSubs {
				if s != sub {
					newSubs = append(newSubs, s)
				}
			}
			if len(newSubs) == 0 {
				delete(s.topics, t)
			} else {
				ap.Store(&subList{subs: newSubs})
			}
		}
		s.mu.Unlock()
	}

	// Signal done. The event channel is NOT closed — this avoids the
	// send-on-closed-channel race entirely.
	if sub.closed.CompareAndSwap(false, true) {
		close(sub.done)
	}
}

// Publish delivers an event to all subscribers of the event's topic.
// The hot path performs: 1 atomic pointer load, 0 allocations, 0 locks.
func (tm *MemoryManager) Publish(_ context.Context, event marketdata.MarketEvent) {
	topic := event.EventSymbol()
	si := tm.hashTopic(topic)
	s := &tm.shards[si]

	// SL1/COW fix: Read the atomic pointer under read lock, then iterate
	// the immutable list with no locks held. The list pointer and its
	// contents are stable — writers create new lists, never mutate.
	s.mu.RLock()
	ap := s.topics[topic]
	s.mu.RUnlock()

	if ap == nil {
		return
	}
	list := ap.Load()
	if list == nil {
		return
	}

	for _, sub := range list.subs {
		if sub.closed.Load() {
			continue
		}
		select {
		case sub.ch <- event:
		default:
		}
	}
}

func (tm *MemoryManager) SubscriberCount(topic Topic) int {
	si := tm.hashTopic(topic)
	s := &tm.shards[si]

	s.mu.RLock()
	ap := s.topics[topic]
	s.mu.RUnlock()

	if ap == nil {
		return 0
	}
	if list := ap.Load(); list != nil {
		return len(list.subs)
	}
	return 0
}

func (tm *MemoryManager) TopicCount() int {
	count := 0
	for i := range tm.shards {
		s := &tm.shards[i]
		s.mu.RLock()
		count += len(s.topics)
		s.mu.RUnlock()
	}
	return count
}

func (tm *MemoryManager) Topics() []Topic {
	var result []Topic
	for i := range tm.shards {
		s := &tm.shards[i]
		s.mu.RLock()
		for t := range s.topics {
			result = append(result, t)
		}
		s.mu.RUnlock()
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
func (h *handle) C() <-chan marketdata.MarketEvent { return h.sub.ch }

// Done returns a channel that is closed when the subscription is cancelled.
// Select on this alongside C() to detect termination.
func (h *handle) Done() <-chan struct{} { return h.sub.done }

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
