package pubsub

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
)

const channelBuffer = 256

// MemoryBus is an in-memory Bus implementation. A single instance is shared
// by the entire application. Safe for concurrent use.
type MemoryBus struct {
	mu          sync.RWMutex
	subscribers map[string][]*entry // symbol → entries
	log         zerolog.Logger
	metrics     *platform.Metrics

	// snapshot stores the most recent event per symbol for new subscribers.
	snapshotMu sync.RWMutex
	snapshot   map[string]marketdata.MarketEvent
}

// entry tracks one subscriber's interest in one symbol.
type entry struct {
	sub     *memorySub
	ch      chan marketdata.MarketEvent
	closed  atomic.Bool
	dropped atomic.Uint64
}

// memorySub implements Subscription.
type memorySub struct {
	ch      chan marketdata.MarketEvent // receive-side (exposed via C())
	sendCh  chan marketdata.MarketEvent // send-side (closed on Cancel)
	id      string
	bus     *MemoryBus
	symbols []string
	closed  atomic.Bool
}

func (s *memorySub) C() <-chan marketdata.MarketEvent { return s.ch }
func (s *memorySub) Cancel() {
	if s.closed.CompareAndSwap(false, true) {
		s.bus.unsubscribe(s)
	}
}

// NewMemoryBus creates a ready-to-use MemoryBus.
func NewMemoryBus(log zerolog.Logger, metrics *platform.Metrics) *MemoryBus {
	return &MemoryBus{
		subscribers: make(map[string][]*entry),
		snapshot:    make(map[string]marketdata.MarketEvent),
		log:         log,
		metrics:     metrics,
	}
}

// Subscribe implements Bus.
func (b *MemoryBus) Subscribe(id string, symbols ...string) Subscription {
	ch := make(chan marketdata.MarketEvent, channelBuffer)
	s := &memorySub{
		ch:      ch,
		sendCh:  ch,
		id:      id,
		bus:     b,
		symbols: symbols,
	}
	e := &entry{sub: s, ch: ch}

	b.mu.Lock()
	for _, sym := range symbols {
		b.subscribers[sym] = append(b.subscribers[sym], e)
	}
	b.mu.Unlock()

	// W8: Snapshot — send the most recent event for each symbol
	// before live events. This prevents the "snapshot problem" where
	// new subscribers see stale or missing initial state.
	b.snapshotMu.RLock()
	for _, sym := range symbols {
		if last, ok := b.snapshot[sym]; ok {
			select {
			case ch <- last:
			default:
				// buffer full — drop snapshot (subscriber will catch up)
			}
		}
	}
	b.snapshotMu.RUnlock()

	b.metrics.SubscribersActive.Inc()
	b.metrics.SubscriptionEvents.WithLabelValues("subscribe").Inc()
	b.log.Debug().Str("id", id).Strs("symbols", symbols).Msg("subscriber added")

	return s
}

func (b *MemoryBus) unsubscribe(s *memorySub) {
	b.mu.Lock()
	for _, sym := range s.symbols {
		entries := b.subscribers[sym]
		filtered := entries[:0]
		for _, e := range entries {
			if e.sub != s {
				filtered = append(filtered, e)
			} else {
				e.closed.Store(true)
			}
		}
		if len(filtered) == 0 {
			delete(b.subscribers, sym)
		} else {
			b.subscribers[sym] = filtered
		}
	}
	b.mu.Unlock()

	close(s.sendCh)

	b.metrics.SubscribersActive.Dec()
	b.metrics.SubscriptionEvents.WithLabelValues("unsubscribe").Inc()
	b.log.Debug().Str("id", s.id).Msg("subscriber removed")
}

// Publish implements Publisher. It fans out a MarketEvent to every
// subscriber registered for event.EventSymbol(). The contract is
// non-blocking: if a subscriber's buffer is full the event is dropped
// for that subscriber. Publish also updates the snapshot cache.
func (b *MemoryBus) Publish(ctx context.Context, event marketdata.MarketEvent) {
	sym := event.EventSymbol()

	// W8: Update snapshot — keep the most recent event per symbol
	// so new subscribers immediately see the latest state.
	b.snapshotMu.Lock()
	b.snapshot[sym] = event
	b.snapshotMu.Unlock()

	b.mu.RLock()
	entries := b.subscribers[sym]
	b.mu.RUnlock()

	sent := 0
	dropped := 0
	for _, e := range entries {
		if e.closed.Load() {
			continue
		}
		select {
		case e.ch <- event:
			sent++
		default:
			dropped++
			e.dropped.Add(1)
		}
	}

	if sent > 0 {
		b.metrics.BroadcastsTotal.Add(float64(sent))
	}
	if dropped > 0 {
		b.metrics.EventsDroppedTotal.WithLabelValues(sym).Add(float64(dropped))
	}
}

// Run drains the quotes channel and calls Publish for each event until ctx is
// cancelled or quotes is closed. This is a convenience for wiring the feed
// pipeline; it is not part of the Bus interface.
func (b *MemoryBus) Run(ctx context.Context, quotes <-chan marketdata.Quote) {
	for {
		select {
		case q, ok := <-quotes:
			if !ok {
				return
			}
			b.Publish(ctx, q)
		case <-ctx.Done():
			return
		}
	}
}
