package topicmanager

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/clientqueue"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
)

// mockRedisSub records Subscribe/Unsubscribe calls for verification.
type mockRedisSub struct {
	mu           sync.Mutex
	subscribed   []string
	unsubscribed []string
}

func (m *mockRedisSub) Subscribe(symbol string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribed = append(m.subscribed, symbol)
}

func (m *mockRedisSub) Unsubscribe(symbol string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsubscribed = append(m.unsubscribed, symbol)
}

func (m *mockRedisSub) getSubscribed() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.subscribed))
	copy(cp, m.subscribed)
	return cp
}

func (m *mockRedisSub) getUnsubscribed() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.unsubscribed))
	copy(cp, m.unsubscribed)
	return cp
}

func newTestTM(t *testing.T) Manager {
	t.Helper()
	queueCfg := clientqueue.DefaultConfig()
	queueCfg.QueueSize = 64
	queueCfg.MaxAge = 100 * time.Millisecond
	log := log.New(nil, "test")
	return NewWithQueue(0, &queueCfg, log, nil, nil)
}

func TestDistributedRouter_SubscribeFirstClientTriggersRedis(t *testing.T) {
	tm := newTestTM(t)
	mock := &mockRedisSub{}
	log := log.New(nil, "test")

	router := NewDistributedRouter(tm, "market:", log, "gateway-test",
		WithSubscriptionChangeCallback(func(symbol string, change SubscriptionChange) {
			if change == SubscribeRequested {
				mock.Subscribe(symbol)
			}
		}),
	)

	h := router.Subscribe("client-1", "AAPL")
	if h == nil {
		t.Fatal("expected non-nil handle")
	}

	subs := mock.getSubscribed()
	if len(subs) != 1 || subs[0] != "AAPL" {
		t.Fatalf("expected [AAPL] subscribed, got %v", subs)
	}

	h2 := router.Subscribe("client-2", "AAPL")
	if h2 == nil {
		t.Fatal("expected non-nil handle")
	}

	subs = mock.getSubscribed()
	if len(subs) != 1 {
		t.Fatalf("expected 1 total subscribe call, got %d", len(subs))
	}

	if router.ActiveRedisSubscriptions() != 1 {
		t.Fatalf("expected 1 active Redis sub, got %d", router.ActiveRedisSubscriptions())
	}
}

func TestDistributedRouter_LastUnsubscribeTriggersRedisUnsubscribe(t *testing.T) {
	tm := newTestTM(t)
	mock := &mockRedisSub{}
	log := log.New(nil, "test")

	router := NewDistributedRouter(tm, "market:", log, "gateway-test",
		WithSubscriptionChangeCallback(func(symbol string, change SubscriptionChange) {
			switch change {
			case SubscribeRequested:
				mock.Subscribe(symbol)
			case UnsubscribeRequested:
				mock.Unsubscribe(symbol)
			}
		}),
	)

	router.Subscribe("client-1", "AAPL")
	router.Subscribe("client-2", "AAPL")

	router.Unsubscribe("client-1")

	unsubs := mock.getUnsubscribed()
	if len(unsubs) != 0 {
		t.Fatalf("expected 0 unsubscribe calls, got %d: %v", len(unsubs), unsubs)
	}

	router.Unsubscribe("client-2")

	unsubs = mock.getUnsubscribed()
	if len(unsubs) != 1 || unsubs[0] != "AAPL" {
		t.Fatalf("expected [AAPL] unsubscribed, got %v", unsubs)
	}

	if router.ActiveRedisSubscriptions() != 0 {
		t.Fatalf("expected 0 active Redis subs, got %d", router.ActiveRedisSubscriptions())
	}
}

func TestDistributedRouter_MultipleSymbols(t *testing.T) {
	tm := newTestTM(t)
	mock := &mockRedisSub{}
	log := log.New(nil, "test")

	router := NewDistributedRouter(tm, "market:", log, "gateway-test",
		WithSubscriptionChangeCallback(func(symbol string, change SubscriptionChange) {
			switch change {
			case SubscribeRequested:
				mock.Subscribe(symbol)
			case UnsubscribeRequested:
				mock.Unsubscribe(symbol)
			}
		}),
	)

	router.Subscribe("client-1", "AAPL", "MSFT", "GOOG")

	subs := mock.getSubscribed()
	if len(subs) != 3 {
		t.Fatalf("expected 3 subscribe calls, got %d: %v", len(subs), subs)
	}

	router.Unsubscribe("client-1")

	unsubs := mock.getUnsubscribed()
	if len(unsubs) != 3 {
		t.Fatalf("expected 3 unsubscribe calls, got %d: %v", len(unsubs), unsubs)
	}
}

func TestDistributedRouter_PublishRoutesLocally(t *testing.T) {
	tm := newTestTM(t)
	log := log.New(nil, "test")
	router := NewDistributedRouter(tm, "market:", log, "gateway-test")

	h := router.Subscribe("client-1", "AAPL")

	event := marketdata.Quote{
		Symbol:    "AAPL",
		Price:     150.0,
		Timestamp: time.Now(),
	}
	router.Publish(context.Background(), event)

	select {
	case <-h.C():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestDistributedRouter_NeedsSymbol(t *testing.T) {
	tm := newTestTM(t)
	log := log.New(nil, "test")
	router := NewDistributedRouter(tm, "market:", log, "gateway-test")

	if router.NeedsSymbol("AAPL") {
		t.Fatal("expected NeedsSymbol false before subscribe")
	}

	router.Subscribe("client-1", "AAPL")

	if !router.NeedsSymbol("AAPL") {
		t.Fatal("expected NeedsSymbol true after subscribe")
	}

	router.Unsubscribe("client-1")

	if router.NeedsSymbol("AAPL") {
		t.Fatal("expected NeedsSymbol false after unsubscribe")
	}
}

func TestDistributedRouter_SubscriberCount(t *testing.T) {
	tm := newTestTM(t)
	log := log.New(nil, "test")
	router := NewDistributedRouter(tm, "market:", log, "gateway-test")

	if router.SubscriberCount("AAPL") != 0 {
		t.Fatal("expected 0 subscribers")
	}

	router.Subscribe("client-1", "AAPL")
	if router.SubscriberCount("AAPL") != 1 {
		t.Fatal("expected 1 subscriber")
	}

	router.Subscribe("client-2", "AAPL")
	if router.SubscriberCount("AAPL") != 2 {
		t.Fatal("expected 2 subscribers")
	}

	router.Unsubscribe("client-1")
	if router.SubscriberCount("AAPL") != 1 {
		t.Fatal("expected 1 subscriber after unsubscribe")
	}
}

func TestDistributedRouter_Subscribers(t *testing.T) {
	tm := newTestTM(t)
	log := log.New(nil, "test")
	router := NewDistributedRouter(tm, "market:", log, "gateway-test")

	router.Subscribe("client-1", "AAPL", "MSFT")
	router.Subscribe("client-2", "AAPL", "GOOG")

	subs := router.Subscribers()
	if len(subs) != 2 {
		t.Fatalf("expected 2 subscribers, got %d", len(subs))
	}

	s1 := subs["client-1"]
	if len(s1) != 2 || s1[0] != "AAPL" || s1[1] != "MSFT" {
		t.Fatalf("expected [AAPL MSFT], got %v", s1)
	}

	s2 := subs["client-2"]
	if len(s2) != 2 || s2[0] != "AAPL" || s2[1] != "GOOG" {
		t.Fatalf("expected [AAPL GOOG], got %v", s2)
	}
}

func TestDistributedRouter_EventsRouted(t *testing.T) {
	tm := newTestTM(t)
	log := log.New(nil, "test")
	router := NewDistributedRouter(tm, "market:", log, "gateway-test")

	if router.EventsRouted() != 0 {
		t.Fatal("expected 0 events routed")
	}

	event := marketdata.Quote{
		Symbol:    "AAPL",
		Price:     150.0,
		Timestamp: time.Now(),
	}
	router.Publish(context.Background(), event)
	router.Publish(context.Background(), event)

	if router.EventsRouted() != 2 {
		t.Fatalf("expected 2 events routed, got %d", router.EventsRouted())
	}
}

func TestDistributedRouter_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	tm := newTestTM(t)
	mock := &mockRedisSub{}
	log := log.New(nil, "test")

	router := NewDistributedRouter(tm, "market:", log, "gateway-test",
		WithSubscriptionChangeCallback(func(symbol string, change SubscriptionChange) {
			switch change {
			case SubscribeRequested:
				mock.Subscribe(symbol)
			case UnsubscribeRequested:
				mock.Unsubscribe(symbol)
			}
		}),
	)

	// Simulate multiple clients subscribing and unsubscribing concurrently.
	// Each goroutine gets a unique client ID.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientID := ID("client-" + fmt.Sprintf("%d", id))
			router.Subscribe(clientID, "AAPL")
			time.Sleep(time.Millisecond)
			router.Unsubscribe(clientID)
		}(i)
	}
	wg.Wait()

	if router.ActiveRedisSubscriptions() != 0 {
		t.Fatalf("expected 0 active Redis subs, got %d", router.ActiveRedisSubscriptions())
	}
}

func TestDistributedRouter_ShardsReduceContention(t *testing.T) {
	tm := newTestTM(t)
	log := log.New(nil, "test")
	router := NewDistributedRouter(tm, "market:", log, "gateway-test")

	// Subscribe to many symbols — each should land on different shards.
	symbols := []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "NFLX"}
	router.Subscribe("client-1", symbols...)

	// Verify all symbols are tracked correctly.
	for _, sym := range symbols {
		if !router.NeedsSymbol(sym) {
			t.Fatalf("expected NeedsSymbol true for %s", sym)
		}
		if router.SymbolLocalCount(sym) != 1 {
			t.Fatalf("expected 1 local sub for %s, got %d", sym, router.SymbolLocalCount(sym))
		}
	}

	router.Unsubscribe("client-1")

	for _, sym := range symbols {
		if router.NeedsSymbol(sym) {
			t.Fatalf("expected NeedsSymbol false for %s after unsubscribe", sym)
		}
	}

	if router.ActiveRedisSubscriptions() != 0 {
		t.Fatalf("expected 0 active Redis subs, got %d", router.ActiveRedisSubscriptions())
	}
}

func TestDistributedRouter_Reconcile(t *testing.T) {
	tm := newTestTM(t)
	mock := &mockRedisSub{}
	log := log.New(nil, "test")

	router := NewDistributedRouter(tm, "market:", log, "gateway-test",
		WithSubscriptionChangeCallback(func(symbol string, change SubscriptionChange) {
			switch change {
			case SubscribeRequested:
				mock.Subscribe(symbol)
			case UnsubscribeRequested:
				mock.Unsubscribe(symbol)
			}
		}),
	)

	// Manually create inconsistency: local subscribers exist but Redis not subscribed.
	router.Subscribe("client-1", "AAPL")
	router.Unsubscribe("client-1")

	// Reconcile should detect and fix the inconsistency.
	router.Reconcile()

	// Should have triggered unsubscribe (since local count is 0).
	unsubs := mock.getUnsubscribed()
	if len(unsubs) != 1 || unsubs[0] != "AAPL" {
		t.Fatalf("expected reconciliation to trigger [AAPL] unsubscribe, got %v", unsubs)
	}
}

func TestDistributedRouter_SymbolToRedisChannel(t *testing.T) {
	tm := newTestTM(t)
	log := log.New(nil, "test")
	router := NewDistributedRouter(tm, "market:", log, "gateway-test")

	// Known symbol → group channel.
	ch := router.SymbolToRedisChannel("AAPL")
	if ch != "market:equities" {
		t.Fatalf("expected market:equities, got %s", ch)
	}

	// Unknown symbol → per-symbol channel.
	ch = router.SymbolToRedisChannel("XYZZZ")
	if ch != "market:XYZZZ" {
		t.Fatalf("expected market:XYZZZ, got %s", ch)
	}
}

func TestTopicGroups(t *testing.T) {
	if g := SymbolToGroup("AAPL"); g != "equities" {
		t.Fatalf("expected equities, got %s", g)
	}

	if g := SymbolToGroup("BTCUSD"); g != "crypto" {
		t.Fatalf("expected crypto, got %s", g)
	}

	if g := SymbolToGroup("UNKNOWN"); g != "" {
		t.Fatalf("expected empty, got %s", g)
	}

	ch := SymbolToChannel("AAPL", "market:")
	if ch != "market:equities" {
		t.Fatalf("expected market:equities, got %s", ch)
	}

	channels := AllGroupChannels("market:")
	if len(channels) != 4 {
		t.Fatalf("expected 4 channels, got %d", len(channels))
	}

	symbols := GroupSymbols("equities")
	if len(symbols) < 5 {
		t.Fatalf("expected at least 5 equity symbols, got %d", len(symbols))
	}
}

func TestDistributedRouter_TopicCount(t *testing.T) {
	tm := newTestTM(t)
	log := log.New(nil, "test")
	router := NewDistributedRouter(tm, "market:", log, "gateway-test")

	if router.TopicCount() != 0 {
		t.Fatal("expected 0 topics")
	}

	router.Subscribe("client-1", "AAPL", "MSFT")
	if router.TopicCount() != 2 {
		t.Fatalf("expected 2 topics, got %d", router.TopicCount())
	}

	router.Unsubscribe("client-1")
	if router.TopicCount() != 0 {
		t.Fatalf("expected 0 topics after unsubscribe, got %d", router.TopicCount())
	}
}
