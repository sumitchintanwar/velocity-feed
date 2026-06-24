package topicmanager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/marketdata"
)

func newTestManager(t *testing.T) *MemoryManager {
	t.Helper()
	return New(0)
}

func testQuote(symbol string, price float64) marketdata.Quote {
	return marketdata.Quote{
		Symbol:    symbol,
		Type:      marketdata.QuoteTypeTrade,
		Price:     price,
		Timestamp: time.Now(),
	}
}

// recvEvent attempts to receive one event from a handle within timeout.
// Returns the CachedEvent and true if received, nil and false on timeout.
func recvEvent(h Handle, timeout time.Duration) (*marketdata.CachedEvent, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case ev := <-h.C():
			return ev, true
		case <-h.Done():
			return nil, false
		case <-timer.C:
			return nil, false
		}
	}
}

// ---------- Subscribe / Unsubscribe ----------

func TestSubscribe_DeliversEvents(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")
	defer h.Cancel()

	m.Publish(context.Background(), testQuote("AAPL", 150.0))

	cached, ok := recvEvent(h, 100*time.Millisecond)
	if !ok {
		t.Fatal("timed out")
	}
	quote, ok := cached.Event.(marketdata.Quote)
	if !ok {
		t.Fatalf("expected Quote, got %T", cached.Event)
	}
	if quote.Symbol != "AAPL" || quote.Price != 150.0 {
		t.Errorf("unexpected: %+v", quote)
	}
	// Verify pre-encoded JSON is non-empty.
	if len(cached.EncodedMsg) == 0 {
		t.Error("expected non-empty pre-encoded JSON")
	}
}

func TestSubscribe_MultipleTopics(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL", "MSFT")
	defer h.Cancel()

	m.Publish(context.Background(), testQuote("AAPL", 150.0))
	m.Publish(context.Background(), testQuote("MSFT", 300.0))

	got := 0
	for got < 2 {
		if _, ok := recvEvent(h, 100*time.Millisecond); !ok {
			t.Fatalf("timed out after %d events", got)
		}
		got++
	}
}

func TestSubscribe_WrongSymbolNotDelivered(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")
	defer h.Cancel()

	m.Publish(context.Background(), testQuote("TSLA", 200.0))

	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-h.C():
		t.Fatal("received event for wrong symbol")
	case <-h.Done():
		t.Fatal("subscription cancelled unexpectedly")
	case <-timer.C:
	}
}

func TestSubscribe_Idempotent(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")
	m.Subscribe("c1", "AAPL")

	m.Publish(context.Background(), testQuote("AAPL", 150.0))

	if _, ok := recvEvent(h, 100*time.Millisecond); !ok {
		t.Fatal("timed out")
	}
}

func TestSubscribe_MergeTopics_SameHandle(t *testing.T) {
	m := newTestManager(t)
	h1 := m.Subscribe("c1", "AAPL")
	h2 := m.Subscribe("c1", "MSFT")

	if h1 != h2 {
		t.Fatal("expected same handle for same subscriber ID")
	}

	m.Publish(context.Background(), testQuote("AAPL", 150.0))
	m.Publish(context.Background(), testQuote("MSFT", 300.0))

	got := 0
	for got < 2 {
		if _, ok := recvEvent(h1, 100*time.Millisecond); !ok {
			t.Fatalf("timed out after %d events", got)
		}
		got++
	}
}

func TestUnsubscribe_DoneChannelClosed(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")

	h.Cancel()

	select {
	case <-h.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Done channel not closed after Cancel")
	}
}

func TestUnsubscribe_Idempotent(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")
	h.Cancel()
	h.Cancel() // must not panic
}

func TestUnsubscribe_NoMoreEventsAfter(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")

	h.Cancel()
	m.Publish(context.Background(), testQuote("AAPL", 150.0))

	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case _, ok := <-h.C():
		if ok {
			t.Fatal("received event after unsubscribe")
		}
	case <-h.Done():
	case <-timer.C:
	}
}

func TestUnsubscribe_OtherSubscribersUnaffected(t *testing.T) {
	m := newTestManager(t)
	hA := m.Subscribe("a", "AAPL")
	hB := m.Subscribe("b", "AAPL")
	defer hB.Cancel()

	hA.Cancel()
	m.Publish(context.Background(), testQuote("AAPL", 150.0))

	// A should get nothing.
	timerA := time.NewTimer(50 * time.Millisecond)
	defer timerA.Stop()
	select {
	case _, ok := <-hA.C():
		if ok {
			t.Fatal("unsubscribed subscriber received event")
		}
	case <-hA.Done():
	case <-timerA.C:
	}

	// B should get the event.
	if _, ok := recvEvent(hB, 100*time.Millisecond); !ok {
		t.Fatal("active subscriber did not receive event")
	}
}

func TestUnsubscribe_TopicCountCleanup(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("a", "AAPL")
	h.Cancel()

	if n := m.TopicCount(); n != 0 {
		t.Errorf("expected 0 topics after unsubscribe, got %d", n)
	}
}

// ---------- Publish ----------

func TestPublish_FanOut(t *testing.T) {
	m := newTestManager(t)
	const n = 100
	subs := make([]Handle, n)
	for i := range subs {
		id := fmt.Sprintf("client-%d", i)
		subs[i] = m.Subscribe(id, "AAPL")
		defer subs[i].Cancel()
	}

	m.Publish(context.Background(), testQuote("AAPL", 150.0))

	for i, h := range subs {
		if _, ok := recvEvent(h, 100*time.Millisecond); !ok {
			t.Fatalf("subscriber %d timed out", i)
		}
	}
}

func TestPublish_DropOnFull(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")
	defer h.Cancel()

	for i := 0; i < defaultBuffer+10; i++ {
		m.Publish(context.Background(), testQuote("AAPL", float64(i)))
	}

	drained := 0
	for drained < defaultBuffer {
		select {
		case <-h.C():
			drained++
		default:
			goto check
		}
	}
check:
	if drained != defaultBuffer {
		t.Errorf("expected full at %d, drained %d", defaultBuffer, drained)
	}
}

func TestPublish_NoEventsForEmptyTopic(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")
	defer h.Cancel()

	m.Publish(context.Background(), testQuote("MSFT", 300.0))

	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-h.C():
		t.Fatal("received event for unsubscribed topic")
	case <-h.Done():
		t.Fatal("subscription cancelled")
	case <-timer.C:
	}
}

func TestPublish_SnapshotIsolation(t *testing.T) {
	m := newTestManager(t)
	h1 := m.Subscribe("a", "AAPL")
	defer h1.Cancel()

	// h1 subscribes, then we add h2 while publishing to h1.
	// h1 should still receive events — snapshot was taken before h2 was added.
	m.Publish(context.Background(), testQuote("AAPL", 100.0))

	h2 := m.Subscribe("b", "AAPL")
	defer h2.Cancel()

	m.Publish(context.Background(), testQuote("AAPL", 200.0))

	if _, ok := recvEvent(h1, 100*time.Millisecond); !ok {
		t.Fatal("h1 timed out")
	}
	if _, ok := recvEvent(h2, 100*time.Millisecond); !ok {
		t.Fatal("h2 timed out")
	}
}

// ---------- Metrics ----------

func TestSubscriberCount(t *testing.T) {
	m := newTestManager(t)
	if n := m.SubscriberCount("AAPL"); n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	m.Subscribe("a", "AAPL")
	m.Subscribe("b", "AAPL")

	if n := m.SubscriberCount("AAPL"); n != 2 {
		t.Errorf("expected 2, got %d", n)
	}
}

func TestTopicCount(t *testing.T) {
	m := newTestManager(t)
	if n := m.TopicCount(); n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	m.Subscribe("a", "AAPL", "MSFT")
	m.Subscribe("b", "AAPL", "TSLA")

	if n := m.TopicCount(); n != 3 {
		t.Errorf("expected 3, got %d", n)
	}
}

func TestTopics(t *testing.T) {
	m := newTestManager(t)
	m.Subscribe("a", "AAPL", "MSFT")

	topics := m.Topics()
	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}
}

// ---------- Concurrent Safety ----------

func TestConcurrent_PublishAndSubscribe(t *testing.T) {
	m := newTestManager(t)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				m.Publish(context.Background(), testQuote("AAPL", float64(j)))
			}
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cid := fmt.Sprintf("c-%d", id)
			h := m.Subscribe(cid, "AAPL")
			time.Sleep(time.Millisecond)
			h.Cancel()
		}(i)
	}

	wg.Wait()
}

func TestConcurrent_ManyPublishersOneTopic(t *testing.T) {
	m := newTestManager(t)
	h := m.Subscribe("c1", "AAPL")
	defer h.Cancel()

	const goroutines = 50
	const publishes = 1000
	var delivered atomic.Int64

	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			select {
			case <-h.C():
				delivered.Add(1)
			case <-h.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < publishes; j++ {
				m.Publish(context.Background(), testQuote("AAPL", float64(j)))
			}
		}()
	}
	wg.Wait()
	time.Sleep(10 * time.Millisecond)
	h.Cancel()
	<-recvDone

	if d := delivered.Load(); d == 0 {
		t.Fatal("no events delivered")
	}
}

func TestConcurrent_SubscribeUnsubscribeCycle(t *testing.T) {
	m := newTestManager(t)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cid := fmt.Sprintf("c-%d", id)
			h := m.Subscribe(cid, "AAPL")
			time.Sleep(time.Microsecond)
			h.Cancel()
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				m.Publish(context.Background(), testQuote("AAPL", float64(j)))
			}
		}()
	}

	wg.Wait()
}

// ---------- Interface Compliance ----------

func TestMemoryManager_ImplementsManager(t *testing.T) {
	var _ Manager = (*MemoryManager)(nil)
}

// ---------- nextPowerOf2 ----------

func TestNextPowerOf2(t *testing.T) {
	tests := []struct{ in, want int }{
		{0, 0}, {1, 1}, {2, 2}, {3, 4}, {4, 4},
		{5, 8}, {7, 8}, {8, 8}, {9, 16}, {15, 16}, {16, 16},
	}
	for _, tc := range tests {
		if got := nextPowerOf2(tc.in); got != tc.want {
			t.Errorf("nextPowerOf2(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
