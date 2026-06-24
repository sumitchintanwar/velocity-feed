package clientqueue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/backpressure"
	"github.com/sumit/rtmds/internal/marketdata"
)

func testEvent(symbol string, price float64) marketdata.MarketEvent {
	return marketdata.Quote{
		Symbol:    symbol,
		Type:      marketdata.QuoteTypeQuote,
		Price:     price,
		Timestamp: time.Now(),
	}
}

func testLogger() zerolog.Logger {
	return zerolog.Nop()
}

func testRegistry() prometheus.Registerer {
	return prometheus.NewPedanticRegistry()
}

// ---------- Queue basics ----------

func TestQueue_SendReceive(t *testing.T) {
	q := New("client-1", DefaultConfig(), testLogger(), testRegistry(), nil)
	defer q.Close()

	if !q.Send(testEvent("AAPL", 100)) {
		t.Fatal("Send returned false")
	}

	// The event may be in the ring or already forwarded to the channel.
	// Drain from the consumer channel with a timeout.
	select {
	case ev := <-q.C():
		if ev.(marketdata.Quote).Price != 100 {
			t.Fatalf("received price = %v, want 100", ev.(marketdata.Quote).Price)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestQueue_DropOldest(t *testing.T) {
	cfg := Config{QueueSize: 4, Policy: backpressure.PolicyDropOldest}
	q := New("client-1", cfg, testLogger(), testRegistry(), nil)
	defer q.Close()

	// Fill and overflow.
	for i := 0; i < 10; i++ {
		q.Send(testEvent("AAPL", float64(i)))
	}

	if q.TotalDropped() == 0 {
		t.Fatal("expected drops")
	}

	// Drain — should get the last 4 events.
	var events []marketdata.MarketEvent
	timeout := time.After(time.Second)
	for {
		select {
		case ev, ok := <-q.C():
			if !ok {
				t.Fatal("channel closed")
			}
			events = append(events, ev)
			if len(events) == 4 {
				goto done
			}
		case <-timeout:
			goto done
		}
	}
done:
	if len(events) == 0 {
		t.Fatal("received 0 events")
	}
}

func TestQueue_DropNewest(t *testing.T) {
	cfg := Config{QueueSize: 4, Policy: backpressure.PolicyDropNewest}
	q := New("client-1", cfg, testLogger(), testRegistry(), nil)
	defer q.Close()

	// Fill.
	for i := 0; i < 4; i++ {
		q.Send(testEvent("AAPL", float64(i)))
	}
	// This should be dropped.
	if q.Send(testEvent("AAPL", 99)) {
		t.Error("Send on full DropNewest should return false")
	}
	if q.TotalDropped() != 1 {
		t.Fatalf("TotalDropped() = %d, want 1", q.TotalDropped())
	}
}

func TestQueue_Disconnect(t *testing.T) {
	var disconnected atomic.Bool
	cfg := Config{
		QueueSize:            1,
		Policy:               backpressure.PolicyDisconnect,
		MaxConsecutiveDrops:  5,
		DropWindow:           time.Second,
		DropThreshold:        0.9,
	}
	q := New("client-1", cfg, testLogger(), testRegistry(), func(reason string) {
		disconnected.Store(true)
	})
	defer q.Close()

	// Buffer of 1: every push after first overflows.
	for i := 0; i < 20; i++ {
		q.Send(testEvent("AAPL", float64(i)))
	}

	time.Sleep(100 * time.Millisecond)
	if !disconnected.Load() {
		t.Error("expected disconnect")
	}
}

func TestQueue_Close(t *testing.T) {
	q := New("client-1", DefaultConfig(), testLogger(), testRegistry(), nil)
	q.Close()

	// Send after close should return false.
	if q.Send(testEvent("AAPL", 1)) {
		t.Error("Send after Close should return false")
	}

	// Done should be closed.
	select {
	case <-q.Done():
	default:
		t.Error("Done should be closed after Close")
	}
}

func TestQueue_CloseIdempotent(t *testing.T) {
	q := New("client-1", DefaultConfig(), testLogger(), testRegistry(), nil)
	q.Close()
	q.Close() // should not panic
}

func TestQueue_ResetDrops(t *testing.T) {
	cfg := Config{
		QueueSize:            1,
		Policy:               backpressure.PolicyDisconnect,
		MaxConsecutiveDrops:  10,
		DropWindow:           time.Second,
		DropThreshold:        0.9,
	}
	q := New("client-1", cfg, testLogger(), testRegistry(), nil)
	defer q.Close()

	for i := 0; i < 10; i++ {
		q.Send(testEvent("AAPL", float64(i)))
		time.Sleep(time.Microsecond)
	}

	if q.TotalDropped() == 0 {
		t.Fatal("expected non-zero total drops")
	}

	q.ResetDrops()
	if q.ConsecutiveDrops() != 0 {
		t.Errorf("ConsecutiveDrops() = %d after reset, want 0", q.ConsecutiveDrops())
	}
}

func TestQueue_ID(t *testing.T) {
	q := New("my-client", DefaultConfig(), testLogger(), testRegistry(), nil)
	defer q.Close()
	if q.ID() != "my-client" {
		t.Errorf("ID() = %q, want my-client", q.ID())
	}
}

func TestQueue_Config(t *testing.T) {
	cfg := Config{QueueSize: 512, Policy: backpressure.PolicyDropNewest}
	q := New("c1", cfg, testLogger(), testRegistry(), nil)
	defer q.Close()
	if q.Config().QueueSize != 512 {
		t.Errorf("Config().QueueSize = %d, want 512", q.Config().QueueSize)
	}
}

func TestQueue_Metrics(t *testing.T) {
	reg := testRegistry()
	q := New("metrics-client", DefaultConfig(), testLogger(), reg, nil)
	defer q.Close()

	q.Send(testEvent("AAPL", 100))
	q.Send(testEvent("AAPL", 200))
	q.Send(testEvent("AAPL", 300))

	gathered, err := reg.(prometheus.Gatherer).Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	var foundEnqueued, foundSent bool
	for _, mf := range gathered {
		name := mf.GetName()
		if name == "rtmds_client_queue_enqueued_total" {
			foundEnqueued = true
		}
		if name == "rtmds_client_queue_sent_total" {
			foundSent = true
		}
	}
	if !foundEnqueued {
		t.Error("enqueued_total metric not found")
	}
	if !foundSent {
		t.Error("sent_total metric not found")
	}
}

// ---------- Manager ----------

func TestManager_Create(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())
	defer mgr.CloseAll()

	q := mgr.Create("c1", nil)
	if q == nil {
		t.Fatal("Create returned nil")
	}
	if mgr.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", mgr.Count())
	}
}

func TestManager_Get(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())
	defer mgr.CloseAll()

	mgr.Create("c1", nil)
	q := mgr.Get("c1")
	if q == nil {
		t.Fatal("Get returned nil")
	}
	if q.ID() != "c1" {
		t.Errorf("ID() = %q, want c1", q.ID())
	}
}

func TestManager_GetMissing(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())
	if q := mgr.Get("nonexistent"); q != nil {
		t.Error("Get on missing ID should return nil")
	}
}

func TestManager_Remove(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())

	mgr.Create("c1", nil)
	mgr.Remove("c1")
	if mgr.Count() != 0 {
		t.Fatalf("Count() = %d after Remove, want 0", mgr.Count())
	}
}

func TestManager_Snapshot(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())
	defer mgr.CloseAll()

	mgr.Create("c1", nil)
	mgr.Create("c2", nil)
	mgr.Create("c3", nil)

	ids := mgr.Snapshot()
	if len(ids) != 3 {
		t.Fatalf("Snapshot() returned %d IDs, want 3", len(ids))
	}
}

func TestManager_Replace(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())
	defer mgr.CloseAll()

	mgr.Create("c1", nil)
	mgr.Create("c1", nil) // replace

	if mgr.Count() != 1 {
		t.Fatalf("Count() = %d after replace, want 1", mgr.Count())
	}
}

func TestManager_AggregateStats(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())
	defer mgr.CloseAll()

	mgr.Create("c1", nil)
	mgr.Create("c2", nil)

	q1 := mgr.Get("c1")
	q1.Send(testEvent("AAPL", 100))
	q1.Send(testEvent("AAPL", 200))

	stats := mgr.AggregateStats()
	if stats.ActiveQueues != 2 {
		t.Errorf("ActiveQueues = %d, want 2", stats.ActiveQueues)
	}
	if stats.TotalEnqueued != 2 {
		t.Errorf("TotalEnqueued = %d, want 2", stats.TotalEnqueued)
	}
}

func TestManager_CloseAll(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())

	mgr.Create("c1", nil)
	mgr.Create("c2", nil)
	mgr.CloseAll()

	if mgr.Count() != 0 {
		t.Fatalf("Count() = %d after CloseAll, want 0", mgr.Count())
	}
}

// ---------- Concurrent ----------

func TestQueue_Concurrent(t *testing.T) {
	cfg := Config{QueueSize: 64, Policy: backpressure.PolicyDropOldest}
	q := New("concurrent", cfg, testLogger(), testRegistry(), nil)
	defer q.Close()

	var wg sync.WaitGroup
	const producers = 8
	const eventsPerProducer = 1000

	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < eventsPerProducer; j++ {
				q.Send(testEvent("AAPL", float64(j)))
			}
		}(i)
	}
	wg.Wait()

	t.Logf("TotalDropped=%d, enqueued=%d, sent=%d",
		q.TotalDropped(), q.enqueued.Load(), q.sent.Load())
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager(DefaultConfig(), testLogger(), testRegistry())
	defer mgr.CloseAll()

	var wg sync.WaitGroup
	const clients = 100

	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func(id int) {
			defer wg.Done()
			mgr.Create(string(rune('A'+id%26)), nil)
			mgr.Get(string(rune('A' + id%26)))
			mgr.Count()
		}(i)
	}
	wg.Wait()
}
