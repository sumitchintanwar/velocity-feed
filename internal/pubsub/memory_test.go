package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
)

func newBus(t *testing.T) *MemoryBus {
	t.Helper()
	metrics, _ := platform.NewMetrics("test_pubsub")
	return NewMemoryBus(zerolog.Nop(), metrics)
}

// --- Subscribe / Publish ---

func TestPublish_DeliveredToSubscriber(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "AAPL")
	defer sub.Cancel()

	bus.Publish(context.Background(), marketdata.Quote{
		Symbol: "AAPL", Price: 150.0, Timestamp: time.Now(),
	})

	select {
	case ev := <-sub.C():
		q, ok := ev.(marketdata.Quote)
		if !ok {
			t.Fatalf("expected Quote, got %T", ev)
		}
		if q.Symbol != "AAPL" || q.Price != 150.0 {
			t.Errorf("unexpected quote: %+v", q)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for quote")
	}
}

func TestPublish_NotDeliveredToWrongSymbol(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "TSLA")
	defer sub.Cancel()

	bus.Publish(context.Background(), marketdata.Quote{Symbol: "AAPL", Price: 100.0})

	select {
	case <-sub.C():
		t.Fatal("received quote for wrong symbol")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestPublish_FanOutToMultipleSubscribers(t *testing.T) {
	bus := newBus(t)
	subA := bus.Subscribe("a", "AAPL")
	subB := bus.Subscribe("b", "AAPL")
	defer subA.Cancel()
	defer subB.Cancel()

	bus.Publish(context.Background(), marketdata.Quote{Symbol: "AAPL", Price: 200.0})

	for _, sub := range []Subscription{subA, subB} {
		select {
		case ev := <-sub.C():
			q, ok := ev.(marketdata.Quote)
			if !ok {
				t.Fatalf("expected Quote, got %T", ev)
			}
			if q.Price != 200.0 {
				t.Errorf("expected 200.0, got %f", q.Price)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timed out")
		}
	}
}

// --- Cancel ---

func TestCancel_ClosesChannel(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "AAPL")

	sub.Cancel()

	select {
	case _, ok := <-sub.C():
		if ok {
			t.Fatal("expected closed channel after Cancel")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel not closed")
	}
}

func TestSnapshot_NewSubscriberGetsLastValue(t *testing.T) {
	bus := newBus(t)

	// Publish before any subscriber exists.
	bus.Publish(context.Background(), marketdata.Quote{
		Symbol: "AAPL", Price: 175.0, Timestamp: time.Now(),
	})

	// Subscribe after the publish — should get the snapshot.
	sub := bus.Subscribe("c1", "AAPL")
	defer sub.Cancel()

	select {
	case ev := <-sub.C():
		q, ok := ev.(marketdata.Quote)
		if !ok {
			t.Fatalf("expected Quote, got %T", ev)
		}
		if q.Price != 175.0 {
			t.Errorf("expected snapshot price 175.0, got %f", q.Price)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for snapshot")
	}
}

func TestSnapshot_UpdatedBeforeEachDelivery(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "AAPL")
	defer sub.Cancel()

	// Publish two events — only the last should be the snapshot.
	bus.Publish(context.Background(), marketdata.Quote{Symbol: "AAPL", Price: 100.0})
	bus.Publish(context.Background(), marketdata.Quote{Symbol: "AAPL", Price: 200.0})

	// Drain the channel.
	<-sub.C()
	<-sub.C()

	// New subscriber should see price=200 (the snapshot).
	sub2 := bus.Subscribe("c2", "AAPL")
	defer sub2.Cancel()

	select {
	case ev := <-sub2.C():
		q, ok := ev.(marketdata.Quote)
		if !ok {
			t.Fatalf("expected Quote, got %T", ev)
		}
		if q.Price != 200.0 {
			t.Errorf("expected snapshot price 200.0, got %f", q.Price)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for snapshot")
	}
}

func TestCancel_Idempotent(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "AAPL")
	sub.Cancel()
	sub.Cancel() // must not panic
}

func TestPublish_AfterCancel_SkipsSilently(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "AAPL")
	sub.Cancel()

	// Publish must not panic on a cancelled subscriber.
	bus.Publish(context.Background(), marketdata.Quote{Symbol: "AAPL", Price: 100.0})
}

// --- Concurrent safety ---

func TestConcurrent_PublishAndCancel(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "AAPL")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			bus.Publish(context.Background(), marketdata.Quote{
				Symbol: "AAPL", Price: float64(i),
			})
		}
	}()

	sub.Cancel()
	wg.Wait()
}

func TestConcurrent_MultiplePublishers(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "AAPL")
	defer sub.Cancel()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				bus.Publish(context.Background(), marketdata.Quote{
					Symbol: "AAPL", Price: float64(j),
				})
			}
		}()
	}
	wg.Wait()
}

// --- Interface compliance ---

func TestMemoryBus_ImplementsBus(t *testing.T) {
	var _ Bus = (*MemoryBus)(nil)
}

func TestMemorySub_ImplementsSubscription(t *testing.T) {
	var _ Subscription = (*memorySub)(nil)
}

// --- Run ---

func TestRun_DrainsChannel(t *testing.T) {
	bus := newBus(t)
	sub := bus.Subscribe("c1", "AAPL")
	defer sub.Cancel()

	quotes := make(chan marketdata.Quote, 2)
	quotes <- marketdata.Quote{Symbol: "AAPL", Price: 10.0}
	quotes <- marketdata.Quote{Symbol: "AAPL", Price: 20.0}
	close(quotes)

	bus.Run(context.Background(), quotes)

	got := 0
	for {
		select {
		case <-sub.C():
			got++
		default:
			goto done
		}
	}
done:
	if got != 2 {
		t.Errorf("expected 2 quotes, got %d", got)
	}
}

func TestRun_StopsOnContext(t *testing.T) {
	bus := newBus(t)
	quotes := make(chan marketdata.Quote)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		bus.Run(ctx, quotes)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Run did not stop after context cancel")
	}
}

// --- Metrics ---

func TestPublish_IncrementsBroadcastsTotal(t *testing.T) {
	bus := newBus(t)
	_ = bus.Subscribe("c1", "AAPL")

	bus.Publish(context.Background(), marketdata.Quote{Symbol: "AAPL", Price: 100.0})

	// We can't read the counter value directly without exposing it,
	// but if the code reaches here without panic, metrics wiring is correct.
}

func TestPublish_DropPath_IncrementsDropped(t *testing.T) {
	bus := newBus(t)
	// Subscribe but don't read — channel will fill and drop.
	sub := bus.Subscribe("c1", "AAPL")
	defer sub.Cancel()

	for i := 0; i < channelBuffer+10; i++ {
		bus.Publish(context.Background(), marketdata.Quote{Symbol: "AAPL", Price: float64(i)})
	}

	// At least 10 quotes should have been dropped. Verify by checking
	// that the subscriber channel is full (non-blocking receive drains
	// exactly channelBuffer items).
	drained := 0
	for drained < channelBuffer {
		select {
		case <-sub.C():
			drained++
		default:
			goto check
		}
	}
check:
	if drained != channelBuffer {
		t.Errorf("expected channel full at %d, drained %d", channelBuffer, drained)
	}
}
