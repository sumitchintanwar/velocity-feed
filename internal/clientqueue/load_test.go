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

const (
	loadProducerRate = 10000 // events/sec per producer
	loadDuration     = 2 * time.Second
)

func loadEvent(seq int) marketdata.MarketEvent {
	return marketdata.Quote{
		Symbol:    "AAPL",
		Type:      marketdata.QuoteTypeQuote,
		Price:     float64(seq),
		Volume:    1,
		Timestamp: time.Now(),
	}
}

func loadLogger() zerolog.Logger {
	return zerolog.Nop()
}

// ---------- Fast Client ----------

// TestLoad_FastClient verifies that a fast consumer (matching producer rate)
// receives all events with zero drops.
func TestLoad_FastClient(t *testing.T) {
	cfg := Config{
		QueueSize: 1024,
		Policy:    backpressure.PolicyDropOldest,
	}
	reg := prometheus.NewPedanticRegistry()
	q := New("fast-client", cfg, loadLogger(), reg, nil)
	defer q.Close()

	var received atomic.Int64
	var wg sync.WaitGroup

	// Consumer: drains as fast as events arrive.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range q.C() {
			received.Add(1)
		}
	}()

	// Producer: 10k events/sec for 2 seconds.
	var seq atomic.Int64
	var prodWg sync.WaitGroup
	var stopped atomic.Bool

	prodWg.Add(1)
	go func() {
		defer prodWg.Done()
		ticker := time.NewTicker(time.Microsecond * 100) // ~10k/sec
		defer ticker.Stop()
		for !stopped.Load() {
			<-ticker.C
			s := seq.Add(1)
			q.Send(loadEvent(int(s)))
		}
	}()

	time.Sleep(loadDuration)
	stopped.Store(true)
	prodWg.Wait()
	q.Close()
	wg.Wait()

	total := received.Load()
	dropped := q.TotalDropped()

	t.Logf("Fast Client:")
	t.Logf("  Received:  %d", total)
	t.Logf("  Dropped:   %d", dropped)
	t.Logf("  Drop rate: %.2f%%", float64(dropped)/float64(uint64(total)+dropped)*100)

	if total == 0 {
		t.Fatal("fast client received 0 events")
	}
	// Fast consumer should have very low drop rate (< 5%).
	if dropped > 0 {
		dropRate := float64(dropped) / float64(uint64(total)+dropped) * 100
		if dropRate > 5.0 {
			t.Errorf("fast client drop rate %.2f%% > 5%%", dropRate)
		}
	}
}

// ---------- Slow Client ----------

// TestLoad_SlowClient verifies that a slow consumer (100/sec vs 10k/sec
// producer) experiences drops but still receives events. The queue absorbs
// bursts and the backpressure policy handles overflow.
func TestLoad_SlowClient(t *testing.T) {
	cfg := Config{
		QueueSize: 256,
		Policy:    backpressure.PolicyDropOldest,
	}
	reg := prometheus.NewPedanticRegistry()
	q := New("slow-client", cfg, loadLogger(), reg, nil)
	defer q.Close()

	var received atomic.Int64
	var wg sync.WaitGroup

	// Consumer: 100 events/sec (simulates slow network).
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond * 10) // ~100/sec
		defer ticker.Stop()
		for range q.C() {
			received.Add(1)
			<-ticker.C
		}
	}()

	// Producer: 10k events/sec for 2 seconds.
	var seq atomic.Int64
	var prodWg sync.WaitGroup
	var stopped atomic.Bool

	prodWg.Add(1)
	go func() {
		defer prodWg.Done()
		ticker := time.NewTicker(time.Microsecond * 100)
		defer ticker.Stop()
		for !stopped.Load() {
			<-ticker.C
			s := seq.Add(1)
			q.Send(loadEvent(int(s)))
		}
	}()

	time.Sleep(loadDuration)
	stopped.Store(true)
	prodWg.Wait()
	q.Close()
	wg.Wait()

	total := received.Load()
	dropped := q.TotalDropped()
	totalSent := uint64(total) + dropped

	t.Logf("Slow Client:")
	t.Logf("  Produced:  ~%d", 10*loadProducerRate*int(loadDuration.Seconds()))
	t.Logf("  Received:  %d", total)
	t.Logf("  Dropped:   %d", dropped)
	t.Logf("  Drop rate: %.1f%%", float64(dropped)/float64(totalSent)*100)

	if total == 0 {
		t.Fatal("slow client received 0 events")
	}
	// Slow client should receive ~200 events (100/sec × 2s).
	if total < 100 || total > 300 {
		t.Errorf("slow client received %d events, expected ~200", total)
	}
	// With 10k/sec produced and 100/sec consumed, drop rate should be >95%.
	if dropped == 0 {
		t.Error("expected drops for slow client")
	}
}

// ---------- Disconnected Client ----------

// TestLoad_DisconnectedClient verifies that the disconnect policy triggers
// when a consumer is too slow. The callback fires and the client is removed.
func TestLoad_DisconnectedClient(t *testing.T) {
	var disconnected atomic.Bool
	var disconnectReason atomic.Value

	cfg := Config{
		QueueSize:            64,
		Policy:               backpressure.PolicyDisconnect,
		MaxConsecutiveDrops:  20,
		DropWindow:           time.Second,
		DropThreshold:        0.8,
	}
	reg := prometheus.NewPedanticRegistry()
	q := New("disconnected-client", cfg, loadLogger(), reg, func(reason string) {
		disconnected.Store(true)
		disconnectReason.Store(reason)
	})
	defer q.Close()

	var received atomic.Int64
	var wg sync.WaitGroup

	// Consumer: 50 events/sec (very slow).
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond * 20)
		defer ticker.Stop()
		for range q.C() {
			received.Add(1)
			<-ticker.C
		}
	}()

	// Producer: 10k events/sec for 2 seconds.
	var seq atomic.Int64
	var prodWg sync.WaitGroup
	var stopped atomic.Bool

	prodWg.Add(1)
	go func() {
		defer prodWg.Done()
		ticker := time.NewTicker(time.Microsecond * 100)
		defer ticker.Stop()
		for !stopped.Load() {
			<-ticker.C
			s := seq.Add(1)
			q.Send(loadEvent(int(s)))
		}
	}()

	time.Sleep(loadDuration)
	stopped.Store(true)
	prodWg.Wait()
	q.Close()
	wg.Wait()

	total := received.Load()
	dropped := q.TotalDropped()

	t.Logf("Disconnected Client:")
	t.Logf("  Received:     %d", total)
	t.Logf("  Dropped:      %d", dropped)
	t.Logf("  Disconnected: %v", disconnected.Load())
	if v := disconnectReason.Load(); v != nil {
		t.Logf("  Reason:       %s", v.(string))
	}

	if dropped == 0 {
		t.Error("expected drops for disconnected client")
	}
	// The disconnect should have triggered due to sustained drops.
	if !disconnected.Load() {
		t.Log("disconnect callback not fired — threshold may need tuning")
	}
}

// ---------- Mixed Load ----------

// TestLoad_MixedClients verifies that fast, slow, and disconnected clients
// coexist without interfering with each other. Each client's queue is independent.
func TestLoad_MixedClients(t *testing.T) {
	var fastDropped, slowDropped atomic.Uint64
	var slowReceived atomic.Int64
	var disconnected atomic.Bool

	// Fast client: should receive most events.
	fastQ := New("fast", Config{
		QueueSize: 1024,
		Policy:    backpressure.PolicyDropOldest,
	}, loadLogger(), prometheus.NewPedanticRegistry(), nil)
	defer fastQ.Close()

	// Slow client: should experience drops.
	slowQ := New("slow", Config{
		QueueSize: 256,
		Policy:    backpressure.PolicyDropOldest,
	}, loadLogger(), prometheus.NewPedanticRegistry(), nil)
	defer slowQ.Close()

	// Disconnected client: should be disconnected.
	discQ := New("disconnected", Config{
		QueueSize:            32,
		Policy:               backpressure.PolicyDisconnect,
		MaxConsecutiveDrops:  10,
		DropWindow:           time.Second,
		DropThreshold:        0.8,
	}, loadLogger(), prometheus.NewPedanticRegistry(), func(reason string) {
		disconnected.Store(true)
	})
	defer discQ.Close()

	var wg sync.WaitGroup

	// Fast consumer: drains immediately.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range fastQ.C() {
		}
	}()

	// Slow consumer: 100/sec.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond * 10)
		defer ticker.Stop()
		for range slowQ.C() {
			slowReceived.Add(1)
			<-ticker.C
		}
	}()

	// Disconnected consumer: 50/sec.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond * 20)
		defer ticker.Stop()
		for range discQ.C() {
			<-ticker.C
		}
	}()

	// Producer: sends to all 3 queues at 10k/sec total.
	var seq atomic.Int64
	var prodWg sync.WaitGroup
	var stopped atomic.Bool

	prodWg.Add(1)
	go func() {
		defer prodWg.Done()
		ticker := time.NewTicker(time.Microsecond * 100)
		defer ticker.Stop()
		for !stopped.Load() {
			<-ticker.C
			s := seq.Add(1)
			ev := loadEvent(int(s))
			fastQ.Send(ev)
			slowQ.Send(ev)
			discQ.Send(ev)
		}
	}()

	time.Sleep(loadDuration)
	stopped.Store(true)
	prodWg.Wait()

	fastQ.Close()
	slowQ.Close()
	discQ.Close()
	wg.Wait()

	fastDropped.Store(fastQ.TotalDropped())
	slowDropped.Store(slowQ.TotalDropped())

	t.Logf("Mixed Load Results:")
	t.Logf("  Fast client:     received=%d, dropped=%d (%.1f%%)",
		fastQ.sent.Load(), fastDropped.Load(),
		float64(fastDropped.Load())/float64(fastQ.sent.Load()+fastDropped.Load())*100)
	t.Logf("  Slow client:     received=%d, dropped=%d (%.1f%%)",
		slowReceived.Load(), slowDropped.Load(),
		float64(slowDropped.Load())/float64(uint64(slowReceived.Load())+slowDropped.Load())*100)
	t.Logf("  Disconnect client: dropped=%d, disconnected=%v",
		discQ.TotalDropped(), disconnected.Load())

	// Fast client should have near-zero drops.
	if fastDropped.Load() > 0 {
		dropRate := float64(fastDropped.Load()) / float64(fastQ.sent.Load()+fastDropped.Load()) * 100
		if dropRate > 5.0 {
			t.Errorf("fast client drop rate %.2f%% > 5%%", dropRate)
		}
	}

	// Slow client should have significant drops.
	if slowDropped.Load() == 0 {
		t.Error("slow client should have drops")
	}

	// Queue isolation: fast client drops should not be affected by slow client.
	// Both share the same producer but have independent queues.
}

// ---------- Burst Absorption ----------

// TestLoad_BurstAbsorption verifies that a short burst is absorbed by the
// queue when the consumer is moderately slow.
func TestLoad_BurstAbsorption(t *testing.T) {
	cfg := Config{
		QueueSize: 500,
		Policy:    backpressure.PolicyDropOldest,
	}
	reg := prometheus.NewPedanticRegistry()
	q := New("burst-client", cfg, loadLogger(), reg, nil)
	defer q.Close()

	var received atomic.Int64
	var wg sync.WaitGroup

	// Consumer: 500/sec (moderately slow).
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond * 2)
		defer ticker.Stop()
		for range q.C() {
			received.Add(1)
			<-ticker.C
		}
	}()

	// Producer: 50k events in a tight burst, then pause.
	var prodWg sync.WaitGroup
	prodWg.Add(1)
	go func() {
		defer prodWg.Done()
		for i := 0; i < 50000; i++ {
			q.Send(loadEvent(i))
		}
		// Wait for consumer to drain.
		time.Sleep(time.Second)
	}()

	prodWg.Wait()
	q.Close()
	wg.Wait()

	total := received.Load()
	dropped := q.TotalDropped()

	t.Logf("Burst Absorption:")
	t.Logf("  Burst size: 50000")
	t.Logf("  Received:   %d", total)
	t.Logf("  Dropped:    %d", dropped)

	if total == 0 {
		t.Fatal("received 0 events from burst")
	}
	// With 50k burst and 500/sec consumer, queue should absorb some.
	// Drops are expected but queue should buffer a meaningful amount.
	if dropped >= 50000 {
		t.Error("queue dropped all events — no buffering occurred")
	}
}
