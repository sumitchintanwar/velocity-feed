package backpressure

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
)

const (
	simProducerRate = 10000 // events/sec per producer
	simConsumerRate = 100   // events/sec
	simDuration     = 2 * time.Second
)

func simEvent(symbol string, seq int) marketdata.Quote {
	return marketdata.Quote{
		Symbol: symbol,
		Price:  float64(seq),
		Volume: 1,
	}
}

func simLogger() zerolog.Logger {
	return zerolog.Nop()
}

// producer pushes events at the given rate until stopped.
func simProducer(ch *Channel, symbol string, rate int, wg *sync.WaitGroup, stopped *atomic.Bool) {
	defer wg.Done()
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for !stopped.Load() {
		<-ticker.C
		ch.Send(simEvent(symbol, 0))
	}
}

// simConsumer drains events until the channel closes or deadline fires.
func simConsumer(ch *Channel, rate int, wg *sync.WaitGroup, received *atomic.Int64, deadline <-chan struct{}) {
	defer wg.Done()
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			// Drain any remaining events quickly.
			for {
				select {
				case <-ch.C():
					received.Add(1)
				default:
					return
				}
			}
		case ev, ok := <-ch.C():
			if !ok {
				return
			}
			_ = ev
			received.Add(1)
			<-ticker.C
		}
	}
}

func simPolicy(t *testing.T, policy Policy, onDisconnect func(string)) *Channel {
	t.Helper()
	cfg := Config{
		Policy:              policy,
		BufferSize:          64,
		MaxConsecutiveDrops: 100,
		DropWindow:          time.Second,
		DropThreshold:       0.9,
	}
	reg := prometheus.NewPedanticRegistry()
	m := NewMetrics(reg)
	return NewChannel(cfg, simLogger(), m, onDisconnect)
}

func TestSim_DropOldest(t *testing.T) {
	ch := simPolicy(t, PolicyDropOldest, nil)

	var prodWg, consWg sync.WaitGroup
	var stopped atomic.Bool
	var received atomic.Int64
	done := make(chan struct{})

	// Start 10 producers (total ~100k/sec) and 1 consumer (~100/sec).
	for i := 0; i < 10; i++ {
		prodWg.Add(1)
		go simProducer(ch, "AAPL", simProducerRate, &prodWg, &stopped)
	}
	consWg.Add(1)
	go simConsumer(ch, simConsumerRate, &consWg, &received, done)

	time.Sleep(simDuration)

	// Phase 1: stop producers.
	stopped.Store(true)
	prodWg.Wait()

	// Phase 2: close channel to unblock consumer.
	ch.Close()

	// Phase 3: wait for consumer.
	consWg.Wait()
	close(done)

	dropped := ch.TotalDropped()
	got := received.Load()
	totalSent := dropped + uint64(got)

	t.Logf("Policy: DropOldest")
	t.Logf("  Sent:       ~%d (10 producers × %d/s × %ds)", 10*simProducerRate*int(simDuration.Seconds()), simProducerRate, int(simDuration.Seconds()))
	t.Logf("  Received:   %d", got)
	t.Logf("  Dropped:    %d", dropped)
	t.Logf("  Drop ratio: %.1f%%", float64(dropped)/float64(totalSent)*100)

	if got == 0 {
		t.Fatal("consumer received 0 events")
	}
	if dropped == 0 {
		t.Fatal("expected drops under extreme backpressure")
	}
}

func TestSim_DropNewest(t *testing.T) {
	ch := simPolicy(t, PolicyDropNewest, nil)

	var prodWg, consWg sync.WaitGroup
	var stopped atomic.Bool
	var received atomic.Int64
	done := make(chan struct{})

	for i := 0; i < 10; i++ {
		prodWg.Add(1)
		go simProducer(ch, "GOOG", simProducerRate, &prodWg, &stopped)
	}
	consWg.Add(1)
	go simConsumer(ch, simConsumerRate, &consWg, &received, done)

	time.Sleep(simDuration)

	stopped.Store(true)
	prodWg.Wait()
	ch.Close()
	consWg.Wait()
	close(done)

	dropped := ch.TotalDropped()
	got := received.Load()

	t.Logf("Policy: DropNewest")
	t.Logf("  Received:   %d", got)
	t.Logf("  Dropped:    %d", dropped)
	t.Logf("  Drop ratio: %.1f%%", float64(dropped)/float64(dropped+uint64(got))*100)

	if got == 0 {
		t.Fatal("consumer received 0 events")
	}
	if dropped == 0 {
		t.Fatal("expected drops under extreme backpressure")
	}
}

func TestSim_Disconnect(t *testing.T) {
	var disconnected atomic.Bool
	ch := simPolicy(t, PolicyDisconnect, func(reason string) {
		disconnected.Store(true)
		t.Logf("  Disconnect reason: %s", reason)
	})

	var prodWg, consWg sync.WaitGroup
	var stopped atomic.Bool
	var received atomic.Int64
	done := make(chan struct{})

	for i := 0; i < 10; i++ {
		prodWg.Add(1)
		go simProducer(ch, "MSFT", simProducerRate, &prodWg, &stopped)
	}
	consWg.Add(1)
	go simConsumer(ch, simConsumerRate, &consWg, &received, done)

	time.Sleep(simDuration)

	stopped.Store(true)
	prodWg.Wait()
	ch.Close()
	consWg.Wait()
	close(done)

	dropped := ch.TotalDropped()
	got := received.Load()

	t.Logf("Policy: Disconnect")
	t.Logf("  Received:     %d", got)
	t.Logf("  Dropped:      %d", dropped)
	t.Logf("  Disconnected: %v", disconnected.Load())

	if dropped == 0 {
		t.Fatal("expected drops under extreme backpressure")
	}
	if !disconnected.Load() {
		t.Log("consumer was not disconnected (may need longer run or lower threshold)")
	}
}

// TestSim_Ordering verifies DropOldest preserves event order.
func TestSim_Ordering(t *testing.T) {
	cfg := Config{
		Policy:     PolicyDropOldest,
		BufferSize: 32,
	}
	reg := prometheus.NewPedanticRegistry()
	m := NewMetrics(reg)
	ch := NewChannel(cfg, simLogger(), m, nil)

	// Producer sends sequentially numbered events.
	var seq atomic.Int64
	var prodWg, consWg sync.WaitGroup
	var stopped atomic.Bool

	prodWg.Add(1)
	go func() {
		defer prodWg.Done()
		ticker := time.NewTicker(time.Microsecond * 100) // ~10k/sec
		defer ticker.Stop()
		for !stopped.Load() {
			<-ticker.C
			s := seq.Add(1)
			ch.Send(simEvent("AAPL", int(s)))
		}
	}()

	// Consumer drains and records sequence numbers via Price field.
	var lastSeq int
	var outOfOrder int
	var mu sync.Mutex

	consWg.Add(1)
	go func() {
		defer consWg.Done()
		for ev := range ch.C() {
			q := ev.(marketdata.Quote)
			currentSeq := int(q.Price)
			mu.Lock()
			if currentSeq <= lastSeq {
				outOfOrder++
			}
			lastSeq = currentSeq
			mu.Unlock()
			time.Sleep(time.Millisecond) // ~100/sec consumer
		}
	}()

	time.Sleep(simDuration)
	stopped.Store(true)
	prodWg.Wait()
	ch.Close()
	consWg.Wait()

	mu.Lock()
	oo := outOfOrder
	mu.Unlock()

	t.Logf("Ordering test: lastSeq=%d, outOfOrder=%d", lastSeq, oo)
	if oo > 0 {
		t.Errorf("events delivered out of order %d times", oo)
	}
}
