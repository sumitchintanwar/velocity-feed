package bench

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/feed"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/pubsub"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/websocket"
)

// ---------- Slow Consumer ----------

func BenchmarkBackpressure_SlowConsumer(b *testing.B) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	m, log := newBenchMetrics("bench_bp_slow")
	bus := pubsub.NewMemoryBus(log, m)

	var fastDelivered atomic.Int64
	fastSubs := make([]pubsub.Subscription, 10)
	for i := 0; i < 10; i++ {
		fastSubs[i] = bus.Subscribe(fmt.Sprintf("fast-%d", i), "AAPL")
		go func() {
			for range fastSubs[i].C() {
				fastDelivered.Add(1)
			}
		}()
	}

	var slowDelivered atomic.Int64
	slowSub := bus.Subscribe("slow-0", "AAPL")
	go func() {
		for range slowSub.C() {
			slowDelivered.Add(1)
			time.Sleep(time.Millisecond)
		}
	}()

	p := feed.NewPipeline(f, bus, log, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		for fastDelivered.Load() == 0 {
			time.Sleep(time.Microsecond)
		}
		fastDelivered.Add(-1)
	}

	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "fast_events_sec")
	b.ReportMetric(float64(slowDelivered.Load())/elapsed.Seconds(), "slow_events_sec")

	cancel()
	<-done
	for _, s := range fastSubs {
		s.Cancel()
	}
	slowSub.Cancel()
}

// ---------- Slow vs Fast Consumer Ratio ----------

func BenchmarkBackpressure_SlowRatio(b *testing.B) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	m, log := newBenchMetrics("bench_bp_ratio")
	bus := pubsub.NewMemoryBus(log, m)

	var fastDelivered atomic.Int64
	var slowDelivered atomic.Int64

	fastSubs := make([]pubsub.Subscription, 10)
	for i := 0; i < 10; i++ {
		fastSubs[i] = bus.Subscribe(fmt.Sprintf("fast-%d", i), "AAPL")
		go func() {
			for range fastSubs[i].C() {
				fastDelivered.Add(1)
			}
		}()
	}

	slowSub := bus.Subscribe("slow-0", "AAPL")
	go func() {
		for range slowSub.C() {
			slowDelivered.Add(1)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	p := feed.NewPipeline(f, bus, log, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		time.Sleep(time.Microsecond)
	}

	b.StopTimer()
	fastRate := float64(fastDelivered.Load()) / b.Elapsed().Seconds()
	slowRate := float64(slowDelivered.Load()) / b.Elapsed().Seconds()
	b.ReportMetric(fastRate, "fast_events_sec")
	b.ReportMetric(slowRate, "slow_events_sec")

	cancel()
	<-done
	for _, s := range fastSubs {
		s.Cancel()
	}
	slowSub.Cancel()
}

// ---------- Burst Load ----------

func BenchmarkBackpressure_BurstLoad(b *testing.B) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	m, log := newBenchMetrics("bench_bp_burst")
	bus := pubsub.NewMemoryBus(log, m)

	var delivered atomic.Int64
	subs := make([]pubsub.Subscription, 100)
	for i := 0; i < 100; i++ {
		subs[i] = bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		go func() {
			for range subs[i].C() {
				delivered.Add(1)
			}
		}()
	}

	p := feed.NewPipeline(f, bus, log, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	burstCount := 1000
	for i := 0; i < b.N; i++ {
		for j := 0; j < burstCount; j++ {
			q := marketdata.Quote{
				Symbol:    "AAPL",
				Type:      marketdata.QuoteTypeTrade,
				Price:     100.0,
				Volume:    1000,
				Timestamp: time.Now(),
				Provider:  "burst",
			}
			bus.Publish(ctx, q)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(b.N*burstCount)/elapsed.Seconds(), "events_sec")

	cancel()
	<-done
	for _, s := range subs {
		s.Cancel()
	}
}

// ---------- WebSocket Gateway Backpressure ----------

func BenchmarkBackpressure_Gateway(b *testing.B) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	m, log := newBenchMetrics("bench_bp_gw")
	bus := pubsub.NewMemoryBus(log, m)
	tm := topicmanager.New(0)
	gw := websocket.NewGateway(tm, log, m, 500, "test-gw")

	_ = gw

	p := feed.NewPipeline(f, bus, log, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		time.Sleep(time.Microsecond)
	}

	cancel()
	<-done
	gw.Shutdown(context.Background())
}

// ---------- Subscription Churn ----------

func BenchmarkBackpressure_SubscriptionChurn(b *testing.B) {
	m, log := newBenchMetrics("bench_bp_churn")
	bus := pubsub.NewMemoryBus(log, m)
	ctx := context.Background()

	// Pre-publish so snapshot has data.
	bus.Publish(ctx, marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 100.0, Timestamp: time.Now(), Provider: "bench",
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s := bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		select {
		case <-s.C():
		case <-time.After(time.Millisecond):
		}
		s.Cancel()
	}
}
