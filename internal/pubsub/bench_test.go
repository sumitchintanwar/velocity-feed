package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
)

func benchQuote(symbol string) marketdata.Quote {
	return marketdata.Quote{
		Symbol:    symbol,
		Type:      marketdata.QuoteTypeTrade,
		Price:     100.0,
		Bid:       99.95,
		Ask:       100.05,
		Volume:    1000,
		Timestamp: time.Now(),
		Provider:  "bench",
	}
}

func newBenchBus(b *testing.B) *MemoryBus {
	b.Helper()
	metrics, _ := platform.NewMetrics("bench_pubsub")
	return NewMemoryBus(zerolog.Nop(), metrics)
}

// ---------- Publish Throughput ----------

func BenchmarkPublish_1Sub(b *testing.B) {
	benchPublish(b, 1)
}

func BenchmarkPublish_10Subs(b *testing.B) {
	benchPublish(b, 10)
}

func BenchmarkPublish_100Subs(b *testing.B) {
	benchPublish(b, 100)
}

func BenchmarkPublish_1000Subs(b *testing.B) {
	benchPublish(b, 1000)
}

func benchPublish(b *testing.B, numSubs int) {
	bus := newBenchBus(b)
	for i := 0; i < numSubs; i++ {
		s := bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		defer s.Cancel()
	}
	ev := benchQuote("AAPL")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(ctx, ev)
	}
}

// ---------- Publish Parallel ----------

func BenchmarkPublishParallel_100Subs(b *testing.B) {
	benchPublishParallel(b, 100)
}

func BenchmarkPublishParallel_1000Subs(b *testing.B) {
	benchPublishParallel(b, 1000)
}

func BenchmarkPublishParallel_10000Subs(b *testing.B) {
	benchPublishParallel(b, 10000)
}

func benchPublishParallel(b *testing.B, numSubs int) {
	bus := newBenchBus(b)
	for i := 0; i < numSubs; i++ {
		s := bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		defer s.Cancel()
	}
	ev := benchQuote("AAPL")
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bus.Publish(ctx, ev)
		}
	})
}

// ---------- Subscribe / Unsubscribe ----------

func BenchmarkSubscribe_1Symbol(b *testing.B) {
	benchSubscribe(b, 1)
}

func BenchmarkSubscribe_10Symbols(b *testing.B) {
	benchSubscribe(b, 10)
}

func BenchmarkSubscribe_100Symbols(b *testing.B) {
	benchSubscribe(b, 100)
}

func benchSubscribe(b *testing.B, numSymbols int) {
	bus := newBenchBus(b)
	syms := make([]string, numSymbols)
	for i := range syms {
		syms[i] = fmt.Sprintf("SYM%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := bus.Subscribe(fmt.Sprintf("c%d", i), syms...)
		s.Cancel()
	}
}

func BenchmarkUnsubscribe_100Subs(b *testing.B) {
	bus := newBenchBus(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		subs := make([]Subscription, 100)
		for j := 0; j < 100; j++ {
			subs[j] = bus.Subscribe(fmt.Sprintf("c%d-%d", i, j), "AAPL")
		}
		b.StartTimer()
		for _, s := range subs {
			s.Cancel()
		}
	}
}

// ---------- Mixed Workload ----------

func BenchmarkMixed_80_20(b *testing.B) {
	bus := newBenchBus(b)
	for i := 0; i < 100; i++ {
		bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
	}
	ev := benchQuote("AAPL")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20% subscribe/unsubscribe
			s := bus.Subscribe(fmt.Sprintf("bench-%d", i), "AAPL")
			s.Cancel()
		} else {
			// 80% publish
			bus.Publish(ctx, ev)
		}
	}
}

// ---------- Snapshot Delivery ----------

func BenchmarkSnapshotDelivery(b *testing.B) {
	bus := newBenchBus(b)
	// Pre-populate snapshot.
	bus.Publish(context.Background(), benchQuote("AAPL"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		s.Cancel()
	}
}

// ---------- Memory per Subscriber ----------

func BenchmarkMemoryPerSubscriber(b *testing.B) {
	bus := newBenchBus(b)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		s.Cancel()
	}
}
