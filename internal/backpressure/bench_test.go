package backpressure

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
)

func benchEvent() marketdata.MarketEvent {
	return marketdata.Quote{
		Symbol:    "AAPL",
		Type:      marketdata.QuoteTypeQuote,
		Price:     150.00,
		Timestamp: time.Now(),
	}
}

func benchMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	return NewMetrics(reg)
}

// ---------- Ring buffer benchmarks ----------

func BenchmarkRing_Push(b *testing.B) {
	r := NewRing(1024)
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Push(ev)
	}
}

func BenchmarkRing_PushNoDrop(b *testing.B) {
	r := NewRing(1 << 20) // large buffer, no drops
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Push(ev)
	}
}

func BenchmarkRing_PushDrop(b *testing.B) {
	r := NewRing(4) // tiny buffer, every push drops
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Push(ev)
	}
}

func BenchmarkRing_Pop(b *testing.B) {
	r := NewRing(1024)
	ev := benchEvent()
	// Pre-fill.
	for i := 0; i < 1024; i++ {
		r.Push(ev)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Pop()
	}
}

func BenchmarkRing_PushPop(b *testing.B) {
	r := NewRing(64)
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Push(ev)
		r.Pop()
	}
}

// ---------- DropOldest channel benchmarks ----------

func BenchmarkChannel_DropOldest_Push(b *testing.B) {
	cfg := Config{Policy: PolicyDropOldest, BufferSize: 1024}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Send(ev)
	}
}

func BenchmarkChannel_DropOldest_PushDrop(b *testing.B) {
	cfg := Config{Policy: PolicyDropOldest, BufferSize: 4}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Send(ev)
	}
}

func BenchmarkChannel_DropOldest_Concurrent(b *testing.B) {
	cfg := Config{Policy: PolicyDropOldest, BufferSize: 1024}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch.Send(ev)
		}
	})
}

// ---------- DropNewest channel benchmarks ----------

func BenchmarkChannel_DropNewest_Push(b *testing.B) {
	cfg := Config{Policy: PolicyDropNewest, BufferSize: 1024}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Send(ev)
	}
}

func BenchmarkChannel_DropNewest_PushDrop(b *testing.B) {
	cfg := Config{Policy: PolicyDropNewest, BufferSize: 4}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Send(ev)
	}
}

func BenchmarkChannel_DropNewest_Concurrent(b *testing.B) {
	cfg := Config{Policy: PolicyDropNewest, BufferSize: 1024}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch.Send(ev)
		}
	})
}

// ---------- Disconnect channel benchmarks ----------

func BenchmarkChannel_Disconnect_Push(b *testing.B) {
	cfg := Config{
		Policy:              PolicyDisconnect,
		BufferSize:          1024,
		MaxConsecutiveDrops: 100,
	}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Send(ev)
	}
}

func BenchmarkChannel_Disconnect_Concurrent(b *testing.B) {
	cfg := Config{
		Policy:              PolicyDisconnect,
		BufferSize:          1024,
		MaxConsecutiveDrops: 100,
	}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch.Send(ev)
		}
	})
}

// ---------- End-to-end: producer + consumer ----------

func BenchmarkChannel_EndToEnd_DropOldest(b *testing.B) {
	cfg := Config{Policy: PolicyDropOldest, BufferSize: 256}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range ch.C() {
			// consumer reads
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Send(ev)
	}
	b.StopTimer()
	ch.Close()
	wg.Wait()
}

func BenchmarkChannel_EndToEnd_DropNewest(b *testing.B) {
	cfg := Config{Policy: PolicyDropNewest, BufferSize: 256}
	m := benchMetrics()
	ch := NewChannel(cfg, nopLogger(), m, nil)
	defer ch.Close()
	ev := benchEvent()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range ch.C() {
			// consumer reads
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Send(ev)
	}
	b.StopTimer()
	ch.Close()
	wg.Wait()
}

// ---------- Helpers ----------

func nopLogger() zerolog.Logger {
	return zerolog.Nop()
}
