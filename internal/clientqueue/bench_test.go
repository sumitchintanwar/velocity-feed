package clientqueue

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/backpressure"
	"github.com/sumit/rtmds/internal/marketdata"
)

func benchEvent() marketdata.MarketEvent {
	return marketdata.Quote{
		Symbol:    "AAPL",
		Type:      marketdata.QuoteTypeQuote,
		Price:     100.0,
		Timestamp: time.Now(),
	}
}

func benchLogger() zerolog.Logger {
	return zerolog.Nop()
}

// ---------- Queue Send ----------

func BenchmarkQueue_DropOldest_Send(b *testing.B) {
	cfg := Config{QueueSize: 256, Policy: backpressure.PolicyDropOldest}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Send(ev)
	}
}

func BenchmarkQueue_DropNewest_Send(b *testing.B) {
	cfg := Config{QueueSize: 256, Policy: backpressure.PolicyDropNewest}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Send(ev)
	}
}

func BenchmarkQueue_Disconnect_Send(b *testing.B) {
	cfg := Config{
		QueueSize:            256,
		Policy:               backpressure.PolicyDisconnect,
		MaxConsecutiveDrops:  1000,
		DropWindow:           time.Second,
		DropThreshold:        0.9,
	}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Send(ev)
	}
}

// ---------- Queue Send+Receive (end-to-end) ----------

func BenchmarkQueue_EndToEnd_DropOldest(b *testing.B) {
	cfg := Config{QueueSize: 256, Policy: backpressure.PolicyDropOldest}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	// Consumer goroutine drains continuously.
	go func() {
		for range q.C() {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Send(ev)
	}
}

func BenchmarkQueue_EndToEnd_DropNewest(b *testing.B) {
	cfg := Config{QueueSize: 256, Policy: backpressure.PolicyDropNewest}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	go func() {
		for range q.C() {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Send(ev)
	}
}

// ---------- Concurrent Send ----------

func BenchmarkQueue_Concurrent_DropOldest(b *testing.B) {
	cfg := Config{QueueSize: 1024, Policy: backpressure.PolicyDropOldest}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	go func() {
		for range q.C() {
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Send(ev)
		}
	})
}

func BenchmarkQueue_Concurrent_DropNewest(b *testing.B) {
	cfg := Config{QueueSize: 1024, Policy: backpressure.PolicyDropNewest}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	go func() {
		for range q.C() {
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Send(ev)
		}
	})
}

// ---------- Manager ----------

func BenchmarkManager_Create(b *testing.B) {
	cfg := DefaultConfig()
	reg := prometheus.NewRegistry()
	mgr := NewManager(cfg, benchLogger(), reg)
	defer mgr.CloseAll()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Create("bench", nil)
	}
}

func BenchmarkManager_Get(b *testing.B) {
	cfg := DefaultConfig()
	reg := prometheus.NewRegistry()
	mgr := NewManager(cfg, benchLogger(), reg)
	defer mgr.CloseAll()

	for i := 0; i < 1000; i++ {
		mgr.Create(string(rune('A'+i%26))+string(rune('0'+i/26)), nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Get("bench")
	}
}

// ---------- Multi-client fan-out ----------

func BenchmarkFanOut_100Clients(b *testing.B) {
	cfg := Config{QueueSize: 256, Policy: backpressure.PolicyDropOldest}
	reg := prometheus.NewRegistry()
	mgr := NewManager(cfg, benchLogger(), reg)
	defer mgr.CloseAll()

	const numClients = 100
	queues := make([]*Queue, numClients)
	for i := 0; i < numClients; i++ {
		q := mgr.Create(string(rune('A'+i%26))+string(rune('0'+i/26)), nil)
		queues[i] = q
		go func(q *Queue) {
			for range q.C() {
			}
		}(q)
	}

	ev := benchEvent()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, q := range queues {
			q.Send(ev)
		}
	}
}

func BenchmarkFanOut_1000Clients(b *testing.B) {
	cfg := Config{QueueSize: 256, Policy: backpressure.PolicyDropOldest}
	reg := prometheus.NewRegistry()
	mgr := NewManager(cfg, benchLogger(), reg)
	defer mgr.CloseAll()

	const numClients = 1000
	queues := make([]*Queue, numClients)
	for i := 0; i < numClients; i++ {
		q := mgr.Create(string(rune('A'+i%26))+string(rune('0'+i/26)), nil)
		queues[i] = q
		go func(q *Queue) {
			for range q.C() {
			}
		}(q)
	}

	ev := benchEvent()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, q := range queues {
			q.Send(ev)
		}
	}
}

// ---------- Memory per queue ----------

func BenchmarkQueue_Alloc(b *testing.B) {
	cfg := Config{QueueSize: 256, Policy: backpressure.PolicyDropOldest}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Send(ev)
	}
}

// ---------- Burst absorption ----------

func BenchmarkBurst_Absorption(b *testing.B) {
	cfg := Config{QueueSize: 500, Policy: backpressure.PolicyDropOldest}
	reg := prometheus.NewRegistry()
	q := New("bench", cfg, benchLogger(), reg, nil)
	defer q.Close()
	ev := benchEvent()

	// Slow consumer: 1 event per 100 events produced.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for range q.C() {
			count++
			if count%100 == 0 {
				// Simulate slow drain.
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Send(ev)
	}
	b.StopTimer()
	q.Close()
	wg.Wait()
}
