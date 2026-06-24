package workerpool

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// benchPublisher counts events without doing real work.
type benchPublisher struct {
	count atomic.Int64
}

func (p *benchPublisher) Publish(_ context.Context, _ any) {
	p.count.Add(1)
}

// ---------- Throughput (enqueue + publish, E2E) ----------

func BenchmarkPool_Throughput_1Worker(b *testing.B) {
	benchPoolThroughput(b, 1, 4096)
}

func BenchmarkPool_Throughput_4Workers(b *testing.B) {
	benchPoolThroughput(b, 4, 4096)
}

func BenchmarkPool_Throughput_8Workers(b *testing.B) {
	benchPoolThroughput(b, 8, 4096)
}

func BenchmarkPool_Throughput_16Workers(b *testing.B) {
	benchPoolThroughput(b, 16, 4096)
}

func benchPoolThroughput(b *testing.B, workers, queueCap int) {
	pub := &benchPublisher{}
	cfg := Config{Workers: workers, QueueCapacity: queueCap, ShutdownTimeout: 10 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Enqueue(i)
	}
	// Wait for all events to be processed before stopping the timer.
	_ = p.Shutdown(ctx)
	b.StopTimer()

	eventsPerSec := float64(pub.count.Load()) / b.Elapsed().Seconds()
	b.ReportMetric(eventsPerSec, "events_sec")
}

// ---------- Queue Sizing ----------

func BenchmarkPool_Queue_1024(b *testing.B) {
	benchPoolQueue(b, 1024)
}

func BenchmarkPool_Queue_4096(b *testing.B) {
	benchPoolQueue(b, 4096)
}

func BenchmarkPool_Queue_16384(b *testing.B) {
	benchPoolQueue(b, 16384)
}

func benchPoolQueue(b *testing.B, queueCap int) {
	pub := &benchPublisher{}
	cfg := Config{Workers: 8, QueueCapacity: queueCap, ShutdownTimeout: 10 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Enqueue(i)
	}
	_ = p.Shutdown(ctx)
	b.StopTimer()
}

// ---------- Drop Rate Under Pressure ----------

func BenchmarkPool_DropRate(b *testing.B) {
	// 1 worker, tiny queue — forces drops.
	pub := &benchPublisher{}
	cfg := Config{Workers: 1, QueueCapacity: 16, ShutdownTimeout: 10 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Enqueue(i)
	}
	_ = p.Shutdown(ctx)
	b.StopTimer()

	dropped := p.Stats().Dropped.Load()
	total := int64(b.N)
	if dropped > 0 {
		b.ReportMetric(float64(dropped)/float64(total)*100, "drop_pct")
	}
}

// ---------- Scaling ----------

func BenchmarkPool_Scaling(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			pub := &benchPublisher{}
			cfg := Config{Workers: workers, QueueCapacity: 4096, ShutdownTimeout: 10 * time.Second}
			p := New(cfg, pub, nopLogger(), nil)

			ctx := context.Background()
			p.Start(ctx)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				p.Enqueue(i)
			}
			_ = p.Shutdown(ctx)
			b.StopTimer()

			eventsPerSec := float64(pub.count.Load()) / b.Elapsed().Seconds()
			b.ReportMetric(eventsPerSec, "events_sec")
		})
	}
}
