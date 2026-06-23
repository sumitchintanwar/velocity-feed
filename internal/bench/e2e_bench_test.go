package bench

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/feed"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/pubsub"
	"github.com/sumit/rtmds/internal/topicmanager"
)

// latencyRecorder captures generation-to-delivery latency in a histogram.
type latencyRecorder struct {
	histogram []time.Duration
}

func newLatencyRecorder(capacity int) *latencyRecorder {
	return &latencyRecorder{histogram: make([]time.Duration, 0, capacity)}
}

func (r *latencyRecorder) Record(d time.Duration) {
	r.histogram = append(r.histogram, d)
}

func (r *latencyRecorder) Percentile(p float64) time.Duration {
	if len(r.histogram) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(r.histogram))
	copy(sorted, r.histogram)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

func (r *latencyRecorder) Summary() string {
	return fmt.Sprintf(
		"P50=%s P95=%s P99=%s P99.9=%s (n=%d)",
		r.Percentile(0.50),
		r.Percentile(0.95),
		r.Percentile(0.99),
		r.Percentile(0.999),
		len(r.histogram),
	)
}

// BucketCounts returns the number of samples in each latency bucket.
// Buckets: 0-100us, 100us-500us, 500us-1ms, 1-2ms, 2-5ms, 5-10ms, 10-50ms, 50ms+
func (r *latencyRecorder) BucketCounts() map[string]int {
	buckets := map[string]int{
		"0-100us":   0,
		"100us-500us": 0,
		"500us-1ms": 0,
		"1-2ms":     0,
		"2-5ms":     0,
		"5-10ms":    0,
		"10-50ms":   0,
		"50ms+":     0,
	}
	for _, d := range r.histogram {
		us := d.Microseconds()
		switch {
		case us < 100:
			buckets["0-100us"]++
		case us < 500:
			buckets["100us-500us"]++
		case us < 1000:
			buckets["500us-1ms"]++
		case us < 2000:
			buckets["1-2ms"]++
		case us < 5000:
			buckets["2-5ms"]++
		case us < 10000:
			buckets["5-10ms"]++
		case us < 50000:
			buckets["10-50ms"]++
		default:
			buckets["50ms+"]++
		}
	}
	return buckets
}

// HistogramString returns a human-readable histogram of latency buckets.
func (r *latencyRecorder) HistogramString() string {
	buckets := r.BucketCounts()
	total := len(r.histogram)
	if total == 0 {
		return "no samples"
	}
	keys := []string{"0-100us", "100us-500us", "500us-1ms", "1-2ms", "2-5ms", "5-10ms", "10-50ms", "50ms+"}
	maxBar := 40
	maxCount := 0
	for _, k := range keys {
		if buckets[k] > maxCount {
			maxCount = buckets[k]
		}
	}
	var s string
	for _, k := range keys {
		count := buckets[k]
		pct := float64(count) / float64(total) * 100
		barLen := 0
		if maxCount > 0 {
			barLen = buckets[k] * maxBar / maxCount
		}
		bar := ""
		for i := 0; i < barLen; i++ {
			bar += "#"
		}
		s += fmt.Sprintf("  %-14s %6d (%5.1f%%) %s\n", k, count, pct, bar)
	}
	return s
}

// timedFeed is a feed that embeds generation timestamps for latency measurement.
type timedFeed struct {
	symbols []string
}

func (f *timedFeed) Name() string                                       { return "timed-feed" }
func (f *timedFeed) Subscribe(symbols ...string) error                  { return nil }
func (f *timedFeed) Unsubscribe(symbols ...string) error                { return nil }
func (f *timedFeed) Bars() (<-chan marketdata.Bar, error)               { return nil, nil }

func (f *timedFeed) Run(ctx context.Context) (<-chan marketdata.Quote, error) {
	out := make(chan marketdata.Quote, 1024)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				now := time.Now()
				for _, sym := range f.symbols {
					q := marketdata.Quote{
						Symbol:    sym,
						Type:      marketdata.QuoteTypeTrade,
						Price:     100.0,
						Volume:    1000,
						Timestamp: now,
						Provider:  "timed",
					}
					select {
					case out <- q:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}

func newBenchMetrics(prefix string) (*platform.Metrics, zerolog.Logger) {
	m, _ := platform.NewMetrics(prefix)
	return m, zerolog.Nop()
}

// ---------- End-to-End Latency: Pipeline → Bus → Subscriber ----------

func BenchmarkEndToEndLatency_1Sub(b *testing.B) {
	benchE2ELatency(b, 1)
}

func BenchmarkEndToEndLatency_10Subs(b *testing.B) {
	benchE2ELatency(b, 10)
}

func BenchmarkEndToEndLatency_100Subs(b *testing.B) {
	benchE2ELatency(b, 100)
}

func BenchmarkEndToEndLatency_1000Subs(b *testing.B) {
	benchE2ELatency(b, 1000)
}

func benchE2ELatency(b *testing.B, numSubs int) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	m, log := newBenchMetrics("bench_e2e")
	bus := pubsub.NewMemoryBus(log, m)

	rec := newLatencyRecorder(b.N)
	var delivered atomic.Int64

	subs := make([]pubsub.Subscription, numSubs)
	for i := 0; i < numSubs; i++ {
		subs[i] = bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		go func() {
			for ev := range subs[i].C() {
				lat := time.Since(ev.(marketdata.Quote).Timestamp)
				rec.Record(lat)
				delivered.Add(1)
			}
		}()
	}

	p := feed.NewPipeline(f, bus, log)
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
		for delivered.Load() == 0 {
			time.Sleep(time.Microsecond)
		}
		delivered.Add(-1)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
	b.ReportMetric(float64(rec.Percentile(0.50).Microseconds()), "p50_us")
	b.ReportMetric(float64(rec.Percentile(0.95).Microseconds()), "p95_us")
	b.ReportMetric(float64(rec.Percentile(0.99).Microseconds()), "p99_us")

	b.Logf("Latency: %s", rec.Summary())

	cancel()
	<-done
	for _, s := range subs {
		s.Cancel()
	}
}

// ---------- End-to-End Throughput ----------

func BenchmarkEndToEndThroughput(b *testing.B) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	m, log := newBenchMetrics("bench_e2e_tput")
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

	p := feed.NewPipeline(f, bus, log)
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
		for delivered.Load() == 0 {
			time.Sleep(time.Microsecond)
		}
		delivered.Add(-1)
	}

	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "events_sec")
	b.ReportMetric(float64(b.N)/elapsed.Seconds()/100, "per_subscriber_sec")

	cancel()
	<-done
	for _, s := range subs {
		s.Cancel()
	}
}

// ---------- Topic Manager Interaction (E2E via TopicManager) ----------

func BenchmarkEndToEndWithTopicManager(b *testing.B) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	_, log := newBenchMetrics("bench_e2e_tm")
	tm := topicmanager.New(0)
	pub := topicmanagerPublisherAdapter{tm: tm}

	var delivered atomic.Int64
	subs := make([]topicmanager.Handle, 100)
	for i := 0; i < 100; i++ {
		subs[i] = tm.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		go func() {
			for range subs[i].C() {
				delivered.Add(1)
			}
		}()
	}

	p := feed.NewPipeline(f, &pub, log)
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
		for delivered.Load() == 0 {
			time.Sleep(time.Microsecond)
		}
		delivered.Add(-1)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")

	cancel()
	<-done
	for _, h := range subs {
		h.Cancel()
	}
}

// topicmanagerPublisherAdapter adapts topicmanager.Manager to pubsub.Publisher.
type topicmanagerPublisherAdapter struct {
	tm topicmanager.Manager
}

func (a *topicmanagerPublisherAdapter) Publish(ctx context.Context, event marketdata.MarketEvent) {
	a.tm.Publish(ctx, event)
}

// ---------- Memory per Event ----------

func BenchmarkMemoryPerEvent(b *testing.B) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	m, log := newBenchMetrics("bench_e2e_mem")
	bus := pubsub.NewMemoryBus(log, m)

	var delivered atomic.Int64
	subs := make([]pubsub.Subscription, 10)
	for i := 0; i < 10; i++ {
		subs[i] = bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		go func() {
			for range subs[i].C() {
				delivered.Add(1)
			}
		}()
	}

	p := feed.NewPipeline(f, bus, log)
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
		for delivered.Load() == 0 {
			time.Sleep(time.Microsecond)
		}
		delivered.Add(-1)
	}

	cancel()
	<-done
	for _, s := range subs {
		s.Cancel()
	}
}

// ---------- Goroutine Leak Detection ----------

func BenchmarkGoroutineLeakDetection(b *testing.B) {
	m, log := newBenchMetrics("bench_e2e_leak")
	bus := pubsub.NewMemoryBus(log, m)
	ctx := context.Background()

	// Pre-publish so subscriptions receive a snapshot immediately.
	bus.Publish(ctx, marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 100.0, Timestamp: time.Now(), Provider: "bench",
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		before := runtime.NumGoroutine()
		s := bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		// Drain one event to avoid blocking the publisher.
		select {
		case <-s.C():
		case <-time.After(time.Millisecond):
		}
		s.Cancel()
		time.Sleep(5 * time.Millisecond) // allow cleanup
		after := runtime.NumGoroutine()
		_ = after - before // leak would show as growth
	}
}

// ---------- Latency Histogram ----------

func BenchmarkLatencyHistogram_100Subs(b *testing.B) {
	f := &timedFeed{symbols: []string{"AAPL"}}
	m, log := newBenchMetrics("bench_hist")
	bus := pubsub.NewMemoryBus(log, m)

	rec := newLatencyRecorder(b.N)
	var delivered atomic.Int64

	subs := make([]pubsub.Subscription, 100)
	for i := 0; i < 100; i++ {
		subs[i] = bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		go func() {
			for ev := range subs[i].C() {
				lat := time.Since(ev.(marketdata.Quote).Timestamp)
				rec.Record(lat)
				delivered.Add(1)
			}
		}()
	}

	p := feed.NewPipeline(f, bus, log)
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
		for delivered.Load() == 0 {
			time.Sleep(time.Microsecond)
		}
		delivered.Add(-1)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
	b.ReportMetric(float64(rec.Percentile(0.50).Microseconds()), "p50_us")
	b.ReportMetric(float64(rec.Percentile(0.95).Microseconds()), "p95_us")
	b.ReportMetric(float64(rec.Percentile(0.99).Microseconds()), "p99_us")

	b.Logf("Latency: %s", rec.Summary())
	b.Logf("Histogram:\n%s", rec.HistogramString())

	cancel()
	<-done
	for _, s := range subs {
		s.Cancel()
	}
}

// ---------- Topic Explosion ----------

func BenchmarkTopicExplosion_1000Topics(b *testing.B) {
	benchTopicExplosion(b, 1000)
}

func BenchmarkTopicExplosion_10000Topics(b *testing.B) {
	benchTopicExplosion(b, 10000)
}

func benchTopicExplosion(b *testing.B, numTopics int) {
	tm := topicmanager.New(0)
	topics := make([]topicmanager.Topic, numTopics)
	for i := range topics {
		topics[i] = fmt.Sprintf("SYM%d", i)
	}

	// Register one subscriber per topic.
	var handles []topicmanager.Handle
	for i, t := range topics {
		handles = append(handles, tm.Subscribe(fmt.Sprintf("c%d", i), t))
	}

	ev := marketdata.Quote{
		Symbol: topics[0], Type: marketdata.QuoteTypeTrade,
		Price: 100.0, Timestamp: time.Now(),
	}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tm.Publish(ctx, ev)
	}
	b.StopTimer()
	b.ReportMetric(float64(numTopics), "topics")

	for _, h := range handles {
		h.Cancel()
	}
}

// ---------- Memory Scaling ----------

func BenchmarkMemoryScaling(b *testing.B) {
	for _, numSubs := range []int{10, 100, 1000, 5000} {
		b.Run(fmt.Sprintf("subs_%d", numSubs), func(b *testing.B) {
			f := &timedFeed{symbols: []string{"AAPL"}}
			m, log := newBenchMetrics("bench_mem_scale")
			bus := pubsub.NewMemoryBus(log, m)

			var delivered atomic.Int64
			subs := make([]pubsub.Subscription, numSubs)
			for i := 0; i < numSubs; i++ {
				subs[i] = bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
				go func() {
					for range subs[i].C() {
						delivered.Add(1)
					}
				}()
			}

			p := feed.NewPipeline(f, bus, log)
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
				for delivered.Load() == 0 {
					time.Sleep(time.Microsecond)
				}
				delivered.Add(-1)
			}

			b.StopTimer()
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			b.ReportMetric(float64(memStats.NumGC), "gc_cycles")
			b.ReportMetric(float64(memStats.Sys), "sys_bytes")

			cancel()
			<-done
			for _, s := range subs {
				s.Cancel()
			}
		})
	}
}
