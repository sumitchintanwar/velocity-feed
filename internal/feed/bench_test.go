package feed

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/pubsub"
)

// benchFeed is a mock feed that generates quotes as fast as possible.
type benchFeed struct {
	symbols []string
}

func (f *benchFeed) Name() string                                       { return "bench-feed" }
func (f *benchFeed) Subscribe(symbols ...string) error                  { return nil }
func (f *benchFeed) Unsubscribe(symbols ...string) error                { return nil }
func (f *benchFeed) Bars() (<-chan marketdata.Bar, error)               { return nil, nil }

func (f *benchFeed) Run(ctx context.Context) (<-chan marketdata.Quote, error) {
	out := make(chan marketdata.Quote, 1024)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				for _, sym := range f.symbols {
					q := marketdata.Quote{
						Symbol:    sym,
						Type:      marketdata.QuoteTypeTrade,
						Price:     100.0,
						Volume:    1000,
						Timestamp: time.Now(),
						Provider:  "bench",
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

// benchPublisher counts published events without fan-out overhead.
type benchPublisher struct {
	count atomic.Int64
}

func (p *benchPublisher) Publish(_ context.Context, _ marketdata.MarketEvent) {
	p.count.Add(1)
}

// ---------- Pipeline Throughput ----------

func BenchmarkPipeline_1Symbol(b *testing.B) {
	benchPipeline(b, []string{"AAPL"})
}

func BenchmarkPipeline_5Symbols(b *testing.B) {
	benchPipeline(b, []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"})
}

func BenchmarkPipeline_100Symbols(b *testing.B) {
	syms := make([]string, 100)
	for i := range syms {
		syms[i] = fmt.Sprintf("SYM%d", i)
	}
	benchPipeline(b, syms)
}

func benchPipeline(b *testing.B, symbols []string) {
	f := &benchFeed{symbols: symbols}
	pub := &benchPublisher{}
	p := NewPipeline(f, pub, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	pub.count.Store(0)

	for i := 0; i < b.N; i++ {
		for pub.count.Load() == 0 {
			time.Sleep(time.Microsecond)
		}
		pub.count.Add(-1)
	}

	elapsed := time.Since(start)
	b.StopTimer()

	eventsPerSec := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(eventsPerSec, "events/sec")
}

// ---------- Pipeline with MemoryBus (Subsystem) ----------

func BenchmarkPipeline_MemoryBus_1Sub(b *testing.B) {
	benchPipelineWithBus(b, 1)
}

func BenchmarkPipeline_MemoryBus_10Subs(b *testing.B) {
	benchPipelineWithBus(b, 10)
}

func BenchmarkPipeline_MemoryBus_100Subs(b *testing.B) {
	benchPipelineWithBus(b, 100)
}

func benchPipelineWithBus(b *testing.B, numSubs int) {
	f := &benchFeed{symbols: []string{"AAPL"}}
	metrics, _ := platform.NewMetrics("bench_feed")
	bus := pubsub.NewMemoryBus(zerolog.Nop(), metrics)

	subs := make([]pubsub.Subscription, numSubs)
	for i := 0; i < numSubs; i++ {
		subs[i] = bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
		go func() {
			for range subs[i].C() {
			}
		}()
	}

	p := NewPipeline(f, bus, zerolog.Nop())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		time.Sleep(time.Microsecond)
	}

	elapsed := time.Since(start)
	b.StopTimer()
	eventsPerSec := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(eventsPerSec, "events/sec")

	cancel()
	<-done
	for _, s := range subs {
		s.Cancel()
	}
}

// ---------- Pipeline Scaling ----------

func BenchmarkPipeline_Scaling(b *testing.B) {
	for _, numSubs := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("subs_%d", numSubs), func(b *testing.B) {
			f := &benchFeed{symbols: []string{"AAPL"}}
			metrics, _ := platform.NewMetrics("bench_feed")
			bus := pubsub.NewMemoryBus(zerolog.Nop(), metrics)

			subs := make([]pubsub.Subscription, numSubs)
			for i := 0; i < numSubs; i++ {
				subs[i] = bus.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
				go func() {
					for range subs[i].C() {
					}
				}()
			}

			p := NewPipeline(f, bus, zerolog.Nop())
			ctx, cancel := context.WithCancel(context.Background())

			done := make(chan struct{})
			go func() {
				p.Run(ctx)
				close(done)
			}()
			time.Sleep(10 * time.Millisecond)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				time.Sleep(time.Microsecond)
			}
			b.StopTimer()

			cancel()
			<-done
			for _, s := range subs {
				s.Cancel()
			}
		})
	}
}
