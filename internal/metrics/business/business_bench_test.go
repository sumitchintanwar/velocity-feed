package business_test

import (
	"sync"
	"testing"

	"github.com/sumit/rtmds/internal/metrics/business/publisher"
	"github.com/sumit/rtmds/internal/metrics/config"
	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/registry"
)

func BenchmarkBusinessMetrics_PublisherThroughput(b *testing.B) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "marketdata")
	
	pub, _ := publisher.NewMetrics(f)
	adapter := pub.NewAdapterMetrics("NASDAQ", "EQUITY")
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		adapter.MessagesTotal.Inc()
	}
}

func TestBusinessMetrics_StressConcurrency(t *testing.T) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "marketdata")
	
	pub, _ := publisher.NewMetrics(f)
	
	var wg sync.WaitGroup
	workers := 100
	updatesPerWorker := 1000

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			adapter := pub.NewAdapterMetrics("NYSE", "EQUITY")
			for j := 0; j < updatesPerWorker; j++ {
				adapter.MessagesTotal.Inc()
				adapter.BytesTotal.Add(256)
			}
		}()
	}

	wg.Wait()
}
