package runtime_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/metrics/config"
	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/registry"
	"github.com/sumit/rtmds/internal/metrics/runtime/collector"
	"github.com/sumit/rtmds/internal/metrics/runtime/gc"
	"github.com/sumit/rtmds/internal/metrics/runtime/goroutine"
	"github.com/sumit/rtmds/internal/metrics/runtime/memory"
)

func BenchmarkRuntimeMetrics_ManagerPoll(b *testing.B) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "marketdata")

	mem, _ := memory.NewMetrics(f)
	g, _ := gc.NewMetrics(f)
	gr, _ := goroutine.NewMetrics(f)

	mgr := collector.NewManager(mem, g, gr, time.Minute) // We will poll manually
	_ = mgr // Prevent unused variable error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start it but it won't tick often.
	mgr.Start(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	// We'll benchmark allocating memory to force runtime stats to change
	for i := 0; i < b.N; i++ {
		// Just force a tiny allocation
		_ = make([]byte, 1024)
	}
}

func TestRuntimeMetrics_StressConcurrency(t *testing.T) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "marketdata")

	mem, _ := memory.NewMetrics(f)
	g, _ := gc.NewMetrics(f)
	gr, _ := goroutine.NewMetrics(f)

	// Aggressive polling
	mgr := collector.NewManager(mem, g, gr, 1*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.Start(ctx)

	var wg sync.WaitGroup
	workers := 100
	updatesPerWorker := 100

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < updatesPerWorker; j++ {
				// Allocate and trigger GC indirectly
				_ = make([]byte, 8192)
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}
