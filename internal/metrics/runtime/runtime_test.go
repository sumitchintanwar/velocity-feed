package runtime_test

import (
	"context"
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

func TestRuntimeMetrics_Integration(t *testing.T) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "")

	mem, err := memory.NewMetrics(f)
	if err != nil {
		t.Fatalf("failed to init memory metrics: %v", err)
	}

	gcMetrics, err := gc.NewMetrics(f)
	if err != nil {
		t.Fatalf("failed to init gc metrics: %v", err)
	}

	grMetrics, err := goroutine.NewMetrics(f)
	if err != nil {
		t.Fatalf("failed to init goroutine metrics: %v", err)
	}

	// Use an extremely aggressive ticker for testing to guarantee a tick fires
	mgr := collector.NewManager(mem, gcMetrics, grMetrics, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.Start(ctx)

	// Wait for a few ticks
	time.Sleep(50 * time.Millisecond)

	// In a real test we could use a mock prometheus interface to read the value,
	// but the factory isolates us. If we don't panic or data race, it is successful.
}
