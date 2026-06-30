package factory_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sumit/rtmds/internal/metrics/config"
	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/registry"
)

func TestFactory_Registration(t *testing.T) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "test_component")

	counter, err := f.NewCounter("messages_total", "Total messages", []string{"status"})
	if err != nil {
		t.Fatalf("failed to register counter: %v", err)
	}
	if counter == nil {
		t.Fatal("counter is nil")
	}

	// Test Duplicate Registration handling
	counter2, err := f.NewCounter("messages_total", "Total messages", []string{"status"})
	if err != nil {
		t.Fatalf("duplicate registration failed to return safely: %v", err)
	}

	// In our safe registry wrapper, duplicate registration shouldn't panic, it should just return the existing.
	if counter2 == nil {
		t.Fatal("duplicate registration returned nil")
	}

	counter.With("ok").Inc()
}

func TestFactory_Gauge(t *testing.T) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "gateway")

	gauge, err := f.NewGauge("active_connections", "Number of active connections", []string{"type"})
	if err != nil {
		t.Fatalf("failed to register gauge: %v", err)
	}

	gauge.With("ws").Add(5)
	gauge.With("ws").Sub(2)
}

func TestFactory_Histogram(t *testing.T) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "publisher")

	hist, err := f.NewHistogram("publish_latency", "Latency in seconds", prometheus.DefBuckets, []string{})
	if err != nil {
		t.Fatalf("failed to register histogram: %v", err)
	}

	hist.Observe(0.5)
}
