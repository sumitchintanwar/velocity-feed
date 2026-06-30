package factory_test

import (
	"testing"

	"github.com/sumit/rtmds/internal/metrics/config"
	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/registry"
)

func BenchmarkCounterInc(b *testing.B) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "bench")

	counter, _ := f.NewCounter("ops_total", "Total operations", []string{"label"})
	counterWithLabel := counter.With("value")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		counterWithLabel.Inc()
	}
}

func BenchmarkGaugeAdd(b *testing.B) {
	reg := registry.New()
	cfg := config.DefaultConfig()
	f := factory.New(reg, cfg, "bench")

	gauge, _ := f.NewGauge("active", "Active things", []string{})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		gauge.Add(1.0)
	}
}
