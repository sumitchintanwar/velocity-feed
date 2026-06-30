// Package goroutine provides concurrency observability.
package goroutine

import (
	"fmt"
	"runtime"
	"runtime/pprof"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates goroutine counts and concurrency scaling.
type Metrics struct {
	Active  interfaces.Gauge
	Threads interfaces.Gauge
}

// NewMetrics instantiates and registers the runtime Goroutine metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	active, err := f.NewGauge(
		"marketdata_runtime_goroutines_active",
		"Current number of active goroutines",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register goroutines_active: %w", err)
	}

	threads, err := f.NewGauge(
		"marketdata_runtime_threads_active",
		"Current number of physical OS threads",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register threads_active: %w", err)
	}

	return &Metrics{
		Active:  active,
		Threads: threads,
	}, nil
}

// Update reads current goroutine count and OS thread count and pushes it to Prometheus.
func (m *Metrics) Update() {
	m.Active.Set(float64(runtime.NumGoroutine()))

	if profile := pprof.Lookup("threadcreate"); profile != nil {
		m.Threads.Set(float64(profile.Count()))
	}
}
