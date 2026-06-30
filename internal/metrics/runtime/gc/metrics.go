// Package gc provides runtime garbage collection observability.
package gc

import (
	"fmt"
	"runtime"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates garbage collection pressure and timing metrics.
type Metrics struct {
	CountTotal  interfaces.Gauge // Tracks total GC cycles (Gauge because MemStats is absolute)
	CPUFraction interfaces.Gauge
	// We can't perfectly track pause as a histogram via background polling without hooking into runtime trace,
	// but we can track the total pause time and derive it. For now we just track absolute pause total.
	PauseTotalSeconds interfaces.Gauge
	
	PauseSeconds interfaces.Histogram // New accurate histogram
	lastNumGC    uint32             // Tracks the last processed GC cycle
}

// NewMetrics instantiates and registers the runtime GC metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	count, err := f.NewGauge(
		"marketdata_runtime_gc_count_total",
		"Total number of completed GC cycles",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gc_count_total: %w", err)
	}

	cpuFrac, err := f.NewGauge(
		"marketdata_runtime_gc_cpu_fraction",
		"Fraction of CPU time used by the GC since application start",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gc_cpu_fraction: %w", err)
	}

	pause, err := f.NewGauge(
		"marketdata_runtime_gc_pause_total_seconds",
		"Total duration of GC stop-the-world pauses since application start",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gc_pause_total_seconds: %w", err)
	}

	pauseHist, err := f.NewHistogram(
		"marketdata_runtime_gc_pause_seconds",
		"Individual duration of GC stop-the-world pauses",
		[]float64{0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.01}, // 1us to 10ms
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gc_pause_seconds: %w", err)
	}

	return &Metrics{
		CountTotal:        count,
		CPUFraction:       cpuFrac,
		PauseTotalSeconds: pause,
		PauseSeconds:      pauseHist,
	}, nil
}

// Update reads current GC stats and pushes them to Prometheus.
func (m *Metrics) Update(stats *runtime.MemStats) {
	m.CountTotal.Set(float64(stats.NumGC))
	m.CPUFraction.Set(stats.GCCPUFraction)
	m.PauseTotalSeconds.Set(float64(stats.PauseTotalNs) / 1e9)

	// Process circular buffer of pauses
	numGC := stats.NumGC
	if numGC > m.lastNumGC {
		// Cap the diff to 256 since PauseNs is a circular buffer of 256
		diff := numGC - m.lastNumGC
		if diff > 256 {
			diff = 256
		}

		for i := uint32(0); i < diff; i++ {
			// Calculate the index in the circular buffer
			idx := (numGC - i - 1) % 256
			pauseNs := stats.PauseNs[idx]
			m.PauseSeconds.Observe(float64(pauseNs) / 1e9)
		}

		m.lastNumGC = numGC
	}
}
