// Package memory provides runtime memory allocation observability.
package memory

import (
	"fmt"
	"runtime"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates heap and OS memory metrics.
type Metrics struct {
	HeapAlloc    interfaces.Gauge
	HeapInuse    interfaces.Gauge
	HeapIdle     interfaces.Gauge
	HeapReleased interfaces.Gauge
}

// NewMetrics instantiates and registers the runtime Memory metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	alloc, err := f.NewGauge(
		"marketdata_runtime_heap_alloc_bytes",
		"Bytes of allocated heap objects",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register heap_alloc_bytes: %w", err)
	}

	inuse, err := f.NewGauge(
		"marketdata_runtime_heap_inuse_bytes",
		"Bytes in in-use spans",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register heap_inuse_bytes: %w", err)
	}

	idle, err := f.NewGauge(
		"marketdata_runtime_heap_idle_bytes",
		"Bytes in idle (unused) spans",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register heap_idle_bytes: %w", err)
	}

	released, err := f.NewGauge(
		"marketdata_runtime_heap_released_bytes",
		"Bytes of idle spans returned to the OS",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register heap_released_bytes: %w", err)
	}

	return &Metrics{
		HeapAlloc:    alloc,
		HeapInuse:    inuse,
		HeapIdle:     idle,
		HeapReleased: released,
	}, nil
}

// Update reads current memory stats and pushes them to Prometheus.
func (m *Metrics) Update(stats *runtime.MemStats) {
	m.HeapAlloc.Set(float64(stats.Alloc))
	m.HeapInuse.Set(float64(stats.HeapInuse))
	m.HeapIdle.Set(float64(stats.HeapIdle))
	m.HeapReleased.Set(float64(stats.HeapReleased))
}
