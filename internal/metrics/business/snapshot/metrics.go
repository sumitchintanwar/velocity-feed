// Package snapshot provides business metrics for the state generation service.
package snapshot

import (
	"fmt"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates observability for in-memory market data snapshots.
type Metrics struct {
	GenerationDuration interfaces.Histogram
}

// NewMetrics instantiates and registers the Snapshot metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	duration, err := f.NewHistogram(
		"marketdata_snapshot_generation_duration_seconds",
		"Time taken to generate and persist a market data snapshot",
		[]float64{0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0}, // tailored up to 60s
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register snapshot_generation_duration_seconds: %w", err)
	}

	return &Metrics{
		GenerationDuration: duration,
	}, nil
}
