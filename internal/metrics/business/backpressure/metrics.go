// Package backpressure provides metrics to detect internal component saturation.
package backpressure

import (
	"fmt"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates saturation points across the platform.
type Metrics struct {
	EventsTotal interfaces.Counter
}

// NewMetrics instantiates and registers the Backpressure metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	events, err := f.NewCounter(
		"marketdata_backpressure_events_total",
		"Total number of backpressure triggers (e.g., full channels, blocked workers)",
		[]string{"component"}, // e.g., "redis_publisher", "gateway_writer"
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register backpressure_events_total: %w", err)
	}

	return &Metrics{
		EventsTotal: events,
	}, nil
}
