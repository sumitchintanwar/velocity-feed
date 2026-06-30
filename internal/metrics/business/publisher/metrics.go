// Package publisher provides throughput metrics for the publisher service.
package publisher

import (
	"fmt"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates all business-level metrics for the Publisher service.
type Metrics struct {
	MessagesTotal interfaces.Counter
	BytesTotal    interfaces.Counter
}

// NewMetrics instantiates and registers the Publisher metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	messages, err := f.NewCounter(
		"marketdata_publisher_messages_total",
		"Total number of market data messages successfully published",
		[]string{"exchange", "asset_class"},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register publisher_messages_total: %w", err)
	}

	bytes, err := f.NewCounter(
		"marketdata_publisher_bytes_total",
		"Total volume of market data published in bytes",
		[]string{"exchange", "asset_class"},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register publisher_bytes_total: %w", err)
	}

	return &Metrics{
		MessagesTotal: messages,
		BytesTotal:    bytes,
	}, nil
}

// AdapterMetrics pre-caches the dimension labels for a specific exchange and asset class.
// This structurally prevents developers from calling .With() dynamically on the hot path,
// ensuring that throughput metric updates process with exactly 0 B/op heap allocation.
type AdapterMetrics struct {
	MessagesTotal interfaces.Counter
	BytesTotal    interfaces.Counter
}

// NewAdapterMetrics pre-caches the counters for a specific exchange connection.
func (m *Metrics) NewAdapterMetrics(exchange, assetClass string) *AdapterMetrics {
	return &AdapterMetrics{
		MessagesTotal: m.MessagesTotal.With(exchange, assetClass),
		BytesTotal:    m.BytesTotal.With(exchange, assetClass),
	}
}
