// Package replay provides business metrics for the historical data extraction API.
package replay

import (
	"fmt"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates throughput metrics for historical Replays.
type Metrics struct {
	RequestsTotal           interfaces.Counter
	MessagesDeliveredTotal  interfaces.Counter
}

// NewMetrics instantiates and registers the Replay metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	requests, err := f.NewCounter(
		"marketdata_replay_requests_total",
		"Total number of historical replay sessions requested by clients",
		[]string{"status"}, // e.g., "success", "failed", "timeout"
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register replay_requests_total: %w", err)
	}

	delivered, err := f.NewCounter(
		"marketdata_replay_messages_delivered_total",
		"Total number of historical market updates successfully delivered to clients",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register replay_messages_delivered_total: %w", err)
	}

	return &Metrics{
		RequestsTotal:          requests,
		MessagesDeliveredTotal: delivered,
	}, nil
}
