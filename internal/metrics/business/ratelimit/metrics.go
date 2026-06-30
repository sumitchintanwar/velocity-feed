// Package ratelimit provides load shedding metrics.
package ratelimit

import (
	"fmt"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates operational metrics for platform protection against abusive traffic.
type Metrics struct {
	RequestsDeniedTotal interfaces.Counter
}

// NewMetrics instantiates and registers the Rate Limit metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	denied, err := f.NewCounter(
		"marketdata_ratelimit_requests_denied_total",
		"Total number of client requests rejected due to rate limiting limits",
		[]string{"endpoint"}, // e.g., "replay", "snapshot"
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register ratelimit_requests_denied_total: %w", err)
	}

	return &Metrics{
		RequestsDeniedTotal: denied,
	}, nil
}
