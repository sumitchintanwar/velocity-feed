// Package reliability provides correctness and safety metrics for the data stream.
package reliability

import (
	"fmt"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

// Metrics encapsulates all operational health indicators for stream integrity.
type Metrics struct {
	SequenceGapsTotal       interfaces.Counter
	DroppedMessagesTotal    interfaces.Counter
	RecoveryAttemptsTotal   interfaces.Counter
}

// NewMetrics instantiates and registers the Reliability metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	gaps, err := f.NewCounter(
		"marketdata_reliability_sequence_gaps_total",
		"Total number of missing sequence numbers detected in the stream",
		[]string{"exchange", "feed"},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register reliability_sequence_gaps_total: %w", err)
	}

	dropped, err := f.NewCounter(
		"marketdata_reliability_dropped_messages_total",
		"Total number of market data messages permanently dropped (e.g., due to parse errors)",
		[]string{"exchange", "feed", "reason"}, // e.g., reason="parse_error", "validation_failed"
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register reliability_dropped_messages_total: %w", err)
	}

	recovery, err := f.NewCounter(
		"marketdata_reliability_recovery_attempts_total",
		"Total number of stream recovery sequences initiated",
		[]string{"status"}, // e.g., "success", "failed"
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register reliability_recovery_attempts_total: %w", err)
	}

	return &Metrics{
		SequenceGapsTotal:     gaps,
		DroppedMessagesTotal:  dropped,
		RecoveryAttemptsTotal: recovery,
	}, nil
}
