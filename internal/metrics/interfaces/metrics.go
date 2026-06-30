// Package interfaces defines the abstract metric types.
//
// Purpose:
// Decouples application logic from the concrete Prometheus library implementation.
// This allows services to use metrics without importing Prometheus, making it 
// easier to mock metrics in unit tests and preventing tight coupling.
//
// Architecture & Design Decisions:
// - Exposes standard methods (Inc, Add, Set, Observe).
// - Uses With(labelValues...) to support high-cardinality vectors dynamically.
package interfaces

import "context"

// Counter represents a cumulative metric that only goes up (e.g., total requests, errors).
type Counter interface {
	// Inc increments the counter by 1.
	Inc()
	// Add increments the counter by the given value.
	Add(val float64)
	// With returns a new Counter bound to the specific label values.
	With(labelValues ...string) Counter
}

// Gauge represents a metric that can go up and down (e.g., memory usage, queue depth).
type Gauge interface {
	// Set sets the gauge to the given value.
	Set(val float64)
	// Inc increments the gauge by 1.
	Inc()
	// Dec decrements the gauge by 1.
	Dec()
	// Add adds the given value to the gauge.
	Add(val float64)
	// Sub subtracts the given value from the gauge.
	Sub(val float64)
	// With returns a new Gauge bound to the specific label values.
	With(labelValues ...string) Gauge
}

// Histogram represents a metric that samples observations and counts them in configurable buckets.
// Primarily used for latencies or request sizes.
type Histogram interface {
	// Observe adds a single observation to the histogram.
	Observe(val float64)
	// ObserveWithContext adds an observation and extracts the active trace_id for Prometheus Exemplars.
	ObserveWithContext(ctx context.Context, val float64)
	// With returns a new Histogram bound to the specific label values.
	With(labelValues ...string) Histogram
}

// Summary represents a metric that calculates sliding-window quantiles.
// Use sparingly, as quantiles cannot be aggregated across instances.
type Summary interface {
	// Observe adds a single observation to the summary.
	Observe(val float64)
	// With returns a new Summary bound to the specific label values.
	With(labelValues ...string) Summary
}
