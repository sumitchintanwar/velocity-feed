package backpressure

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus instruments for backpressure monitoring.
type Metrics struct {
	// EventsDroppedTotal counts events dropped across all channels.
	EventsDroppedTotal prometheus.Counter

	// BufferOccupancy records the current fill ratio of ring buffers.
	// Bucketed histogram for dashboards and alerting.
	BufferOccupancy prometheus.Histogram

	// ConsecutiveDrops tracks the current consecutive drop count per channel.
	// Used for alerting on sustained backpressure.
	ConsecutiveDrops prometheus.Gauge

	// ConsumerDisconnectsTotal counts total consumer disconnections.
	ConsumerDisconnectsTotal prometheus.Counter

	// SendLatencySeconds measures the time to enqueue an event.
	SendLatencySeconds prometheus.Histogram
}

// NewMetrics creates and registers backpressure metrics on the given registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)

	return &Metrics{
		EventsDroppedTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "backpressure",
			Name:      "events_dropped_total",
			Help:      "Total number of events dropped due to backpressure.",
		}),

		BufferOccupancy: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "rtmds",
			Subsystem: "backpressure",
			Name:      "buffer_occupancy_ratio",
			Help:      "Current buffer fill ratio (0.0 to 1.0).",
			Buckets:   []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0},
		}),

		ConsecutiveDrops: f.NewGauge(prometheus.GaugeOpts{
			Namespace: "rtmds",
			Subsystem: "backpressure",
			Name:      "consecutive_drops",
			Help:      "Current consecutive drop count for the slowest consumer.",
		}),

		ConsumerDisconnectsTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "backpressure",
			Name:      "consumer_disconnects_total",
			Help:      "Total number of consumers disconnected due to backpressure.",
		}),

		SendLatencySeconds: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "rtmds",
			Subsystem: "backpressure",
			Name:      "send_latency_seconds",
			Help:      "Time in seconds to enqueue an event into the backpressure buffer.",
			Buckets:   prometheus.DefBuckets,
		}),
	}
}
