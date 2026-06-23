package platform

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus instruments for the application.
// Use promauto so they register automatically with the default registry.
type Metrics struct {
	// Feed layer
	FeedMessagesReceived *prometheus.CounterVec
	FeedErrors           *prometheus.CounterVec

	// Distribution layer
	BroadcastsTotal    prometheus.Counter
	EventsDroppedTotal *prometheus.CounterVec
	SubscribersActive  prometheus.Gauge
	SubscriptionEvents *prometheus.CounterVec

	// WebSocket layer
	WSConnectionsActive prometheus.Gauge
	WSMessagesWritten   prometheus.Counter
	WSWriteErrors       prometheus.Counter

	// Transport layer
	HTTPRequestsTotal    *prometheus.CounterVec
	HTTPRequestDuration  *prometheus.HistogramVec
}

// NewMetrics creates an isolated Prometheus registry and registers all
// application instruments on it. The returned Gatherer must be passed to
// promhttp.HandlerFor so the /metrics endpoint only exposes these metrics.
func NewMetrics(namespace string) (*Metrics, prometheus.Gatherer) {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)

	return &Metrics{
		FeedMessagesReceived: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "messages_received_total",
			Help:      "Total number of raw messages received from upstream feeds.",
		}, []string{"symbol", "provider"}),

		FeedErrors: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "errors_total",
			Help:      "Total number of errors encountered while reading upstream feeds.",
		}, []string{"provider", "kind"}),

		BroadcastsTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distribution",
			Name:      "broadcasts_total",
			Help:      "Total number of market-data events broadcast to subscribers.",
		}),

		EventsDroppedTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distribution",
			Name:      "events_dropped_total",
			Help:      "Total number of events dropped per subscriber (buffer full).",
		}, []string{"subscriber"}),

		SubscribersActive: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "distribution",
			Name:      "subscribers_active",
			Help:      "Current number of active symbol subscribers.",
		}),

		SubscriptionEvents: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distribution",
			Name:      "subscription_events_total",
			Help:      "Total subscribe/unsubscribe events.",
		}, []string{"action"}), // action: subscribe | unsubscribe

		WSConnectionsActive: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "connections_active",
			Help:      "Current number of open WebSocket connections.",
		}),

		WSMessagesWritten: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "messages_written_total",
			Help:      "Total number of messages successfully written to WebSocket clients.",
		}),

		WSWriteErrors: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "write_errors_total",
			Help:      "Total number of errors when writing to WebSocket clients.",
		}),

		HTTPRequestsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests handled.",
		}, []string{"method", "route", "status"}),

		HTTPRequestDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Histogram of HTTP request durations.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}, reg
}
