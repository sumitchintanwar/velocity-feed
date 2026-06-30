package platform

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus instruments for the application.
// Use promauto so they register automatically with the default registry.
//
// Cardinality rules:
//   - NO labels by symbol, topic, ticker, client_id, or ip_address.
//   - Labels use only bounded dimensions (provider, reason, action).
type Metrics struct {
	// Feed layer
	FeedMessagesReceived *prometheus.CounterVec // labels: provider
	FeedErrors           *prometheus.CounterVec // labels: provider, kind
	DataStaleness        prometheus.Gauge       // current_system_time - exchange_timestamp

	// Distribution layer
	BroadcastsTotal    prometheus.Counter
	EventsDroppedTotal prometheus.Counter
	SubscribersActive  prometheus.Gauge
	SubscriptionEvents *prometheus.CounterVec

	// WebSocket layer
	WSConnectionsActive      prometheus.Gauge
	WSConnectionsOpened      prometheus.Counter
	WSConnectionsClosed      prometheus.Counter
	WSConnectionAttempts     prometheus.Counter
	WSActiveSubscriptions    prometheus.Gauge
	WSSlowConsumers          prometheus.Gauge
	WSMessagesWritten        prometheus.Counter
	WSWriteErrors            prometheus.Counter
	WSBytesSent              prometheus.Counter
	WSMessageSize            prometheus.Histogram
	WSDeliveryLatency        prometheus.Histogram
	WSPingLatency            prometheus.Histogram
	WSPingSentTotal          prometheus.Counter
	WSPongReceivedTotal      prometheus.Counter
	WSTimeoutsTotal          prometheus.Counter
	WSHeartbeatCleanupsTotal prometheus.Counter
	WSAuthFailures           prometheus.Counter
	WSHandshakeDuration      prometheus.Histogram

	// Reconnect layer
	WSReconnectAttemptsTotal prometheus.Counter
	WSReconnectSuccessTotal  prometheus.Counter
	WSReconnectFailuresTotal prometheus.Counter
	WSResubscriptionsTotal   prometheus.Counter
	WSSequenceGaps           prometheus.Counter // sequence gaps detected in writePump

	// Build info
	BuildInfo *prometheus.GaugeVec // labels: version, revision

	// Transport layer
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec

	// Distributed routing
	DistRedisSubscriptions    prometheus.Gauge   // current Redis channel subscriptions
	DistRedisSubscribeOps     prometheus.Counter // total Redis SUBSCRIBE commands
	DistRedisUnsubscribeOps   prometheus.Counter // total Redis UNSUBSCRIBE commands
	DistEventsRouted          prometheus.Counter // events received from Redis and routed locally
	DistSymbolsLocalSubs      prometheus.Gauge   // current symbols with local subscribers
	DistSubscriptionEvents    *prometheus.CounterVec // labels: action (subscribe/unsubscribe)
}

// NewMetrics creates an isolated Prometheus registry and registers all
// application instruments on it. The returned Gatherer must be passed to
// promhttp.HandlerFor so the /metrics endpoint only exposes these metrics.
func NewMetrics(namespace string) (*Metrics, prometheus.Gatherer) {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)

	// Register Go runtime and process metrics so /metrics exposes them.
	_ = reg.Register(collectors.NewGoCollector())
	_ = reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return &Metrics{
		// ── Feed layer ─────────────────────────────────────────
		FeedMessagesReceived: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "messages_received_total",
			Help:      "Total number of raw messages received from upstream feeds.",
		}, []string{"provider"}), // cardinality fix: removed `symbol` label

		FeedErrors: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "errors_total",
			Help:      "Total number of errors encountered while reading upstream feeds.",
		}, []string{"provider", "kind"}),

		DataStaleness: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "data_staleness_seconds",
			Help:      "Current data staleness: wall clock minus most recent exchange timestamp.",
		}),

		// ── Distribution layer ──────────────────────────────────
		BroadcastsTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distribution",
			Name:      "broadcasts_total",
			Help:      "Total number of market-data events broadcast to subscribers.",
		}),

		EventsDroppedTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distribution",
			Name:      "events_dropped_total",
			Help:      "Total number of events dropped due to backpressure (buffer full).",
		}), // cardinality fix: removed per-topic/subscriber label

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

		// ── WebSocket layer ─────────────────────────────────────
		WSConnectionsActive: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "connections_active",
			Help:      "Current number of open WebSocket connections.",
		}),

		WSConnectionsOpened: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "connections_opened_total",
			Help:      "Total number of WebSocket connections opened.",
		}),

		WSConnectionsClosed: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "connections_closed_total",
			Help:      "Total number of WebSocket connections closed.",
		}),

		WSConnectionAttempts: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "connection_attempts_total",
			Help:      "Total WebSocket upgrade attempts (including rejected). Use with connections_opened_total to detect reconnection storms.",
		}),

		WSActiveSubscriptions: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "active_subscriptions",
			Help:      "Current number of active WebSocket subscriptions.",
		}),

		WSSlowConsumers: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "slow_consumers",
			Help:      "Current number of slow consumers (backpressure applied).",
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

		WSBytesSent: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "bytes_sent_total",
			Help:      "Total bytes written to WebSocket clients.",
		}),

		WSMessageSize: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "message_size_bytes",
			Help:      "Distribution of WebSocket message sizes in bytes.",
			Buckets:   []float64{64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536},
		}),

		WSDeliveryLatency: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "delivery_latency_seconds",
			Help:      "End-to-end latency from event creation to WebSocket delivery.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		}),

		WSPingLatency: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "ping_latency_seconds",
			Help:      "Round-trip time of WebSocket ping/pong frames.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		}),

		WSPingSentTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "ping_sent_total",
			Help:      "Total number of WebSocket ping frames sent to clients.",
		}),

		WSPongReceivedTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "pong_received_total",
			Help:      "Total number of WebSocket pong frames received from clients.",
		}),

		WSTimeoutsTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "heartbeat_timeouts_total",
			Help:      "Total number of clients disconnected due to heartbeat timeout.",
		}),

		WSHeartbeatCleanupsTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "heartbeat_cleanups_total",
			Help:      "Total number of dead connections cleaned up by the heartbeat system.",
		}),

		WSAuthFailures: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "auth_failures_total",
			Help:      "Total number of WebSocket authentication or handshake failures.",
		}),

		WSHandshakeDuration: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "handshake_duration_seconds",
			Help:      "Duration of the WebSocket HTTP upgrade handshake.",
			Buckets:   prometheus.DefBuckets,
		}),

		WSReconnectAttemptsTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "reconnect_attempts_total",
			Help:      "Total number of WebSocket reconnection attempts.",
		}),

		WSReconnectSuccessTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "reconnect_success_total",
			Help:      "Total number of successful WebSocket reconnections.",
		}),

		WSReconnectFailuresTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "reconnect_failures_total",
			Help:      "Total number of failed WebSocket reconnection attempts.",
		}),

		WSResubscriptionsTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "resubscriptions_total",
			Help:      "Total number of automatic resubscriptions after reconnection.",
		}),

		WSSequenceGaps: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "sequence_gaps_total",
			Help:      "Total number of sequence gaps detected in the writePump hot path.",
		}),

		// ── Build info ──────────────────────────────────────────
		BuildInfo: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "build_info",
			Help:      "Build metadata. Always 1; use labels for version/revision.",
		}, []string{"version", "revision"}),

		// ── Transport layer ─────────────────────────────────────
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

		// ── Distributed routing ───────────────────────────────────
		DistRedisSubscriptions: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "distributed",
			Name:      "redis_subscriptions_active",
			Help:      "Current number of active Redis channel subscriptions managed by the distributed router.",
		}),

		DistRedisSubscribeOps: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distributed",
			Name:      "redis_subscribe_total",
			Help:      "Total number of Redis SUBSCRIBE commands issued by the distributed router.",
		}),

		DistRedisUnsubscribeOps: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distributed",
			Name:      "redis_unsubscribe_total",
			Help:      "Total number of Redis UNSUBSCRIBE commands issued by the distributed router.",
		}),

		DistEventsRouted: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distributed",
			Name:      "events_routed_total",
			Help:      "Total number of events received from Redis and routed to local subscribers.",
		}),

		DistSymbolsLocalSubs: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "distributed",
			Name:      "symbols_with_local_subs",
			Help:      "Current number of symbols with at least one local subscriber.",
		}),

		DistSubscriptionEvents: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distributed",
			Name:      "subscription_events_total",
			Help:      "Total distributed subscription events (subscribe/unsubscribe to Redis channels).",
		}, []string{"action"}),
	}, reg
}
