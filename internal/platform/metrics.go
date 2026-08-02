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
	MessagesGeneratedTotal *prometheus.CounterVec // labels: provider
	PublishErrorsTotal     *prometheus.CounterVec // labels: provider, kind
	DataStaleness          prometheus.Gauge       // current_system_time - exchange_timestamp
	PublishDurationSeconds prometheus.Histogram   // time to write to pub/sub

	// Distribution layer
	MessagesPublishedTotal prometheus.Counter
	DroppedMessagesTotal   prometheus.Counter
	SubscribersActive      prometheus.Gauge
	SubscriptionEvents     *prometheus.CounterVec

	// WebSocket layer
	ActiveConnections         prometheus.Gauge
	WSConnectionsOpened       prometheus.Counter
	WSConnectionsClosed       prometheus.Counter
	WSConnectionAttempts      prometheus.Counter
	WSActiveSubscriptions     prometheus.Gauge
	WSSlowConsumers           prometheus.Gauge
	MessagesSentTotal         prometheus.Counter
	WebsocketWriteErrorsTotal prometheus.Counter
	WSBytesSent               prometheus.Counter
	WSMessageSize             prometheus.Histogram
	WSDeliveryLatency         prometheus.Histogram
	WSPingLatency             prometheus.Histogram
	WSPingSentTotal           prometheus.Counter
	WSPongReceivedTotal       prometheus.Counter
	WSTimeoutsTotal           prometheus.Counter
	WSHeartbeatCleanupsTotal  prometheus.Counter
	WSAuthFailures            prometheus.Counter
	WSHandshakeDuration       prometheus.Histogram
	ConnectionDurationSeconds prometheus.Histogram
	BroadcastDurationSeconds  prometheus.Histogram

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
	DistRedisSubscriptions     prometheus.Gauge       // current Redis channel subscriptions
	DistRedisSubscribeOps      prometheus.Counter     // total Redis SUBSCRIBE commands
	DistRedisUnsubscribeOps    prometheus.Counter     // total Redis UNSUBSCRIBE commands
	MessagesReceivedTotal      prometheus.Counter     // events received from Redis and routed locally
	DistSymbolsLocalSubs       prometheus.Gauge       // current symbols with local subscribers
	DistSubscriptionEvents     *prometheus.CounterVec // labels: action (subscribe/unsubscribe)
	RedisReceiveLatencySeconds prometheus.Histogram

	// Gateway Lifecycle
	GatewayState                 prometheus.Gauge
	GatewayStateTransitionsTotal *prometheus.CounterVec // labels: from_state, to_state
	GatewayRecoveryTime          prometheus.Histogram
	GatewayDowntime              prometheus.Histogram

	// Fault Tolerance
	WSReconnectLatency prometheus.Histogram
	WSReplayDuration   prometheus.Histogram
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
		MessagesGeneratedTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "messages_generated_total",
			Help:      "Total number of raw messages received from upstream feeds.",
		}, []string{"provider"}), // cardinality fix: removed `symbol` label

		PublishErrorsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "publish_errors_total",
			Help:      "Total number of errors encountered while reading upstream feeds or publishing.",
		}, []string{"provider", "kind"}),

		DataStaleness: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "data_staleness_seconds",
			Help:      "Current data staleness: wall clock minus most recent exchange timestamp.",
		}),

		PublishDurationSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "feed",
			Name:      "publish_duration_seconds",
			Help:      "Time taken to publish a message to the distribution layer.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		}),

		// ── Distribution layer ──────────────────────────────────
		MessagesPublishedTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distribution",
			Name:      "messages_published_total",
			Help:      "Total number of market-data events broadcast to subscribers.",
		}),

		DroppedMessagesTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distribution",
			Name:      "dropped_messages_total",
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
		ActiveConnections: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "active_connections",
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

		MessagesSentTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "messages_sent_total",
			Help:      "Total number of messages successfully written to WebSocket clients.",
		}),

		WebsocketWriteErrorsTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "websocket_write_errors_total",
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

		ConnectionDurationSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "connection_duration_seconds",
			Help:      "Duration a WebSocket client stayed connected before disconnecting.",
			Buckets:   []float64{1, 5, 10, 30, 60, 300, 900, 3600, 14400, 86400},
		}),

		BroadcastDurationSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "broadcast_duration_seconds",
			Help:      "Duration taken to fan-out an event to all local subscribers.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
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

		WSReconnectLatency: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "reconnect_latency_seconds",
			Help:      "Histogram of WebSocket reconnect latencies.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		}),

		WSReplayDuration: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "websocket",
			Name:      "replay_duration_seconds",
			Help:      "Histogram of WAL replay durations during reconnection.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
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

		MessagesReceivedTotal: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "distributed",
			Name:      "messages_received_total",
			Help:      "Total number of events received from Redis and routed to local subscribers.",
		}),

		RedisReceiveLatencySeconds: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "distributed",
			Name:      "redis_receive_latency_seconds",
			Help:      "Latency from Redis SUBSCRIBE to Gateway arrival.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
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

		// ── Gateway Lifecycle ──────────────────────────────────────
		GatewayState: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "gateway",
			Name:      "state",
			Help:      "Current lifecycle state of the gateway (0=Starting, 1=Healthy, 2=Draining, 3=Offline)",
		}),

		GatewayStateTransitionsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "gateway",
			Name:      "state_transitions_total",
			Help:      "Total number of state transitions.",
		}, []string{"from_state", "to_state"}),

		GatewayRecoveryTime: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "gateway",
			Name:      "recovery_time_seconds",
			Help:      "Histogram of gateway recovery times (Starting/Draining to Healthy).",
			Buckets:   []float64{0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0},
		}),

		GatewayDowntime: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "gateway",
			Name:      "downtime_seconds",
			Help:      "Histogram of gateway downtime (Offline state duration).",
			Buckets:   []float64{1.0, 5.0, 10.0, 30.0, 60.0, 300.0},
		}),
	}, reg
}
