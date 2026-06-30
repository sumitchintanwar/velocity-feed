package log

// Component-specific logger constructors.
//
// Each function returns a *Logger pre-configured with the component name
// and any static context fields. Dynamic fields (client_id, symbol, etc.)
// are added per-log via the zerolog API.
//
// Event naming convention: noun_action (e.g., "gateway_started",
// "client_connected", "replay_completed").

// FeedGenerator returns a logger for the feed generator component.
// Events: generator_started, generator_stopped, symbol_config_loaded, rate_config_loaded.
func FeedGenerator(l *Logger, feedName string) *Logger {
	return l.Component("feed_generator").WithField("feed", feedName)
}

// Publisher returns a logger for the publisher component.
// Events: publish_failed, queue_backpressure, recovery_event.
func Publisher(l *Logger) *Logger {
	return l.Component("publisher")
}

// RedisLayer returns a logger for the Redis integration layer.
// Events: redis_connected, redis_disconnected, redis_reconnect_attempt, redis_reconnect_success.
func RedisLayer(l *Logger, addr string) *Logger {
	return l.Component("redis").WithField("addr", addr)
}

// TopicManager returns a logger for the topic manager component.
// Events: topic_created, topic_purged, subscription_added, subscription_removed.
func TopicManager(l *Logger) *Logger {
	return l.Component("topic_manager")
}

// WebSocketGateway returns a logger for the WebSocket gateway component.
// Events: client_connected, client_disconnected, subscription_added,
// subscription_removed, heartbeat_timeout.
func WebSocketGateway(l *Logger, gatewayID string) *Logger {
	return l.Component("gateway").WithField("gateway_id", gatewayID)
}

// ReplayAPI returns a logger for the replay API component.
// Events: replay_requested, replay_completed, replay_failed, replay_large_request.
func ReplayAPI(l *Logger) *Logger {
	return l.Component("replay_api")
}

// SnapshotService returns a logger for the snapshot service component.
// Events: snapshot_loaded, checkpoint_saved, recovery_started, recovery_completed.
func SnapshotService(l *Logger) *Logger {
	return l.Component("snapshot")
}

// NewComponentLogger creates a Logger from a Config with a component name.
// This is a convenience constructor for components that need the full
// deployment context (environment, version, region, etc.) plus a component field.
func NewComponentLogger(cfg Config, component string) *Logger {
	l := NewFromConfig(cfg)
	return l.Component(component)
}
