// Package websocket implements the WebSocket Gateway — the external
// access layer that maintains persistent client connections and delivers
// real-time market data.
//
// Architecture:
//
//	Gorilla WebSocket ──► Connection Manager ──► per-client goroutines
//	                           ↓
//	                    topicmanager.Manager (routing)
//
// Design goals:
//   - 5,000+ concurrent connections
//   - Per-client isolation (own goroutines, own queue)
//   - Non-blocking publish (drop on full)
//   - Graceful shutdown with cleanup
package websocket

import (
	"context"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	wscontext "github.com/sumit/rtmds/internal/correlation/websocket"
	logpkg "github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/snapshot"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/tracing"
	"github.com/sumit/rtmds/pkg/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	// maxConnections is the hard limit on concurrent WebSocket connections.
	// Exceeding this returns HTTP 503 before the upgrade.
	// Increased from 5000 to 10000 to handle production load.
	maxConnections = 10000

	// CloseInternalServerErr is re-exported from gorilla/websocket for
	// use by callers that need to evict clients (e.g., Redis failure).
	CloseInternalServerErr = websocket.CloseInternalServerErr
)

var (
	// writeWait is the deadline for a write to the WebSocket.
	writeWait = 10 * time.Second

	// pongWait is the deadline for reading the next pong from the client.
	// Design spec: 90 seconds (allows missing up to 3 pings at 30s interval).
	pongWait = 90 * time.Second

	// pingPeriod is how often the server sends pings. Must be < pongWait.
	// Design spec: 30 seconds.
	pingPeriod = 30 * time.Second

	// maxMessageSize is the maximum message size from the client.
	maxMessageSize = int64(4096)
)

const (
	// clientShardCount is the number of shards for the client map.
	// Reduces lock contention during connection storms.
	clientShardCount = 32
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Rank 5: Pool write buffers across connections — saves ~4KB per conn.
	// At 5k connections: ~20 MB working set reduction.
	WriteBufferPool: &sync.Pool{},
	// Allow all origins in dev. Production should use a whitelist.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// clientShard protects a subset of clients for reduced lock contention.
type clientShard struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

// connRateLimiter implements a simple token-bucket rate limiter for
// new WebSocket connections. This prevents thundering herd effects
// when a gateway comes back online and thousands of clients reconnect
// simultaneously.
type connRateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	maxRate  float64 // tokens per second (max connections/sec)
	lastTime time.Time
}

func newConnRateLimiter(maxConnsPerSec float64) *connRateLimiter {
	return &connRateLimiter{
		tokens:   maxConnsPerSec, // start full
		maxRate:  maxConnsPerSec,
		lastTime: time.Now(),
	}
}

// Allow returns true if a connection is allowed under the rate limit.
func (r *connRateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastTime).Seconds()
	r.lastTime = now

	// Refill tokens based on elapsed time.
	r.tokens += elapsed * r.maxRate
	if r.tokens > r.maxRate {
		r.tokens = r.maxRate
	}

	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}

// Gateway manages all active WebSocket connections. It is the single
// entry point for HTTP upgrades and the owner of all client goroutines.
// Uses sharded client maps to reduce lock contention during connection storms.
type Gateway struct {
	tm         topicmanager.Manager
	snap       *snapshot.Service // optional; nil disables snapshot delivery
	replay     *ReplayHandler    // optional; nil disables replay
	wal        Replayer          // new: WAL replayer for reconnects
	log        *logpkg.Logger
	metrics    *platform.Metrics
	id         string               // unique gateway identifier for sticky session routing
	redirector Redirector           // optional; if set, enforces partition ownership
	negotiator *protocol.Negotiator // handles subprotocol negotiation
	registry   *protocol.Registry   // serializer registry

	shards    []clientShard
	shardMask uint32

	// activeCount is an atomic counter of active connections.
	// Replaces per-shard iteration in ClientCount() to eliminate
	// lock contention during connection storms (fixes 25-30 conn/s bottleneck).
	activeCount atomic.Int64

	// peak tracks the high-water mark for map compaction decisions.
	peak atomic.Int64

	// connLimiter throttles new connections to prevent thundering herd.
	connLimiter *connRateLimiter

	// heartbeat tracks per-client pong timestamps and detects dead connections.
	heartbeat *HeartbeatManager

	// lifecycle manages the gateway state machine (Starting, Healthy, Draining, Offline).
	lifecycle *Lifecycle
}

// NewGateway creates a ready-to-use Gateway.
// maxConnsPerSec limits new connections per second to prevent thundering herd.
// Pass 0 to disable rate limiting.
// gatewayID is a unique identifier for this gateway instance (used in sticky session routing).
// snap is an optional snapshot service; pass nil to disable snapshot delivery on subscribe.
func NewGateway(tm topicmanager.Manager, log *logpkg.Logger, metrics *platform.Metrics, maxConnsPerSec float64, gatewayID ...string) *Gateway {
	return NewGatewayWithSnapshot(tm, nil, log, metrics, maxConnsPerSec, gatewayID...)
}

// NewGatewayWithSnapshot creates a Gateway with snapshot service support.
// When snap is non-nil, newly subscribed clients receive current market
// snapshots before live streaming begins.
func NewGatewayWithSnapshot(tm topicmanager.Manager, snap *snapshot.Service, log *logpkg.Logger, metrics *platform.Metrics, maxConnsPerSec float64, gatewayID ...string) *Gateway {
	return NewGatewayWithReplay(tm, snap, nil, nil, log, metrics, maxConnsPerSec, gatewayID...)
}

// NewGatewayWithReplay creates a Gateway with snapshot and replay support.
// When replay is non-nil, clients can request historical replay via WebSocket.
func NewGatewayWithReplay(tm topicmanager.Manager, snap *snapshot.Service, replay *ReplayHandler, wal Replayer, log *logpkg.Logger, metrics *platform.Metrics, maxConnsPerSec float64, gatewayID ...string) *Gateway {
	n := uint32(clientShardCount)
	id := "default"
	if len(gatewayID) > 0 && gatewayID[0] != "" {
		id = gatewayID[0]
	}
	g := &Gateway{
		tm:         tm,
		snap:       snap,
		replay:     replay,
		wal:        wal,
		log:        log,
		metrics:    metrics,
		id:         id,
		negotiator: protocol.NewNegotiator(),
		registry:   protocol.NewRegistry(),
		shards:     make([]clientShard, n),
		shardMask:  n - 1,
		lifecycle:  NewLifecycle(),
	}
	for i := range g.shards {
		g.shards[i].clients = make(map[string]*Client)
	}
	if maxConnsPerSec > 0 {
		g.connLimiter = newConnRateLimiter(maxConnsPerSec)
	}

	// Register default serializers
	g.registry.Register(protocol.NewJSONSerializer())
	g.registry.Register(protocol.NewProtobufSerializer())
	g.registry.Register(protocol.NewFlatBuffersSerializer())

	// Start the heartbeat manager for dead connection detection.
	g.heartbeat = NewHeartbeatManager(log, metrics, DefaultPingInterval, DefaultMissedHeartbeats)
	go g.heartbeat.Run()

	// Register metrics observer for lifecycle transitions
	if metrics != nil {
		metrics.GatewayState.Set(0) // 0=Starting
		var lastStateTime time.Time
		g.lifecycle.Observe(func(from, to LifecycleState) {
			now := time.Now()
			var duration time.Duration
			if !lastStateTime.IsZero() {
				duration = now.Sub(lastStateTime)
			}
			lastStateTime = now

			var stateVal float64
			switch to {
			case LifecycleStarting:
				stateVal = 0
			case LifecycleHealthy:
				stateVal = 1
				// If transitioning to healthy from starting or draining, it's a recovery
				if duration > 0 && (from == LifecycleStarting || from == LifecycleDraining) {
					metrics.GatewayRecoveryTime.Observe(duration.Seconds())
				}
			case LifecycleDraining:
				stateVal = 2
			case LifecycleOffline:
				stateVal = 3
			}

			// If transitioning out of Offline
			if duration > 0 && from == LifecycleOffline {
				metrics.GatewayDowntime.Observe(duration.Seconds())
			}

			metrics.GatewayState.Set(stateVal)
			metrics.GatewayStateTransitionsTotal.WithLabelValues(string(from), string(to)).Inc()
		})
	}

	_ = g.lifecycle.Transition(LifecycleHealthy)

	return g
}

// State returns the current lifecycle state of the gateway.
func (g *Gateway) State() LifecycleState {
	return g.lifecycle.State()
}

// ObserveLifecycle registers a callback for gateway state transitions.
func (g *Gateway) ObserveLifecycle(observer LifecycleObserver) {
	g.lifecycle.Observe(observer)
}

// SetRedirector injects a topology redirector into the Gateway.
func (g *Gateway) SetRedirector(r Redirector) {
	g.redirector = r
}

// shard returns the client shard for the given ID.
func (g *Gateway) shard(id string) *clientShard {
	h := fnv1a32(id)
	return &g.shards[h&g.shardMask]
}

// fnv1a32 returns a zero-allocation FNV-1a hash for shard selection.
func fnv1a32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// Handler returns an http.HandlerFunc that upgrades connections and
// starts per-client goroutines. Sets the rtmds-gateway-id header
// for sticky session verification by clients and load balancers.
//
// Trace boundary: "websocket.connect" — covers HTTP upgrade through
// client registration. This is a root span when no traceparent header
// is present, or a child span when the client sends trace context.
func (g *Gateway) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		g.metrics.WSConnectionAttempts.Inc()

		// Start a span for the WebSocket connection lifecycle.
		// This span covers: upgrade → registration → goroutine startup.
		ctx, span := tracing.TracerForComponent("websocket").Start(r.Context(), "websocket.connect",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("gateway.id", g.id),
				attribute.String("network.peer.address", r.RemoteAddr),
			),
		)
		defer span.End()

		state := g.lifecycle.State()
		if state == LifecycleDraining || state == LifecycleOffline {
			span.SetAttributes(attribute.Bool("error", true))
			span.AddEvent("connection_rejected", trace.WithAttributes(
				attribute.String("reason", string(state)),
			))
			http.Error(w, "gateway is "+string(state), http.StatusServiceUnavailable)
			return
		}

		// RC2: Reject if at capacity.
		if g.ClientCount() >= maxConnections {
			span.SetAttributes(attribute.Bool("error", true))
			span.AddEvent("connection_rejected", trace.WithAttributes(
				attribute.String("reason", "capacity"),
			))
			http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
			return
		}

		// Thundering herd mitigation: reject connections over the rate limit.
		// This prevents thousands of reconnects from overwhelming the gateway
		// after a restart or network partition.
		if g.connLimiter != nil && !g.connLimiter.Allow() {
			g.metrics.WSAuthFailures.Inc()
			span.SetAttributes(attribute.Bool("error", true))
			span.AddEvent("connection_rejected", trace.WithAttributes(
				attribute.String("reason", "rate_limit"),
			))
			http.Error(w, "connection rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		// Sticky session: set gateway ID header so clients and load balancers
		// can verify which gateway they're connected to. This header is set
		// before the upgrade so it's available in the HTTP response.
		w.Header().Set("rtmds-gateway-id", g.id)

		// Protocol Negotiation
		clientProtocols := websocket.Subprotocols(r)
		format, matchedStr, err := g.negotiator.SelectProtocol(clientProtocols)
		if err != nil {
			g.metrics.WSAuthFailures.Inc()
			span.SetAttributes(attribute.Bool("error", true))
			span.AddEvent("connection_rejected", trace.WithAttributes(
				attribute.String("reason", "unsupported_protocol"),
			))
			http.Error(w, "unsupported subprotocol", http.StatusBadRequest)
			return
		}

		responseHeader := http.Header{}
		if matchedStr != "" {
			responseHeader.Set("Sec-WebSocket-Protocol", matchedStr)
		}

		serializer, err := g.registry.Lookup(format)
		if err != nil {
			// Fallback to JSON if something is misconfigured
			serializer, _ = g.registry.Lookup(protocol.FormatJSON)
			if serializer == nil {
				serializer = protocol.NewJSONSerializer()
			}
		}

		conn, err := upgrader.Upgrade(w, r, responseHeader)
		if err != nil {
			logpkg.Error(r.Context(), g.log).Err(err).Str("event", "websocket_upgrade_failed").Msg("websocket upgrade failed")
			g.metrics.WSAuthFailures.Inc()
			span.RecordError(err)
			return
		}

		// RC3: Clear HTTP write timeout — WebSocket connections live for hours.
		// Without this, the 10s HTTP WriteTimeout would kill every WS connection.
		rc := http.NewResponseController(w)
		_ = rc.SetWriteDeadline(time.Time{})

		id := uuid.NewString()
		span.SetAttributes(attribute.String("client.id", id))

		// Use the WebSocket context utilities to bridge the HTTP context into
		// a long-lived persistent connection context, explicitly severing the
		// cancellation tree while preserving W3C Baggage and Trace Context.
		ctx, cancel := wscontext.NewConnectionContext(r.Context())

		c := newClient(id, conn, g.tm, g.snap, g.log.WithField("client_id", id), cancel, g.metrics, g.heartbeat, g.replay, g.wal, g.redirector, format, serializer)

		g.register(c)
		logpkg.Info(ctx, c.log).Str("remote", r.RemoteAddr).Str("event", "client_connected").Msg("client connected")

		span.AddEvent("client_registered", trace.WithAttributes(
			attribute.String("client.id", id),
		))

		connectTime := time.Now()

		// Use defer for cleanup so it runs reliably, even if readPump panics.
		defer func() {
			g.unregister(id)
			g.metrics.ConnectionDurationSeconds.Observe(time.Since(connectTime).Seconds())
			logpkg.Info(ctx, c.log).Str("event", "client_disconnected").Msg("client disconnected")
		}()

		// Start goroutines. readPump blocks until the connection closes.
		go c.writePump(ctx)
		c.readPump(ctx) // blocks — cleanup happens after return
	}
}

// register adds a client to the connection manager.
func (g *Gateway) register(c *Client) {
	sh := g.shard(c.id)
	sh.mu.Lock()
	sh.clients[c.id] = c
	sh.mu.Unlock()

	// Atomic count — O(1) instead of iterating all shards.
	g.activeCount.Add(1)

	// Track connection lifecycle for Prometheus.
	g.metrics.WSConnectionsOpened.Inc()
	g.metrics.ActiveConnections.Inc()

	// Atomic peak tracking.
	for {
		old := g.peak.Load()
		total := g.activeCount.Load()
		if total <= old || g.peak.CompareAndSwap(old, total) {
			break
		}
	}
}

// unregister removes a client and cancels all its subscriptions.
func (g *Gateway) unregister(id string) {
	sh := g.shard(id)
	sh.mu.Lock()
	c, ok := sh.clients[id]
	if ok {
		delete(sh.clients, id)
	}
	sh.mu.Unlock()

	if ok {
		c.cancelAll()
		g.activeCount.Add(-1)
		g.metrics.WSConnectionsClosed.Inc()
		g.metrics.ActiveConnections.Dec()
	}
}

// ClientCount returns the number of active connections.
// Uses an atomic counter — O(1) with no lock contention.
func (g *Gateway) ClientCount() int {
	return int(g.activeCount.Load())
}

// MaxConnections returns the hard limit on concurrent WebSocket connections.
func MaxConnections() int {
	return maxConnections
}

// ID returns the gateway's unique identifier for sticky session routing.
func (g *Gateway) ID() string {
	return g.id
}

// Broadcast sends a ServerMessage to all connected clients. This is used
// for system-wide notifications such as stale data warnings during Redis
// outages. Messages are sent non-blocking — slow consumers receive the
// message if their buffer has space, otherwise it is dropped.
func (g *Gateway) Broadcast(msg ServerMessage) {
	for i := range g.shards {
		sh := &g.shards[i]
		sh.mu.RLock()
		for _, c := range sh.clients {
			c.sendControl(msg)
		}
		sh.mu.RUnlock()
	}
}

// EvictAll immediately disconnects all active WebSocket clients with the
// specified close code and reason. Unlike Shutdown, this does not wait for
// graceful drain — it's used when a critical dependency (Redis) fails and
// clients must reconnect to a healthy gateway immediately.
func (g *Gateway) EvictAll(code int, reason string) {
	// Collect all clients from all shards.
	var snapshot []*Client
	for i := range g.shards {
		sh := &g.shards[i]
		sh.mu.RLock()
		for _, c := range sh.clients {
			snapshot = append(snapshot, c)
		}
		sh.mu.RUnlock()
	}

	if len(snapshot) == 0 {
		return
	}

	g.log.Underlying().Info().
		Int("clients", len(snapshot)).
		Int("close_code", code).
		Str("reason", reason).
		Str("event", "gateway_evicting_clients").
		Msg("gateway: evicting all clients due to dependency failure")

	// Send close frame to each client concurrently to avoid sequential blocking.
	for _, c := range snapshot {
		go func(client *Client) {
			deadline := time.Now().Add(writeWait)
			// Send close frame with specific code, protected by writeMu to avoid
			// concurrent writes with writePump.
			client.writeMu.Lock()
			_ = client.conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(code, reason),
				deadline,
			)
			client.writeMu.Unlock()
			// Cancel context to force readPump/writePump to exit.
			client.cancelCtx()
			// Force ReadMessage to return immediately.
			_ = client.conn.SetReadDeadline(time.Now().Add(-time.Second))
		}(c)
	}
}

// Shutdown gracefully closes all active connections. It sends a close
// frame to each client and waits for the write pumps to exit.
// GL2: Uses a semaphore channel instead of WaitGroup to avoid goroutine
// leak when ctx expires.
// GL4: Adds randomized delay before draining to stagger shutdown across
// multiple gateway instances, preventing thundering herd on load balancers.
func (g *Gateway) Shutdown(ctx context.Context) {
	_ = g.lifecycle.Transition(LifecycleDraining)

	// GL4: Randomized delay (50-200ms) to stagger drain across gateways.
	// When multiple gateways restart simultaneously, this prevents all of
	// them from closing connections at the exact same moment, which would
	// cause a thundering herd of reconnections to surviving gateways.
	delay := time.Duration(50+rand.Intn(150)) * time.Millisecond
	logpkg.Info(ctx, g.log).Dur("delay", delay).Str("event", "gateway_draining").Msg("gateway: starting graceful drain")
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		_ = g.lifecycle.Transition(LifecycleOffline)
		return
	}

	// Collect all clients from all shards.
	var snapshot []*Client
	for i := range g.shards {
		sh := &g.shards[i]
		sh.mu.RLock()
		for _, c := range sh.clients {
			snapshot = append(snapshot, c)
		}
		sh.mu.RUnlock()
	}

	sem := make(chan struct{}, len(snapshot))
	for _, c := range snapshot {
		go func(c *Client) {
			c.shutdown(ctx)
			sem <- struct{}{}
		}(c)
	}

	for i := 0; i < len(snapshot); i++ {
		select {
		case <-sem:
		case <-ctx.Done():
			_ = g.lifecycle.Transition(LifecycleOffline)
			return
		}
	}

	// Stop the heartbeat manager after all clients are disconnected.
	g.heartbeat.Stop()
	_ = g.lifecycle.Transition(LifecycleOffline)
}
