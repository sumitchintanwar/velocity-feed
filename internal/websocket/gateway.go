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
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
)

const (
	// maxConnections is the hard limit on concurrent WebSocket connections.
	// Exceeding this returns HTTP 503 before the upgrade.
	maxConnections = 5000
)

const (
	// writeWait is the deadline for a write to the WebSocket.
	writeWait = 10 * time.Second

	// pongWait is the deadline for reading the next pong from the client.
	pongWait = 60 * time.Second

	// pingPeriod is how often the server sends pings. Must be < pongWait.
	pingPeriod = (pongWait * 9) / 10

	// maxMessageSize is the maximum message size from the client.
	maxMessageSize = 4096
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

// Gateway manages all active WebSocket connections. It is the single
// entry point for HTTP upgrades and the owner of all client goroutines.
type Gateway struct {
	tm      topicmanager.Manager
	log     zerolog.Logger
	metrics *platform.Metrics

	mu      sync.RWMutex
	clients map[string]*Client // id → client
	// peak tracks the high-water mark for map compaction decisions.
	peak int
}

// NewGateway creates a ready-to-use Gateway.
func NewGateway(tm topicmanager.Manager, log zerolog.Logger, metrics *platform.Metrics) *Gateway {
	return &Gateway{
		tm:      tm,
		log:     log,
		metrics: metrics,
		clients: make(map[string]*Client),
	}
}

// Handler returns an http.HandlerFunc that upgrades connections and
// starts per-client goroutines.
func (g *Gateway) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// RC2: Reject if at capacity.
		if g.ClientCount() >= maxConnections {
			http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			g.log.Error().Err(err).Msg("websocket upgrade failed")
			return
		}

		// RC3: Clear HTTP write timeout — WebSocket connections live for hours.
		// Without this, the 10s HTTP WriteTimeout would kill every WS connection.
		rc := http.NewResponseController(w)
		_ = rc.SetWriteDeadline(time.Time{})

		id := uuid.NewString()
		// RC3: Use background context, not r.Context(), to decouple from HTTP lifecycle.
		ctx, cancel := context.WithCancel(context.Background())
		c := newClient(id, conn, g.tm, g.log.With().Str("client_id", id).Logger(), cancel)

		g.register(c)
		g.metrics.WSConnectionsActive.Inc()
		c.log.Info().Str("remote", r.RemoteAddr).Msg("client connected")

		// Start goroutines. readPump blocks until the connection closes.
		go c.writePump(ctx)
		c.readPump(ctx) // blocks — cleanup happens after return

		g.unregister(id)
		g.metrics.WSConnectionsActive.Dec()
		c.log.Info().Msg("client disconnected")
	}
}

// register adds a client to the connection manager.
func (g *Gateway) register(c *Client) {
	g.mu.Lock()
	g.clients[c.id] = c
	// Track peak for compaction decisions.
	if len(g.clients) > g.peak {
		g.peak = len(g.clients)
	}
	g.mu.Unlock()
}

// unregister removes a client and cancels all its subscriptions.
// It also compacts the map if utilization drops below 25% to reclaim memory.
func (g *Gateway) unregister(id string) {
	g.mu.Lock()
	c, ok := g.clients[id]
	if ok {
		delete(g.clients, id)
	}
	// Compact map if utilization is low (reclaims memory from burst connections).
	// Go maps never shrink, so we must replace with a fresh copy.
	if g.peak > 100 && len(g.clients) < g.peak/4 {
		compacted := make(map[string]*Client, len(g.clients))
		for k, v := range g.clients {
			compacted[k] = v
		}
		g.clients = compacted
		g.peak = len(g.clients)
	}
	g.mu.Unlock()

	if ok {
		c.cancelAll()
	}
}

// ClientCount returns the number of active connections.
func (g *Gateway) ClientCount() int {
	g.mu.RLock()
	n := len(g.clients)
	g.mu.RUnlock()
	return n
}

// MaxConnections returns the hard limit on concurrent WebSocket connections.
func MaxConnections() int {
	return maxConnections
}

// Shutdown gracefully closes all active connections. It sends a close
// frame to each client and waits for the write pumps to exit.
// GL2: Uses a semaphore channel instead of WaitGroup to avoid goroutine
// leak when ctx expires.
func (g *Gateway) Shutdown(ctx context.Context) {
	g.mu.RLock()
	snapshot := make([]*Client, 0, len(g.clients))
	for _, c := range g.clients {
		snapshot = append(snapshot, c)
	}
	g.mu.RUnlock()

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
			return
		}
	}
}
