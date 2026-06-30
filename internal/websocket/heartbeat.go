package websocket

import (
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
)

const (
	// DefaultPingInterval is how often the server sends pings (design spec: 30s).
	DefaultPingInterval = 30 * time.Second

	// DefaultPongTimeout is the deadline for receiving a pong after a ping (design spec: 90s).
	// Allows missing up to 3 pings before disconnection.
	DefaultPongTimeout = 90 * time.Second

	// defaultCleanupInterval is how often the heartbeat manager scans for dead connections.
	defaultCleanupInterval = 10 * time.Second
)

// heartbeatEntry tracks per-client heartbeat state.
type heartbeatEntry struct {
	lastPong    time.Time
	pingSentAt  time.Time
	onTimeout   func() // called when heartbeat times out
}

// HeartbeatManager tracks heartbeat state for all connected clients.
// It provides centralized timeout detection without per-connection timers.
//
// The writePump sends pings and notifies the manager via PongReceived.
// The manager's Run loop periodically checks for stale clients and
// invokes their timeout callbacks for cleanup.
type HeartbeatManager struct {
	mu      sync.RWMutex
	clients map[string]*heartbeatEntry

	pingInterval time.Duration
	pongTimeout  time.Duration
	log          *log.Logger
	metrics      *platform.Metrics

	stopCh chan struct{}
	done   chan struct{}
}

// NewHeartbeatManager creates a HeartbeatManager with the specified timing.
// Pass 0 for pingInterval/pongTimeout to use defaults.
func NewHeartbeatManager(l *log.Logger, metrics *platform.Metrics, pingInterval, pongTimeout time.Duration) *HeartbeatManager {
	if pingInterval <= 0 {
		pingInterval = DefaultPingInterval
	}
	if pongTimeout <= 0 {
		pongTimeout = DefaultPongTimeout
	}
	return &HeartbeatManager{
		clients:      make(map[string]*heartbeatEntry),
		pingInterval: pingInterval,
		pongTimeout:  pongTimeout,
		log:          l,
		metrics:      metrics,
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Run starts the heartbeat cleanup loop. It scans for dead connections
// every cleanupInterval and invokes timeout callbacks. Blocks until Stop.
func (hm *HeartbeatManager) Run() {
	defer close(hm.done)
	ticker := time.NewTicker(defaultCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.checkTimeouts()
		case <-hm.stopCh:
			return
		}
	}
}

// Stop signals the heartbeat loop to exit and waits for it to finish.
func (hm *HeartbeatManager) Stop() {
	close(hm.stopCh)
	<-hm.done
}

// Register adds a client to heartbeat tracking. onTimeout is called
// when the client fails to respond within pongTimeout after a ping.
func (hm *HeartbeatManager) Register(clientID string, onTimeout func()) {
	hm.mu.Lock()
	hm.clients[clientID] = &heartbeatEntry{
		lastPong:  time.Now(), // assume healthy at registration
		onTimeout: onTimeout,
	}
	hm.mu.Unlock()
	hm.log.Underlying().Debug().Str("client_id", clientID).Str("event", "heartbeat_registered").Msg("heartbeat: client registered")
}

// Unregister removes a client from heartbeat tracking.
func (hm *HeartbeatManager) Unregister(clientID string) {
	hm.mu.Lock()
	delete(hm.clients, clientID)
	hm.mu.Unlock()
}

// RecordPing records that a ping was sent to the given client.
// Called by the writePump when it sends a ping frame.
func (hm *HeartbeatManager) RecordPing(clientID string) {
	hm.mu.Lock()
	if entry, ok := hm.clients[clientID]; ok {
		// Only set pingSentAt if it's zero to track the oldest unacknowledged ping.
		// Otherwise, subsequent pings overwrite it, breaking RTT metrics and timeouts.
		if entry.pingSentAt.IsZero() {
			entry.pingSentAt = time.Now()
		}
	}
	hm.mu.Unlock()
	hm.metrics.WSPingSentTotal.Inc()
}

// RecordPong records that a pong was received from the given client.
// Called by the readPump when a pong frame arrives.
func (hm *HeartbeatManager) RecordPong(clientID string) {
	hm.mu.Lock()
	if entry, ok := hm.clients[clientID]; ok {
		entry.lastPong = time.Now()
		// Record RTT if we know when the ping was sent.
		if !entry.pingSentAt.IsZero() {
			rtt := time.Since(entry.pingSentAt)
			hm.metrics.WSPingLatency.Observe(rtt.Seconds())
			entry.pingSentAt = time.Time{}
		}
	}
	hm.mu.Unlock()
	hm.metrics.WSPongReceivedTotal.Inc()
}

// checkTimeouts scans all clients and invokes onTimeout for any that
// have not responded within pongTimeout.
func (hm *HeartbeatManager) checkTimeouts() {
	now := time.Now()
	hm.mu.RLock()
	var timedOut []string
	for id, entry := range hm.clients {
		// Do not timeout if we haven't sent any pings that are awaiting a response.
		if entry.pingSentAt.IsZero() {
			continue // no ping outstanding
		}
		// Use lastPong to accurately determine how long it's been since the client responded.
		// If 90s have passed since the last pong (or registration), the client is dead.
		if now.Sub(entry.lastPong) > hm.pongTimeout {
			timedOut = append(timedOut, id)
		}
	}
	hm.mu.RUnlock()

	for _, id := range timedOut {
		hm.mu.Lock()
		entry, ok := hm.clients[id]
		if ok {
			hm.log.Underlying().Warn().Str("client_id", id).Dur("timeout", hm.pongTimeout).
				Str("event", "heartbeat_timeout").
				Msg("heartbeat: client timed out")
			hm.metrics.WSTimeoutsTotal.Inc()
			hm.metrics.WSHeartbeatCleanupsTotal.Inc()
			if entry.onTimeout != nil {
				// Call outside lock to avoid deadlock.
				cb := entry.onTimeout
				hm.mu.Unlock()
				cb()
				hm.mu.Lock()
			}
		}
		hm.mu.Unlock()
	}
}

// ClientCount returns the number of tracked clients (for testing).
func (hm *HeartbeatManager) ClientCount() int {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return len(hm.clients)
}
