package websocket

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
)

const (
	// DefaultPingInterval is how often the server sends pings.
	DefaultPingInterval = 30 * time.Second

	// DefaultMissedHeartbeats is the number of missed pongs before disconnect.
	DefaultMissedHeartbeats = 3

	// defaultCleanupInterval is how often the heartbeat manager scans for dead connections.
	defaultCleanupInterval = 10 * time.Second

	heartbeatShards = 32
)

func hashString(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32() % heartbeatShards
}

// heartbeatEntry tracks per-client heartbeat state.
type heartbeatEntry struct {
	lastPong   time.Time
	pingSentAt time.Time
	onTimeout  func() // called when heartbeat times out
	sendPing   func() // called to trigger a ping
}

type heartbeatShard struct {
	mu      sync.RWMutex
	clients map[string]*heartbeatEntry
}

// HeartbeatManager tracks heartbeat state for all connected clients.
// It provides centralized timeout detection and ping generation without per-connection timers.
type HeartbeatManager struct {
	shards [heartbeatShards]*heartbeatShard

	pingInterval     time.Duration
	missedHeartbeats int
	pongTimeout      time.Duration
	cleanupInterval  time.Duration
	log              *log.Logger
	metrics          *platform.Metrics

	stopCh chan struct{}
	done   chan struct{}
}

// NewHeartbeatManager creates a HeartbeatManager with the specified timing.
func NewHeartbeatManager(l *log.Logger, metrics *platform.Metrics, pingInterval time.Duration, missedHeartbeats int) *HeartbeatManager {
	if pingInterval <= 0 {
		pingInterval = DefaultPingInterval
	}
	if missedHeartbeats <= 0 {
		missedHeartbeats = DefaultMissedHeartbeats
	}
	pongTimeout := pingInterval * time.Duration(missedHeartbeats)

	hm := &HeartbeatManager{
		pingInterval:     pingInterval,
		missedHeartbeats: missedHeartbeats,
		pongTimeout:      pongTimeout,
		cleanupInterval:  defaultCleanupInterval,
		log:              l,
		metrics:          metrics,
		stopCh:           make(chan struct{}),
		done:             make(chan struct{}),
	}
	for i := 0; i < heartbeatShards; i++ {
		hm.shards[i] = &heartbeatShard{
			clients: make(map[string]*heartbeatEntry),
		}
	}
	return hm
}

// Run starts the heartbeat and cleanup loop.
func (hm *HeartbeatManager) Run() {
	defer close(hm.done)
	pingTicker := time.NewTicker(hm.pingInterval)
	cleanupTicker := time.NewTicker(hm.cleanupInterval)
	defer pingTicker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-pingTicker.C:
			hm.broadcastPings()
		case <-cleanupTicker.C:
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

// Register adds a client to heartbeat tracking.
func (hm *HeartbeatManager) Register(clientID string, onTimeout func(), sendPing func()) {
	shard := hm.shards[hashString(clientID)]
	shard.mu.Lock()
	shard.clients[clientID] = &heartbeatEntry{
		lastPong:  time.Now(),
		onTimeout: onTimeout,
		sendPing:  sendPing,
	}
	shard.mu.Unlock()
	hm.log.Underlying().Debug().Str("client_id", clientID).Str("event", "heartbeat_registered").Msg("heartbeat: client registered")
}

// Unregister removes a client from heartbeat tracking.
func (hm *HeartbeatManager) Unregister(clientID string) {
	shard := hm.shards[hashString(clientID)]
	shard.mu.Lock()
	delete(shard.clients, clientID)
	shard.mu.Unlock()
}

// broadcastPings triggers the sendPing callback for all registered clients.
func (hm *HeartbeatManager) broadcastPings() {
	for i := 0; i < heartbeatShards; i++ {
		shard := hm.shards[i]
		var pingFuncs []func()
		shard.mu.Lock()
		for _, entry := range shard.clients {
			if entry.sendPing != nil {
				pingFuncs = append(pingFuncs, entry.sendPing)
			}
			if entry.pingSentAt.IsZero() {
				entry.pingSentAt = time.Now()
			}
		}
		shard.mu.Unlock()

		for _, pf := range pingFuncs {
			pf()
		}
	}
}

// RecordPing records that a ping was sent to the given client.
func (hm *HeartbeatManager) RecordPing(clientID string) {
	shard := hm.shards[hashString(clientID)]
	shard.mu.Lock()
	if entry, ok := shard.clients[clientID]; ok {
		if entry.pingSentAt.IsZero() {
			entry.pingSentAt = time.Now()
		}
	}
	shard.mu.Unlock()
	hm.metrics.WSPingSentTotal.Inc()
}

// RecordPong records that a pong was received from the given client.
func (hm *HeartbeatManager) RecordPong(clientID string) {
	shard := hm.shards[hashString(clientID)]
	shard.mu.Lock()
	if entry, ok := shard.clients[clientID]; ok {
		entry.lastPong = time.Now()
		if !entry.pingSentAt.IsZero() {
			rtt := time.Since(entry.pingSentAt)
			hm.metrics.WSPingLatency.Observe(rtt.Seconds())
			entry.pingSentAt = time.Time{}
		}
	}
	shard.mu.Unlock()
	hm.metrics.WSPongReceivedTotal.Inc()
}

// checkTimeouts scans all clients and invokes onTimeout for dead clients.
func (hm *HeartbeatManager) checkTimeouts() {
	now := time.Now()
	for i := 0; i < heartbeatShards; i++ {
		shard := hm.shards[i]
		var timedOut []string

		shard.mu.RLock()
		for id, entry := range shard.clients {
			if entry.pingSentAt.IsZero() {
				continue
			}
			if now.Sub(entry.lastPong) > hm.pongTimeout {
				timedOut = append(timedOut, id)
			}
		}
		shard.mu.RUnlock()

		for _, id := range timedOut {
			shard.mu.Lock()
			entry, ok := shard.clients[id]
			if ok {
				hm.log.Underlying().Warn().Str("client_id", id).Dur("timeout", hm.pongTimeout).
					Str("event", "heartbeat_timeout").
					Msg("heartbeat: client timed out")
				hm.metrics.WSTimeoutsTotal.Inc()
				hm.metrics.WSHeartbeatCleanupsTotal.Inc()
				if entry.onTimeout != nil {
					cb := entry.onTimeout
					shard.mu.Unlock()
					cb()
					shard.mu.Lock()
				}
			}
			shard.mu.Unlock()
		}
	}
}

// ClientCount returns the number of tracked clients (for testing).
func (hm *HeartbeatManager) ClientCount() int {
	var count int
	for i := 0; i < heartbeatShards; i++ {
		hm.shards[i].mu.RLock()
		count += len(hm.shards[i].clients)
		hm.shards[i].mu.RUnlock()
	}
	return count
}
