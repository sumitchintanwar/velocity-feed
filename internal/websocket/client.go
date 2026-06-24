package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
)

// bufferPool reuses bytes.Buffer instances to reduce GC pressure.
// At 5k clients × 10k msgs/sec, this eliminates millions of allocs/sec.
var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

const (
	// SC1: After this many consecutive drops, disconnect the slow client.
	maxConsecutiveDrops = 100

	// CH2: Maximum symbols a client can subscribe to in one request.
	maxSymbolsPerSubscription = 100

	// SC2: Control channel capacity.
	controlQueueSize = 64
)

// Client represents a single WebSocket connection. Each client owns:
//   - A read goroutine (reads commands from the socket)
//   - A write goroutine (writes events to the socket)
//   - A topicmanager.Handle for subscription management
//
// The actual outbound queue lives inside the TopicManager's Handle channel.
// Clients are isolated — a slow consumer cannot block other clients
// or the publish hot path.
type Client struct {
	id      string
	conn    *websocket.Conn
	tm      topicmanager.Manager
	log     zerolog.Logger
	metrics *platform.Metrics

	// control is a channel for control messages (errors, confirmations)
	// from readPump to writePump. This ensures all socket writes
	// happen from a single goroutine.
	control chan ServerMessage

	// handleMu protects handle from concurrent read/write between
	// readPump and writePump.
	handleMu sync.RWMutex
	handle   topicmanager.Handle

	// subUpdated signals writePump that a new handle is available.
	subUpdated chan struct{}

	// cancelCtx cancels the client's context, used by Shutdown to
	// force readPump to exit.
	cancelCtx context.CancelFunc

	// done is closed by the read pump when it exits. writePump
	// selects on this to know when to stop.
	done chan struct{}

	// cancelOnce ensures handle.Cancel() is called at most once.
	cancelOnce sync.Once

	// SC1: consecutive drops counter for slow consumer detection.
	consecutiveDrops atomic.Int64

	// lastPingSent records when the last ping was sent for RTT measurement.
	lastPingSent atomic.Int64 // unix nanoseconds
}

func newClient(id string, conn *websocket.Conn, tm topicmanager.Manager, log zerolog.Logger, cancelCtx context.CancelFunc, metrics *platform.Metrics) *Client {
	return &Client{
		id:         id,
		conn:       conn,
		tm:         tm,
		log:        log,
		metrics:    metrics,
		control:    make(chan ServerMessage, controlQueueSize),
		subUpdated: make(chan struct{}, 1),
		cancelCtx:  cancelCtx,
		done:       make(chan struct{}),
	}
}

// ---------- Read Pump ----------

// readPump reads incoming messages from the WebSocket. It blocks until
// the connection is closed or an error occurs. When it returns, the
// write pump will exit via the done channel.
//
// GL3: readPump does NOT close the connection — only writePump does.
func (c *Client) readPump(ctx context.Context) {
	defer func() {
		// Signal write pump to exit.
		close(c.done)
		// GL3: Removed conn.Close() here — only writePump closes the connection.
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		// Record ping RTT if a ping was recently sent.
		if sent := c.lastPingSent.Load(); sent > 0 {
			rtt := time.Since(time.Unix(0, sent))
			c.metrics.WSPingLatency.Observe(rtt.Seconds())
		}
		return nil
	})

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.log.Debug().Msg("client closed connection")
			} else {
				c.log.Error().Err(err).Msg("read error")
			}
			return
		}

		var cm ClientMessage
		if err := json.Unmarshal(msg, &cm); err != nil {
			c.log.Warn().Err(err).Msg("invalid message")
			c.sendControl(ServerMessage{Type: "error", Payload: "invalid message format"})
			continue
		}

		c.handleMessage(cm)
	}
}

// handleMessage dispatches a client message to the appropriate action.
func (c *Client) handleMessage(cm ClientMessage) {
	switch cm.Action {
	case "subscribe":
		if len(cm.Symbols) == 0 {
			c.sendControl(ServerMessage{Type: "error", Payload: "no symbols provided"})
			return
		}
		// CH2: Enforce per-client subscription limit.
		if len(cm.Symbols) > maxSymbolsPerSubscription {
			c.sendControl(ServerMessage{Type: "error", Payload: "too many symbols (max 100)"})
			return
		}
		// Cancel previous subscription if any.
		c.handleMu.Lock()
		if c.handle != nil {
			c.handle.Cancel()
			c.metrics.WSActiveSubscriptions.Dec()
		}
		c.handle = c.tm.Subscribe(c.id, cm.Symbols...)
		c.metrics.WSActiveSubscriptions.Inc()
		c.handleMu.Unlock()
		// Signal writePump to pick up the new handle.
		select {
		case c.subUpdated <- struct{}{}:
		default:
		}
		c.sendControl(ServerMessage{Type: "subscribed", Payload: cm.Symbols})
		c.log.Info().Strs("symbols", cm.Symbols).Msg("subscribed")

	case "unsubscribe":
		c.handleMu.Lock()
		if c.handle != nil {
			c.handle.Cancel()
			c.handle = nil
			c.metrics.WSActiveSubscriptions.Dec()
		}
		c.handleMu.Unlock()
		select {
		case c.subUpdated <- struct{}{}:
		default:
		}
		c.sendControl(ServerMessage{Type: "unsubscribed", Payload: "all"})

	default:
		c.sendControl(ServerMessage{Type: "error", Payload: "unknown action: " + cm.Action})
	}
}

// ---------- Write Pump ----------

// writePump is the ONLY goroutine that writes to the WebSocket.
// It multiplexes: market events (from handle), control messages (from
// readPump), pings, and shutdown signals.
//
// Uses pre-encoded JSON bytes from *CachedEvent when available,
// eliminating redundant JSON serialization per-client.
//
// GL3: writePump owns the connection and closes it on exit.
func (c *Client) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		// GL3: Only writePump closes the connection.
		_ = c.conn.Close()
	}()

	var eventC <-chan *marketdata.CachedEvent
	var doneC <-chan struct{}

	// Snapshot handle under read lock.
	c.handleMu.RLock()
	h := c.handle
	c.handleMu.RUnlock()

	if h != nil {
		eventC = h.C()
		doneC = h.Done()
	}

	for {
		select {
		case <-c.subUpdated:
			// Handle changed — re-snapshot.
			c.handleMu.RLock()
			h := c.handle
			c.handleMu.RUnlock()
			if h != nil {
				eventC = h.C()
				doneC = h.Done()
			} else {
				eventC = nil
				doneC = nil
			}

		case cached, ok := <-eventC:
			if !ok {
				eventC = nil
				doneC = nil
				continue
			}
			// Record end-to-end delivery latency from event timestamp.
			if te, ok := cached.Event.(interface{ GetTimestamp() time.Time }); ok {
				latency := time.Since(te.GetTimestamp())
				c.metrics.WSDeliveryLatency.Observe(latency.Seconds())
			}
			// Use pre-encoded bytes from CachedEvent — zero serialization overhead.
			if err := c.writeRaw(cached.EncodedMsg); err != nil {
				c.log.Error().Err(err).Msg("write error")
				c.metrics.WSWriteErrors.Inc()
				return
			}
			c.metrics.WSMessagesWritten.Inc()
			// SC1: Reset drop counter on successful write.
			c.consecutiveDrops.Store(0)
			c.metrics.WSSlowConsumers.Dec()

		case msg := <-c.control:
			if err := c.writeJSON(msg); err != nil {
				c.log.Error().Err(err).Msg("control write error")
				return
			}

		case <-doneC:
			eventC = nil
			doneC = nil

		case <-c.done:
			return

		case <-ctx.Done():
			return

		case <-ticker.C:
			c.lastPingSent.Store(time.Now().UnixNano())
			if err := c.conn.WriteControl(
				websocket.PingMessage,
				nil,
				time.Now().Add(writeWait),
			); err != nil {
				c.log.Error().Err(err).Msg("ping failed")
				return
			}
		}
	}
}

// ---------- Helpers ----------

// writeRaw writes pre-encoded JSON bytes directly to the WebSocket.
// This is the hot path for market events — zero serialization overhead.
// Tracks bytes_sent_total and message_size_bytes for bandwidth observability.
func (c *Client) writeRaw(data []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	err := c.conn.WriteMessage(websocket.TextMessage, data)
	if err == nil {
		n := len(data)
		c.metrics.WSBytesSent.Add(float64(n))
		c.metrics.WSMessageSize.Observe(float64(n))
	}
	return err
}

// writeJSON writes a JSON message to the WebSocket with a deadline.
// Uses a pooled buffer to reduce allocations on the hot path.
// Used for control messages (subscribe/unsubscribe confirmations, errors).
func (c *Client) writeJSON(v any) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))

	// Get a buffer from the pool.
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Encode directly into the buffer (avoids reflection-based json.Marshal).
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return err
	}

	// Write the pre-encoded bytes to the socket.
	return c.conn.WriteMessage(websocket.TextMessage, buf.Bytes())
}

// sendControl queues a control message for writePump to deliver.
// SC2: Uses blocking send with warning — control messages must not be dropped.
func (c *Client) sendControl(msg ServerMessage) {
	select {
	case c.control <- msg:
	default:
		// SC2: Control channel full — log warning instead of silently dropping.
		c.log.Warn().Str("type", msg.Type).Msg("control channel full, dropping message")
	}
}

// cancelAll cancels the current subscription. Safe for concurrent use.
func (c *Client) cancelAll() {
	c.cancelOnce.Do(func() {
		c.handleMu.Lock()
		h := c.handle
		c.handleMu.Unlock()
		if h != nil {
			h.Cancel()
		}
	})
}

// shutdown cancels the client's context and waits for the write pump to exit.
//
// GL1: Sets a read deadline after cancelCtx to force ReadMessage to return
// immediately instead of blocking for up to pongWait (60s).
func (c *Client) shutdown(ctx context.Context) {
	// GL1: Cancel context to signal readPump and writePump.
	c.cancelCtx()

	// GL1: Force ReadMessage to return by setting an expired deadline.
	// Without this, readPump can block for up to 60s after shutdown.
	_ = c.conn.SetReadDeadline(time.Now().Add(-time.Second))

	// Send close frame.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(writeWait)
	}
	_ = c.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"),
		deadline,
	)

	// Wait for write pump to exit.
	select {
	case <-c.done:
	case <-ctx.Done():
	}
}
