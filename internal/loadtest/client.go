package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumit/rtmds/internal/marketdata"
)

// client represents a single WebSocket load test client.
type client struct {
	id       int
	conn     *websocket.Conn
	cfg      Config
	latency  *LatencyCollector
	received *ThroughputCounter
	errors   *atomic.Int64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newClient creates a load test client.
func newClient(id int, cfg Config, latency *LatencyCollector, received *ThroughputCounter, errors *atomic.Int64) *client {
	return &client{
		id:       id,
		cfg:      cfg,
		latency:  latency,
		received: received,
		errors:   errors,
	}
}

// connect establishes the WebSocket connection and subscribes to symbols.
func (c *client) connect(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, _, err := dialer.DialContext(dialCtx, c.cfg.ServerURL, nil)
	if err != nil {
		return fmt.Errorf("client %d: dial: %w", c.id, err)
	}
	c.conn = conn

	// Subscribe to symbols.
	sub := marketdata.SubscribeRequest{
		Action:  "subscribe",
		Symbols: c.cfg.Symbols,
	}
	if err := conn.WriteJSON(sub); err != nil {
		conn.Close()
		return fmt.Errorf("client %d: subscribe: %w", c.id, err)
	}

	return nil
}

// run starts the read loop. Blocks until ctx is cancelled.
func (c *client) run(ctx context.Context) {
	c.wg.Add(1)
	defer c.wg.Done()
	defer c.conn.Close()

	// Set read deadline to detect stale connections.
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			c.errors.Add(1)
			return
		}

		// Parse the server message to extract the timestamp.
		var serverMsg marketdata.ServerMessage
		if err := json.Unmarshal(msg, &serverMsg); err != nil {
			c.errors.Add(1)
			continue
		}

		// Extract timestamp from payload.
		var ts time.Time
		switch payload := serverMsg.Payload.(type) {
		case map[string]interface{}:
			if t, ok := payload["timestamp"].(string); ok {
				ts, _ = time.Parse(time.RFC3339Nano, t)
			}
		}

		if !ts.IsZero() {
			latency := time.Since(ts)
			c.latency.Record(latency)
		}

		c.received.Inc()

		// Apply read delay for slow consumer testing.
		if c.cfg.ReadDelay > 0 {
			time.Sleep(c.cfg.ReadDelay)
		}
	}
}

// close shuts down the client gracefully.
func (c *client) close() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		c.conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		// Send close message.
		c.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		c.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	}
	c.wg.Wait()
}
