// Package client provides a Go client SDK for consuming the Real-Time Market
// Data System WebSocket API. It implements the "fat client" pattern: the client
// owns subscription state and automatically re-sends subscriptions on reconnect,
// enabling truly stateless gateways.
//
// Usage:
//
//	c, err := client.New("ws://localhost:8080/ws")
//	if err != nil { log.Fatal(err) }
//	defer c.Close()
//
//	c.Subscribe("AAPL", "TSLA")
//	for msg := range c.Messages() {
//	    fmt.Println(msg)
//	}
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// Message is the envelope received from the server.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Options configures client behavior.
type Options struct {
	// Reconnect enables automatic reconnection on disconnect.
	Reconnect bool
	// MaxReconnectAttempts limits reconnect tries (0 = unlimited).
	MaxReconnectAttempts int
	// InitialBackoff is the starting delay between reconnect attempts.
	InitialBackoff time.Duration
	// MaxBackoff is the maximum delay between reconnect attempts.
	MaxBackoff time.Duration
}

// DefaultOptions returns sensible defaults for the client.
func DefaultOptions() Options {
	return Options{
		Reconnect:            true,
		MaxReconnectAttempts: 0,
		InitialBackoff:       500 * time.Millisecond,
		MaxBackoff:           30 * time.Second,
	}
}

// Client is a WebSocket client for the RTMDS server. It implements the
// fat client pattern: subscriptions are tracked client-side and automatically
// re-sent after a reconnect, so any gateway can serve any client.
type Client struct {
	url    string
	opts   Options
	conn   *websocket.Conn
	msgCh  chan Message
	doneCh chan struct{}

	// mu protects conn and subscriptions during reconnect.
	mu            sync.Mutex
	subscriptions []string // client-side subscription state (source of truth)
	reconnecting  bool
	cancelReconnect context.CancelFunc
}

// New dials the RTMDS WebSocket endpoint and returns a ready Client.
func New(url string, opts ...Options) (*Client, error) {
	o := DefaultOptions()
	if len(opts) > 0 {
		o = opts[0]
	}

	conn, _, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		return nil, fmt.Errorf("client: dial %q: %w", url, err)
	}

	c := &Client{
		url:  url,
		opts: o,
		conn: conn,
		msgCh:  make(chan Message, 256),
		doneCh: make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// Subscribe sends a subscribe command to the server and records the
// subscription client-side. On reconnect, all recorded subscriptions
// are automatically re-sent to the new gateway.
func (c *Client) Subscribe(symbols ...string) error {
	c.mu.Lock()
	// Merge new symbols into tracked subscriptions (deduplicate).
	seen := make(map[string]struct{}, len(c.subscriptions))
	for _, s := range c.subscriptions {
		seen[s] = struct{}{}
	}
	for _, s := range symbols {
		if _, ok := seen[s]; !ok {
			c.subscriptions = append(c.subscriptions, s)
		}
	}
	c.mu.Unlock()

	return c.writeSubscribe(symbols)
}

// Unsubscribe sends an unsubscribe command and removes symbols from
// the tracked subscription list.
func (c *Client) Unsubscribe(symbols ...string) error {
	c.mu.Lock()
	remove := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		remove[s] = struct{}{}
	}
	filtered := c.subscriptions[:0]
	for _, s := range c.subscriptions {
		if _, ok := remove[s]; !ok {
			filtered = append(filtered, s)
		}
	}
	c.subscriptions = filtered
	c.mu.Unlock()

	return c.writeUnsubscribe(symbols)
}

// Subscriptions returns a snapshot of the currently tracked subscriptions.
func (c *Client) Subscriptions() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.subscriptions))
	copy(out, c.subscriptions)
	return out
}

// Messages returns the channel on which server messages are delivered.
// The channel is closed when the connection is closed (and reconnect is disabled).
func (c *Client) Messages() <-chan Message {
	return c.msgCh
}

// Done returns a channel that is closed when the client permanently shuts down
// (either Close is called or reconnect is exhausted).
func (c *Client) Done() <-chan struct{} {
	return c.doneCh
}

// Close gracefully closes the WebSocket connection and disables reconnect.
func (c *Client) Close() error {
	// Disable reconnect before closing.
	c.mu.Lock()
	c.reconnecting = false
	if c.cancelReconnect != nil {
		c.cancelReconnect()
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.conn.Close(websocket.StatusNormalClosure, "")
	<-c.doneCh
	_ = ctx
	return nil
}

// RunUntil blocks until ctx is cancelled, then closes the connection.
func (c *Client) RunUntil(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return c.Close()
	case <-c.doneCh:
		return nil
	}
}

// writeSubscribe sends a subscribe message to the server.
func (c *Client) writeSubscribe(symbols []string) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("client: not connected")
	}
	return conn.Write(context.Background(), websocket.MessageText, mustMarshal(map[string]any{
		"action":  "subscribe",
		"symbols": symbols,
	}))
}

// writeUnsubscribe sends an unsubscribe message to the server.
func (c *Client) writeUnsubscribe(symbols []string) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("client: not connected")
	}
	return conn.Write(context.Background(), websocket.MessageText, mustMarshal(map[string]any{
		"action":  "unsubscribe",
		"symbols": symbols,
	}))
}

// readLoop reads messages from the WebSocket and dispatches them.
// On connection loss, it attempts to reconnect if enabled.
func (c *Client) readLoop() {
	defer func() {
		close(c.msgCh)
		close(c.doneCh)
	}()

	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			if c.opts.Reconnect {
				c.reconnect()
				return // readLoop exits; reconnectLoop starts a new one
			}
			return
		}
		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		c.msgCh <- msg
	}
}

// reconnect attempts to re-establish the WebSocket connection and
// re-send all tracked subscriptions to the new gateway.
func (c *Client) reconnect() {
	c.mu.Lock()
	if c.reconnecting {
		c.mu.Unlock()
		return
	}
	c.reconnecting = true
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelReconnect = cancel
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			c.reconnecting = false
			c.mu.Unlock()
		}()

		backoff := c.opts.InitialBackoff
		attempts := 0

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if c.opts.MaxReconnectAttempts > 0 && attempts >= c.opts.MaxReconnectAttempts {
				return
			}
			attempts++

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			conn, _, err := websocket.Dial(context.Background(), c.url, nil)
			if err != nil {
				// Exponential backoff with cap.
				backoff = backoff * 2
				if backoff > c.opts.MaxBackoff {
					backoff = c.opts.MaxBackoff
				}
				continue
			}

			c.mu.Lock()
			c.conn = conn
			c.mu.Unlock()

			// Re-send all tracked subscriptions to the new gateway.
			c.mu.Lock()
			subs := make([]string, len(c.subscriptions))
			copy(subs, c.subscriptions)
			c.mu.Unlock()

			if len(subs) > 0 {
				if err := c.writeSubscribe(subs); err != nil {
					c.mu.Lock()
					c.conn.Close(websocket.StatusNormalClosure, "")
					c.mu.Unlock()
					backoff = backoff * 2
					if backoff > c.opts.MaxBackoff {
						backoff = c.opts.MaxBackoff
					}
					continue
				}
			}

			// Reset backoff on successful reconnect.
			backoff = c.opts.InitialBackoff
			attempts = 0

			// Start a new read loop for the reconnected socket.
			go c.readLoop()
			return
		}
	}()
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
