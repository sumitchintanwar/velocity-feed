// Package client provides a Go client SDK for consuming the Real-Time Market
// Data System WebSocket API. It implements the "fat client" pattern: the client
// owns subscription state and automatically re-sends subscriptions on reconnect,
// enabling truly stateless gateways.
//
// Usage:
//
//	c, err := client.Connect("ws://localhost:8080/ws", protocol.FormatProtobuf, protocol.FormatJSON)
//	if err != nil { panic(err) }
//	defer c.Close()
//
//	c.Subscribe("AAPL", "TSLA")
//	for event := range c.Receive() {
//	    fmt.Printf("%s: %s\n", event.EventSymbol(), event.EventType())
//	}
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sumit/rtmds/pkg/marketdata"
	"github.com/sumit/rtmds/pkg/protocol"
	"nhooyr.io/websocket"
)

// ErrDisconnected is returned when an operation requires an active connection
// but the client is currently disconnected.
var ErrDisconnected = errors.New("client: disconnected")

// ControlMessage is the envelope received from the server for non-market data.
type ControlMessage struct {
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
	// DialTimeout is the timeout for WebSocket dial operations.
	DialTimeout time.Duration
}

// DefaultOptions returns sensible defaults for the client.
func DefaultOptions() Options {
	return Options{
		Reconnect:            true,
		MaxReconnectAttempts: 0,
		InitialBackoff:       1 * time.Second,
		MaxBackoff:           30 * time.Second,
		DialTimeout:          5 * time.Second,
	}
}

const dialTimeout = 5 * time.Second

func newRng() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
}

// Client is a WebSocket client for the RTMDS server.
type Client struct {
	url     string
	opts    Options
	conn    *websocket.Conn
	msgCh   chan marketdata.MarketEvent
	doneCh  chan struct{}
	closeCh chan struct{}

	registry   *protocol.Registry
	serializer protocol.Serializer
	formats    []protocol.Format

	mu              sync.Mutex
	subscriptions   []string
	reconnecting    bool
	cancelReconnect context.CancelFunc

	closing      bool
	shutdownOnce sync.Once
	resumeSeq    map[string]uint64
}

// Connect dials the RTMDS WebSocket endpoint and negotiates the subprotocol.
func Connect(url string, opts Options, formats ...protocol.Format) (*Client, error) {
	if len(formats) == 0 {
		formats = []protocol.Format{protocol.FormatFlatBuffers, protocol.FormatProtobuf, protocol.FormatJSON}
	}

	registry := protocol.NewRegistry()
	registry.Register(protocol.NewJSONSerializer())
	registry.Register(protocol.NewProtobufSerializer())
	registry.Register(protocol.NewFlatBuffersSerializer())

	dialCtx, dialCancel := context.WithTimeout(context.Background(), opts.DialTimeout)
	defer dialCancel()

	dialOpts := &websocket.DialOptions{}
	for _, f := range formats {
		dialOpts.Subprotocols = append(dialOpts.Subprotocols, protocol.FormatToSubprotocol(f))
	}

	conn, resp, err := websocket.Dial(dialCtx, url, dialOpts)
	if err != nil {
		return nil, fmt.Errorf("client: dial %q: %w", url, err)
	}

	negotiated := resp.Header.Get("Sec-WebSocket-Protocol")
	negotiator := protocol.NewNegotiator()
	format, _, _ := negotiator.SelectProtocol([]string{negotiated})

	serializer, _ := registry.Lookup(format)
	if serializer == nil {
		// Fallback to JSON if negotiation failed or server ignored it
		serializer, _ = registry.Lookup(protocol.FormatJSON)
	}

	c := &Client{
		url:        url,
		opts:       opts,
		conn:       conn,
		msgCh:      make(chan marketdata.MarketEvent, 256),
		doneCh:     make(chan struct{}),
		closeCh:    make(chan struct{}),
		registry:   registry,
		serializer: serializer,
		formats:    formats,
		resumeSeq:  make(map[string]uint64),
	}
	go c.readLoop()
	return c, nil
}

// Subscribe sends a subscribe command to the server and records it.
func (c *Client) Subscribe(symbols ...string) error {
	c.mu.Lock()
	seen := make(map[string]struct{}, len(c.subscriptions))
	for _, s := range c.subscriptions {
		seen[s] = struct{}{}
	}
	for _, s := range symbols {
		if _, ok := seen[s]; !ok {
			c.subscriptions = append(c.subscriptions, s)
		}
	}
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return ErrDisconnected
	}
	return c.writeSubscribe(symbols)
}

// Receive returns the channel on which server market events are delivered.
func (c *Client) Receive() <-chan marketdata.MarketEvent {
	return c.msgCh
}

// Done returns a channel that is closed when the client is permanently shut down.
func (c *Client) Done() <-chan struct{} {
	return c.doneCh
}

// Reconnect triggers a manual reconnect.
func (c *Client) Reconnect() {
	go c.reconnect()
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return nil
	}
	c.reconnecting = false
	c.opts.Reconnect = false
	c.closing = true
	if c.cancelReconnect != nil {
		c.cancelReconnect()
	}
	close(c.closeCh)
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if c.conn != nil {
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
	}
	<-c.doneCh
	_ = ctx
	return nil
}

func (c *Client) writeSubscribe(symbols []string) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return ErrDisconnected
	}
	return conn.Write(context.Background(), websocket.MessageText, mustMarshal(map[string]any{
		"action":  "subscribe",
		"symbols": symbols,
	}))
}

func (c *Client) shutdown() {
	c.shutdownOnce.Do(func() {
		close(c.msgCh)
		close(c.doneCh)
	})
}

func (c *Client) readLoop() {
	for {
		c.mu.Lock()
		conn := c.conn
		closing := c.closing
		c.mu.Unlock()

		if closing {
			c.shutdown()
			return
		}

		msgType, data, err := conn.Read(context.Background())
		if err != nil {
			_ = conn.Close(websocket.StatusNormalClosure, "")

			c.mu.Lock()
			closing = c.closing
			c.mu.Unlock()
			if c.opts.Reconnect && !closing {
				c.reconnect()
				return
			}
			c.shutdown()
			return
		}

		// Attempt to deserialize as a market event first
		if msgType == websocket.MessageBinary || c.serializer.Format() == protocol.FormatJSON {
			event, err := c.serializer.Deserialize(data)
			if err == nil {
				// Automatic heartbeat reply
				if ping, ok := event.(marketdata.PingMessage); ok {
					b, _ := json.Marshal(map[string]any{
						"action":    "pong",
						"timestamp": ping.Timestamp,
					})
					c.mu.Lock()
					if c.conn != nil {
						_ = c.conn.Write(context.Background(), websocket.MessageText, b)
					}
					c.mu.Unlock()
					continue
				}

				if seqEv, ok := event.(marketdata.SequencedEvent); ok {
					seq := uint64(seqEv.GetSeq())
					if seq > 0 {
						c.mu.Lock()
						c.resumeSeq[event.EventSymbol()] = seq
						c.mu.Unlock()
					}
				}

				select {
				case c.msgCh <- event:
				case <-c.closeCh:
					c.shutdown()
					return
				}
				continue
			}
		}

		// Fallback for JSON control messages
		if msgType == websocket.MessageText {
			var msg ControlMessage
			if err := json.Unmarshal(data, &msg); err == nil {
				// We can handle control messages like 'error' or 'subscribed' here
				continue
			}
		}

		// Log if we completely failed to parse a message
		fmt.Printf("client %p: failed to parse message: %s\n", c, string(data))
	}
}

func (c *Client) reconnect() {
	c.mu.Lock()
	if c.reconnecting {
		c.mu.Unlock()
		return
	}
	c.reconnecting = true
	c.conn = nil
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelReconnect = cancel
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			c.reconnecting = false
			c.mu.Unlock()
		}()

		rng := newRng()
		backoff := c.opts.InitialBackoff
		attempts := 0

		for {
			select {
			case <-ctx.Done():
				c.shutdown()
				return
			default:
			}

			if c.opts.MaxReconnectAttempts > 0 && attempts >= c.opts.MaxReconnectAttempts {
				c.shutdown()
				return
			}
			attempts++

			jitter := (rng.Float64() * 0.4) - 0.2
			sleepDuration := time.Duration(float64(backoff) * (1.0 + jitter))

			select {
			case <-ctx.Done():
				c.shutdown()
				return
			case <-time.After(sleepDuration):
			}

			timeout := c.opts.DialTimeout
			if timeout == 0 {
				timeout = dialTimeout
			}
			dialCtx, dialCancel := context.WithTimeout(context.Background(), timeout)
			dialOpts := &websocket.DialOptions{}
			for _, f := range c.formats {
				dialOpts.Subprotocols = append(dialOpts.Subprotocols, protocol.FormatToSubprotocol(f))
			}
			conn, resp, err := websocket.Dial(dialCtx, c.url, dialOpts)
			dialCancel()
			if err != nil {
				backoff = backoff * 2
				if backoff > c.opts.MaxBackoff {
					backoff = c.opts.MaxBackoff
				}
				continue
			}

			negotiated := resp.Header.Get("Sec-WebSocket-Protocol")
			negotiator := protocol.NewNegotiator()
			format, _, _ := negotiator.SelectProtocol([]string{negotiated})
			serializer, _ := c.registry.Lookup(format)
			if serializer == nil {
				serializer, _ = c.registry.Lookup(protocol.FormatJSON)
			}

			c.mu.Lock()
			c.conn = conn
			c.serializer = serializer
			subs := make([]string, len(c.subscriptions))
			copy(subs, c.subscriptions)
			resumeSeqCopy := make(map[string]uint64, len(c.resumeSeq))
			for k, v := range c.resumeSeq {
				resumeSeqCopy[k] = v
			}
			c.mu.Unlock()

			if len(subs) > 0 {
				payload := map[string]any{
					"action":  "reconnect",
					"symbols": subs,
				}
				if len(resumeSeqCopy) > 0 {
					payload["resume_seq"] = resumeSeqCopy
				} else {
					payload["action"] = "subscribe"
				}
				// Pass the reconnect start time to measure reconnect latency on the server
				if payload["action"] == "reconnect" {
					payload["reconnect_start"] = time.Now().UnixNano()
				}
				b, _ := json.Marshal(payload)

				if err := conn.Write(context.Background(), websocket.MessageText, b); err != nil {
					c.mu.Lock()
					c.conn.Close(websocket.StatusNormalClosure, "")
					c.conn = nil
					c.mu.Unlock()
					backoff = backoff * 2
					if backoff > c.opts.MaxBackoff {
						backoff = c.opts.MaxBackoff
					}
					continue
				}
			}

			go c.readLoop()
			return
		}
	}()
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
