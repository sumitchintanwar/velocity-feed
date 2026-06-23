// Package client provides a Go client SDK for consuming the Real-Time Market
// Data System WebSocket API. It can be imported by external programs.
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
	"time"

	"nhooyr.io/websocket"
)

// Message is the envelope received from the server.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Client is a WebSocket client for the RTMDS server.
type Client struct {
	conn   *websocket.Conn
	msgCh  chan Message
	doneCh chan struct{}
}

// New dials the RTMDS WebSocket endpoint and returns a ready Client.
func New(url string) (*Client, error) {
	conn, _, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		return nil, fmt.Errorf("client: dial %q: %w", url, err)
	}

	c := &Client{
		conn:   conn,
		msgCh:  make(chan Message, 256),
		doneCh: make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// Subscribe sends a subscribe command to the server for the given symbols.
func (c *Client) Subscribe(symbols ...string) error {
	return c.conn.Write(context.Background(), websocket.MessageText, mustMarshal(map[string]any{
		"action":  "subscribe",
		"symbols": symbols,
	}))
}

// Unsubscribe sends an unsubscribe command to the server.
func (c *Client) Unsubscribe(symbols ...string) error {
	return c.conn.Write(context.Background(), websocket.MessageText, mustMarshal(map[string]any{
		"action":  "unsubscribe",
		"symbols": symbols,
	}))
}

// Messages returns the channel on which server messages are delivered.
// The channel is closed when the connection is closed.
func (c *Client) Messages() <-chan Message {
	return c.msgCh
}

// Close gracefully closes the WebSocket connection.
func (c *Client) Close() error {
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

func (c *Client) readLoop() {
	defer close(c.msgCh)
	defer close(c.doneCh)

	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			return
		}
		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		c.msgCh <- msg
	}
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
