package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

// mockGateway acts as a simple RTMDS gateway that records subscriptions
// and allows forced disconnections.
type mockGateway struct {
	t             *testing.T
	mu            sync.Mutex
	connCount     int32
	subscriptions map[string]bool
	clients       map[*websocket.Conn]struct{}
}

func newMockGateway(t *testing.T) *mockGateway {
	return &mockGateway{
		t:             t,
		subscriptions: make(map[string]bool),
		clients:       make(map[*websocket.Conn]struct{}),
	}
}

func (m *mockGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	atomic.AddInt32(&m.connCount, 1)

	m.mu.Lock()
	m.clients[c] = struct{}{}
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.clients, c)
		m.mu.Unlock()
		c.Close(websocket.StatusInternalError, "gateway closed")
	}()

	for {
		_, data, err := c.Read(context.Background())
		if err != nil {
			break
		}

		var req struct {
			Action  string   `json:"action"`
			Symbols []string `json:"symbols"`
		}
		if err := json.Unmarshal(data, &req); err == nil {
			m.mu.Lock()
			if req.Action == "subscribe" {
				for _, sym := range req.Symbols {
					m.subscriptions[sym] = true
				}
			} else if req.Action == "unsubscribe" {
				for _, sym := range req.Symbols {
					m.subscriptions[sym] = false
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *mockGateway) closeAllClients() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for c := range m.clients {
		c.Close(websocket.StatusNormalClosure, "forced disconnect")
	}
}

func (m *mockGateway) hasSubscription(sym string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.subscriptions[sym]
}

func (m *mockGateway) resetSubscriptions() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscriptions = make(map[string]bool)
}

func TestClient_ReconnectAndResubscribe(t *testing.T) {
	gateway := newMockGateway(t)
	server := httptest.NewServer(gateway)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Use short backoff for testing
	opts := Options{
		Reconnect:            true,
		MaxReconnectAttempts: 5,
		InitialBackoff:       10 * time.Millisecond,
		MaxBackoff:           100 * time.Millisecond,
	}

	c, err := New(wsURL, opts)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	// Initial subscription
	err = c.Subscribe("AAPL", "MSFT")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Wait for subscriptions to reach gateway
	time.Sleep(50 * time.Millisecond)

	if !gateway.hasSubscription("AAPL") || !gateway.hasSubscription("MSFT") {
		t.Fatalf("gateway did not receive initial subscriptions")
	}

	// Reset gateway state and force disconnect
	gateway.resetSubscriptions()
	gateway.closeAllClients()

	// The client should detect the disconnect and automatically reconnect,
	// then automatically resubscribe to "AAPL" and "MSFT".
	// Wait a bit for reconnect and resubscribe to complete.
	time.Sleep(200 * time.Millisecond)

	if !gateway.hasSubscription("AAPL") || !gateway.hasSubscription("MSFT") {
		t.Fatalf("client did not automatically resubscribe after reconnect")
	}

	if atomic.LoadInt32(&gateway.connCount) < 2 {
		t.Fatalf("client did not reconnect (connCount = %d)", gateway.connCount)
	}
}

func TestClient_MaxReconnectAttempts(t *testing.T) {
	gateway := newMockGateway(t)
	server := httptest.NewServer(gateway)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	
	opts := Options{
		Reconnect:            true,
		MaxReconnectAttempts: 3,
		InitialBackoff:       10 * time.Millisecond,
		MaxBackoff:           50 * time.Millisecond,
	}

	c, err := New(wsURL, opts)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	
	// Shut down server so reconnects fail
	server.Close()
	gateway.closeAllClients()

	// Wait for the max reconnect attempts to be exhausted
	select {
	case <-c.Done():
		// Success, client gave up
	case <-time.After(2 * time.Second):
		t.Fatalf("client did not give up after max reconnect attempts")
	}
}
