package websocket

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
)

// --- Test helpers ---

func setupTestGateway(t *testing.T) (*Gateway, topicmanager.Manager) {
	t.Helper()
	metrics, _ := platform.NewMetrics("test_ws")
	tm := topicmanager.New(0)
	gw := NewGateway(tm, zerolog.Nop(), metrics)
	return gw, tm
}

func dialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func waitForClientCount(gw *Gateway, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if gw.ClientCount() == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// --- Connection lifecycle ---

func TestGateway_ConnectAndDisconnect(t *testing.T) {
	gw, _ := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	if !waitForClientCount(gw, 1, 2*time.Second) {
		t.Fatalf("expected 1 client, got %d", gw.ClientCount())
	}

	conn.Close()
	if !waitForClientCount(gw, 0, 2*time.Second) {
		t.Errorf("expected 0 clients after close, got %d", gw.ClientCount())
	}
}

func TestGateway_MultipleConnections(t *testing.T) {
	gw, _ := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	const n = 10
	conns := make([]*websocket.Conn, n)
	for i := 0; i < n; i++ {
		conns[i] = dialWS(t, wsURL)
	}

	if !waitForClientCount(gw, n, 2*time.Second) {
		t.Errorf("expected %d clients, got %d", n, gw.ClientCount())
	}

	for _, conn := range conns {
		conn.Close()
	}

	if !waitForClientCount(gw, 0, 2*time.Second) {
		t.Errorf("expected 0 clients after close, got %d", gw.ClientCount())
	}
}

// --- Subscribe ---

func TestGateway_Subscribe(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	// Send subscribe request.
	req := ClientMessage{Action: "subscribe", Symbols: []string{"AAPL"}}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read subscribed confirmation.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp ServerMessage
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Type != "subscribed" {
		t.Errorf("expected type=subscribed, got %q", resp.Type)
	}

	// Verify topic manager has the subscription.
	if n := tm.SubscriberCount("AAPL"); n != 1 {
		t.Errorf("expected 1 subscriber for AAPL, got %d", n)
	}
}

func TestGateway_SubscribeMultipleSymbols(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	req := ClientMessage{Action: "subscribe", Symbols: []string{"AAPL", "MSFT", "GOOG"}}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read confirmation.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var resp ServerMessage
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Type != "subscribed" {
		t.Errorf("expected type=subscribed, got %q", resp.Type)
	}

	if n := tm.SubscriberCount("AAPL"); n != 1 {
		t.Errorf("AAPL: expected 1 subscriber, got %d", n)
	}
	if n := tm.SubscriberCount("MSFT"); n != 1 {
		t.Errorf("MSFT: expected 1 subscriber, got %d", n)
	}
	if n := tm.SubscriberCount("GOOG"); n != 1 {
		t.Errorf("GOOG: expected 1 subscriber, got %d", n)
	}
}

// --- Unsubscribe ---

func TestGateway_Unsubscribe(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	// Subscribe.
	req := ClientMessage{Action: "subscribe", Symbols: []string{"AAPL"}}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = conn.ReadMessage() // subscribed confirmation

	// Unsubscribe.
	req = ClientMessage{Action: "unsubscribe"}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("unsubscribe write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var resp ServerMessage
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Type != "unsubscribed" {
		t.Errorf("expected type=unsubscribed, got %q", resp.Type)
	}

	// Topic manager should have 0 subscribers.
	if n := tm.SubscriberCount("AAPL"); n != 0 {
		t.Errorf("expected 0 subscribers after unsubscribe, got %d", n)
	}
}

func TestGateway_ResubscribeReplacesPrevious(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	// Subscribe to AAPL.
	if err := conn.WriteJSON(ClientMessage{Action: "subscribe", Symbols: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = conn.ReadMessage()

	// Re-subscribe to MSFT (should replace AAPL).
	if err := conn.WriteJSON(ClientMessage{Action: "subscribe", Symbols: []string{"MSFT"}}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = conn.ReadMessage()

	if n := tm.SubscriberCount("AAPL"); n != 0 {
		t.Errorf("AAPL should have 0 subscribers after resubscribe, got %d", n)
	}
	if n := tm.SubscriberCount("MSFT"); n != 1 {
		t.Errorf("MSFT should have 1 subscriber, got %d", n)
	}
}

// --- Event delivery ---

func TestGateway_EventDelivery(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	// Subscribe.
	if err := conn.WriteJSON(ClientMessage{Action: "subscribe", Symbols: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = conn.ReadMessage() // subscribed confirmation

	// Publish an event via topic manager.
	tm.Publish(context.Background(), marketdata.Quote{
		Symbol: "AAPL", Price: 150.0,
		Type: marketdata.QuoteTypeTrade, Timestamp: time.Now(),
	})

	// Read the event.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read event: %v", err)
	}

	var env ServerMessage
	if err := json.Unmarshal(msg, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Type != "trade" {
		t.Errorf("expected type=trade, got %q", env.Type)
	}
}

func TestGateway_NoDeliveryForUnsubscribedTopic(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	// Subscribe to AAPL only.
	if err := conn.WriteJSON(ClientMessage{Action: "subscribe", Symbols: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = conn.ReadMessage()

	// Publish to MSFT.
	tm.Publish(context.Background(), marketdata.Quote{
		Symbol: "MSFT", Price: 300.0,
		Type: marketdata.QuoteTypeTrade, Timestamp: time.Now(),
	})

	// Expect no message within timeout.
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("expected timeout but received a message")
	}
}

// --- Error handling ---

func TestGateway_InvalidJSON(t *testing.T) {
	gw, _ := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	// Send invalid JSON.
	if err := conn.WriteMessage(websocket.TextMessage, []byte("not json")); err != nil {
		t.Fatal(err)
	}

	// Expect error response.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp ServerMessage
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Type != "error" {
		t.Errorf("expected type=error, got %q", resp.Type)
	}
}

func TestGateway_EmptySymbols(t *testing.T) {
	gw, _ := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	// Subscribe with empty symbols.
	if err := conn.WriteJSON(ClientMessage{Action: "subscribe", Symbols: []string{}}); err != nil {
		t.Fatal(err)
	}

	// Expect error response.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp ServerMessage
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Type != "error" {
		t.Errorf("expected type=error, got %q", resp.Type)
	}
}

func TestGateway_UnknownAction(t *testing.T) {
	gw, _ := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	if err := conn.WriteJSON(ClientMessage{Action: "bogus"}); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp ServerMessage
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Type != "error" {
		t.Errorf("expected type=error, got %q", resp.Type)
	}
}

// --- Shutdown ---

func TestGateway_Shutdown(t *testing.T) {
	gw, _ := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// Dial sequentially to avoid server overload.
	const n = 5
	conns := make([]*websocket.Conn, n)
	for i := 0; i < n; i++ {
		conns[i] = dialWS(t, wsURL)
	}

	if !waitForClientCount(gw, n, 5*time.Second) {
		t.Fatalf("expected %d clients, got %d", n, gw.ClientCount())
	}

	// Shutdown should close all connections.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gw.Shutdown(ctx)

	if !waitForClientCount(gw, 0, 2*time.Second) {
		t.Errorf("expected 0 clients after shutdown, got %d", gw.ClientCount())
	}
}

// --- ClientCount ---

func TestGateway_ClientCount(t *testing.T) {
	gw, _ := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	if c := gw.ClientCount(); c != 0 {
		t.Errorf("expected 0, got %d", c)
	}

	conn1 := dialWS(t, wsURL)
	if !waitForClientCount(gw, 1, 2*time.Second) {
		t.Errorf("expected 1, got %d", gw.ClientCount())
	}

	conn2 := dialWS(t, wsURL)
	if !waitForClientCount(gw, 2, 2*time.Second) {
		t.Errorf("expected 2, got %d", gw.ClientCount())
	}

	conn1.Close()
	if !waitForClientCount(gw, 1, 2*time.Second) {
		t.Errorf("expected 1 after first close, got %d", gw.ClientCount())
	}

	conn2.Close()
	if !waitForClientCount(gw, 0, 2*time.Second) {
		t.Errorf("expected 0 after second close, got %d", gw.ClientCount())
	}
}

// --- Stress test ---

func TestGateway_ConcurrentPublishAndSubscribe(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// Connect 20 clients and subscribe to AAPL.
	conns := make([]*websocket.Conn, 20)
	for i := range conns {
		conns[i] = dialWS(t, wsURL)
		req := ClientMessage{Action: "subscribe", Symbols: []string{"AAPL"}}
		if err := conns[i].WriteJSON(req); err != nil {
			t.Fatalf("client %d subscribe: %v", i, err)
		}
		_ = conns[i].SetReadDeadline(time.Now().Add(time.Second))
		_, _, _ = conns[i].ReadMessage() // subscribed confirmation
	}

	// Publish 100 events concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(price float64) {
			defer wg.Done()
			tm.Publish(context.Background(), marketdata.Quote{
				Symbol: "AAPL", Price: price,
				Type: marketdata.QuoteTypeTrade, Timestamp: time.Now(),
			})
		}(float64(i))
	}
	wg.Wait()

	// Drain all connections to avoid goroutine leaks.
	for _, conn := range conns {
		conn.Close()
	}
}
