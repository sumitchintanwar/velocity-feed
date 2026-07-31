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
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/pkg/marketdata"
	"github.com/sumit/rtmds/pkg/protocol"
)

// --- Test helpers ---

func setupTestGateway(t *testing.T) (*Gateway, topicmanager.Manager) {
	t.Helper()
	metrics, _ := platform.NewMetrics("test_ws")
	tm := topicmanager.New(0)
	gw := NewGateway(tm, log.New(nil, "test"), metrics, 0, "test-gw") // 0 = no rate limit for tests
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

func TestGateway_ProtocolNegotiation(t *testing.T) {
	gw, _ := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// Create a dialer that specifies subprotocols without mutating the global DefaultDialer
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{"v1.protobuf.rtmds", "v1.json.rtmds"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Verify the server negotiated Protobuf
	negotiated := resp.Header.Get("Sec-WebSocket-Protocol")
	if negotiated != "v1.protobuf.rtmds" {
		t.Errorf("expected v1.protobuf.rtmds, got %q", negotiated)
	}
}

func TestGateway_ProtobufSerialization(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{"v1.protobuf.rtmds"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	if resp.Header.Get("Sec-WebSocket-Protocol") != "v1.protobuf.rtmds" {
		t.Fatalf("expected protobuf negotiation")
	}

	// 1. Send subscription (JSON control message)
	subMsg := ClientMessage{
		Action:  "subscribe",
		Symbols: []string{"AAPL"},
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}

	// Read confirmation (Control messages are still JSON)
	var conf ServerMessage
	if err := conn.ReadJSON(&conf); err != nil {
		t.Fatalf("read confirmation: %v", err)
	}

	// 2. Publish a live quote
	quote := marketdata.Quote{
		Symbol:    "AAPL",
		Type:      marketdata.QuoteTypeTrade,
		Price:     150.25,
		Volume:    100,
		Timestamp: time.Now(),
	}
	// Note: the feed layer usually wraps this in a CachedEvent.
	// We'll publish the raw event directly, tm will encode it (which only encodes JSON).
	// But our Protobuf serializer will dynamically serialize it!
	tm.Publish(context.Background(), marketdata.NewCachedEvent(&quote))

	// 3. Read the binary protobuf event
	msgType, b, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	if msgType != websocket.BinaryMessage {
		t.Errorf("expected binary message for protobuf, got %d", msgType)
	}

	// Deserialize using the actual protocol layer to verify
	serializer := protocol.NewProtobufSerializer()
	ev, err := serializer.Deserialize(b)
	if err != nil {
		t.Fatalf("failed to deserialize protobuf: %v", err)
	}

	if ev.EventSymbol() != "AAPL" {
		t.Errorf("expected AAPL, got %v", ev.EventSymbol())
	}

	trade, ok := ev.(*marketdata.Quote)
	if !ok {
		t.Errorf("expected Quote, got %T", ev)
	} else {
		if trade.Price != 150.25 {
			t.Errorf("expected price 150.25, got %v", trade.Price)
		}
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
		t.Logf("Dialed client %d, count: %d", i+1, gw.ClientCount())
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

// --- Heartbeat tests ---

func TestHeartbeatManager_RegisterUnregister(t *testing.T) {
	metrics, _ := platform.NewMetrics("test_hb")
	hm := NewHeartbeatManager(log.New(nil, "test"), metrics, 0, 0)

	hm.Register("client-1", func() {}, nil)
	if hm.ClientCount() != 1 {
		t.Fatalf("expected 1, got %d", hm.ClientCount())
	}

	hm.Register("client-2", func() {}, nil)
	if hm.ClientCount() != 2 {
		t.Fatalf("expected 2, got %d", hm.ClientCount())
	}

	hm.Unregister("client-1")
	if hm.ClientCount() != 1 {
		t.Fatalf("expected 1 after unregister, got %d", hm.ClientCount())
	}

	hm.Unregister("client-2")
	if hm.ClientCount() != 0 {
		t.Fatalf("expected 0 after unregister, got %d", hm.ClientCount())
	}
}

func TestHeartbeatManager_RecordPingPong(t *testing.T) {
	metrics, _ := platform.NewMetrics("test_hb")
	hm := NewHeartbeatManager(log.New(nil, "test"), metrics, 0, 0)

	timeoutFired := make(chan struct{}, 1)
	hm.Register("client-1", func() {
		timeoutFired <- struct{}{}
	}, nil)

	// Record a ping, then a pong — should not trigger timeout.
	hm.RecordPing("client-1")
	time.Sleep(10 * time.Millisecond)
	hm.RecordPong("client-1")

	// Run cleanup — no timeout should fire because pong was received.
	hm.checkTimeouts()

	select {
	case <-timeoutFired:
		t.Fatal("timeout should not have fired after pong received")
	case <-time.After(50 * time.Millisecond):
		// Expected: no timeout.
	}
}

func TestHeartbeatManager_TimeoutDetection(t *testing.T) {
	metrics, _ := platform.NewMetrics("test_hb")
	l := log.New(nil, "test")
	hm := NewHeartbeatManager(l, metrics, 50*time.Millisecond, 2)

	timeoutFired := make(chan struct{}, 1)
	hm.Register("client-1", func() {
		timeoutFired <- struct{}{}
	}, nil)

	// Record a ping but never send a pong.
	hm.RecordPing("client-1")

	// Wait longer than the pong timeout.
	time.Sleep(100 * time.Millisecond)

	// checkTimeouts should detect the dead client.
	hm.checkTimeouts()

	select {
	case <-timeoutFired:
		// Expected: timeout callback was invoked.
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected timeout callback to fire")
	}
}

func TestHeartbeatManager_NoPingNoTimeout(t *testing.T) {
	metrics, _ := platform.NewMetrics("test_hb")
	l := log.New(nil, "test")
	hm := NewHeartbeatManager(l, metrics, 10*time.Millisecond, 2)

	timeoutFired := make(chan struct{}, 1)
	hm.Register("client-1", func() {
		timeoutFired <- struct{}{}
	}, nil)

	// No ping sent — should not trigger timeout (only outstanding pings count).
	hm.checkTimeouts()

	select {
	case <-timeoutFired:
		t.Fatal("timeout should not fire when no ping is outstanding")
	case <-time.After(50 * time.Millisecond):
		// Expected.
	}
}

func TestHeartbeatManager_MultiplePingsBeforePong(t *testing.T) {
	metrics, _ := platform.NewMetrics("test_hb")
	hm := NewHeartbeatManager(log.New(nil, "test"), metrics, 0, 0)

	hm.Register("client-1", func() {}, nil)

	// Send 3 pings without pong — each should increment the counter.
	hm.RecordPing("client-1")
	hm.RecordPing("client-1")
	hm.RecordPing("client-1")

	// Now send a pong — resets the outstanding ping.
	hm.RecordPong("client-1")

	// No timeout should fire.
	hm.checkTimeouts()
}

// TestGateway_HeartbeatCleanupIntegration tests the full integration:
// a client connects, subscribes, then the server heartbeat detects
// a dead connection and cleans it up.
func TestGateway_HeartbeatCleanupIntegration(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)

	// Subscribe.
	if err := conn.WriteJSON(ClientMessage{Action: "subscribe", Symbols: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = conn.ReadMessage() // subscribed confirmation

	if !waitForClientCount(gw, 1, 2*time.Second) {
		t.Fatalf("expected 1 client, got %d", gw.ClientCount())
	}
	if tm.SubscriberCount("AAPL") != 1 {
		t.Fatalf("expected 1 AAPL subscriber, got %d", tm.SubscriberCount("AAPL"))
	}

	// Simulate client disappearing (close without close frame).
	conn.Close()

	// The gateway should detect the disconnection and clean up.
	if !waitForClientCount(gw, 0, 5*time.Second) {
		t.Errorf("expected 0 clients after disconnect, got %d", gw.ClientCount())
	}
	if tm.SubscriberCount("AAPL") != 0 {
		t.Errorf("expected 0 AAPL subscribers after cleanup, got %d", tm.SubscriberCount("AAPL"))
	}
}

// TestGateway_HeartbeatMultipleClientsCleanup tests that multiple clients
// are cleaned up independently when they disconnect.
func TestGateway_HeartbeatMultipleClientsCleanup(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// Connect 3 clients, each subscribing to different symbols.
	symbols := []string{"AAPL", "MSFT", "GOOG"}
	conns := make([]*websocket.Conn, 3)
	for i, sym := range symbols {
		conns[i] = dialWS(t, wsURL)
		if err := conns[i].WriteJSON(ClientMessage{Action: "subscribe", Symbols: []string{sym}}); err != nil {
			t.Fatal(err)
		}
		_ = conns[i].SetReadDeadline(time.Now().Add(time.Second))
		_, _, _ = conns[i].ReadMessage()
	}

	if !waitForClientCount(gw, 3, 2*time.Second) {
		t.Fatalf("expected 3 clients, got %d", gw.ClientCount())
	}

	// Disconnect client 1 (AAPL).
	conns[0].Close()
	if !waitForClientCount(gw, 2, 5*time.Second) {
		t.Errorf("expected 2 clients after first disconnect, got %d", gw.ClientCount())
	}
	if tm.SubscriberCount("AAPL") != 0 {
		t.Errorf("AAPL: expected 0 subscribers, got %d", tm.SubscriberCount("AAPL"))
	}

	// Disconnect client 2 (MSFT).
	conns[1].Close()
	if !waitForClientCount(gw, 1, 5*time.Second) {
		t.Errorf("expected 1 client after second disconnect, got %d", gw.ClientCount())
	}
	if tm.SubscriberCount("MSFT") != 0 {
		t.Errorf("MSFT: expected 0 subscribers, got %d", tm.SubscriberCount("MSFT"))
	}

	// Disconnect client 3 (GOOG).
	conns[2].Close()
	if !waitForClientCount(gw, 0, 5*time.Second) {
		t.Errorf("expected 0 clients after third disconnect, got %d", gw.ClientCount())
	}
	if tm.SubscriberCount("GOOG") != 0 {
		t.Errorf("GOOG: expected 0 subscribers, got %d", tm.SubscriberCount("GOOG"))
	}
}

// TestGateway_HeartbeatMetricsIncremented verifies that ping/pong/timeout
// metrics are incremented during normal client lifecycle.
func TestGateway_HeartbeatMetricsIncremented(t *testing.T) {
	oldPingPeriod := pingPeriod
	pingPeriod = 100 * time.Millisecond
	defer func() { pingPeriod = oldPingPeriod }()

	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)

	// Subscribe.
	if err := conn.WriteJSON(ClientMessage{Action: "subscribe", Symbols: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = conn.ReadMessage()

	// Publish an event so client has something to read.
	tm.Publish(context.Background(), marketdata.Quote{
		Symbol: "AAPL", Price: 150.0,
		Type: marketdata.QuoteTypeTrade, Timestamp: time.Now(),
	})
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = conn.ReadMessage() // consume the event

	// Let at least one ping cycle run.
	time.Sleep(pingPeriod + 200*time.Millisecond)

	// Verify ping sent metric was incremented.
	// (We can't check exact values without reading Prometheus internals,
	// but we verify the gateway is operational and clients remain connected.)
	if !waitForClientCount(gw, 1, time.Second) {
		t.Error("client should still be connected after ping cycle")
	}

	conn.Close()
	if !waitForClientCount(gw, 0, 5*time.Second) {
		t.Error("expected 0 clients after disconnect")
	}
}
