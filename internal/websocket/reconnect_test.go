package websocket

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/topicmanager"
)

// --- Backoff tests ---

func TestBackoff_Delay(t *testing.T) {
	b := Backoff{
		Initial:    1 * time.Second,
		Max:        30 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.0, // no jitter for deterministic test
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 30 * time.Second}, // capped at max
		{6, 30 * time.Second}, // still capped
	}

	for _, tt := range tests {
		got := b.Delay(tt.attempt)
		if got != tt.expected {
			t.Errorf("attempt %d: got %v, want %v", tt.attempt, got, tt.expected)
		}
	}
}

func TestBackoff_DelayWithJitter(t *testing.T) {
	b := Backoff{
		Initial:    1 * time.Second,
		Max:        30 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.2, // ±20%
	}

	// Run multiple times and verify jitter stays within bounds.
	for i := 0; i < 100; i++ {
		delay := b.Delay(2) // base = 4s
		min := 3200 * time.Millisecond
		max := 4800 * time.Millisecond
		if delay < min || delay > max {
			t.Errorf("jitter delay %v out of range [%v, %v]", delay, min, max)
		}
	}
}

func TestDefaultBackoff(t *testing.T) {
	b := DefaultBackoff()
	if b.Initial != 1*time.Second {
		t.Errorf("expected initial 1s, got %v", b.Initial)
	}
	if b.Max != 30*time.Second {
		t.Errorf("expected max 30s, got %v", b.Max)
	}
	if b.Multiplier != 2.0 {
		t.Errorf("expected multiplier 2.0, got %v", b.Multiplier)
	}
	if b.Jitter != 0.2 {
		t.Errorf("expected jitter 0.2, got %v", b.Jitter)
	}
}

// --- Subscription registry tests ---

func TestReconnectClient_Subscribe(t *testing.T) {
	rc := NewReconnectClient("ws://localhost:9999/ws", zerolog.Nop())

	rc.Subscribe("AAPL", "MSFT")
	symbols := rc.Symbols()

	if len(symbols) != 2 {
		t.Fatalf("expected 2 symbols, got %d", len(symbols))
	}
	if !rc.subscriptions["AAPL"] {
		t.Error("AAPL not in subscriptions")
	}
	if !rc.subscriptions["MSFT"] {
		t.Error("MSFT not in subscriptions")
	}
}

func TestReconnectClient_Unsubscribe(t *testing.T) {
	rc := NewReconnectClient("ws://localhost:9999/ws", zerolog.Nop())

	rc.Subscribe("AAPL", "MSFT", "GOOG")
	rc.Unsubscribe("MSFT")

	symbols := rc.Symbols()
	if len(symbols) != 2 {
		t.Fatalf("expected 2 symbols after unsubscribe, got %d", len(symbols))
	}
	if !rc.subscriptions["AAPL"] {
		t.Error("AAPL should still be subscribed")
	}
	if rc.subscriptions["MSFT"] {
		t.Error("MSFT should have been unsubscribed")
	}
	if !rc.subscriptions["GOOG"] {
		t.Error("GOOG should still be subscribed")
	}
}

// --- Reconnect integration tests ---

// helper: start a gateway, connect a client, subscribe, return everything.
func setupReconnectTest(t *testing.T) (*Gateway, *httptest.Server, *ReconnectClient, topicmanager.Manager) {
	t.Helper()
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	log := zerolog.New(zerolog.NewTestWriter(t)).With().Timestamp().Logger()
	rc := NewReconnectClient(wsURL, log,
		WithBackoff(Backoff{
			Initial:    50 * time.Millisecond,
			Max:        200 * time.Millisecond,
			Multiplier: 2.0,
			Jitter:     0.0,
		}),
	)

	return gw, ts, rc, tm
}

func TestReconnectClient_BasicConnect(t *testing.T) {
	_, ts, rc, _ := setupReconnectTest(t)
	defer ts.Close()

	rc.Subscribe("AAPL")
	if err := rc.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rc.Stop()

	// Wait for connection.
	if !waitForConnected(rc, 2*time.Second) {
		t.Fatal("client did not connect")
	}
}

func TestReconnectClient_ReconnectAfterServerRestart(t *testing.T) {
	gw, tm := setupTestGateway(t)
	ts := httptest.NewServer(gw.Handler())
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	log := zerolog.New(zerolog.NewTestWriter(t)).With().Timestamp().Logger()
	rc := NewReconnectClient(wsURL, log,
		WithBackoff(Backoff{
			Initial:    50 * time.Millisecond,
			Max:        200 * time.Millisecond,
			Multiplier: 2.0,
			Jitter:     0.0,
		}),
	)
	rc.Subscribe("AAPL")

	// Start and wait for initial connection.
	if err := rc.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !waitForConnected(rc, 2*time.Second) {
		t.Fatal("client did not connect")
	}
	if !waitForSubscriberCount(tm, "AAPL", 1, 2*time.Second) {
		t.Fatalf("initial: expected 1 AAPL subscriber, got %d", tm.SubscriberCount("AAPL"))
	}

	// === PHASE 1: Kill the server ===
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	gw.Shutdown(ctx)
	cancel()
	ts.Close()

	// Wait until the connection is actually gone.
	waitForDisconnected(rc, 5*time.Second)

	// === PHASE 2: Restart on new port ===
	gw2, tm2 := setupTestGateway(t)
	ts2 := httptest.NewServer(gw2.Handler())
	defer ts2.Close()
	newURL := "ws" + strings.TrimPrefix(ts2.URL, "http") + "/ws"
	rc.connMu.Lock()
	rc.url = newURL
	rc.connMu.Unlock()

	// === PHASE 3: Wait for reconnect + resubscribe ===
	if !waitForConnected(rc, 5*time.Second) {
		t.Fatal("client did not reconnect to new server")
	}
	if !waitForSubscriberCount(tm2, "AAPL", 1, 5*time.Second) {
		t.Fatalf("reconnect: expected 1 AAPL subscriber on new gateway, got %d", tm2.SubscriberCount("AAPL"))
	}

	// === PHASE 4: Verify event delivery works ===
	drainUntilTimeout(rc, 300*time.Millisecond)
	tm2.Publish(context.Background(), marketdata.Quote{
		Symbol: "AAPL", Price: 150.0,
		Type: marketdata.QuoteTypeTrade, Timestamp: time.Now(),
	})
	select {
	case msg := <-rc.Events():
		if msg.Type != "trade" {
			t.Errorf("expected trade event, got %q", msg.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event after reconnect")
	}

	rc.Stop()
}

// waitForDisconnected polls until rc.conn is nil or timeout.
func waitForDisconnected(rc *ReconnectClient, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rc.connMu.Lock()
		conn := rc.conn
		rc.connMu.Unlock()
		if conn == nil {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func TestReconnectClient_MultipleSubscriptionsResubscribe(t *testing.T) {
	gw, ts, rc, _ := setupReconnectTest(t)

	rc.Subscribe("AAPL", "MSFT", "GOOG")
	if err := rc.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	if !waitForConnected(rc, 2*time.Second) {
		t.Fatal("client did not connect")
	}

	// Kill server.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	gw.Shutdown(ctx)
	cancel()
	ts.Close()

	// Restart.
	ts2 := httptest.NewServer(
		func() *Gateway {
			gw, _ := setupTestGateway(t)
			return gw
		}().Handler(),
	)
	defer ts2.Close()

	rc.connMu.Lock()
	rc.url = "ws" + strings.TrimPrefix(ts2.URL, "http") + "/ws"
	rc.connMu.Unlock()

	if !waitForConnected(rc, 5*time.Second) {
		t.Fatal("client did not reconnect")
	}

	// Verify all 3 subscriptions were resubscribed.
	symbols := rc.Symbols()
	if len(symbols) != 3 {
		t.Errorf("expected 3 subscriptions, got %d: %v", len(symbols), symbols)
	}

	rc.Stop()
}

func TestReconnectClient_ExponentialBackoff(t *testing.T) {
	// Use a non-existent server to test backoff behavior.
	rc := NewReconnectClient("ws://localhost:1/ws", zerolog.Nop(),
		WithBackoff(Backoff{
			Initial:    10 * time.Millisecond,
			Max:        100 * time.Millisecond,
			Multiplier: 2.0,
			Jitter:     0.0,
		}),
	)

	var mu sync.Mutex
	var attempts []time.Time

	origFn := rc.backoffFn
	rc.backoffFn = func(attempt int) time.Duration {
		d := origFn(attempt)
		mu.Lock()
		attempts = append(attempts, time.Now())
		mu.Unlock()
		return d
	}

	if err := rc.Start(); err == nil {
		t.Fatal("expected error on first connect to non-existent server")
	}

	// Wait for a few retry attempts.
	time.Sleep(150 * time.Millisecond)
	rc.Stop()

	mu.Lock()
	count := len(attempts)
	mu.Unlock()

	if count < 2 {
		t.Errorf("expected at least 2 backoff attempts, got %d", count)
	}
}

func TestReconnectClient_ConnectedState(t *testing.T) {
	_, ts, rc, _ := setupReconnectTest(t)
	defer ts.Close()

	if rc.Connected() {
		t.Error("should not be connected before start")
	}

	rc.Subscribe("AAPL")
	if err := rc.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	if !waitForConnected(rc, 2*time.Second) {
		t.Fatal("client did not connect")
	}

	rc.Stop()

	// After stop, should not be connected.
	time.Sleep(100 * time.Millisecond)
	if rc.Connected() {
		t.Error("should not be connected after stop")
	}
}

func TestReconnectClient_StopDuringReconnect(t *testing.T) {
	// Test that Stop works cleanly even when not connected.
	rc := NewReconnectClient("ws://localhost:1/ws", zerolog.Nop(),
		WithBackoff(Backoff{
			Initial:    10 * time.Millisecond,
			Max:        50 * time.Millisecond,
			Multiplier: 2.0,
			Jitter:     0.0,
		}),
	)

	_ = rc.Start()
	time.Sleep(30 * time.Millisecond) // let it attempt a few reconnects
	rc.Stop()                          // should not deadlock
}

func TestReconnectClient_EventsChannelClosed(t *testing.T) {
	_, ts, rc, _ := setupReconnectTest(t)
	defer ts.Close()

	rc.Subscribe("AAPL")
	if err := rc.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	if !waitForConnected(rc, 2*time.Second) {
		t.Fatal("client did not connect")
	}

	rc.Stop()

	// Events channel should be closed after Stop.
	select {
	case _, ok := <-rc.Events():
		if ok {
			t.Error("expected events channel to be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for events channel to close")
	}
}

// --- Helper functions ---

func waitForConnected(rc *ReconnectClient, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if rc.Connected() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// drainEvents reads all pending events from the channel.
func drainEvents(rc *ReconnectClient) []ServerMessage {
	var msgs []ServerMessage
	for {
		select {
		case msg := <-rc.events:
			msgs = append(msgs, msg)
		default:
			return msgs
		}
	}
}

// drainUntilTimeout drains the events channel for up to the given duration.
func drainUntilTimeout(rc *ReconnectClient, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-rc.events:
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// waitForSubscriberCount polls until the topic manager has the expected
// number of subscribers for the given topic, or returns false on timeout.
func waitForSubscriberCount(tm interface{ SubscriberCount(string) int }, topic string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tm.SubscriberCount(topic) == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
