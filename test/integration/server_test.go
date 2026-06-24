// Package integration contains end-to-end tests that exercise the full
// request path: HTTP server → WebSocket → TopicManager → Feed.
//
// These tests use httptest.NewServer so they don't require a running binary.
package integration

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/transport"
	"github.com/sumit/rtmds/internal/websocket"
)

// mockHealthReporter implements transport.HealthReporter for testing.
type mockHealthReporter struct{}

func (m *mockHealthReporter) HealthReport(ctx context.Context) map[string]platform.HealthStatus {
	return map[string]platform.HealthStatus{
		"test": platform.OK(),
	}
}

// buildTestServer creates a minimal in-process server with a topic manager
// and returns the URL for the WebSocket endpoint.
func buildTestServer(t *testing.T) (wsURL string, tm topicmanager.Manager, cancel context.CancelFunc) {
	t.Helper()

	log := zerolog.Nop()
	metrics, gatherer := platform.NewMetrics("test")

	tm = topicmanager.New(0)
	gw := websocket.NewGateway(tm, log, metrics, 0)

	cfg := &config.Config{
		Metrics: config.MetricsConfig{Enabled: false},
	}

	router := transport.NewRouter(cfg, gw, log, metrics, gatherer, &mockHealthReporter{}, nil)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	wsURL = "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	ctx, c := context.WithCancel(context.Background())
	cancel = c

	// Publish a synthetic quote after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		tm.Publish(ctx, marketdata.Quote{
			Symbol:    "AAPL",
			Type:      marketdata.QuoteTypeTrade,
			Price:     150.00,
			Timestamp: time.Now(),
		})
		<-ctx.Done()
	}()

	return wsURL, tm, cancel
}

func TestWebSocket_ReceivesQuoteAfterSubscribe(t *testing.T) {
	wsURL, _, cancel := buildTestServer(t)
	defer cancel()

	ctx := context.Background()
	conn, _, err := gws.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Subscribe to AAPL.
	req := map[string]any{"action": "subscribe", "symbols": []string{"AAPL"}}
	data, _ := json.Marshal(req)
	if err := conn.WriteMessage(gws.TextMessage, data); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Expect at least one message within 1 second.
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, msgBytes, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var msg websocket.ServerMessage
	if err := json.Unmarshal(msgBytes, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.Type == "" {
		t.Errorf("expected non-empty type, got %q", msg.Type)
	}
}

func TestWebSocket_NoMessagesForUnsubscribedSymbol(t *testing.T) {
	wsURL, tm, cancel := buildTestServer(t)
	defer cancel()

	ctx := context.Background()
	conn, _, err := gws.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Subscribe to TSLA (not AAPL).
	req := map[string]any{"action": "subscribe", "symbols": []string{"TSLA"}}
	data, _ := json.Marshal(req)
	_ = conn.WriteMessage(gws.TextMessage, data)

	// Read the "subscribed" confirmation so the channel is clear.
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, _, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("read subscribed confirmation: %v", err)
	}

	// Publish an AAPL quote — should not arrive on TSLA subscriber.
	tm.Publish(context.Background(), marketdata.Quote{Symbol: "AAPL", Price: 150.0, Timestamp: time.Now()})

	// Expect no message within 200ms.
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Error("expected timeout but received a message")
	}
}
