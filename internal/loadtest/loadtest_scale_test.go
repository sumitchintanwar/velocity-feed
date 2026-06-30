package loadtest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumit/rtmds/internal/marketdata"
)

// startScaleMockServer creates a lightweight WebSocket server that pushes
// quotes at 1 kHz per connected client. Returns the address to connect to.
func startScaleMockServer(t *testing.T) (string, func()) {
	t.Helper()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	var msgCount atomic.Int64

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Read subscribe request.
		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}

		// Push quotes at 10 Hz.
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				q := marketdata.ServerMessage{
					Type: "quote",
					Payload: marketdata.Quote{
						Symbol:    "AAPL",
						Type:      marketdata.QuoteTypeQuote,
						Price:     100.0,
						Timestamp: time.Now(),
					},
				}
				if err := conn.WriteJSON(q); err != nil {
					return
				}
				msgCount.Add(1)
			}
		}
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := &http.Server{Handler: handler}
	go server.Serve(listener)

	addr := fmt.Sprintf("ws://%s/ws", listener.Addr().String())
	cleanup := func() {
		server.Close()
		listener.Close()
	}

	t.Cleanup(cleanup)
	return addr, cleanup
}

func runScaleTest(t *testing.T, connections int, duration time.Duration) {
	t.Helper()

	addr, _ := startScaleMockServer(t)

	cfg := Config{
		ServerURL:       addr,
		Connections:     connections,
		Symbols:         []string{"AAPL", "MSFT", "GOOG", "TSLA", "NVDA"},
		Duration:        duration,
		ReportInterval:  2 * time.Second,
	}

	pool := NewPool(cfg)
	result, err := pool.Run(context.Background())
	if err != nil {
		t.Fatalf("load test failed: %v", err)
	}

	path, err := SaveResult(result, cfg)
	if err != nil {
		t.Fatalf("save result: %v", err)
	}
	t.Logf("Results saved to: %s", path)
	t.Logf("Connections: %d/%d, Messages: %d, Rate: %.0f/s, P50: %v, P99: %v",
		result.ConnectionsSucceeded, result.ConnectionsAttempted,
		result.MessagesReceived, result.MsgPerSec,
		result.Latency.P50.Round(time.Microsecond),
		result.Latency.P99.Round(time.Microsecond))
}

func TestLoadTest_1000Clients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}
	runScaleTest(t, 1000, 10*time.Second)
}

func TestLoadTest_5000Clients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}
	runScaleTest(t, 5000, 10*time.Second)
}

func TestLoadTest_10000Clients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}
	runScaleTest(t, 10000, 10*time.Second)
}
