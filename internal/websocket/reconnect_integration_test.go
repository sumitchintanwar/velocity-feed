package websocket_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumit/rtmds/internal/clientqueue"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/replay"
	"github.com/sumit/rtmds/internal/sequencer"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/wal"
	ws "github.com/sumit/rtmds/internal/websocket"
	"github.com/sumit/rtmds/pkg/marketdata"
)

func TestGatewayReconnectIntegration(t *testing.T) {
	// Setup dependencies
	tmpDir := t.TempDir()
	walDir := tmpDir + "/wal"

	logger := log.NewFromConfig(log.Config{Level: "error", Format: "console"})
	metrics, _ := platform.NewMetrics("test")

	// Setup WAL SegmentManager & Replay Service
	cfg := wal.DefaultConfig
	cfg.Dir = walDir
	walLog, err := wal.NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to create wal log: %v", err)
	}
	defer walLog.Close()

	alloc := sequencer.NewAllocator()
	replayEngine := replay.NewEngine(walLog)
	replaySvc := replay.NewService(replayEngine, alloc)

	// Setup TopicManager
	queueCfg := clientqueue.DefaultConfig()
	tm := topicmanager.NewWithQueue(0, &queueCfg, logger, nil, metrics)

	// Create Gateway
	gateway := ws.NewGatewayWithReplay(tm, nil, nil, replaySvc, logger, metrics, 0, "test-gw")

	// Create test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gateway.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()

	// Pre-populate WAL with 5 historical messages
	symbol := "AAPL"
	for i := 1; i <= 5; i++ {
		ev := marketdata.Quote{
			Symbol: symbol,
			Bid:    float64(i * 100),
			Seq:    int64(i),
		}
		payload, _ := json.Marshal(ev)
		msg := &wal.Message{
			Sequence:  uint64(i),
			Timestamp: time.Now().UnixNano(),
			Topic:     symbol,
			Payload:   payload,
		}
		if _, _, err := walLog.Append(msg); err != nil {
			t.Fatalf("failed to append to wal: %v", err)
		}
	}
	walLog.Sync()
	alloc.Set(5) // update sequencer

	// Connect a WebSocket client
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close()

	// We will trigger the publisher after we get the "subscribed" message.
	publishLiveCh := make(chan struct{})

	// Create a publisher for live events (to test concurrent live stream merging)
	go func() {
		<-publishLiveCh
		for i := 6; i <= 10; i++ {
			ev := &marketdata.Quote{
				Symbol: symbol,
				Bid:    float64(i * 100),
				Seq:    int64(i),
			}
			tm.Publish(context.Background(), ev)
			time.Sleep(5 * time.Millisecond) // simulate live tick rate
		}
	}()

	// Send reconnect message indicating we received up to sequence 2
	reconnectMsg := ws.ClientMessage{
		Action: "reconnect",
		ResumeSeq: map[string]uint64{
			symbol: 2,
		},
	}
	if err := conn.WriteJSON(reconnectMsg); err != nil {
		t.Fatalf("failed to send reconnect message: %v", err)
	}

	// We must also subscribe to the live stream
	subMsg := ws.ClientMessage{
		Action:  "subscribe",
		Symbols: []string{symbol},
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		t.Fatalf("failed to send subscribe message: %v", err)
	}

	// Read responses. We expect:
	// 1. Control message "reconnected"
	// 2. Control message "subscribed"
	// 3. Historical stream: Seq 3, 4, 5
	// 4. Live stream: Seq 6, 7, 8, 9, 10

	expectedSeq := int64(3)

	for {
		var msg json.RawMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			t.Fatalf("failed to read from ws: %v", err)
		}

		var generic map[string]interface{}
		json.Unmarshal(msg, &generic)

		if typ, ok := generic["type"].(string); ok && (typ == "reconnected" || typ == "subscribed" || typ == "error") {
			if typ == "error" {
				t.Fatalf("received error from server: %v", string(msg))
			}
			if typ == "subscribed" {
				// Now that we are subscribed, we can start publishing live events
				close(publishLiveCh)
			}
			continue // skip control messages
		}

		// It must be a quote payload wrapped in ServerMessage
		var env ws.ServerMessage
		if err := json.Unmarshal(msg, &env); err != nil {
			t.Fatalf("unexpected message format: %s", string(msg))
		}

		fmt.Printf("Raw msg: %s\n", string(msg))

		payloadBytes, _ := json.Marshal(env.Payload)
		var quote marketdata.Quote
		if err := json.Unmarshal(payloadBytes, &quote); err != nil {
			t.Fatalf("failed to parse quote payload: %v", err)
		}

		if quote.Seq != expectedSeq {
			t.Fatalf("expected sequence %d, got %d", expectedSeq, quote.Seq)
		}

		fmt.Printf("Received Seq %d with Bid %.2f\n", quote.Seq, quote.Bid)

		expectedSeq++
		if expectedSeq > 10 {
			break // all messages received successfully
		}
	}
}
