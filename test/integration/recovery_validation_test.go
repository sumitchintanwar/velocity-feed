package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/clientqueue"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/replay"
	"github.com/sumit/rtmds/internal/sequencer"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/wal"
	ws "github.com/sumit/rtmds/internal/websocket"
	"github.com/sumit/rtmds/pkg/client"
	"github.com/sumit/rtmds/pkg/marketdata"
	"github.com/sumit/rtmds/test/chaos"
)

// TestRecoveryValidation verifies:
// - Reconnect success (via client options and chaos disconnect)
// - No message loss (all messages received)
// - No duplicates (count equals exact number of sent)
// - Replay correctness (client automatically requested missing gap)
func TestRecoveryValidation(t *testing.T) {
	// 1. Setup Dependencies
	tmpDir := t.TempDir()
	walDir := tmpDir + "/wal"

	logger := log.NewFromConfig(log.Config{Level: "error", Format: "console"})
	metrics, _ := platform.NewMetrics("test_recovery")

	// WAL & Replay
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

	// TopicManager
	queueCfg := clientqueue.DefaultConfig()
	tm := topicmanager.NewWithQueue(0, &queueCfg, logger, nil, metrics)

	// Gateway
	gateway := ws.NewGatewayWithReplay(tm, nil, nil, replaySvc, logger, metrics, 0, "test-gw")

	// 2. Setup Chaos Engine & HTTP Server
	engine := chaos.NewEngine()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gateway.Handler().ServeHTTP(w, r)
	}))
	server.Listener = engine.WrapListener(server.Listener)
	server.Start()
	defer server.Close()

	// 3. Start Client
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cli, err := client.Connect(wsURL, client.Options{
		Reconnect:            true,
		MaxReconnectAttempts: 10,
		InitialBackoff:       10 * time.Millisecond,
		MaxBackoff:           100 * time.Millisecond,
		DialTimeout:          1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	// Subscribe
	symbol := "AAPL"
	if err := cli.Subscribe(symbol); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	// 4. Capture Received Messages
	received := make(map[uint64]int)
	var mu sync.Mutex

	go func() {
		for ev := range cli.Receive() {
			if q, ok := ev.(*marketdata.Quote); ok {
				mu.Lock()
				received[uint64(q.Seq)]++
				mu.Unlock()
			}
		}
	}()

	// Wait for client subscription to be registered
	time.Sleep(100 * time.Millisecond)

	// Pre-populate messages (1 to 5)
	for i := 1; i <= 5; i++ {
		ev := marketdata.Quote{
			Symbol: symbol,
			Type:   "trade",
			Bid:    float64(i * 100),
			Seq:    int64(i),
		}
		// Write to WAL so it's available for replay
		payload, _ := json.Marshal(ev)
		msg := &wal.Message{
			Sequence:  uint64(i),
			Timestamp: time.Now().UnixNano(),
			Topic:     symbol,
			Payload:   payload,
		}
		walLog.Append(msg)
		alloc.Set(uint64(i))

		// Publish live
		tm.Publish(context.Background(), marketdata.NewCachedEvent(&ev))
		time.Sleep(5 * time.Millisecond)
	}
	walLog.Sync()

	time.Sleep(100 * time.Millisecond)

	// INJECT CHAOS: Drop connections!
	engine.DisconnectClients()

	// Publish more messages (6 to 10) while client is disconnected / reconnecting
	for i := 6; i <= 10; i++ {
		ev := marketdata.Quote{
			Symbol: symbol,
			Type:   "trade",
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
		walLog.Append(msg)
		alloc.Set(uint64(i))
		tm.Publish(context.Background(), marketdata.NewCachedEvent(&ev))
	}
	walLog.Sync()

	// Wait for reconnect and replay
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Verify all messages 1 to 10 were received exactly once
	for i := 1; i <= 10; i++ {
		count := received[uint64(i)]
		if count == 0 {
			t.Errorf("missing message %d", i)
		} else if count > 1 {
			t.Errorf("duplicate message %d (received %d times)", i, count)
		}
	}

	if len(received) != 10 {
		t.Errorf("expected exactly 10 unique messages, got %d", len(received))
	}
}
