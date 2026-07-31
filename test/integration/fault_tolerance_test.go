package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	"github.com/sumit/rtmds/pkg/protocol"
	"github.com/sumit/rtmds/test/chaos"
)

func TestFaultToleranceChaos(t *testing.T) {
	tmpDir := t.TempDir()
	walDir := tmpDir + "/wal"

	logger := log.NewFromConfig(log.Config{Level: "error", Format: "console"})
	metrics, _ := platform.NewMetrics("test_chaos")

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

	queueCfg := clientqueue.DefaultConfig()
	tm := topicmanager.NewWithQueue(0, &queueCfg, logger, nil, metrics)

	gateway := ws.NewGatewayWithReplay(tm, nil, nil, replaySvc, logger, metrics, 0, "test-gw")
	engine := chaos.NewEngine()
	engine.SetConfig(chaos.Config{
		DropProbability:       0.0,
		DelayProbability:      0.0,
		MaxDelay:              0,
		DisconnectProbability: 0.0,
	})

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gateway.Handler().ServeHTTP(w, r)
	}))
	server.Listener = engine.WrapListener(server.Listener)
	server.Start()
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	cli, err := client.Connect(wsURL, client.Options{
		Reconnect:            true,
		MaxReconnectAttempts: 100, // Very high because of chaos
		InitialBackoff:       10 * time.Millisecond,
		MaxBackoff:           100 * time.Millisecond,
		DialTimeout:          1 * time.Second,
	}, protocol.FormatJSON)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	_ = cli.Subscribe("AAPL")

	// Enable Chaos after successful connection
	engine.SetConfig(chaos.Config{
		DropProbability:       0.0, // Silent application-layer drops break TCP abstraction
		DelayProbability:      0.1,
		MaxDelay:              50 * time.Millisecond,
		DisconnectProbability: 0.1,
	})

	// Pre-populate WAL
	for i := 1; i <= 50; i++ {
		ev := marketdata.Quote{Symbol: "AAPL", Type: "quote", Bid: float64(i), Seq: int64(i)}
		payload, _ := json.Marshal(ev)
		walLog.Append(&wal.Message{Sequence: uint64(i), Timestamp: time.Now().UnixNano(), Topic: "AAPL", Type: "quote", Payload: payload})
		walLog.Sync()
		alloc.Set(uint64(i))
		tm.Publish(context.Background(), marketdata.NewCachedEvent(&ev))
		time.Sleep(2 * time.Millisecond)
	}

	// Wait to receive messages
	received := 0
	timeout := time.After(15 * time.Second)

receiveLoop:
	for {
		select {
		case ev := <-cli.Receive():
			if _, ok := ev.(*marketdata.Quote); ok {
				received++
				if received == 50 {
					break receiveLoop
				}
			}
		case <-timeout:
			t.Fatalf("timed out waiting for 50 messages under chaos, received %d", received)
		}
	}

	// Verify stats
	stats := engine.Stats()
	if stats.Drops == 0 && stats.Delays == 0 && stats.Disconnects == 0 {
		t.Log("Warning: No chaos faults were injected. Test might be flaky depending on PRNG.")
	} else {
		t.Logf("Chaos injected: %d drops, %d delays, %d disconnects", stats.Drops, stats.Delays, stats.Disconnects)
	}
}
