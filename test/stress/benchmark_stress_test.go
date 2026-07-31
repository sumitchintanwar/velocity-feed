package stress

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

func setupTestEnvironment(b *testing.B) (*ws.Gateway, *chaos.Engine, *httptest.Server, topicmanager.Manager, *wal.SegmentManager, *sequencer.Allocator) {
	tmpDir := b.TempDir()
	walDir := tmpDir + "/wal"

	logger := log.NewFromConfig(log.Config{Level: "error", Format: "console"})
	metrics, _ := platform.NewMetrics("test_stress")

	cfg := wal.DefaultConfig
	cfg.Dir = walDir
	walLog, err := wal.NewSegmentManager(cfg)
	if err != nil {
		b.Fatalf("failed to create wal log: %v", err)
	}

	alloc := sequencer.NewAllocator()
	replayEngine := replay.NewEngine(walLog)
	replaySvc := replay.NewService(replayEngine, alloc)

	queueCfg := clientqueue.DefaultConfig()
	tm := topicmanager.NewWithQueue(0, &queueCfg, logger, nil, metrics)

	gateway := ws.NewGatewayWithReplay(tm, nil, nil, replaySvc, logger, metrics, 0, "test-gw")
	engine := chaos.NewEngine()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gateway.Handler().ServeHTTP(w, r)
	}))
	server.Listener = engine.WrapListener(server.Listener)
	server.Start()

	return gateway, engine, server, tm, walLog, alloc
}

// Benchmark1000ReconnectingClients simulates 1000 clients connecting, dropping them all,
// and measuring the time it takes for all to reconnect and replay successfully.
func Benchmark1000ReconnectingClients(b *testing.B) {
	gateway, engine, server, tm, walLog, alloc := setupTestEnvironment(b)
	defer server.Close()
	defer walLog.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var clients []*client.Client
		const numClients = 1000

		// 1. Pre-generate WAL data
		for j := 1; j <= 10; j++ {
			ev := marketdata.Quote{Symbol: "AAPL", Type: "quote", Bid: float64(j), Seq: int64(j)}
			payload, _ := json.Marshal(ev)
			walLog.Append(&wal.Message{Sequence: uint64(j), Timestamp: time.Now().UnixNano(), Topic: "AAPL", Type: "quote", Payload: payload})
			alloc.Set(uint64(j))
			tm.Publish(context.Background(), marketdata.NewCachedEvent(&ev))
		}
		walLog.Sync()

		// 2. Connect clients
		var wg sync.WaitGroup
		wg.Add(numClients)
		var mu sync.Mutex

		for c := 0; c < numClients; c++ {
			go func() {
				defer wg.Done()
				cli, err := client.Connect(wsURL, client.Options{
					Reconnect:            true,
					MaxReconnectAttempts: 10,
					InitialBackoff:       10 * time.Millisecond,
					MaxBackoff:           50 * time.Millisecond,
					DialTimeout:          1 * time.Second,
				})
				if err == nil {
					mu.Lock()
					clients = append(clients, cli)
					mu.Unlock()
					_ = cli.Subscribe("AAPL")
				}
			}()
		}
		wg.Wait()

		b.StartTimer()

		// 3. Drop all connections
		engine.DisconnectClients()

		// 4. Wait for all to receive replay event from reconnect
		var recvWg sync.WaitGroup
		recvWg.Add(len(clients))
		for _, cli := range clients {
			go func(c *client.Client) {
				defer recvWg.Done()
				for ev := range c.Receive() {
					if q, ok := ev.(*marketdata.Quote); ok && q.Seq == 10 {
						break
					}
				}
			}(cli)
		}
		recvWg.Wait()

		b.StopTimer()

		// Cleanup
		for _, cli := range clients {
			cli.Close()
		}
		gateway.EvictAll(1000, "reset")
	}
}

// BenchmarkReplayDuringReconnect measures performance of replaying messages while clients are reconnecting.
func BenchmarkReplayDuringReconnect(b *testing.B) {
	_, engine, server, _, walLog, alloc := setupTestEnvironment(b)
	defer server.Close()
	defer walLog.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cli, err := client.Connect(wsURL, client.Options{Reconnect: true, MaxReconnectAttempts: 5, InitialBackoff: 10 * time.Millisecond})
		if err != nil {
			b.Fatal(err)
		}
		_ = cli.Subscribe("AAPL")

		// Push 5k events
		for j := 1; j <= 5000; j++ {
			ev := marketdata.Quote{Symbol: "AAPL", Type: "quote", Bid: 1, Seq: int64(j)}
			payload, _ := json.Marshal(ev)
			walLog.Append(&wal.Message{Sequence: uint64(j), Timestamp: time.Now().UnixNano(), Topic: "AAPL", Type: "quote", Payload: payload})
			alloc.Set(uint64(j))
		}
		walLog.Sync()

		engine.DisconnectClients()

		b.StartTimer()

		// Measure time to replay 5000 messages upon reconnect
		var received atomic.Int64
		for ev := range cli.Receive() {
			if _, ok := ev.(*marketdata.Quote); ok {
				if received.Add(1) == 5000 {
					break
				}
			}
		}

		b.StopTimer()
		cli.Close()
	}
}
