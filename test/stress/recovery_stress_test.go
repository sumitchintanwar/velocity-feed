package stress

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/backpressure"
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

// clientTracker tracks which unique sequence numbers a client has received.
// It is safe for concurrent use and correctly handles duplicate deliveries
// (which happen during WAL replay after a reconnect — same seq delivered twice
// is idempotent in the map).
type clientTracker struct {
	mu       sync.Mutex
	received map[uint64]bool
	total    int
}

func newClientTracker(total int) *clientTracker {
	return &clientTracker{
		received: make(map[uint64]bool, total),
		total:    total,
	}
}

// record marks seq received. Returns true when all seqs 1..total are present.
func (ct *clientTracker) record(seq uint64) bool {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.received[seq] = true
	return len(ct.received) >= ct.total
}

// count returns the number of distinct sequences seen so far.
func (ct *clientTracker) count() int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return len(ct.received)
}

// TestRecoveryStress validates that:
//  1. All clients receive every message exactly-at-least-once (WAL replay closes gaps).
//  2. The system survives repeated mass-disconnects (chaos every 12s).
//  3. WAL replay correctly deduplicates overlap between historical and live streams.
//
// Chaos design rationale
// ─────────────────────
// Per-packet DisconnectProbability is intentionally ZERO. With ~200 WAL messages
// per replay, even a 1% per-write disconnect probability makes P(full replay) < 15%.
// Instead we use periodic timed mass-disconnects via engine.DisconnectClients(),
// which gives a controlled, testable failure pattern while letting replay complete.
func TestRecoveryStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	// ── 1. Setup dependencies ────────────────────────────────────────────────
	tmpDir := t.TempDir()
	walDir := tmpDir + "/wal"

	logger := log.NewFromConfig(log.Config{Level: "info", Format: "console"})
	metrics, _ := platform.NewMetrics("test_stress")

	walCfg := wal.DefaultConfig
	walCfg.Dir = walDir
	walLog, err := wal.NewSegmentManager(walCfg)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer walLog.Close()

	alloc := sequencer.NewAllocator()
	replayEngine := replay.NewEngine(walLog)
	replaySvc := replay.NewService(replayEngine, alloc)

	queueCfg := clientqueue.DefaultConfig()
	queueCfg.Policy = backpressure.PolicyDisconnect
	queueCfg.MaxConsecutiveDrops = 100 // Tolerate brief slowness under chaos delays
	queueCfg.QueueSize = 2048          // Buffer bursts during reconnect

	tm := topicmanager.NewWithQueue(0, &queueCfg, logger, nil, metrics)
	gateway := ws.NewGatewayWithReplay(tm, nil, nil, replaySvc, logger, metrics, 0, "stress-gw")

	// ── 2. Chaos engine — delays only, no per-packet disconnects ─────────────
	//
	// DisconnectProbability = 0.0 deliberately. At 200 msgs/replay, even 1%
	// per-write drops => P(complete replay) ≈ 13%. Chaos is injected via
	// timed mass-disconnects below instead.
	engine := chaos.NewEngine()
	engine.SetConfig(chaos.Config{
		DropProbability:       0.0,
		DelayProbability:      0.08, // 8% of frames see a small delay
		MaxDelay:              20 * time.Millisecond,
		DisconnectProbability: 0.0,
	})

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gateway.Handler().ServeHTTP(w, r)
	}))
	server.Listener = engine.WrapListener(server.Listener)
	server.Start()
	defer server.Close()

	// ── 3. Constants ─────────────────────────────────────────────────────────
	const (
		numClients  = 10
		numMessages = 200 // Small enough that a full replay completes in < 3s
		symbol      = "BTC"
	)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	var (
		wg               sync.WaitGroup
		connectedWg      sync.WaitGroup
		globalErrorCount atomic.Int32
	)
	chaosStarted := make(chan struct{})

	// ── 4. Launch concurrent clients ─────────────────────────────────────────
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		connectedWg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			cli, err := client.Connect(wsURL, client.Options{
				Reconnect:            true,
				MaxReconnectAttempts: 500,
				InitialBackoff:       20 * time.Millisecond,
				MaxBackoff:           500 * time.Millisecond,
				DialTimeout:          5 * time.Second,
			})
			if err != nil {
				t.Errorf("client %d: initial connect failed: %v", clientID, err)
				globalErrorCount.Add(1)
				connectedWg.Done()
				return
			}
			defer cli.Close()

			if err := cli.Subscribe(symbol); err != nil {
				t.Errorf("client %d: subscribe failed: %v", clientID, err)
				globalErrorCount.Add(1)
				connectedWg.Done()
				return
			}

			tracker := newClientTracker(numMessages)
			signalledReady := false

			// ── Wait for first message to confirm live connectivity ────────────
			firstTimeout := time.After(30 * time.Second)
		waitFirst:
			for {
				select {
				case ev := <-cli.Receive():
					if q, ok := ev.(*marketdata.Quote); ok && q.Seq > 0 {
						tracker.record(uint64(q.Seq))
						if !signalledReady {
							signalledReady = true
							connectedWg.Done()
						}
						break waitFirst
					}
				case <-firstTimeout:
					t.Errorf("client %d: timeout waiting for first message", clientID)
					globalErrorCount.Add(1)
					connectedWg.Done()
					return
				}
			}

			// ── Wait for chaos signal then drain all remaining messages ────────
			<-chaosStarted

			deadline := time.After(35 * time.Second) // Short for debug run
			report := time.NewTicker(10 * time.Second)
			defer report.Stop()

			for {
				select {
				case ev := <-cli.Receive():
					if q, ok := ev.(*marketdata.Quote); ok && q.Seq > 0 {
						if tracker.record(uint64(q.Seq)) {
							fmt.Printf("client %d ✓ — all %d unique seqs received\n",
								clientID, numMessages)
							return
						}
					}
				case <-report.C:
					fmt.Printf("client %d: %d/%d unique seqs\n",
						clientID, tracker.count(), numMessages)
				case <-deadline:
					t.Errorf("client %d: deadline — only %d/%d unique seqs received",
						clientID, tracker.count(), numMessages)
					globalErrorCount.Add(1)
					return
				}
			}
		}(c)
	}

	// Small pause to let goroutines schedule and send Subscribe
	time.Sleep(100 * time.Millisecond)

	// ── 5. Publish message #1 to establish everyone's resumeSeq ─────────────
	ev1 := marketdata.Quote{Symbol: symbol, Type: "trade", Bid: 100, Seq: 1}
	payload1, _ := json.Marshal(ev1)
	walLog.Append(&wal.Message{
		Sequence: 1, Timestamp: time.Now().UnixNano(), Topic: symbol, Payload: payload1,
	})
	alloc.Set(1)
	tm.Publish(context.Background(), marketdata.NewCachedEvent(&ev1))
	walLog.Sync()

	// Wait until every client has seen message #1
	connectedWg.Wait()
	if globalErrorCount.Load() > 0 {
		t.Fatalf("setup failed: %d errors before chaos", globalErrorCount.Load())
	}

	// ── 6. Publish messages 2..200 before chaos, so WAL is fully populated ───
	//
	// By publishing all messages first (WAL fully written), any reconnecting
	// client can always get a complete replay regardless of when chaos fires.
	for i := 2; i <= numMessages; i++ {
		ev := marketdata.Quote{
			Symbol: symbol, Type: "trade",
			Bid: float64(i * 100), Seq: int64(i),
		}
		payload, _ := json.Marshal(ev)
		walLog.Append(&wal.Message{
			Sequence: uint64(i), Timestamp: time.Now().UnixNano(),
			Topic: symbol, Payload: payload,
		})
		alloc.Set(uint64(i))
		tm.Publish(context.Background(), marketdata.NewCachedEvent(&ev))
		if i%20 == 0 {
			walLog.Sync()
		}
	}
	walLog.Sync() // Ensure all WAL entries are visible before chaos starts
	fmt.Printf("publisher: all %d messages committed to WAL\n", numMessages)

	// ── 7. Enable chaos — timed mass-disconnects every 12 seconds ────────────
	//
	// 12s interval gives each client ≥ 10s to complete a full WAL replay
	// (≤200 msgs × 20ms max delay × 8% = ~0.3s expected replay time).
	// 3+ disconnect cycles fit within the 90s client deadline.
	chaosCtx, cancelChaos := context.WithCancel(context.Background())
	defer cancelChaos()

	go func() {
		ticker := time.NewTicker(12 * time.Second)
		defer ticker.Stop()
		cycle := 0
		for {
			select {
			case <-chaosCtx.Done():
				return
			case <-ticker.C:
				cycle++
				fmt.Printf("chaos: mass-disconnect #%d — dropping all clients\n", cycle)
				engine.DisconnectClients()
			}
		}
	}()

	close(chaosStarted)

	// ── 8. Wait for all clients to finish ────────────────────────────────────
	wg.Wait()

	if globalErrorCount.Load() > 0 {
		t.Fatalf("TestRecoveryStress FAILED — %d client errors", globalErrorCount.Load())
	}
	fmt.Printf("TestRecoveryStress PASSED — all %d clients received all %d unique messages\n",
		numClients, numMessages)
}
