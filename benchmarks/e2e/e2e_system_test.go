package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/sumit/rtmds/internal/app"
	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/internal/distribution/redisbus"
	applog "github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/pkg/marketdata"
)

type LatencyMeasurement struct {
	P50 float64
	P95 float64
	P99 float64
	Max float64
}

// TestEndToEndSystem measures Publisher -> Redis -> Gateway -> WebSocket Clients latency.
func TestEndToEndSystem(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E benchmark in short mode")
	}

	// 1. Boot miniredis (Pure Go Redis server)
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	// 2. Configure Gateway App
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	cfg.Redis.Enabled = true
	cfg.Redis.Addr = mr.Addr()
	cfg.Server.Port = 9091
	cfg.Feed.Enabled = false // Gateway shouldn't generate events

	// 3. Start Gateway Server
	gatewayApp, err := app.NewGatewayApp(cfg)
	if err != nil {
		t.Fatalf("Failed to build gateway app: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gwWg sync.WaitGroup
	gwWg.Add(1)
	go func() {
		defer gwWg.Done()
		_ = gatewayApp.Run(ctx)
	}()

	// Wait for server to boot
	time.Sleep(1 * time.Second)

	// 4. Spawn 1000 WebSocket Clients
	const numClients = 1000
	var receivedCount atomic.Int64
	latencies := make([]time.Duration, 0, 100000)
	var mu sync.Mutex

	dialer := websocket.DefaultDialer
	url := fmt.Sprintf("ws://localhost:%d/ws", cfg.Server.Port)

	var connectedCount atomic.Int32
	for i := 0; i < numClients; i++ {
		go func(id int) {
			conn, _, err := dialer.Dial(url, nil)
			if err != nil {
				return // Some may fail if OS limits hit, ignore for E2E
			}
			defer conn.Close()
			connectedCount.Add(1)

			// Subscribe to AAPL
			subMsg := map[string]interface{}{
				"action":  "subscribe",
				"symbols": []string{"AAPL"},
			}
			_ = conn.WriteJSON(subMsg)

			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					return
				}

				// Extract receive time
				recvTime := time.Now()

				// Parse E2E timestamp embedded in the payload
				var raw map[string]interface{}
				if err := json.Unmarshal(msg, &raw); err == nil {
					if p, ok := raw["payload"].(map[string]interface{}); ok {
						if tsStr, ok := p["timestamp"].(string); ok {
							if ts, err := time.Parse(time.RFC3339Nano, tsStr); err == nil {
								latency := recvTime.Sub(ts)
								mu.Lock()
								latencies = append(latencies, latency)
								mu.Unlock()
								receivedCount.Add(1)
							}
						}
					}
				}
			}
		}(i)
	}

	// Wait for clients to connect
	time.Sleep(2 * time.Second)
	log.Printf("Successfully connected %d/%d clients", connectedCount.Load(), numClients)

	// 5. Blast Events directly to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	pub := redisbus.NewPublisher(rdb, applog.NewFromLegacy("info", "json", "e2e-test"))
	defer pub.Close()

	log.Printf("Starting E2E benchmark run for 5 seconds...")
	start := time.Now()

	publishCount := 0
	for time.Since(start) < 5*time.Second {
		quote := marketdata.Quote{
			Symbol:    "AAPL",
			Type:      marketdata.QuoteTypeTrade,
			Price:     150.0 + float64(publishCount%10),
			Timestamp: time.Now(), // E2E TIMESTAMP
		}

		ev := marketdata.NewCachedEvent(quote)
		pub.Publish(context.Background(), ev)

		publishCount++
		time.Sleep(50 * time.Microsecond) // Prevent overwhelming miniredis
	}

	log.Printf("Published %d events to Redis", publishCount)

	// Allow time for final flights
	time.Sleep(1 * time.Second)

	// Cancel gateway
	cancel()
	gwWg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(latencies) == 0 {
		t.Fatal("No events received by clients")
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	totalReceived := receivedCount.Load()
	p50 := latencies[len(latencies)*50/100].Seconds() * 1000
	p95 := latencies[len(latencies)*95/100].Seconds() * 1000
	p99 := latencies[len(latencies)*99/100].Seconds() * 1000
	maxLat := latencies[len(latencies)-1].Seconds() * 1000

	fmt.Printf("\n=== E2E System Benchmark Results ===\n")
	fmt.Printf("Total Clients:           %d\n", numClients)
	fmt.Printf("Events Published:        %d\n", publishCount)
	fmt.Printf("Total Fan-out Received:  %d\n", totalReceived)
	fmt.Printf("E2E Throughput:          %.2f msgs/sec\n", float64(totalReceived)/5.0)
	fmt.Printf("P50 Latency (E2E):       %.2f ms\n", p50)
	fmt.Printf("P95 Latency (E2E):       %.2f ms\n", p95)
	fmt.Printf("P99 Latency (E2E):       %.2f ms\n", p99)
	fmt.Printf("Max Latency (E2E):       %.2f ms\n", maxLat)
	fmt.Printf("====================================\n\n")
}
