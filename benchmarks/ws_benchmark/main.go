package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	wsURL       = flag.String("url", "ws://localhost:8080/ws", "WebSocket URL")
	clientCount = flag.Int("clients", 100, "Number of concurrent clients")
	duration    = flag.Duration("duration", 10*time.Second, "Test duration")
	chaos       = flag.Float64("chaos", 0.05, "Probability of a client dropping connection per second to test reconnects")
)

type Metrics struct {
	ConnectTimes   []time.Duration
	ReconnectTimes []time.Duration
	ReceiveLatency []time.Duration
	DroppedMsg     uint64
	Errors         uint64
	Received       uint64
}

func main() {
	flag.Parse()
	log.Printf("Starting Dedicated WS Benchmark (Clients: %d, Duration: %s, Chaos: %.2f)", *clientCount, *duration, *chaos)

	metrics := &Metrics{}
	var mu sync.Mutex

	start := time.Now()
	var clientWg sync.WaitGroup

	for i := 0; i < *clientCount; i++ {
		clientWg.Add(1)
		
		// Batch connections to prevent flooding
		if i > 0 && i%100 == 0 {
			time.Sleep(50 * time.Millisecond)
		}

		go func(id int) {
			defer clientWg.Done()
			
			dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
			
			connectStart := time.Now()
			conn, _, err := dialer.Dial(*wsURL, nil)
			if err != nil {
				atomic.AddUint64(&metrics.Errors, 1)
				log.Printf("Client %d dial failed: %v", id, err)
				return
			}
			
			mu.Lock()
			metrics.ConnectTimes = append(metrics.ConnectTimes, time.Since(connectStart))
			mu.Unlock()
			
			subMsg := map[string]interface{}{
				"type": "subscribe",
				"topics": []string{"SYM1"},
			}
			conn.WriteJSON(subMsg)
			
			connected := true
			
			for {
				if time.Since(start) > *duration {
					conn.Close()
					return
				}
				
				if connected && rand.Float64() < (*chaos / 10.0) { // simulate drop occasionally
					conn.Close()
					connected = false
				}
				
				if !connected {
					recStart := time.Now()
					conn, _, err = dialer.Dial(*wsURL, nil)
					if err != nil {
						atomic.AddUint64(&metrics.Errors, 1)
						time.Sleep(1 * time.Second)
						continue
					}
					mu.Lock()
					metrics.ReconnectTimes = append(metrics.ReconnectTimes, time.Since(recStart))
					mu.Unlock()
					conn.WriteJSON(subMsg)
					connected = true
				}
				
				conn.SetReadDeadline(time.Now().Add(1 * time.Second))
				_, msg, err := conn.ReadMessage()
				if err != nil {
					if connected {
						atomic.AddUint64(&metrics.Errors, 1)
					}
					continue
				}
				
				var ev struct {
					Time int64 `json:"time"`
				}
				if err := json.Unmarshal(msg, &ev); err == nil && ev.Time > 0 {
					lat := time.Since(time.Unix(0, ev.Time*int64(time.Millisecond)))
					mu.Lock()
					metrics.ReceiveLatency = append(metrics.ReceiveLatency, lat)
					mu.Unlock()
					atomic.AddUint64(&metrics.Received, 1)
				}
			}
		}(i)
	}

	clientWg.Wait()
	
	// Export to JSON for Python plotting
	exportData := map[string]interface{}{
		"clients": *clientCount,
		"connect_times_ms": toMS(metrics.ConnectTimes),
		"reconnect_times_ms": toMS(metrics.ReconnectTimes),
		"receive_latency_ms": toMS(metrics.ReceiveLatency),
		"dropped": metrics.DroppedMsg,
		"errors": metrics.Errors,
		"received": metrics.Received,
	}
	
	outFile := fmt.Sprintf("ws_results_%d.json", *clientCount)
	b, _ := json.Marshal(exportData)
	os.WriteFile(outFile, b, 0644)
	log.Printf("Results written to %s", outFile)
}

func toMS(durs []time.Duration) []float64 {
	ms := make([]float64, len(durs))
	for i, d := range durs {
		ms[i] = float64(d.Microseconds()) / 1000.0
	}
	return ms
}
