package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumit/rtmds/benchmarks/reporter"
)

var (
	wsURL       = flag.String("url", "ws://localhost:8080/ws", "WebSocket URL")
	clientCount = flag.Int("clients", 100, "Number of concurrent clients")
	duration    = flag.Duration("duration", 10*time.Second, "Test duration")
	targetURL   = flag.String("target", "http://localhost:8080/metrics", "Target metrics URL for profiling")
)

func main() {
	flag.Parse()
	log.Printf("Starting Client Benchmark (Clients: %d, Duration: %s)", *clientCount, *duration)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var res *reporter.Result

	// Start metric scraper
	wg.Add(1)
	go func() {
		defer wg.Done()
		res = reporter.PollMetrics(ctx, *targetURL, 1*time.Second)
	}()

	start := time.Now()
	var recv atomic.Uint64
	var errors atomic.Uint64
	latencies := make(chan time.Duration, *clientCount*1000)
	
	// Create all connections
	clientWg := sync.WaitGroup{}
	
	// Connect clients in batches to avoid overwhelming the server handshakes
	batchSize := 100
	for i := 0; i < *clientCount; i++ {
		if i > 0 && i%batchSize == 0 {
			time.Sleep(100 * time.Millisecond) // Throttled connect
		}
		
		clientWg.Add(1)
		go func(id int) {
			defer clientWg.Done()
			
			dialer := websocket.Dialer{
				HandshakeTimeout: 5 * time.Second,
			}
			
			conn, _, err := dialer.Dial(*wsURL, nil)
			if err != nil {
				errors.Add(1)
				log.Printf("Client %d dial failed: %v", id, err)
				return
			}
			defer conn.Close()

			// Subscribe to a generic test topic
			subMsg := map[string]interface{}{
				"type": "subscribe",
				"topics": []string{"SYM1"},
			}
			if err := conn.WriteJSON(subMsg); err != nil {
				errors.Add(1)
				return
			}

			// Read loop
			for {
				if time.Since(start) > *duration {
					return
				}
				
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, msg, err := conn.ReadMessage()
				if err != nil {
					errors.Add(1)
					return
				}
				
				// Parse event to calculate latency
				var ev struct {
					Time int64 `json:"time"`
				}
				if err := json.Unmarshal(msg, &ev); err == nil && ev.Time > 0 {
					lat := time.Since(time.Unix(0, ev.Time*int64(time.Millisecond)))
					latencies <- lat
					recv.Add(1)
				} else {
					// Fallback for control messages
					recv.Add(1)
				}
			}
		}(i)
	}

	// Wait for benchmark duration
	time.Sleep(*duration)
	cancel() // Stop all clients by cancelling ctx but actually clients use time.Since(start).
	clientWg.Wait() // Wait for all reads to timeout or return
	wg.Wait()       // Wait for scraper

	close(latencies)
	
	var allLatencies []time.Duration
	for lat := range latencies {
		allLatencies = append(allLatencies, lat)
	}

	res.Name = "Client Load Benchmark"
	res.Duration = time.Since(start)
	res.Clients = *clientCount
	res.MessagesRecv = recv.Load()
	res.Dropped = errors.Load()
	res.Throughput = float64(res.MessagesRecv) / res.Duration.Seconds()
	
	res.CalculatePercentiles(allLatencies)

	if err := res.WriteCSV(fmt.Sprintf("client_benchmark_%d.csv", *clientCount)); err != nil {
		log.Printf("Failed to write CSV: %v", err)
	}
	if err := res.WriteJSON(fmt.Sprintf("client_benchmark_%d.json", *clientCount)); err != nil {
		log.Printf("Failed to write JSON: %v", err)
	}
	if err := res.WriteMarkdown(fmt.Sprintf("client_benchmark_%d.md", *clientCount)); err != nil {
		log.Printf("Failed to write Markdown: %v", err)
	}
	
	log.Printf("Benchmark complete. Received: %d, Throughput: %.2f msg/s", res.MessagesRecv, res.Throughput)
}
