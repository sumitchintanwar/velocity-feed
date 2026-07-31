package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

var (
	redisAddr = flag.String("redis", "localhost:6379", "Redis address")
	rateLimit = flag.Int("rate", 1000, "Messages per second")
	payloadSz = flag.Int("size", 100, "Payload size in bytes")
	duration  = flag.Duration("duration", 10*time.Second, "Test duration")
)

type Metrics struct {
	PublishLatency   []time.Duration
	SubscribeLatency []time.Duration
	Published        uint64
	Received         uint64
	Errors           uint64
	CPUCores         float64 // Not accurately measured without prometheus, placeholder
	MemoryMB         float64
}

func main() {
	flag.Parse()
	log.Printf("Starting Redis Benchmark (Rate: %d msgs/s, Size: %d, Duration: %s)", *rateLimit, *payloadSz, *duration)

	client := redis.NewClient(&redis.Options{
		Addr:         *redisAddr,
		PoolSize:     1000,
		MinIdleConns: 100,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis ping failed: %v", err)
	}

	metrics := &Metrics{}
	var mu sync.Mutex

	// Start Subscriber
	subWg := sync.WaitGroup{}
	subWg.Add(1)
	
	pubsub := client.Subscribe(ctx, "benchmark_topic")
	_, err := pubsub.Receive(ctx)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	
	ch := pubsub.Channel()
	go func() {
		defer subWg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				parts := strings.SplitN(msg.Payload, "|", 2)
				if len(parts) == 2 {
					ts, _ := strconv.ParseInt(parts[0], 10, 64)
					lat := time.Since(time.Unix(0, ts))
					mu.Lock()
					metrics.SubscribeLatency = append(metrics.SubscribeLatency, lat)
					mu.Unlock()
					atomic.AddUint64(&metrics.Received, 1)
				}
			}
		}
	}()

	start := time.Now()
	
	// Publisher workers
	numWorkers := 50
	if *rateLimit > 50000 {
		numWorkers = 200 // Need more concurrency for high throughput
	}
	
	workerWg := sync.WaitGroup{}
	limiter := rate.NewLimiter(rate.Limit(*rateLimit), *rateLimit)
	dummyPayload := strings.Repeat("A", *payloadSz)

	batchSize := 1
	if *rateLimit >= 50000 {
		batchSize = 100 // Use pipelining for high throughput
	}
	if *rateLimit >= 250000 {
		batchSize = 500
	}
	
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			
			for {
				if time.Since(start) > *duration {
					return
				}
				
				if batchSize > 1 {
					// Pipelined approach
					pipe := client.Pipeline()
					startPub := time.Now()
					for b := 0; b < batchSize; b++ {
						if err := limiter.Wait(ctx); err != nil {
							return
						}
						payload := fmt.Sprintf("%d|%s", time.Now().UnixNano(), dummyPayload)
						pipe.Publish(ctx, "benchmark_topic", payload)
					}
					_, err := pipe.Exec(ctx)
					lat := time.Since(startPub) / time.Duration(batchSize)
					
					if err != nil {
						atomic.AddUint64(&metrics.Errors, uint64(batchSize))
					} else {
						atomic.AddUint64(&metrics.Published, uint64(batchSize))
						mu.Lock()
						for b := 0; b < batchSize; b++ {
							metrics.PublishLatency = append(metrics.PublishLatency, lat)
						}
						mu.Unlock()
					}
				} else {
					// Single publish
					if err := limiter.Wait(ctx); err != nil {
						return
					}
					payload := fmt.Sprintf("%d|%s", time.Now().UnixNano(), dummyPayload)
					startPub := time.Now()
					err := client.Publish(ctx, "benchmark_topic", payload).Err()
					lat := time.Since(startPub)
					
					if err != nil {
						atomic.AddUint64(&metrics.Errors, 1)
					} else {
						atomic.AddUint64(&metrics.Published, 1)
						mu.Lock()
						metrics.PublishLatency = append(metrics.PublishLatency, lat)
						mu.Unlock()
					}
				}
			}
		}()
	}

	workerWg.Wait()
	time.Sleep(1 * time.Second) // wait for trailing subscriber messages
	cancel()
	subWg.Wait()
	
	totalTime := time.Since(start) - 1*time.Second // subtract trail sleep
	
	exportData := map[string]interface{}{
		"target_rate": *rateLimit,
		"duration_s": totalTime.Seconds(),
		"published": metrics.Published,
		"received": metrics.Received,
		"errors": metrics.Errors,
		"pub_latency_ms": toMS(metrics.PublishLatency),
		"sub_latency_ms": toMS(metrics.SubscribeLatency),
		"throughput_msg_sec": float64(metrics.Published) / totalTime.Seconds(),
		"bandwidth_mb_sec": (float64(metrics.Published) * float64(*payloadSz)) / totalTime.Seconds() / 1024.0 / 1024.0,
	}
	
	outFile := fmt.Sprintf("redis_results_%d.json", *rateLimit)
	b, _ := json.Marshal(exportData)
	os.WriteFile(outFile, b, 0644)
	
	log.Printf("Benchmark complete. Throughput: %.2f msg/s", exportData["throughput_msg_sec"])
}

func toMS(durs []time.Duration) []float64 {
	ms := make([]float64, len(durs))
	for i, d := range durs {
		ms[i] = float64(d.Microseconds()) / 1000.0
	}
	return ms
}
