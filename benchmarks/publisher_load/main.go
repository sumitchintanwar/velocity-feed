package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sumit/rtmds/benchmarks/reporter"
	"golang.org/x/time/rate"
)

var (
	redisAddr  = flag.String("redis", "localhost:6379", "Redis address")
	rateLimit  = flag.Int("rate", 10000, "Messages per second")
	payloadSz  = flag.Int("size", 100, "Payload size in bytes")
	duration   = flag.Duration("duration", 10*time.Second, "Test duration")
	targetURL  = flag.String("target", "http://localhost:8080/metrics", "Target metrics URL for profiling")
	topicCount = flag.Int("topics", 10, "Number of distinct topics")
)

func main() {
	flag.Parse()
	log.Printf("Starting Publisher Benchmark (Rate: %d msgs/s, Size: %d, Duration: %s)", *rateLimit, *payloadSz, *duration)

	client := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		PoolSize: 100,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis ping failed: %v", err)
	}

	// Start metric scraper
	var wg sync.WaitGroup
	var res *reporter.Result
	wg.Add(1)
	go func() {
		defer wg.Done()
		res = reporter.PollMetrics(ctx, *targetURL, 1*time.Second)
	}()

	start := time.Now()
	limiter := rate.NewLimiter(rate.Limit(*rateLimit), *rateLimit)

	var sent, errors atomic.Uint64
	var latencySum atomic.Int64
	latencies := make([]time.Duration, 0, *rateLimit*int(duration.Seconds()))
	var latMu sync.Mutex

	dummyPayload := strings.Repeat("A", *payloadSz)

	// Run publisher workers
	numWorkers := 10
	workerWg := sync.WaitGroup{}
	
	// Create channels
	topics := make([]string, *topicCount)
	for i := 0; i < *topicCount; i++ {
		topics[i] = fmt.Sprintf("market:SYM%d", i)
	}

	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			for {
				if time.Since(start) > *duration {
					return
				}
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				
				topic := topics[workerID%len(topics)]
				
				// Generate mock payload that satisfies the size
				dummyTime := time.Now().UnixNano() / int64(time.Millisecond)
				b := []byte(fmt.Sprintf(`{"symbol":"%s","time":%d,"data":"%s"}`, topic, dummyTime, dummyPayload))
				
				pubStart := time.Now()
				err := client.Publish(ctx, topic, b).Err()
				lat := time.Since(pubStart)
				
				if err != nil {
					errors.Add(1)
				} else {
					sent.Add(1)
					latencySum.Add(int64(lat))
					latMu.Lock()
					latencies = append(latencies, lat)
					latMu.Unlock()
				}
			}
		}(i)
	}

	workerWg.Wait()
	cancel() // stop scraper
	wg.Wait()

	totalTime := time.Since(start)
	
	res.Name = "Publisher Load Benchmark"
	res.Duration = totalTime
	res.MessagesSent = sent.Load()
	res.Dropped = errors.Load()
	res.Throughput = float64(res.MessagesSent) / totalTime.Seconds()
	res.NetworkBand_MB = (res.Throughput * float64(*payloadSz)) / 1024.0 / 1024.0
	
	res.CalculatePercentiles(latencies)

	if err := res.WriteCSV("publisher_benchmark.csv"); err != nil {
		log.Printf("Failed to write CSV: %v", err)
	}
	if err := res.WriteJSON("publisher_benchmark.json"); err != nil {
		log.Printf("Failed to write JSON: %v", err)
	}
	if err := res.WriteMarkdown("publisher_benchmark.md"); err != nil {
		log.Printf("Failed to write Markdown: %v", err)
	}
	
	log.Printf("Benchmark complete. Sent: %d, Throughput: %.2f msg/s", res.MessagesSent, res.Throughput)
}
