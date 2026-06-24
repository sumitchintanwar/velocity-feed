// Package main implements a distributed benchmark client for RTMDS.
// It measures end-to-end latency, throughput, and collects system metrics.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// BenchmarkConfig holds benchmark parameters.
type BenchmarkConfig struct {
	WebSocketURL   string
	NumClients     int
	NumSymbols     int
	Duration       time.Duration
	RampUp         time.Duration
	ReportInterval time.Duration
	OutputFile     string
}

// LatencyHistogram tracks latency distribution.
type LatencyHistogram struct {
	Buckets []float64
	Counts  []int
	Total   int
	Sum     float64
	Min     float64
	Max     float64
	mu      sync.Mutex
}

// NewLatencyHistogram creates a histogram with predefined buckets.
func NewLatencyHistogram() *LatencyHistogram {
	return &LatencyHistogram{
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000},
		Counts:  make([]int, 14),
		Min:     math.MaxFloat64,
	}
}

// Record adds a latency sample in milliseconds.
func (h *LatencyHistogram) Record(ms float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.Total++
	h.Sum += ms
	if ms < h.Min {
		h.Min = ms
	}
	if ms > h.Max {
		h.Max = ms
	}

	for i, bucket := range h.Buckets {
		if ms <= bucket {
			h.Counts[i]++
			return
		}
	}
	h.Counts[len(h.Counts)-1]++ // overflow bucket
}

// Percentile returns the p-th percentile latency.
func (h *LatencyHistogram) Percentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.Total == 0 {
		return 0
	}
	target := int(math.Ceil(float64(h.Total) * p / 100.0))
	cumulative := 0
	for i, count := range h.Counts {
		cumulative += count
		if cumulative >= target {
			if i < len(h.Buckets) {
				return h.Buckets[i]
			}
			return h.Buckets[len(h.Buckets)-1] * 2
		}
	}
	return h.Max
}

// Mean returns the average latency.
func (h *LatencyHistogram) Mean() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.Total == 0 {
		return 0
	}
	return h.Sum / float64(h.Total)
}

// Print outputs the histogram to stdout.
func (h *LatencyHistogram) Print() {
	h.mu.Lock()
	defer h.mu.Unlock()

	fmt.Println("  Latency Distribution:")
	for i, bucket := range h.Buckets {
		pct := float64(h.Counts[i]) / float64(h.Total) * 100
		bar := ""
		for j := 0; j < int(pct/2); j++ {
			bar += "#"
		}
		label := fmt.Sprintf("%.1fms", bucket)
		if i == len(h.Buckets)-1 {
			label = fmt.Sprintf(">%0.1fms", h.Buckets[i-1])
		}
		fmt.Printf("    %10s  %6d (%5.1f%%) %s\n", label, h.Counts[i], pct, bar)
	}
}

// ClientStats holds per-client statistics.
type ClientStats struct {
	MessagesReceived int64
	BytesReceived    int64
	Connected        bool
	StartTime        time.Time
	EndTime          time.Time
}

// BenchmarkResult holds the final benchmark results.
type BenchmarkResult struct {
	Timestamp        string            `json:"timestamp"`
	Config           BenchmarkConfig   `json:"config"`
	TotalMessages    int64             `json:"total_messages"`
	TotalBytes       int64             `json:"total_bytes"`
	Duration         string            `json:"duration"`
	MessagesPerSec   float64           `json:"messages_per_sec"`
	BytesPerSec      float64           `json:"bytes_per_sec"`
	ConnectedClients int               `json:"connected_clients"`
	FailedClients    int               `json:"failed_clients"`
	Latency          LatencyStats      `json:"latency"`
	Histogram        []HistogramBucket `json:"histogram"`
}

// LatencyStats holds latency statistics.
type LatencyStats struct {
	MeanMs float64 `json:"mean_ms"`
	MinMs  float64 `json:"min_ms"`
	MaxMs  float64 `json:"max_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	P999Ms float64 `json:"p999_ms"`
}

// HistogramBucket holds one bucket of the latency histogram.
type HistogramBucket struct {
	UpperBound string  `json:"upper_bound"`
	Count      int     `json:"count"`
	Percent    float64 `json:"percent"`
}

var (
	totalMessages atomic.Int64
	totalBytes    atomic.Int64
	connected     atomic.Int32
	failed        atomic.Int32
	latencyHist   = NewLatencyHistogram()
)

func main() {
	config := parseFlags()

	ctx, cancel := context.WithTimeout(context.Background(), config.Duration+config.RampUp+10*time.Second)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║         RTMDS Distributed Benchmark Client              ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Printf("\nConfig: %d clients, %d symbols, %v duration\n",
		config.NumClients, config.NumSymbols, config.Duration)
	fmt.Printf("Target: %s\n\n", config.WebSocketURL)

	// Wait for system to be ready
	fmt.Print("Waiting for system to be ready...")
	for i := 0; i < 30; i++ {
		time.Sleep(time.Second)
		if checkHealth(config.WebSocketURL) {
			fmt.Println(" Ready!")
			break
		}
		if i == 29 {
			fmt.Println(" Timeout!")
			os.Exit(1)
		}
	}

	// Start metrics collector
	metricsCtx, metricsCancel := context.WithCancel(ctx)
	go collectMetrics(metricsCtx, config.ReportInterval)

	// Ramp up clients
	fmt.Printf("Ramping up %d clients...\n", config.NumClients)
	clients := make([]*websocket.Conn, 0, config.NumClients)
	var clientsMu sync.Mutex

	rampUpInterval := config.RampUp / time.Duration(config.NumClients)
	if rampUpInterval < 10*time.Millisecond {
		rampUpInterval = 10 * time.Millisecond
	}

	interrupted := false
	for i := 0; i < config.NumClients && !interrupted; i++ {
		select {
		case <-sigCh:
			fmt.Println("\nInterrupted during ramp-up")
			interrupted = true
		case <-ctx.Done():
			interrupted = true
		default:
		}

		if interrupted {
			break
		}

		// Retry connection with backoff
		var conn *websocket.Conn
		for retries := 0; retries < 3; retries++ {
			var err error
			conn, err = connectAndSubscribe(config.WebSocketURL, config.NumSymbols)
			if err == nil {
				break
			}
			log.Printf("Client %d connect attempt %d failed: %v", i, retries+1, err)
			time.Sleep(time.Duration(retries+1) * 100 * time.Millisecond)
		}

		if conn == nil {
			failed.Add(1)
			log.Printf("Client %d failed after 3 retries", i)
			continue
		}

		clientsMu.Lock()
		clients = append(clients, conn)
		clientsMu.Unlock()
		connected.Add(1)

		// Start receiver goroutine with reconnection
		go receiveLoopWithReconnect(ctx, conn, i, config)

		time.Sleep(rampUpInterval)

		if (i+1)%100 == 0 {
			fmt.Printf("  Connected: %d/%d\n", i+1, config.NumClients)
		}
	}

	fmt.Printf("\nConnected: %d, Failed: %d\n", connected.Load(), failed.Load())

	if !interrupted && connected.Load() > 0 {
		fmt.Printf("Running benchmark for %v...\n\n", config.Duration)

		benchStart := time.Now()

		// Wait for duration or signal
		select {
		case <-sigCh:
			fmt.Println("\nInterrupted during benchmark")
		case <-time.After(config.Duration):
			fmt.Println("\nBenchmark duration elapsed")
		}

		benchEnd := time.Now()
		actualDuration := benchEnd.Sub(benchStart)

		// Generate results
		result := generateResult(config, actualDuration, benchStart)

		// Print results
		printResults(result)

		// Save results
		if config.OutputFile != "" {
			saveResults(config.OutputFile, result)
		}
	}

	metricsCancel()

	// Close all connections
	clientsMu.Lock()
	for _, conn := range clients {
		conn.Close()
	}
	clientsMu.Unlock()

	fmt.Println("\nBenchmark complete.")
}

func parseFlags() *BenchmarkConfig {
	config := &BenchmarkConfig{}
	flag.StringVar(&config.WebSocketURL, "url", "ws://localhost:8080/ws", "WebSocket URL")
	flag.IntVar(&config.NumClients, "clients", 100, "Number of concurrent clients")
	flag.IntVar(&config.NumSymbols, "symbols", 5, "Number of symbols to subscribe")
	flag.DurationVar(&config.Duration, "duration", 30*time.Second, "Benchmark duration")
	flag.DurationVar(&config.RampUp, "rampup", 10*time.Second, "Client ramp-up duration")
	flag.DurationVar(&config.ReportInterval, "report", 5*time.Second, "Metrics report interval")
	flag.StringVar(&config.OutputFile, "output", "", "Output JSON file")
	flag.Parse()
	return config
}

func checkHealth(wsURL string) bool {
	httpURL := "http" + wsURL[2:] // ws:// -> http://
	httpURL = httpURL[:len(httpURL)-3] // remove /ws
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(httpURL + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func connectAndSubscribe(wsURL string, numSymbols int) (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	symbols := generateSymbols(numSymbols)
	subMsg := map[string]interface{}{
		"action":  "subscribe",
		"symbols": symbols,
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	return conn, nil
}

func generateSymbols(n int) []string {
	symbols := []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "JNJ",
		"WMT", "PG", "MA", "UNH", "HD", "DIS", "BAC", "XOM", "CSCO", "VZ",
		"INTC", "KO", "CVX", "MRK", "PFE", "TMO", "ABT", "COST", "AVGO", "NKE"}
	if n > len(symbols) {
		n = len(symbols)
	}
	return symbols[:n]
}

func receiveLoop(ctx context.Context, conn *websocket.Conn, clientID int) {
	defer func() {
		connected.Add(-1)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			return
		}

		totalMessages.Add(1)
		totalBytes.Add(int64(len(message)))

		// Extract timestamp from message for latency measurement
		var msg struct {
			Type    string `json:"type"`
			Payload struct {
				Timestamp string `json:"timestamp"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(message, &msg); err == nil && msg.Payload.Timestamp != "" {
			if ts, err := time.Parse(time.RFC3339Nano, msg.Payload.Timestamp); err == nil {
				latencyMs := float64(time.Since(ts).Microseconds()) / 1000.0
				if latencyMs > 0 && latencyMs < 10000 { // filter outliers
					latencyHist.Record(latencyMs)
				}
			}
		}
	}
}

// receiveLoopWithReconnect handles messages and reconnects on failure.
func receiveLoopWithReconnect(ctx context.Context, initialConn *websocket.Conn, clientID int, config *BenchmarkConfig) {
	defer func() {
		connected.Add(-1)
	}()

	conn := initialConn
	backoff := 500 * time.Millisecond
	maxBackoff := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			// Connection lost — attempt reconnect
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Exponential backoff with jitter
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			waitTime := backoff + jitter
			if waitTime > maxBackoff {
				waitTime = maxBackoff
			}

			time.Sleep(waitTime)
			backoff = backoff * 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			// Try to reconnect
			newConn, err := connectAndSubscribe(config.WebSocketURL, config.NumSymbols)
			if err != nil {
				// Reconnect failed, continue loop and try again
				continue
			}

			// Success — reset backoff and continue
			conn = newConn
			backoff = 500 * time.Millisecond
			continue
		}

		// Reset backoff on successful message
		backoff = 500 * time.Millisecond

		totalMessages.Add(1)
		totalBytes.Add(int64(len(message)))

		// Extract timestamp from message for latency measurement
		var msg struct {
			Type    string `json:"type"`
			Payload struct {
				Timestamp string `json:"timestamp"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(message, &msg); err == nil && msg.Payload.Timestamp != "" {
			if ts, err := time.Parse(time.RFC3339Nano, msg.Payload.Timestamp); err == nil {
				latencyMs := float64(time.Since(ts).Microseconds()) / 1000.0
				if latencyMs > 0 && latencyMs < 10000 { // filter outliers
					latencyHist.Record(latencyMs)
				}
			}
		}
	}
}

func collectMetrics(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msgs := totalMessages.Load()
			conns := connected.Load()
			fmt.Printf("  [Metrics] messages=%d connected=%d\n", msgs, conns)
		}
	}
}

func generateResult(config *BenchmarkConfig, duration time.Duration, startTime time.Time) *BenchmarkResult {
	msgs := totalMessages.Load()
	bytes := totalBytes.Load()

	result := &BenchmarkResult{
		Timestamp:        startTime.Format(time.RFC3339),
		Config:           *config,
		TotalMessages:    msgs,
		TotalBytes:       bytes,
		Duration:         duration.String(),
		MessagesPerSec:   float64(msgs) / duration.Seconds(),
		BytesPerSec:      float64(bytes) / duration.Seconds(),
		ConnectedClients: int(connected.Load()),
		FailedClients:    int(failed.Load()),
		Latency: LatencyStats{
			MeanMs: latencyHist.Mean(),
			MinMs:  latencyHist.Min,
			MaxMs:  latencyHist.Max,
			P50Ms:  latencyHist.Percentile(50),
			P95Ms:  latencyHist.Percentile(95),
			P99Ms:  latencyHist.Percentile(99),
			P999Ms: latencyHist.Percentile(99.9),
		},
		Histogram: make([]HistogramBucket, 0),
	}

	for i, bucket := range latencyHist.Buckets {
		count := latencyHist.Counts[i]
		pct := float64(count) / float64(latencyHist.Total) * 100
		label := fmt.Sprintf("%.1fms", bucket)
		if i == len(latencyHist.Buckets)-1 {
			label = fmt.Sprintf(">%0.1fms", latencyHist.Buckets[i-1])
		}
		result.Histogram = append(result.Histogram, HistogramBucket{
			UpperBound: label,
			Count:      count,
			Percent:    pct,
		})
	}

	return result
}

func printResults(result *BenchmarkResult) {
	fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║                    BENCHMARK RESULTS                    ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Printf("\nDuration:        %s\n", result.Duration)
	fmt.Printf("Connected:       %d clients\n", result.ConnectedClients)
	fmt.Printf("Failed:          %d clients\n", result.FailedClients)
	fmt.Printf("Total Messages:  %d\n", result.TotalMessages)
	fmt.Printf("Total Bytes:     %d\n", result.TotalBytes)
	fmt.Printf("\nThroughput:      %.0f msg/sec\n", result.MessagesPerSec)
	fmt.Printf("Bandwidth:       %.2f MB/sec\n", result.BytesPerSec/1024/1024)
	fmt.Println("\nLatency:")
	fmt.Printf("  Mean:          %.2f ms\n", result.Latency.MeanMs)
	fmt.Printf("  Min:           %.2f ms\n", result.Latency.MinMs)
	fmt.Printf("  Max:           %.2f ms\n", result.Latency.MaxMs)
	fmt.Printf("  P50:           %.2f ms\n", result.Latency.P50Ms)
	fmt.Printf("  P95:           %.2f ms\n", result.Latency.P95Ms)
	fmt.Printf("  P99:           %.2f ms\n", result.Latency.P99Ms)
	fmt.Printf("  P99.9:         %.2f ms\n", result.Latency.P999Ms)

	if latencyHist.Total > 0 {
		fmt.Println("\nHistogram:")
		latencyHist.Print()
	}
}

func saveResults(filename string, result *BenchmarkResult) {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal results: %v", err)
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Printf("Failed to write results: %v", err)
		return
	}

	fmt.Printf("\nResults saved to: %s\n", filename)
}


