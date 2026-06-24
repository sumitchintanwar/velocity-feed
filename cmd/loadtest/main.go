// Command loadtest runs a WebSocket load test against the market data gateway.
//
// Usage:
//
//	go run ./cmd/loadtest -url ws://localhost:8080/ws -connections 1000 -duration 30s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sumit/rtmds/internal/loadtest"
)

func main() {
	var (
		url         string
		connections int
		symbols     string
		duration    time.Duration
		rampUp      time.Duration
		readDelay   time.Duration
		reportEvery time.Duration
	)

	flag.StringVar(&url, "url", "ws://localhost:8080/ws", "WebSocket gateway URL")
	flag.IntVar(&connections, "connections", 100, "number of WebSocket clients")
	flag.StringVar(&symbols, "symbols", "AAPL,MSFT,GOOG,TSLA,NVDA", "comma-separated symbols to subscribe to")
	flag.DurationVar(&duration, "duration", 30*time.Second, "load test duration")
	flag.DurationVar(&rampUp, "ramp-up", 0, "time to stagger connection establishment")
	flag.DurationVar(&readDelay, "read-delay", 0, "artificial read delay per message (slow consumer)")
	flag.DurationVar(&reportEvery, "report", 5*time.Second, "progress report interval")
	flag.Parse()

	cfg := loadtest.Config{
		ServerURL:       url,
		Connections:     connections,
		Symbols:         strings.Split(symbols, ","),
		Duration:        duration,
		RampUp:          rampUp,
		ReadDelay:       readDelay,
		ReportInterval:  reportEvery,
	}

	fmt.Printf("Load Test Configuration:\n")
	fmt.Printf("  Server:      %s\n", cfg.ServerURL)
	fmt.Printf("  Connections: %d\n", cfg.Connections)
	fmt.Printf("  Symbols:     %v\n", cfg.Symbols)
	fmt.Printf("  Duration:    %v\n", cfg.Duration)
	fmt.Printf("  Ramp-up:     %v\n", cfg.RampUp)
	fmt.Printf("  Read Delay:  %v\n", cfg.ReadDelay)
	fmt.Println()

	ctx := context.Background()
	pool := loadtest.NewPool(cfg)

	result, err := pool.Run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Load test failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(loadtest.FormatResult(result))

	// Save results to docs/results/.
	path, err := loadtest.SaveResult(result, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not save results: %v\n", err)
	} else {
		fmt.Printf("Results saved to: %s\n", path)
	}
}
