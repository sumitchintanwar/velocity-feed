package loadtest

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Pool manages a set of load test clients.
type Pool struct {
	cfg      Config
	clients  []*client
	latency  *LatencyCollector
	received *ThroughputCounter
	errors   atomic.Int64

	// Connection stats (atomic for concurrent updates).
	connSucceeded atomic.Int64
	connFailed    atomic.Int64
}

// NewPool creates a client pool with the given config.
func NewPool(cfg Config) *Pool {
	estimatedSamples := cfg.Connections * 1000 // rough estimate
	return &Pool{
		cfg:      cfg,
		clients:  make([]*client, cfg.Connections),
		latency:  NewLatencyCollector(estimatedSamples),
		received: &ThroughputCounter{},
	}
}

// Run executes the full load test and returns results.
func (p *Pool) Run(ctx context.Context) (*Result, error) {
	result := &Result{
		ConnectionsAttempted: p.cfg.Connections,
	}

	// Phase 1: Connect all clients.
	fmt.Printf("Connecting %d clients...\n", p.cfg.Connections)
	connectStart := time.Now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, 100) // limit concurrent connections

	for i := 0; i < p.cfg.Connections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Ramp-up delay.
			if p.cfg.RampUp > 0 {
				delay := time.Duration(float64(id) / float64(p.cfg.Connections) * float64(p.cfg.RampUp))
				time.Sleep(delay)
			}

			sem <- struct{}{}
			defer func() { <-sem }()

			c := newClient(id, p.cfg, p.latency, p.received, &p.errors)
			clientCtx, cancel := context.WithCancel(ctx)
			c.cancel = cancel
			p.clients[id] = c

			if err := c.connect(clientCtx); err != nil {
				fmt.Printf("  Client %d: connection failed: %v\n", id, err)
				p.connFailed.Add(1)
				cancel()
				return
			}

			p.connSucceeded.Add(1)

			// Start read loop.
			go c.run(clientCtx)
		}(i)
	}
	wg.Wait()

	result.ConnectionsSucceeded = int(p.connSucceeded.Load())
	result.ConnectionsFailed = int(p.connFailed.Load())
	result.ConnectionTime = time.Since(connectStart)
	fmt.Printf("Connected: %d/%d in %v\n",
		result.ConnectionsSucceeded, p.cfg.Connections, result.ConnectionTime.Round(time.Millisecond))

	if result.ConnectionsSucceeded == 0 {
		return result, fmt.Errorf("no connections established")
	}

	// Phase 2: Run load test for duration.
	fmt.Printf("Running load test for %v...\n", p.cfg.Duration)

	testCtx, testCancel := context.WithTimeout(ctx, p.cfg.Duration)
	defer testCancel()

	// Progress reporter.
	go p.reportProgress(testCtx)

	// Wait for duration.
	<-testCtx.Done()

	// Phase 3: Collect results.
	fmt.Println("Collecting results...")

	// Wait a bit for in-flight messages.
	time.Sleep(100 * time.Millisecond)

	result.MessagesReceived = p.received.Total()
	result.Errors = make([]string, 0)
	if p.errors.Load() > 0 {
		result.Errors = append(result.Errors, fmt.Sprintf("%d read errors", p.errors.Load()))
	}

	elapsed := p.cfg.Duration.Seconds()
	if elapsed > 0 {
		result.MsgPerSec = float64(result.MessagesReceived) / elapsed
	}

	result.Latency = p.latency.Stats()

	// Phase 4: Disconnect.
	fmt.Println("Disconnecting...")
	p.disconnect()

	return result, nil
}

// reportProgress prints periodic progress updates.
func (p *Pool) reportProgress(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.ReportInterval)
	defer ticker.Stop()

	var lastTotal int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			total := p.received.Total()
			rate := float64(total-lastTotal) / p.cfg.ReportInterval.Seconds()
			lastTotal = total

			stats := p.latency.Stats()
			fmt.Printf("  [progress] total=%d rate=%.0f/s latency_p50=%v p99=%v errors=%d\n",
				total, rate, stats.P50.Round(time.Microsecond), stats.P99.Round(time.Microsecond), p.errors.Load())
		}
	}
}

// disconnect gracefully shuts down all clients.
func (p *Pool) disconnect() {
	for _, c := range p.clients {
		if c != nil {
			c.close()
		}
	}
}

// FormatResult returns a human-readable summary of the load test results.
func FormatResult(r *Result) string {
	s := fmt.Sprintf(`
═══════════════════════════════════════════════════
  LOAD TEST RESULTS
═══════════════════════════════════════════════════

  Connections
    Attempted:   %d
    Succeeded:   %d
    Failed:      %d
    Connect Time: %v

  Throughput
    Messages Received:  %d
    Rate:               %.0f msg/sec

  Latency (end-to-end)
    Count:  %d
    Min:    %v
    P50:    %v
    P95:    %v
    P99:    %v
    P99.9:  %v
    Max:    %v
    Mean:   %v
`,
		r.ConnectionsAttempted,
		r.ConnectionsSucceeded,
		r.ConnectionsFailed,
		r.ConnectionTime.Round(time.Millisecond),

		r.MessagesReceived,
		r.MsgPerSec,

		r.Latency.Count,
		r.Latency.Min.Round(time.Microsecond),
		r.Latency.P50.Round(time.Microsecond),
		r.Latency.P95.Round(time.Microsecond),
		r.Latency.P99.Round(time.Microsecond),
		r.Latency.P999.Round(time.Microsecond),
		r.Latency.Max.Round(time.Microsecond),
		r.Latency.Mean.Round(time.Microsecond),
	)

	if len(r.Errors) > 0 {
		s += "\n  Errors\n"
		for _, e := range r.Errors {
			s += fmt.Sprintf("    - %s\n", e)
		}
	}

	s += "\n═══════════════════════════════════════════════════\n"
	return s
}

// findModuleRoot walks up from the current directory looking for go.mod.
func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir || parent == string(filepath.Separator) || strings.HasSuffix(parent, ":") {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = parent
	}
}

// validPercentileIndex returns a valid index for percentile calculation.
func validPercentileIndex(n int, pct float64) int {
	idx := int(math.Ceil(float64(n)*pct)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

// SaveResult writes the load test results to a markdown file in docs/results/.
// Filename format: LOAD_TEST_YYYY-MM-DD_HH-MM-SS.md
// Returns the file path on success.
func SaveResult(r *Result, cfg Config) (string, error) {
	root, err := findModuleRoot()
	if err != nil {
		root = "."
	}
	dir := filepath.Join(root, "docs", "results")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create results dir: %w", err)
	}

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	filename := filepath.Join(dir, fmt.Sprintf("LOAD_TEST_%s.md", timestamp))

	content := formatMarkdownResult(r, cfg)

	if err := os.WriteFile(filename, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("write result file: %w", err)
	}

	return filename, nil
}

// formatMarkdownResult formats the result as a markdown document with header.
func formatMarkdownResult(r *Result, cfg Config) string {
	now := time.Now().Format("2006-01-02 15:04:05 MST")
	cb := "```" // code fence

	s := "# Load Test Results\n\n"
	s += fmt.Sprintf("**Date:** %s\n\n", now)
	s += "## Configuration\n\n"
	s += "| Parameter | Value |\n"
	s += "|:---|:---|\n"
	s += fmt.Sprintf("| Server URL | `%s` |\n", cfg.ServerURL)
	s += fmt.Sprintf("| Connections | %d |\n", cfg.Connections)
	s += fmt.Sprintf("| Symbols | %v |\n", cfg.Symbols)
	s += fmt.Sprintf("| Duration | %v |\n", cfg.Duration)
	s += fmt.Sprintf("| Ramp-up | %v |\n", cfg.RampUp)
	s += fmt.Sprintf("| Read Delay | %v |\n", cfg.ReadDelay)
	s += "\n## Results\n\n"
	s += cb + "\n"
	s += "LOAD TEST RESULTS\n"
	s += "═══════════════════════════════════════════════════\n\n"
	s += "  Connections\n"
	s += fmt.Sprintf("    Attempted:    %d\n", r.ConnectionsAttempted)
	s += fmt.Sprintf("    Succeeded:    %d\n", r.ConnectionsSucceeded)
	s += fmt.Sprintf("    Failed:       %d\n", r.ConnectionsFailed)
	s += fmt.Sprintf("    Connect Time: %v\n", r.ConnectionTime.Round(time.Millisecond))
	s += "\n  Throughput\n"
	s += fmt.Sprintf("    Messages Received:  %d\n", r.MessagesReceived)
	s += fmt.Sprintf("    Rate:               %.0f msg/sec\n", r.MsgPerSec)
	s += "\n  Latency (end-to-end)\n"
	s += fmt.Sprintf("    Count:  %d\n", r.Latency.Count)
	s += fmt.Sprintf("    Min:    %v\n", r.Latency.Min.Round(time.Microsecond))
	s += fmt.Sprintf("    P50:    %v\n", r.Latency.P50.Round(time.Microsecond))
	s += fmt.Sprintf("    P95:    %v\n", r.Latency.P95.Round(time.Microsecond))
	s += fmt.Sprintf("    P99:    %v\n", r.Latency.P99.Round(time.Microsecond))
	s += fmt.Sprintf("    P99.9:  %v\n", r.Latency.P999.Round(time.Microsecond))
	s += fmt.Sprintf("    Max:    %v\n", r.Latency.Max.Round(time.Microsecond))
	s += fmt.Sprintf("    Mean:   %v\n", r.Latency.Mean.Round(time.Microsecond))
	s += cb + "\n"

	if len(r.Errors) > 0 {
		s += "\n## Errors\n\n"
		for _, e := range r.Errors {
			s += fmt.Sprintf("- %s\n", e)
		}
	}

	return s
}
