package loadtest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumit/rtmds/internal/marketdata"
)

// ---------- LatencyCollector ----------

func TestLatencyCollector_Basic(t *testing.T) {
	lc := NewLatencyCollector(100)

	lc.Record(1 * time.Millisecond)
	lc.Record(2 * time.Millisecond)
	lc.Record(3 * time.Millisecond)

	stats := lc.Stats()
	if stats.Count != 3 {
		t.Errorf("Count = %d, want 3", stats.Count)
	}
	if stats.Min != 1*time.Millisecond {
		t.Errorf("Min = %v, want 1ms", stats.Min)
	}
	if stats.Max != 3*time.Millisecond {
		t.Errorf("Max = %v, want 3ms", stats.Max)
	}
	if stats.Mean != 2*time.Millisecond {
		t.Errorf("Mean = %v, want 2ms", stats.Mean)
	}
}

func TestLatencyCollector_P50(t *testing.T) {
	lc := NewLatencyCollector(100)

	// 100 samples: 1-100ms
	for i := 1; i <= 100; i++ {
		lc.Record(time.Duration(i) * time.Millisecond)
	}

	stats := lc.Stats()
	if stats.P50 != 50*time.Millisecond {
		t.Errorf("P50 = %v, want 50ms", stats.P50)
	}
	if stats.P95 != 95*time.Millisecond {
		t.Errorf("P95 = %v, want 95ms", stats.P95)
	}
	if stats.P99 != 99*time.Millisecond {
		t.Errorf("P99 = %v, want 99ms", stats.P99)
	}
}

func TestLatencyCollector_Empty(t *testing.T) {
	lc := NewLatencyCollector(100)
	stats := lc.Stats()
	if stats.Count != 0 {
		t.Errorf("Count = %d, want 0", stats.Count)
	}
}

func TestLatencyCollector_SingleSample(t *testing.T) {
	lc := NewLatencyCollector(10)
	lc.Record(5 * time.Millisecond)

	stats := lc.Stats()
	if stats.Min != 5*time.Millisecond || stats.Max != 5*time.Millisecond {
		t.Errorf("Min/Max = %v/%v, want 5ms/5ms", stats.Min, stats.Max)
	}
	if stats.P50 != 5*time.Millisecond {
		t.Errorf("P50 = %v, want 5ms", stats.P50)
	}
}

func TestLatencyCollector_Concurrent(t *testing.T) {
	lc := NewLatencyCollector(10000)

	var wg sync.WaitGroup
	const goroutines = 100
	const perGoroutine = 100

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				lc.Record(time.Duration(j) * time.Microsecond)
			}
		}(i)
	}
	wg.Wait()

	stats := lc.Stats()
	if stats.Count != goroutines*perGoroutine {
		t.Errorf("Count = %d, want %d", stats.Count, goroutines*perGoroutine)
	}
}

func TestLatencyCollector_MinMaxTracking(t *testing.T) {
	lc := NewLatencyCollector(100)

	lc.Record(5 * time.Millisecond)
	lc.Record(1 * time.Millisecond)
	lc.Record(10 * time.Millisecond)

	stats := lc.Stats()
	if stats.Min != 1*time.Millisecond {
		t.Errorf("Min = %v, want 1ms", stats.Min)
	}
	if stats.Max != 10*time.Millisecond {
		t.Errorf("Max = %v, want 10ms", stats.Max)
	}
}

// ---------- ThroughputCounter ----------

func TestThroughputCounter_Inc(t *testing.T) {
	tc := &ThroughputCounter{}

	tc.Inc()
	tc.Inc()
	tc.Inc()

	if tc.Total() != 3 {
		t.Errorf("Total() = %d, want 3", tc.Total())
	}
}

func TestThroughputCounter_IncN(t *testing.T) {
	tc := &ThroughputCounter{}

	tc.IncN(100)
	tc.IncN(200)

	if tc.Total() != 300 {
		t.Errorf("Total() = %d, want 300", tc.Total())
	}
}

func TestThroughputCounter_Load(t *testing.T) {
	tc := &ThroughputCounter{}

	tc.IncN(50)
	count := tc.Load()

	if count != 50 {
		t.Errorf("Load() = %d, want 50", count)
	}
	if tc.Total() != 0 {
		t.Errorf("Total() after Load = %d, want 0", tc.Total())
	}

	tc.IncN(30)
	count = tc.Load()
	if count != 30 {
		t.Errorf("Load() = %d, want 30", count)
	}
}

func TestThroughputCounter_Concurrent(t *testing.T) {
	tc := &ThroughputCounter{}

	var wg sync.WaitGroup
	const goroutines = 100
	const perGoroutine = 1000

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				tc.Inc()
			}
		}()
	}
	wg.Wait()

	if tc.Total() != goroutines*perGoroutine {
		t.Errorf("Total() = %d, want %d", tc.Total(), goroutines*perGoroutine)
	}
}

// ---------- Pool Integration ----------

func TestPool_Run_MockServer(t *testing.T) {
	// Start a mock WebSocket server.
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	var connected atomic.Int32
	var msgCount atomic.Int64

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			connected.Add(1)

			// Read subscribe request.
			_, _, err = conn.ReadMessage()
			if err != nil {
				return
			}

			// Send quotes.
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
					q := marketdata.ServerMessage{
						Type: "quote",
						Payload: marketdata.Quote{
							Symbol:    "AAPL",
							Type:      marketdata.QuoteTypeQuote,
							Price:     100.0,
							Timestamp: time.Now(),
						},
					}
					if err := conn.WriteJSON(q); err != nil {
						return
					}
					msgCount.Add(1)
				}
			}
		}),
	}

	// Find a free port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	go server.Serve(listener)
	defer server.Close()

	addr := fmt.Sprintf("ws://%s/ws", listener.Addr().String())

	cfg := Config{
		ServerURL:       addr,
		Connections:     5,
		Symbols:         []string{"AAPL"},
		Duration:        2 * time.Second,
		ReportInterval:  500 * time.Millisecond,
	}

	pool := NewPool(cfg)
	result, err := pool.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.ConnectionsSucceeded != 5 {
		t.Errorf("ConnectionsSucceeded = %d, want 5", result.ConnectionsSucceeded)
	}
	if result.MessagesReceived == 0 {
		t.Error("MessagesReceived = 0, want > 0")
	}
	if result.Latency.Count == 0 {
		t.Error("Latency.Count = 0, want > 0")
	}

	t.Logf("Result: %d messages, %.0f/s, P50=%v, P99=%v",
		result.MessagesReceived, result.MsgPerSec,
		result.Latency.P50.Round(time.Microsecond),
		result.Latency.P99.Round(time.Microsecond))
}

func TestFormatResult(t *testing.T) {
	r := &Result{
		ConnectionsAttempted: 100,
		ConnectionsSucceeded: 95,
		ConnectionsFailed:    5,
		ConnectionTime:       2 * time.Second,
		MessagesReceived:     50000,
		MsgPerSec:            1666.7,
		Latency: LatencyStats{
			Min:   100 * time.Microsecond,
			Max:   50 * time.Millisecond,
			Mean:  2 * time.Millisecond,
			P50:   1 * time.Millisecond,
			P95:   5 * time.Millisecond,
			P99:   15 * time.Millisecond,
			P999:  40 * time.Millisecond,
			Count: 50000,
		},
	}

	s := FormatResult(r)
	if s == "" {
		t.Fatal("FormatResult returned empty string")
	}
	if !contains(s, "95") {
		t.Error("Result missing connection success count")
	}
}

func TestSaveResult(t *testing.T) {
	r := &Result{
		ConnectionsAttempted: 100,
		ConnectionsSucceeded: 95,
		ConnectionsFailed:    5,
		ConnectionTime:       2 * time.Second,
		MessagesReceived:     50000,
		MsgPerSec:            1666.7,
		Latency: LatencyStats{
			Min:   100 * time.Microsecond,
			Max:   50 * time.Millisecond,
			Mean:  2 * time.Millisecond,
			P50:   1 * time.Millisecond,
			P95:   5 * time.Millisecond,
			P99:   15 * time.Millisecond,
			P999:  40 * time.Millisecond,
			Count: 50000,
		},
	}

	cfg := DefaultConfig()

	path, err := SaveResult(r, cfg)
	if err != nil {
		t.Fatalf("SaveResult: %v", err)
	}

	// Verify file exists.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatalf("file does not exist: %s", path)
	}

	// Read and verify content.
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	s := string(content)
	if !containsSubstring(s, "Load Test Results") {
		t.Error("missing header")
	}
	if !containsSubstring(s, "Connections") {
		t.Error("missing connections section")
	}
	if !containsSubstring(s, "P99:") {
		t.Error("missing P99 latency")
	}

	// Cleanup.
	os.Remove(path)
	t.Logf("Saved and verified: %s", path)
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstring(s, sub))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---------- Benchmarks ----------

func BenchmarkLatencyCollector_Record(b *testing.B) {
	lc := NewLatencyCollector(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.Record(time.Duration(i) * time.Microsecond)
	}
}

func BenchmarkThroughputCounter_Inc(b *testing.B) {
	tc := &ThroughputCounter{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.Inc()
	}
}
