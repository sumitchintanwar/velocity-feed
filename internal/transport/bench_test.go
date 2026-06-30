package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/websocket"
)

type benchHealthReporter struct{}

func (b *benchHealthReporter) HealthReport(_ context.Context) map[string]platform.HealthStatus {
	return map[string]platform.HealthStatus{"bench": platform.OK()}
}

func newBenchRouter(b *testing.B) http.Handler {
	b.Helper()
	metrics, gatherer := platform.NewMetrics("bench_http")
	tm := topicmanager.New(0)
	logger := log.New(io.Discard, "bench")
	gw := websocket.NewGateway(tm, logger, metrics, 100.0)
	cfg := &config.Config{
		Metrics: config.MetricsConfig{Enabled: true, Path: "/metrics"},
	}
	return NewRouter(cfg, gw, logger, metrics, gatherer, &benchHealthReporter{}, nil, nil, nil, nil, nil)
}

// ---------- Health Endpoint ----------

func BenchmarkHealthEndpoint(b *testing.B) {
	handler := newBenchRouter(b)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(w, req)
	}
}

// ---------- Ready Endpoint ----------

func BenchmarkReadyEndpoint(b *testing.B) {
	handler := newBenchRouter(b)
	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(w, req)
	}
}

// ---------- Health Detail Endpoint ----------

func BenchmarkHealthDetailEndpoint(b *testing.B) {
	handler := newBenchRouter(b)
	req := httptest.NewRequest("GET", "/health/detail", nil)
	w := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(w, req)
	}
}

// ---------- Metrics Endpoint ----------

func BenchmarkMetricsEndpoint(b *testing.B) {
	handler := newBenchRouter(b)
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(w, req)
	}
}

// ---------- Root Endpoint ----------

func BenchmarkRootEndpoint(b *testing.B) {
	handler := newBenchRouter(b)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(w, req)
	}
}

// ---------- Middleware Stack ----------

func BenchmarkMiddlewareStack(b *testing.B) {
	_, _ = platform.NewMetrics("bench_mw")

	handler := newBenchRouter(b)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(w, req)
	}
}

// ---------- Concurrent Requests ----------

func BenchmarkConcurrentRequests(b *testing.B) {
	handler := newBenchRouter(b)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		for pb.Next() {
			handler.ServeHTTP(w, req)
		}
	})
}
