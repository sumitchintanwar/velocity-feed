// Package transport wires together the HTTP router, middleware, and all
// handler registrations. It keeps net/http concerns out of the domain packages.
package transport

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/internal/platform"
	ws "github.com/sumit/rtmds/internal/websocket"
)

// HealthReporter provides health status for all application components.
type HealthReporter interface {
	HealthReport(ctx context.Context) map[string]platform.HealthStatus
}

// NewRouter builds and returns the application HTTP router.
//
// Routes:
//
//	GET  /health          liveness probe
//	GET  /health/detail   detailed component health
//	GET  /ready           readiness probe
//	GET  /ws              WebSocket upgrade endpoint
//	GET  /metrics         Prometheus scrape endpoint (if enabled)
func NewRouter(
	cfg *config.Config,
	gw *ws.Gateway,
	log zerolog.Logger,
	metrics *platform.Metrics,
	gatherer prometheus.Gatherer,
	healthReporter HealthReporter,
) http.Handler {
	r := chi.NewRouter()

	// --- Global middleware ---
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(zerologMiddleware(log))
	r.Use(prometheusMiddleware(metrics))
	r.Use(middleware.Recoverer)

	// --- Routes ---
	r.Get("/", handleRoot())
	r.Get("/health", handleHealth())
	r.Get("/health/detail", handleHealthDetail(healthReporter))
	r.Get("/ready", handleReady())
	r.Get("/ws", gw.Handler())

	if cfg.Metrics.Enabled {
		r.Handle(cfg.Metrics.Path, promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	}

	return r
}

// handleRoot returns a landing page with API documentation.
func handleRoot() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>RTMDS</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 640px; margin: 40px auto; padding: 0 20px; color: #222; }
  h1 { margin-bottom: 4px; }
  p.sub { color: #666; margin-top: 0; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  table { border-collapse: collapse; width: 100%%; margin: 16px 0; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #ddd; }
  th { background: #f9f9f9; }
  a { color: #0066cc; }
</style>
</head>
<body>
<h1>Real-Time Market Data System</h1>
<p class="sub">Go WebSocket gateway for streaming market data.</p>

<table>
<tr><th>Endpoint</th><th>Method</th><th>Description</th></tr>
<tr><td><code>/</code></td><td>GET</td><td>This page</td></tr>
<tr><td><code>/health</code></td><td>GET</td><td>Liveness probe</td></tr>
<tr><td><code>/health/detail</code></td><td>GET</td><td>Detailed component health</td></tr>
<tr><td><code>/ready</code></td><td>GET</td><td>Readiness probe</td></tr>
<tr><td><code>/ws</code></td><td>GET</td><td>WebSocket upgrade</td></tr>
<tr><td><code>/metrics</code></td><td>GET</td><td>Prometheus metrics</td></tr>
</table>

<h3>WebSocket Usage</h3>
<p>Connect with a WebSocket client and send:</p>
<pre><code>{"action":"subscribe","symbols":["AAPL","MSFT","GOOG"]}</code></pre>
<p>Example with <a href="https://github.com/vi/websocat">websocat</a>:</p>
<pre><code>websocat ws://localhost:8080/ws
# then paste: {"action":"subscribe","symbols":["AAPL"]}</code></pre>
</body></html>`))
	}
}

// handleHealth returns a 200 OK liveness probe.
func handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

// handleHealthDetail returns detailed health status of all components.
func handleHealthDetail(reporter HealthReporter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		report := reporter.HealthReport(r.Context())

		// Determine overall status
		allOK := true
		for _, status := range report {
			if !status.OK {
				allOK = false
				break
			}
		}

		response := map[string]interface{}{
			"status":     "ok",
			"components": report,
		}
		if !allOK {
			response["status"] = "degraded"
		}

		w.Header().Set("Content-Type", "application/json")
		if !allOK {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(response)
	}
}

// handleReady returns a 200 OK readiness probe. Extend this to check
// downstream dependencies (Redis, feed connectivity) before returning 200.
func handleReady() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}
}
