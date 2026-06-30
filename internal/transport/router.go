// Package transport wires together the HTTP router, middleware, and all
// handler registrations. It keeps net/http concerns out of the domain packages.
package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/internal/correlation/generator"
	"github.com/sumit/rtmds/internal/discovery"
	"github.com/sumit/rtmds/internal/eventlog"
	"github.com/sumit/rtmds/internal/healthcheck"
	logpkg "github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/middleware"
	"github.com/sumit/rtmds/internal/platform"
	adminapi "github.com/sumit/rtmds/internal/platform/admin/api"
	"github.com/sumit/rtmds/internal/platform/admin/audit"
	"github.com/sumit/rtmds/internal/platform/admin/commands"
	adminmw "github.com/sumit/rtmds/internal/platform/admin/middleware"
	"github.com/sumit/rtmds/internal/platform/lifecycle"
	ws "github.com/sumit/rtmds/internal/websocket"
	"go.uber.org/zap"
)

// HealthReporter provides health status for all application components.
type HealthReporter interface {
	HealthReport(ctx context.Context) map[string]platform.HealthStatus
}

// LogLevelChanger allows dynamic log level changes at runtime.
// This is critical for incident response — enabling DEBUG logging
// without restarting the server (which would drop WebSocket clients
// and clear the snapshot cache).
type LogLevelChanger interface {
	SetLogLevel(level string) error
}

// NewRouter builds and returns the application HTTP router.
//
// Routes:
//
//	GET  /health          liveness probe
//	GET  /health/detail   detailed component health
//	GET  /ready           readiness probe
//	GET  /liveness        liveness probe (alias for /health)
//	GET  /readiness       readiness probe (alias for /ready)
//	GET  /ws              WebSocket upgrade endpoint
//	GET  /replay          historical event replay
//	GET  /replay/export   bulk export of historical events (CSV/JSON)
//	GET  /metrics         Prometheus scrape endpoint (if enabled)
//	GET  /gateways        list active gateways (if discovery enabled)
//	POST /admin/log-level change log level at runtime (incident response)
func NewRouter(
	cfg *config.Config,
	gw *ws.Gateway,
	log *logpkg.Logger,
	metrics *platform.Metrics,
	gatherer prometheus.Gatherer,
	healthReporter HealthReporter,
	registry *discovery.Registry,
	eventLog eventlog.Repository,
	healthRegistry *healthcheck.Registry,
	heartbeat *healthcheck.Heartbeat,
	logChanger LogLevelChanger,
) http.Handler {
	r := chi.NewRouter()

	// --- Global middleware ---
	// Install the centralized observability pipeline
	pipeline := middleware.HTTPPipeline(log, metrics, cfg.Server.GetGatewayID(), generator.NewUUIDv7Generator())
	for _, mw := range pipeline {
		r.Use(mw)
	}

	// Gateway ID for sticky session verification
	gatewayID := gw.ID()

	// --- Routes ---
	r.Get("/", handleRoot())
	r.Get("/health", handleHealth(healthReporter, gatewayID))
	r.Get("/health/detail", handleHealthDetail(healthReporter, gatewayID))
	r.Get("/ready", handleReady(healthReporter, gatewayID))
	r.Get("/liveness", handleLiveness(healthRegistry, heartbeat, gatewayID))
	r.Get("/readiness", handleReadiness(healthRegistry, gatewayID))
	r.Get("/ws", gw.Handler())

	if eventLog != nil {
		limiter := newConcurrencyLimiter(maxConcurrentQueriesPerClient)
		r.Get("/replay", handleReplay(eventLog, limiter))
		r.Get("/replay/export", handleReplayExport(eventLog, limiter))
	}

	if registry != nil {
		r.Get("/gateways", handleGateways(registry))
	}

	if cfg.Metrics.Enabled {
		r.Handle(cfg.Metrics.Path, promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	}

	// Dynamic log level endpoint — critical for incident response.
	// Allows enabling DEBUG logging without restarting the server.
	if logChanger != nil {
		r.Post("/admin/log-level", handleLogLevelChange(logChanger))
	}

	// ── Admin API (Diagnostics, Pprof, Operations) ──────────────
	// Mount the authenticated admin API under /admin.
	// Tokens are read from environment variables with sensible defaults.
	adminToken := envOrDefault("RTMDS_ADMIN_TOKEN", "admin")
	operatorToken := envOrDefault("RTMDS_OPERATOR_TOKEN", "operator")
	viewerToken := envOrDefault("RTMDS_VIEWER_TOKEN", "viewer")

	authenticator := &adminmw.StaticTokenAuthenticator{
		AdminToken:    adminToken,
		OperatorToken: operatorToken,
		ViewerToken:   viewerToken,
	}

	// Lifecycle manager wraps the application components for inspection.
	lcm := lifecycle.NewManager()

	// Atomic log level for dynamic changes via the operations API.
	atomicLevel := zap.NewAtomicLevel()

	adminCfg := adminapi.RouterConfig{
		Manager:               lcm,
		Version:               "dev",
		Authenticator:         authenticator,
		AuditLogger:           audit.NewZapAuditLogger(zap.NewNop()),
		CommandBus:            commands.NewCommandBus(),
		PublisherController:   &commands.MockPublisherController{},
		MaintenanceController: &commands.MockMaintenanceController{},
		AtomicLogLevel:        atomicLevel,
	}
	adminMux := adminapi.NewRouter(adminCfg)

	// Mount the admin API under /admin/* (strips the /admin prefix).
	r.Handle("/admin/*", http.StripPrefix("/admin", adminMux))

	return r
}

// envOrDefault returns the value of the environment variable named by
// key, or fallback if the variable is empty or unset.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
<tr><td><code>/health</code></td><td>GET</td><td>Liveness probe (legacy)</td></tr>
<tr><td><code>/health/detail</code></td><td>GET</td><td>Detailed component health</td></tr>
<tr><td><code>/ready</code></td><td>GET</td><td>Readiness probe (legacy)</td></tr>
<tr><td><code>/liveness</code></td><td>GET</td><td>Liveness probe (K8s)</td></tr>
<tr><td><code>/readiness</code></td><td>GET</td><td>Readiness probe (K8s)</td></tr>
<tr><td><code>/ws</code></td><td>GET</td><td>WebSocket upgrade</td></tr>
<tr><td><code>/metrics</code></td><td>GET</td><td>Prometheus metrics</td></tr>
</table>

<h3>WebSocket Usage</h3>
<p>Connect with a WebSocket client and send:</p>
<pre><code>{"action":"subscribe","symbols":["AAPL","MSFT","GOOG"]}</code></pre>
<p>Example with <a href="https://github.com/vi/websocat">websocat</a>:</p>
<pre><code>websocat ws://localhost:9090/ws
# then paste: {"action":"subscribe","symbols":["AAPL"]}</code></pre>
</body></html>`))
	}
}

// handleHealth returns a 200 OK liveness probe. This endpoint NEVER checks
// dependencies (Redis, etc.) — it only proves the process is alive.
// Kubernetes liveness probe: if this returns non-200, the pod is restarted.
// Dependencies are checked by /ready (readiness probe).
// Sets rtmds-gateway-id header for sticky session verification.
func handleHealth(_ HealthReporter, gatewayID string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("rtmds-gateway-id", gatewayID)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

// handleHealthDetail returns detailed health status of all components.
// Sets rtmds-gateway-id header for sticky session verification.
func handleHealthDetail(reporter HealthReporter, gatewayID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		report := reporter.HealthReport(r.Context())

		// Set gateway ID header for sticky session verification
		w.Header().Set("rtmds-gateway-id", gatewayID)

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

// handleReady returns a 200 OK readiness probe if all critical dependencies
// (Redis, etc.) are healthy. Returns 503 if any dependency is down —
// Nginx will stop routing traffic to this gateway but won't kill the pod.
// Sets rtmds-gateway-id header for sticky session verification.
func handleReady(reporter HealthReporter, gatewayID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("rtmds-gateway-id", gatewayID)

		// Check if any critical component is unhealthy
		if reporter != nil {
			report := reporter.HealthReport(r.Context())
			for _, status := range report {
				if !status.OK {
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"status": "not_ready",
						"error":  status.Detail,
					})
					return
				}
			}
		}

		_, _ = w.Write([]byte(`{"status":"ready"}`))
	}
}

// handleLiveness returns a 200 OK liveness probe using the health check registry.
// This endpoint NEVER checks external dependencies — it only proves the process is alive.
// Kubernetes liveness probe: if this returns non-200, the pod is restarted.
//
// If a heartbeat is provided, it checks that the main event loops are still
// running (not deadlocked). This prevents the "deadlock blind spot" where the
// HTTP server continues returning 200 OK while WebSocket readPump or Redis
// worker loops are deadlocked (see health_check_review.md).
func handleLiveness(registry *healthcheck.Registry, heartbeat *healthcheck.Heartbeat, gatewayID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("rtmds-gateway-id", gatewayID)

		// Liveness checks: only process-level health, no external dependencies.
		// A dependency outage should not restart healthy application processes.
		if heartbeat != nil {
			// Check that main event loops are alive (not deadlocked).
			// 10s threshold: allows for brief GC pauses but detects real deadlocks.
			if !heartbeat.IsAlive(10 * time.Second) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"status":"not_ok","error":"heartbeat stale, possible deadlock"}`))
				return
			}
		}

		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

// handleReadiness returns 200 if all critical dependencies are healthy, 503 otherwise.
// This runs the health check registry which checks Redis, PostgreSQL, and internal services.
// Kubernetes readiness probe: if this returns 503, the pod is removed from the service.
func handleReadiness(registry *healthcheck.Registry, gatewayID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("rtmds-gateway-id", gatewayID)

		if registry == nil {
			// No health checks configured — always ready
			_, _ = w.Write([]byte(`{"status":"ready"}`))
			return
		}

		result := registry.Run(r.Context())

		if result.Status != "ok" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "not_ready",
				"checks":  result.Checks,
				"duration": result.Duration.Milliseconds(),
			})
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ready",
			"checks":  result.Checks,
			"duration": result.Duration.Milliseconds(),
		})
	}
}

// handleGateways returns a JSON list of all active gateways from the
// service discovery registry. Only available when discovery is enabled.
func handleGateways(registry *discovery.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gateways, err := registry.List(r.Context())
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "error",
				"error":  err.Error(),
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"gateways": gateways,
			"count":    len(gateways),
		})
	}
}

// handleLogLevelChange allows changing the log level at runtime via HTTP POST.
// This is critical for incident response — enabling DEBUG logging without
// restarting the server (which would drop thousands of WebSocket clients
// and clear the snapshot cache).
//
// Example:
//
//	POST /admin/log-level
//	{"level":"debug"}
//
// Supported levels: debug, info, warn, error
func handleLogLevelChange(changer LogLevelChanger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "only POST is supported",
			})
			return
		}

		var req struct {
			Level string `json:"level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "invalid JSON: " + err.Error(),
			})
			return
		}

		if err := changer.SetLogLevel(req.Level); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": err.Error(),
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"level":  req.Level,
		})
	}
}
