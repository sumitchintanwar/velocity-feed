// Package healthcheck provides a production-grade health check framework.
//
// Three probe types are supported:
//
//   - Liveness: process health only (no external deps)
//   - Readiness: can the service receive traffic? (checks Redis, Postgres, etc.)
//   - Startup: has initialization completed?
//
// Checks run concurrently with per-check timeouts to ensure fast failure detection.
package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Heartbeat maintains an atomic timestamp that is periodically updated by
// the main event/worker loops. The Liveness probe checks this timestamp
// to detect deadlocks — if the timestamp hasn't updated in X seconds,
// the loop is deadlocked and Liveness should fail.
//
// This prevents the "deadlock blind spot" where the HTTP server continues
// returning 200 OK while WebSocket readPump or Redis worker loops are
// deadlocked (see health_check_review.md).
type Heartbeat struct {
	lastBeat atomic.Int64 // Unix nanoseconds
}

// NewHeartbeat creates a new Heartbeat with the current time.
func NewHeartbeat() *Heartbeat {
	h := &Heartbeat{}
	h.Mark()
	return h
}

// Mark updates the heartbeat timestamp to the current time.
// This should be called by the main event/worker loops on each iteration.
func (h *Heartbeat) Mark() {
	h.lastBeat.Store(time.Now().UnixNano())
}

// IsAlive returns true if the heartbeat has been updated within the
// specified duration. If not, the main loop is likely deadlocked.
func (h *Heartbeat) IsAlive(within time.Duration) bool {
	last := h.lastBeat.Load()
	if last == 0 {
		return false
	}
	return time.Since(time.Unix(0, last)) < within
}

// LastBeat returns the time of the last heartbeat.
func (h *Heartbeat) LastBeat() time.Time {
	return time.Unix(0, h.lastBeat.Load())
}

// LivenessCheck creates a health check that verifies the main event loops
// are still running. It checks that the heartbeat has been updated within
// the specified duration. If not, the loop is deadlocked and Liveness fails.
func LivenessCheck(hb *Heartbeat, maxStaleness time.Duration) Check {
	return Check{
		Name: "liveness-heartbeat",
		CheckFn: func(_ context.Context) error {
			if !hb.IsAlive(maxStaleness) {
				return fmt.Errorf("heartbeat stale: last beat %v ago (max %v)",
					time.Since(hb.LastBeat()).Round(time.Millisecond), maxStaleness)
			}
			return nil
		},
		Timeout: 100 * time.Millisecond,
	}
}

// Status represents the health status of a single check.
type Status struct {
	Name     string        `json:"name"`
	OK       bool          `json:"ok"`
	Detail   string        `json:"detail,omitempty"`
	Duration time.Duration `json:"duration_ms"`
}

// CheckFunc is a function that checks the health of a dependency.
// It should return nil if healthy, or an error describing the failure.
type CheckFunc func(ctx context.Context) error

// Check defines a named health check with a timeout.
type Check struct {
	Name    string
	CheckFn CheckFunc
	Timeout time.Duration
}

// Result is the aggregated result of all health checks.
type Result struct {
	Status   string           `json:"status"` // "ok" or "degraded"
	Checks   []Status         `json:"checks"`
	Duration time.Duration    `json:"duration_ms"`
}

// Registry holds a set of health checks and executes them.
type Registry struct {
	checks []Check
}

// NewRegistry creates a new health check registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds a health check to the registry.
func (r *Registry) Register(check Check) {
	r.checks = append(r.checks, check)
}

// Checks returns the registered checks (for introspection).
func (r *Registry) Checks() []Check {
	return r.checks
}

// Run executes all registered checks concurrently and returns the aggregated result.
// Each check runs with its own timeout. The overall result is healthy only if all checks pass.
func (r *Registry) Run(ctx context.Context) Result {
	start := time.Now()

	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results []Status
	)

	for _, c := range r.checks {
		wg.Add(1)
		go func(check Check) {
			defer wg.Done()

			checkStart := time.Now()
			checkCtx, cancel := context.WithTimeout(ctx, check.Timeout)
			defer cancel()

			err := check.CheckFn(checkCtx)
			duration := time.Since(checkStart)

			mu.Lock()
			results = append(results, Status{
				Name:     check.Name,
				OK:       err == nil,
				Detail:   errDetail(err),
				Duration: duration,
			})
			mu.Unlock()
		}(c)
	}

	wg.Wait()

	// Determine overall status
	allOK := true
	for _, s := range results {
		if !s.OK {
			allOK = false
			break
		}
	}

	status := "ok"
	if !allOK {
		status = "degraded"
	}

	return Result{
		Status:   status,
		Checks:   results,
		Duration: time.Since(start),
	}
}

// RunChecks is a convenience function for one-shot checks without a registry.
func RunChecks(ctx context.Context, checks ...Check) Result {
	r := NewRegistry()
	for _, c := range checks {
		r.Register(c)
	}
	return r.Run(ctx)
}

// errDetail extracts a human-readable error message.
func errDetail(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// WriteJSON writes the health check result as JSON to an http.ResponseWriter.
func WriteJSON(w http.ResponseWriter, status int, result Result) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(result)
}

// WriteJSONCompact writes the health check result as compact JSON.
func WriteJSONCompact(w http.ResponseWriter, status int, result Result) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	data, _ := json.Marshal(result)
	_, _ = w.Write(data)
}

// HTTPHandler returns an http.HandlerFunc that runs the registry and writes the result.
func (r *Registry) HTTPHandler(statusCode int) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		result := r.Run(req.Context())
		if result.Status != "ok" {
			WriteJSON(w, http.StatusServiceUnavailable, result)
			return
		}
		WriteJSON(w, statusCode, result)
	}
}

// ReadyHandler returns an http.HandlerFunc suitable for /readiness.
// Returns 200 if all checks pass, 503 otherwise.
func (r *Registry) ReadyHandler() http.HandlerFunc {
	return r.HTTPHandler(http.StatusOK)
}

// LiveHandler returns an http.HandlerFunc suitable for /liveness.
// If a heartbeat is provided, it checks that the main event loops are
// still running (not deadlocked). Otherwise, always returns 200.
//
// Liveness never checks external dependencies (Redis, Postgres, etc.) —
// a dependency outage should not restart healthy application processes.
func LiveHandler(hb *Heartbeat) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// If heartbeat is provided, check that main loops are alive.
		// 10s threshold: allows for brief GC pauses but detects real deadlocks.
		if hb != nil && !hb.IsAlive(10 * time.Second) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ok","error":"heartbeat stale, possible deadlock"}`))
			return
		}

		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

// Summary returns a compact JSON summary suitable for logging.
func (r Result) Summary() string {
	return fmt.Sprintf(`{"status":"%s","checks":%d,"duration_ms":%d}`, r.Status, len(r.Checks), r.Duration.Milliseconds())
}
