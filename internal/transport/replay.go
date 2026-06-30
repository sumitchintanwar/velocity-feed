package transport

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/eventlog"
	"github.com/sumit/rtmds/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// maxConcurrentQueriesPerClient is the maximum number of concurrent replay
// queries allowed per client IP. Prevents slow queries from exhausting the
// database connection pool.
const maxConcurrentQueriesPerClient = 3

// concurrencyLimiter tracks concurrent active queries per client IP.
type concurrencyLimiter struct {
	mu       sync.Mutex
	active   map[string]int
	maxPerIP int
}

// newConcurrencyLimiter creates a limiter with the given per-client max.
func newConcurrencyLimiter(perClient int) *concurrencyLimiter {
	return &concurrencyLimiter{
		active:   make(map[string]int),
		maxPerIP: perClient,
	}
}

// acquire tries to acquire a slot for the given client IP.
// Returns true if allowed, false if at capacity.
func (cl *concurrencyLimiter) acquire(clientIP string) bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.active[clientIP] >= cl.maxPerIP {
		return false
	}
	cl.active[clientIP]++
	return true
}

// release frees a slot for the given client IP.
func (cl *concurrencyLimiter) release(clientIP string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.active[clientIP]--
	if cl.active[clientIP] <= 0 {
		delete(cl.active, clientIP)
	}
}

// getClientIP extracts the client IP from the request, considering
// X-Forwarded-For and X-Real-IP headers for proxied connections.
func getClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// handleReplay returns an HTTP handler for querying historical market events.
//
// Trace boundary: "replay.request" — covers the entire replay operation
// from query parsing to database response. This is one of the most valuable
// trace targets per the design spec: it reveals slow queries, serialization
// bottlenecks, and large responses.
//
// Query parameters:
//
//	symbol  - filter by symbol (optional)
//	from    - start time, RFC3339 format (optional)
//	to      - end time, RFC3339 format (optional)
//	cursor  - composite cursor "timestamp,event_id" to start after (optional)
//	limit   - max events per page, 1-1000, default 100 (optional)
func handleReplay(repo eventlog.Repository, limiter *concurrencyLimiter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientIP := getClientIP(r)

		// Start a span for the replay request.
		ctx, span := tracing.TracerForComponent("replay").Start(r.Context(), "replay.request",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("client.ip", clientIP),
			),
		)
		defer span.End()

		// Concurrency rate limiting: reject if client has too many active queries.
		if !limiter.acquire(clientIP) {
			span.SetAttributes(attribute.Bool("error", true))
			span.AddEvent("rate_limited", trace.WithAttributes(
				attribute.Int("concurrent_queries", maxConcurrentQueriesPerClient),
			))
			writeError(w, http.StatusTooManyRequests,
				"concurrent query limit exceeded (max "+strconv.Itoa(maxConcurrentQueriesPerClient)+" per client)")
			return
		}
		defer limiter.release(clientIP)

		q := eventlog.ReplayQuery{}

		// Parse symbol.
		q.Symbol = r.URL.Query().Get("symbol")
		if q.Symbol != "" {
			span.SetAttributes(attribute.String("replay.symbol", q.Symbol))
		}

		// Parse from time.
		if fromStr := r.URL.Query().Get("from"); fromStr != "" {
			t, err := time.Parse(time.RFC3339Nano, fromStr)
			if err != nil {
				span.RecordError(err)
				writeError(w, http.StatusBadRequest, "invalid 'from' parameter: "+err.Error())
				return
			}
			q.From = t
			span.SetAttributes(attribute.String("replay.from", fromStr))
		}

		// Parse to time.
		if toStr := r.URL.Query().Get("to"); toStr != "" {
			t, err := time.Parse(time.RFC3339Nano, toStr)
			if err != nil {
				span.RecordError(err)
				writeError(w, http.StatusBadRequest, "invalid 'to' parameter: "+err.Error())
				return
			}
			q.To = t
			span.SetAttributes(attribute.String("replay.to", toStr))
		}

		// Parse composite cursor: "timestamp,event_id".
		if cursorStr := r.URL.Query().Get("cursor"); cursorStr != "" {
			cursor, err := parseCursor(cursorStr)
			if err != nil {
				span.RecordError(err)
				writeError(w, http.StatusBadRequest, "invalid 'cursor' parameter: "+err.Error())
				return
			}
			q.Cursor = cursor
		}

		// Parse limit.
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			l, err := strconv.Atoi(limitStr)
			if err != nil || l < 1 {
				span.RecordError(err)
				writeError(w, http.StatusBadRequest, "invalid 'limit' parameter: must be between 1 and 1000")
				return
			}
			q.Limit = l
			span.SetAttributes(attribute.Int("replay.limit", l))
		}

		span.AddEvent("query_started")

		result, err := repo.QueryEvents(ctx, q)
		if err != nil {
			span.RecordError(err)
			writeError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}

		span.AddEvent("query_completed", trace.WithAttributes(
			attribute.Int("replay.result_count", len(result.Events)),
		))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}

// parseCursor parses a composite cursor string "RFC3339Nano,eventID".
func parseCursor(s string) (eventlog.Cursor, error) {
	parts := strings.SplitN(s, ",", 2)
	if len(parts) != 2 {
		return eventlog.Cursor{}, fmt.Errorf("format must be 'timestamp,event_id'")
	}
	ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(parts[0]))
	if err != nil {
		return eventlog.Cursor{}, fmt.Errorf("invalid timestamp: %w", err)
	}
	eid, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return eventlog.Cursor{}, fmt.Errorf("invalid event_id: %w", err)
	}
	return eventlog.Cursor{Timestamp: ts, EventID: eid}, nil
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
	})
}
