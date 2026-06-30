package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/sumit/rtmds/internal/platform/admin/audit"
)

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// Audit creates a middleware that records an audit event for every incoming operational command.
func Audit(logger audit.Logger, actionName string, targetService string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Wrap the writer to capture the response status
			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
			
			// Capture source IP (handles proxies if present, simplified for local)
			sourceIp := r.RemoteAddr
			if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
				sourceIp = strings.Split(forwarded, ",")[0]
			}

			// Generate correlation ID or extract it from headers/context
			reqID := r.Header.Get("X-Request-ID")
			corrID := r.Header.Get("X-Correlation-ID")
			
			// Serve the request
			next.ServeHTTP(rw, r)

			duration := time.Since(start)

			// Determine outcome based on status code
			outcome := audit.OutcomeSuccess
			errMsg := ""
			if rw.status >= 400 {
				outcome = audit.OutcomeFailure
				errMsg = http.StatusText(rw.status)
			}

			// Record audit event
			event := audit.Event{
				Timestamp:     time.Now(),
				UserIdentity:  GetUserIdentity(r.Context()),
				SourceIP:      sourceIp,
				RequestID:     reqID,
				CorrelationID: corrID,
				Action:        actionName,
				TargetService: targetService,
				Outcome:       outcome,
				Duration:      duration,
				ErrorMessage:  errMsg,
			}

			logger.Record(r.Context(), event)
		})
	}
}
