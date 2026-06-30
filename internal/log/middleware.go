package log

import (
	"context"
	"net/http"
	"runtime/debug"

	"github.com/google/uuid"
)

// CorrelationIDHeader is the HTTP header used for end-to-end correlation.
const CorrelationIDHeader = "X-Correlation-ID"

// CorrelationID is HTTP middleware that assigns a correlation ID to every
// request. If the request already carries an X-Correlation-ID header,
// that value is used; otherwise a new UUID v4 is generated.
//
// The correlation ID is stored in the request context and automatically
// included in all log entries created via Info(), Warn(), Error(), Debug().
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationIDHeader)
		if id == "" {
			id = uuid.New().String()
		}

		ctx := SetCorrelationID(r.Context(), id)
		w.Header().Set(CorrelationIDHeader, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Recovery is HTTP middleware that catches panics, logs them as structured
// JSON FATAL entries with the stack trace, and returns HTTP 500.
// The underlying process is NOT terminated — callers can decide whether
// to exit based on their shutdown strategy.
func Recovery(l *Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					stack := debug.Stack()

					Error(r.Context(), l).
						Str("event", "panic_recovered").
						Interface("panic_value", rec).
						Str("stack", string(stack)).
						Msg("panic recovered in HTTP handler")

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"internal server error"}`))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RecoverWithContext is a helper for recovering from panics in goroutines.
// Call it as `defer log.RecoverWithContext(ctx, l)` at the top of a goroutine.
// If a panic occurs, it logs the panic value and stack trace as a FATAL entry.
func RecoverWithContext(ctx context.Context, l *Logger) {
	if rec := recover(); rec != nil {
		stack := debug.Stack()
		Error(ctx, l).
			Str("event", "panic_recovered").
			Interface("panic_value", rec).
			Str("stack", string(stack)).
			Msg("panic recovered in goroutine")
	}
}
