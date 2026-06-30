// Package middleware provides transport-specific correlation extractors and injectors.
package middleware

import (
	"net/http"

	corrcontext "github.com/sumit/rtmds/internal/correlation/context"
	"github.com/sumit/rtmds/internal/correlation/generator"
)

// HTTPMiddleware inspects the incoming request context (which should have already
// been populated by OpenTelemetry's standard HTTP propagators) for a Correlation ID
// in the Baggage. If missing, it generates a new one and adds it to the Baggage.
func HTTPMiddleware(gen generator.Generator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			// 1. Check if OpenTelemetry Baggage already has a Correlation ID
			corrID := corrcontext.CorrelationIDFromContext(ctx)
			if corrID == "" {
				// 2. Generate and inject into Baggage if missing
				corrID = gen.Generate()
				ctx = corrcontext.WithCorrelationID(ctx, corrID)
			}

			// 3. Pass down the enriched context
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
