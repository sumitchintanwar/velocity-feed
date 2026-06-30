// Package middleware provides tracing propagators for external boundaries.
package middleware

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// HTTPTracing returns a standard HTTP middleware that extracts W3C trace context
// from incoming request headers, creates a Server span, and injects the span
// context into the request's context.Context.
func HTTPTracing(operation string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		// otelhttp automatically handles propagation, span creation, and metric tracking
		return otelhttp.NewHandler(next, operation)
	}
}
