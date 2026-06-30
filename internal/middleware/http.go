package middleware

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	corrmw "github.com/sumit/rtmds/internal/correlation/middleware"
	"github.com/sumit/rtmds/internal/correlation/generator"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// HTTPPipeline creates the centralized production observability middleware stack.
// The execution order is critical for context and trace propagation.
// serviceName is used as the OTel component name for span naming.
// gen is the correlation ID generator, injected via DI for testability.
func HTTPPipeline(baseLogger *log.Logger, metrics *platform.Metrics, serviceName string, gen generator.Generator) []func(http.Handler) http.Handler {
	return []func(http.Handler) http.Handler{
		// 1. Context Creation: Ensure request IDs and IP addresses are available.
		chimw.RequestID,
		chimw.RealIP,

		// 2. Timeout: Enforce a global request deadline to prevent cascading failures.
		// If a downstream service hangs, the context is cancelled after 30s.
		chimw.Timeout(30 * time.Second),

		// 3. Correlation ID: Extract from inbound headers, inject into W3C Baggage and context.
		// Uses the correlation middleware that properly propagates via W3C Baggage,
		// ensuring the correlation ID survives across Redis pub/sub boundaries.
		corrmw.HTTPMiddleware(gen),

		// 4. Tracing Context: Extract W3C Traceparent and start the OTel span.
		tracingMiddleware(serviceName),

		// 5. Trace-Log Correlation: Extract span/trace ID and put it in context for logger.
		traceLogMiddleware(),

		// 6. Logger Injection: Create request-scoped context-aware logger with trace/correlation fields.
		loggerMiddleware(baseLogger),

		// 7. Panic Recovery: Must run inside the logger/tracer to fully correlate the panic log.
		// Uses the OTel span to mark the trace as failed when a panic occurs.
		recoveryMiddleware(baseLogger),

		// 8. Metrics Timer: Record route latencies.
		prometheusMiddleware(metrics),
	}
}

// tracingMiddleware wraps the HTTP handler with OpenTelemetry instrumentation.
func tracingMiddleware(serviceName string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return otelhttp.NewHandler(next, serviceName,
			otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
				route := chi.RouteContext(r.Context()).RoutePattern()
				if route == "" {
					route = "unknown"
				}
				return fmt.Sprintf("%s %s %s", serviceName, r.Method, route)
			}),
		)
	}
}

// traceLogMiddleware injects trace_id and span_id from the current OTel span
// into the context. This enables log → trace correlation.
func traceLogMiddleware() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			span := trace.SpanFromContext(r.Context())
			if span != nil {
				sc := span.SpanContext()
				if sc.TraceID().IsValid() {
					r = r.WithContext(log.WithTraceFields(r.Context(),
						sc.TraceID().String(),
						sc.SpanID().String(),
					))
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// loggerMiddleware returns a middleware that logs each request
// with method, path, status, latency, and request-ID using the internal log package.
//
// Logging levels are based on response status to prevent I/O saturation at high RPM:
//   - Status >= 500: Error (always visible)
//   - Status >= 400: Warn (always visible)
//   - Status < 400: Debug (typically disabled in production)
func loggerMiddleware(baseLogger *log.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			status := ww.Status()
			latency := time.Since(start)
			baseFields := func(e *zerolog.Event) *zerolog.Event {
				return e.
					Str("event", "http_request_completed").
					Str("method", r.Method).
					Str("path", r.URL.Path).
					Int("status", status).
					Dur("latency", latency).
					Str("request_id", chimw.GetReqID(r.Context()))
			}

			switch {
			case status >= 500:
				baseFields(log.Error(r.Context(), baseLogger)).Msg("http request")
			case status >= 400:
				baseFields(log.Warn(r.Context(), baseLogger)).Msg("http request")
			default:
				baseFields(log.Debug(r.Context(), baseLogger)).Msg("http request")
			}
		})
	}
}

// recoveryMiddleware recovers from panics, marks the OpenTelemetry span as failed,
// logs the error using the context-aware logger, and returns a 500 response.
//
// If the handler has already written response headers before panicking (e.g., started
// streaming JSON), the middleware detects this via WrapResponseWriter and skips the
// 500 write to avoid the superfluous WriteHeader warning. The connection is aborted
// instead, which is the only correct behavior when headers are already flushed.
func recoveryMiddleware(baseLogger *log.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Wrap the ResponseWriter BEFORE the defer so we can inspect header state
			// inside the panic handler. The raw http.ResponseWriter has no Status() method.
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			defer func() {
				if rvr := recover(); rvr != nil {
					err, ok := rvr.(error)
					if !ok {
						err = fmt.Errorf("%v", rvr)
					}

					// Log the panic with full correlation context.
					log.Error(r.Context(), baseLogger).
						Err(err).
						Str("event", "panic_recovered").
						Str("stack", string(debug.Stack())).
						Msg("panic recovered during http request")

					// Mark the OTel span as failed so the trace is visible in Jaeger.
					span := trace.SpanFromContext(r.Context())
					if span != nil {
						span.RecordError(err)
						span.SetStatus(codes.Error, "panic recovered")
					}

					// Only write 500 if headers haven't been sent yet.
					// ww.Status() returns 0 until WriteHeader is called, so this check
					// distinguishes between "handler completed normally" (status != 0)
					// and "handler panicked before writing headers" (status == 0).
					if ww.Status() == 0 {
						http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
					}
				}
			}()

			next.ServeHTTP(ww, r)
		})
	}
}

// prometheusMiddleware records HTTP request counts and latencies.
func prometheusMiddleware(m *platform.Metrics) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unknown"
			}
			status := fmt.Sprintf("%d", ww.Status())
			dur := time.Since(start).Seconds()

			m.HTTPRequestsTotal.WithLabelValues(r.Method, route, status).Inc()
			m.HTTPRequestDuration.WithLabelValues(r.Method, route).Observe(dur)
		})
	}
}
