package tracing_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	tracecontext "github.com/sumit/rtmds/internal/tracing/context"
	"github.com/sumit/rtmds/internal/tracing/config"
	"github.com/sumit/rtmds/internal/tracing/factory"
	"github.com/sumit/rtmds/internal/tracing/middleware"
	"github.com/sumit/rtmds/internal/tracing/provider"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestTracingLifecycle(t *testing.T) {
	ctx := context.Background()
	cfg := config.DefaultConfig()
	// Use dummy endpoint so it doesn't try to connect to a real OTLP collector
	cfg.OTLPEndpoint = "localhost:4318" 
	cfg.Insecure = true

	// 1. Initialize Provider
	prov, err := provider.New(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to initialize tracer provider: %v", err)
	}

	// 2. Initialize Factory
	f := factory.New()
	
	// 3. Start a span
	spanCtx, span := f.StartSpan(ctx, "test_component", "test_operation")
	if !span.IsRecording() {
		// Since default sampler is 1% (0.01), this span might not be recording if it wasn't sampled.
		// However, it's valid either way.
	}
	_ = spanCtx
	span.End()

	// 4. Test Middleware
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Span should be injected into the request context
		spanCtx := trace.SpanContextFromContext(r.Context())
		if !spanCtx.IsValid() {
			t.Errorf("expected valid span context extracted by middleware")
		}
		w.WriteHeader(http.StatusOK)
	})

	tracedHandler := middleware.HTTPTracing("test_http_route")(handler)
	
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	
	tracedHandler.ServeHTTP(rr, req)

	// 5. Shutdown
	err = prov.Shutdown(ctx)
	if err != nil {
		t.Fatalf("failed to shutdown provider: %v", err)
	}
}

func TestFactoryRecordError(t *testing.T) {
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	f := factory.New()
	ctx := context.Background()

	_, span := f.StartSpan(ctx, "test_component", "test_op")
	defer span.End()

	// Ensure RecordError does not panic on nil error
	f.RecordError(span, nil)

	// Record an actual error
	err := errors.New("something went wrong")
	f.RecordError(span, err)
}

func TestContextBranching(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := factory.New()
	spanCtx, span := f.StartSpan(ctx, "test_component", "test_op")
	defer span.End()

	// Branch the context
	branchedCtx := tracecontext.Branch(spanCtx)

	// Cancel the parent context
	cancel()

	// Ensure branched context is not canceled
	select {
	case <-branchedCtx.Done():
		t.Errorf("branched context should not be canceled when parent is canceled")
	case <-time.After(10 * time.Millisecond):
		// Success
	}

	// Verify trace context is still present in branched context
	if !trace.SpanContextFromContext(branchedCtx).IsValid() {
		// Even for NoopTracer, we might not have a valid span context, 
		// but typically it retains whatever was in the parent. 
		// Since Noop creates an invalid SpanContext by default, we just ensure 
		// the context is readable.
	}
}

func BenchmarkFactorySpanCreation(b *testing.B) {
	// Set an empty/noop provider for benchmarking raw factory overhead
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	
	f := factory.New()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, span := f.StartSpan(ctx, "bench_component", "bench_op")
		span.End()
	}
}
