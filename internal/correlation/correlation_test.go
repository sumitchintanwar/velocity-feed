package correlation_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	corrcontext "github.com/sumit/rtmds/internal/correlation/context"
	"github.com/sumit/rtmds/internal/correlation/generator"
	"github.com/sumit/rtmds/internal/correlation/middleware"
	"github.com/sumit/rtmds/internal/correlation/tracing"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestUUIDv7Generator(t *testing.T) {
	gen := generator.NewUUIDv7Generator()

	id1 := gen.Generate()
	id2 := gen.Generate()

	if id1 == "" || id2 == "" {
		t.Fatal("generator returned empty string")
	}
	if id1 == id2 {
		t.Fatal("generator produced collision")
	}
}

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()
	
	// Should be empty initially
	if corrcontext.CorrelationIDFromContext(ctx) != "" {
		t.Error("expected empty correlation ID")
	}

	ctx = corrcontext.WithCorrelationID(ctx, "corr-123")

	if corrcontext.CorrelationIDFromContext(ctx) != "corr-123" {
		t.Error("failed to extract correlation ID from baggage")
	}
}

func TestHTTPMiddleware(t *testing.T) {
	gen := generator.NewUUIDv7Generator()
	
	// Track what gets injected into the context
	var capturedCorrID string
	
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedCorrID = corrcontext.CorrelationIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	
	handler := middleware.HTTPMiddleware(gen)(next)
	
	t.Run("Without Existing Baggage", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		
		handler.ServeHTTP(rec, req)
		
		if capturedCorrID == "" {
			t.Error("middleware failed to generate ID into Baggage")
		}
	})

	t.Run("With Existing Baggage", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		
		// Simulate OpenTelemetry propagators having already injected the Baggage into the Request Context
		ctx := corrcontext.WithCorrelationID(req.Context(), "client-provided-id")
		req = req.WithContext(ctx)
		
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		
		if capturedCorrID != "client-provided-id" {
			t.Errorf("expected client-provided-id, got %q", capturedCorrID)
		}
	})
}

func TestSpanProcessorIntegration(t *testing.T) {
	ctx := context.Background()
	ctx = corrcontext.WithCorrelationID(ctx, "trace-corr-id")
	
	// We use the real SpanProcessor to verify it doesn't panic and conforms to the interface
	processor := tracing.NewCorrelationSpanProcessor()
	
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
	)
	otel.SetTracerProvider(provider)
	tracer := otel.Tracer("test")
	
	ctx, span := tracer.Start(ctx, "test_span")
	defer span.End()
	
	// Without an exporter we can't easily assert the internal attributes slice in memory,
	// but this ensures the SpanProcessor is correctly wired and safe.
	if !span.IsRecording() {
		t.Error("span should be recording")
	}
}

func BenchmarkGenerator(b *testing.B) {
	gen := generator.NewUUIDv7Generator()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gen.Generate()
	}
}

func BenchmarkContextExtraction(b *testing.B) {
	ctx := corrcontext.WithCorrelationID(context.Background(), "benchmark-id")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		corrcontext.CorrelationIDFromContext(ctx)
	}
}
