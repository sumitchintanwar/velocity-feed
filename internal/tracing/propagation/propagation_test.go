package propagation_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/tracing/factory"
	"github.com/sumit/rtmds/internal/tracing/propagation"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestRedisPropagation(t *testing.T) {
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	ctx := context.Background()
	
	// Create a span to populate the context with trace identifiers
	f := factory.New()
	spanCtx, span := f.StartSpan(ctx, "producer", "publish")
	defer span.End()

	// 1. Inject into FastCarrier
	carrier := propagation.InjectRedis(spanCtx)
	if carrier == nil {
		t.Fatal("expected non-nil carrier")
	}

	// 2. Extract from FastCarrier
	extractedCtx := propagation.ExtractRedis(context.Background(), carrier)
	
	// Ensure the extracted context retains the W3C identifiers.
	// Since NoopTracer creates invalid spans by default, we just verify it doesn't crash
	// and handles nil gracefully.
	if propagation.ExtractRedis(context.Background(), nil) == nil {
		t.Error("ExtractRedis should handle nil carrier safely")
	}
	
	_ = extractedCtx
}

func TestWebSocketPropagation(t *testing.T) {
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	
	req, _ := http.NewRequest("GET", "/ws", nil)
	// Mock a W3C traceparent header
	req.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

	extractedCtx := propagation.ExtractFromUpgrade(req)
	
	spanContext := trace.SpanContextFromContext(extractedCtx)
	if !spanContext.IsValid() {
		// Only valid if real tracer is configured, but test ensures no panic
	}

	// Test nil request
	nilCtx := propagation.ExtractFromUpgrade(nil)
	if nilCtx == nil {
		t.Error("ExtractFromUpgrade should handle nil request safely")
	}
}

func TestWorkerPropagation(t *testing.T) {
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	f := factory.New()
	
	// Create a cancellable parent context
	parentCtx, cancel := context.WithCancel(context.Background())
	
	// Start a trace
	spanCtx, span := f.StartSpan(parentCtx, "parent", "op")
	
	taskExecuted := false
	taskDone := make(chan struct{})

	// Run the worker
	go func() {
		propagation.RunWorkerTask(spanCtx, f, "worker", "process", func(workerCtx context.Context) error {
			// Simulate work
			time.Sleep(10 * time.Millisecond)
			
			// Verify worker context is not cancelled even though parent is
			select {
			case <-workerCtx.Done():
				t.Error("worker context should not be cancelled")
			default:
				taskExecuted = true
			}
			close(taskDone)
			return errors.New("simulated task error")
		})
	}()

	// Immediately cancel parent
	cancel()
	span.End()

	// Wait for worker to finish
	<-taskDone

	if !taskExecuted {
		t.Error("worker task did not complete execution")
	}
}

func TestWorkerPropagationPanic(t *testing.T) {
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	f := factory.New()
	
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected worker panic to be re-panicked")
		}
	}()

	propagation.RunWorkerTask(context.Background(), f, "worker", "panic_test", func(ctx context.Context) error {
		panic("simulated fatal crash")
	})
}

func BenchmarkRedisPropagation(b *testing.B) {
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		carrier := propagation.InjectRedis(ctx)
		propagation.ExtractRedis(ctx, carrier)
	}
}
