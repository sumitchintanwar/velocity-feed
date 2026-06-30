package propagation

import (
	"context"
	"testing"

	corrcontext "github.com/sumit/rtmds/internal/correlation/context"
)

func TestRedisPropagation(t *testing.T) {
	ctx := context.Background()
	ctx = corrcontext.WithCorrelationID(ctx, "test-correlation-123")

	carrier := NewFastCarrier()

	// Simulate producer injecting context into Redis headers
	Inject(ctx, carrier)

	if len(carrier.Keys()) == 0 {
		t.Fatal("expected carrier to be populated")
	}

	// Simulate consumer extracting context from Redis headers
	newCtx := Extract(context.Background(), carrier)

	extractedID := corrcontext.CorrelationIDFromContext(newCtx)
	if extractedID != "test-correlation-123" {
		t.Errorf("expected 'test-correlation-123', got %q", extractedID)
	}
}

func TestWorkerPropagation(t *testing.T) {
	ctx := context.Background()
	ctx = corrcontext.WithCorrelationID(ctx, "worker-corr-456")

	executed := false
	workerFunc := func(workerCtx context.Context) {
		extracted := corrcontext.CorrelationIDFromContext(workerCtx)
		if extracted != "worker-corr-456" {
			t.Errorf("expected 'worker-corr-456', got %q", extracted)
		}
		executed = true
	}

	wrapped := WrapWorkerFunc(ctx, workerFunc)
	wrapped() // execute the closure

	if !executed {
		t.Error("worker function was not executed")
	}
}

func BenchmarkRedisInject(b *testing.B) {
	ctx := context.Background()
	ctx = corrcontext.WithCorrelationID(ctx, "benchmark-correlation-id")
	
	carrier := NewFastCarrier()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		carrier.Reset()
		Inject(ctx, carrier)
	}
}

func BenchmarkRedisExtract(b *testing.B) {
	ctx := context.Background()
	ctx = corrcontext.WithCorrelationID(ctx, "benchmark-correlation-id")
	
	carrier := NewFastCarrier()
	Inject(ctx, carrier)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Extract(context.Background(), carrier)
	}
}
