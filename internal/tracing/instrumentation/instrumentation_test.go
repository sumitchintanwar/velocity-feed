package instrumentation_test

import (
	"context"
	"testing"

	"github.com/sumit/rtmds/internal/tracing/factory"
	"github.com/sumit/rtmds/internal/tracing/instrumentation/postgres"
	"github.com/sumit/rtmds/internal/tracing/instrumentation/publisher"
	"github.com/sumit/rtmds/internal/tracing/instrumentation/recovery"
	"github.com/sumit/rtmds/internal/tracing/instrumentation/replay"
	"github.com/sumit/rtmds/internal/tracing/instrumentation/snapshot"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestInstrumentationPhase1(t *testing.T) {
	// Use NoopTracer so tests run without panic and without backend overhead
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	
	f := factory.New()
	ctx := context.Background()

	t.Run("Postgres", func(t *testing.T) {
		_, span := postgres.StartQuerySpan(ctx, f, "SELECT * FROM events")
		if span == nil {
			t.Error("expected valid span for postgres query")
		}
		span.End()
	})

	t.Run("Snapshot", func(t *testing.T) {
		_, span1 := snapshot.StartSaveSpan(ctx, f, "FULL")
		span1.End()

		_, span2 := snapshot.StartRestoreSpan(ctx, f, "DELTA")
		span2.End()
	})

	t.Run("Replay", func(t *testing.T) {
		_, span1 := replay.StartQueryPlanningSpan(ctx, f, "HISTORICAL")
		span1.End()

		_, span2 := replay.StartSchedulerSpan(ctx, f)
		span2.End()
	})

	t.Run("Recovery", func(t *testing.T) {
		_, span1 := recovery.StartStartupSpan(ctx, f)
		span1.End()

		_, span2 := recovery.StartSyncSpan(ctx, f)
		span2.End()
	})

	t.Run("Publisher", func(t *testing.T) {
		_, span1 := publisher.StartSerializeSpan(ctx, f, "BTC_USD")
		span1.End()

		_, span2 := publisher.StartRedisPublishSpan(ctx, f, "BTC_USD")
		span2.End()
	})
}
