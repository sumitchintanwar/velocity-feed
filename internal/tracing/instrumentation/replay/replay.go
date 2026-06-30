// Package replay provides semantic tracing wrappers for the Replay API.
package replay

import (
	"context"

	"github.com/sumit/rtmds/internal/tracing/attributes"
	"github.com/sumit/rtmds/internal/tracing/factory"
	"go.opentelemetry.io/otel/trace"
)

// Tracer provides Replay tracing capabilities.
type Tracer interface {
	StartQueryPlanningSpan(ctx context.Context, mode string) (context.Context, trace.Span)
	StartSchedulerSpan(ctx context.Context) (context.Context, trace.Span)
}

type tracerImpl struct {
	f *factory.Factory
}

// NewTracer creates a new Replay Tracer.
func NewTracer(f *factory.Factory) Tracer {
	return &tracerImpl{f: f}
}

// StartQueryPlanningSpan initiates an Internal span for planning a replay query.
func (t *tracerImpl) StartQueryPlanningSpan(ctx context.Context, mode string) (context.Context, trace.Span) {
	spanCtx, span := t.f.StartSpanWithKind(ctx, "replay", "query_planning", trace.SpanKindInternal)
	
	span.SetAttributes(
		attributes.ReplayMode(mode),
	)
	
	return spanCtx, span
}

// StartSchedulerSpan initiates an Internal span representing the event scheduling loop.
func (t *tracerImpl) StartSchedulerSpan(ctx context.Context) (context.Context, trace.Span) {
	return t.f.StartSpanWithKind(ctx, "replay", "schedule", trace.SpanKindInternal)
}

// StartQueryPlanningSpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartQueryPlanningSpan(ctx context.Context, f *factory.Factory, mode string) (context.Context, trace.Span) {
	return NewTracer(f).StartQueryPlanningSpan(ctx, mode)
}

// StartSchedulerSpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartSchedulerSpan(ctx context.Context, f *factory.Factory) (context.Context, trace.Span) {
	return NewTracer(f).StartSchedulerSpan(ctx)
}
