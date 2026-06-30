// Package recovery provides semantic tracing wrappers for the Recovery Manager.
package recovery

import (
	"context"

	"github.com/sumit/rtmds/internal/tracing/factory"
	"go.opentelemetry.io/otel/trace"
)

// Tracer provides Recovery tracing capabilities.
type Tracer interface {
	StartStartupSpan(ctx context.Context) (context.Context, trace.Span)
	StartSyncSpan(ctx context.Context) (context.Context, trace.Span)
}

type tracerImpl struct {
	f *factory.Factory
}

// NewTracer creates a new Recovery Tracer.
func NewTracer(f *factory.Factory) Tracer {
	return &tracerImpl{f: f}
}

// StartStartupSpan initiates an Internal span representing the startup sequence.
func (t *tracerImpl) StartStartupSpan(ctx context.Context) (context.Context, trace.Span) {
	return t.f.StartSpanWithKind(ctx, "recovery", "startup", trace.SpanKindInternal)
}

// StartSyncSpan initiates an Internal span representing the state synchronization phase.
func (t *tracerImpl) StartSyncSpan(ctx context.Context) (context.Context, trace.Span) {
	return t.f.StartSpanWithKind(ctx, "recovery", "sync", trace.SpanKindInternal)
}

// StartStartupSpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartStartupSpan(ctx context.Context, f *factory.Factory) (context.Context, trace.Span) {
	return NewTracer(f).StartStartupSpan(ctx)
}

// StartSyncSpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartSyncSpan(ctx context.Context, f *factory.Factory) (context.Context, trace.Span) {
	return NewTracer(f).StartSyncSpan(ctx)
}
