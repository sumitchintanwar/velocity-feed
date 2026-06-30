// Package snapshot provides semantic tracing wrappers for the Snapshot Service.
package snapshot

import (
	"context"

	"github.com/sumit/rtmds/internal/tracing/attributes"
	"github.com/sumit/rtmds/internal/tracing/factory"
	"go.opentelemetry.io/otel/trace"
)

// Tracer provides Snapshot tracing capabilities.
type Tracer interface {
	StartSaveSpan(ctx context.Context, snapType string) (context.Context, trace.Span)
	StartRestoreSpan(ctx context.Context, snapType string) (context.Context, trace.Span)
}

type tracerImpl struct {
	f *factory.Factory
}

// NewTracer creates a new Snapshot Tracer.
func NewTracer(f *factory.Factory) Tracer {
	return &tracerImpl{f: f}
}

// StartSaveSpan initiates an Internal span representing the process of saving a snapshot.
func (t *tracerImpl) StartSaveSpan(ctx context.Context, snapType string) (context.Context, trace.Span) {
	spanCtx, span := t.f.StartSpanWithKind(ctx, "snapshot", "save", trace.SpanKindInternal)
	
	span.SetAttributes(
		attributes.SnapshotType(snapType),
	)
	
	return spanCtx, span
}

// StartRestoreSpan initiates an Internal span representing the process of restoring from a snapshot.
func (t *tracerImpl) StartRestoreSpan(ctx context.Context, snapType string) (context.Context, trace.Span) {
	spanCtx, span := t.f.StartSpanWithKind(ctx, "snapshot", "restore", trace.SpanKindInternal)
	
	span.SetAttributes(
		attributes.SnapshotType(snapType),
	)
	
	return spanCtx, span
}

// StartSaveSpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartSaveSpan(ctx context.Context, f *factory.Factory, snapType string) (context.Context, trace.Span) {
	return NewTracer(f).StartSaveSpan(ctx, snapType)
}

// StartRestoreSpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartRestoreSpan(ctx context.Context, f *factory.Factory, snapType string) (context.Context, trace.Span) {
	return NewTracer(f).StartRestoreSpan(ctx, snapType)
}
