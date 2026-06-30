// Package postgres provides semantic tracing wrappers for PostgreSQL operations.
package postgres

import (
	"context"

	"github.com/sumit/rtmds/internal/tracing/attributes"
	"github.com/sumit/rtmds/internal/tracing/factory"
	"go.opentelemetry.io/otel/trace"
)

// Tracer provides PostgreSQL tracing capabilities.
type Tracer interface {
	StartQuerySpan(ctx context.Context, operation string) (context.Context, trace.Span)
}

type tracerImpl struct {
	f *factory.Factory
}

// NewTracer creates a new Postgres Tracer.
func NewTracer(f *factory.Factory) Tracer {
	return &tracerImpl{f: f}
}

// StartQuerySpan initiates a Client span for a database operation with standard
// OpenTelemetry DB attributes, keeping DB instrumentation decoupled from business logic.
func (t *tracerImpl) StartQuerySpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	spanCtx, span := t.f.StartSpanWithKind(ctx, "postgresql", operation, trace.SpanKindClient)
	
	span.SetAttributes(
		attributes.DBSystem("postgresql"),
		attributes.DBOperation(operation),
	)
	
	return spanCtx, span
}

// StartQuerySpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartQuerySpan(ctx context.Context, f *factory.Factory, operation string) (context.Context, trace.Span) {
	return NewTracer(f).StartQuerySpan(ctx, operation)
}
