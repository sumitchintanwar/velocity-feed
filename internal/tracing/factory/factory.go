// Package factory provides dependency injection wrappers for OpenTelemetry tracers.
package factory

import (
	"context"

	"github.com/sumit/rtmds/internal/tracing/attributes"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Factory abstracts tracer creation to allow for future extensibility and mocking.
type Factory struct{}

// New initializes a new Factory.
func New() *Factory {
	return &Factory{}
}

// Tracer returns an OpenTelemetry Tracer initialized with the component name.
func (f *Factory) Tracer(component string) trace.Tracer {
	// The otel package uses the globally registered TracerProvider.
	return otel.Tracer(component)
}

// StartSpan is a convenience helper that starts a span with standard component attributes.
func (f *Factory) StartSpan(ctx context.Context, component, operation string) (context.Context, trace.Span) {
	return f.StartSpanWithKind(ctx, component, operation, trace.SpanKindInternal)
}

// StartSpanWithKind is a helper that starts a span with a specific trace.SpanKind (e.g. Consumer, Producer).
func (f *Factory) StartSpanWithKind(ctx context.Context, component, operation string, kind trace.SpanKind) (context.Context, trace.Span) {
	tracer := f.Tracer(component)
	return tracer.Start(ctx, operation, trace.WithSpanKind(kind), trace.WithAttributes(attributes.Component(component)))
}

// StartLinkedSpan creates a new root span that is linked to a remote context (e.g. from an async Publisher).
func (f *Factory) StartLinkedSpan(ctx, linkCtx context.Context, component, operation string, kind trace.SpanKind) (context.Context, trace.Span) {
	tracer := f.Tracer(component)
	link := trace.LinkFromContext(linkCtx)
	return tracer.Start(
		ctx, 
		operation, 
		trace.WithSpanKind(kind),
		trace.WithAttributes(attributes.Component(component)),
		trace.WithLinks(link),
	)
}

// RecordError is a convenience helper that records an error on the span
// and automatically sets the span status to Error.
func (f *Factory) RecordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
