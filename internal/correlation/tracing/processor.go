// Package tracing integrates the Correlation ID framework with OpenTelemetry.
package tracing

import (
	"context"

	corrcontext "github.com/sumit/rtmds/internal/correlation/context"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const AttrCorrelationID = attribute.Key("correlation_id")

// CorrelationSpanProcessor is an OpenTelemetry SpanProcessor that automatically
// extracts the Correlation ID from W3C Baggage and attaches it as a span attribute
// to every newly created span. This eliminates the need for manual span enrichment.
type CorrelationSpanProcessor struct{}

// NewCorrelationSpanProcessor creates a new CorrelationSpanProcessor.
func NewCorrelationSpanProcessor() sdktrace.SpanProcessor {
	return &CorrelationSpanProcessor{}
}

// OnStart is called when a span is started. It extracts the Correlation ID from
// the context baggage and sets it as a span attribute.
func (p *CorrelationSpanProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	corrID := corrcontext.CorrelationIDFromContext(parent)
	if corrID != "" {
		s.SetAttributes(AttrCorrelationID.String(corrID))
	}
}

// OnEnd is called when a span is ended. It is a no-op for this processor.
func (p *CorrelationSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {}

// Shutdown is called when the SDK shuts down. It is a no-op for this processor.
func (p *CorrelationSpanProcessor) Shutdown(ctx context.Context) error {
	return nil
}

// ForceFlush is called to export spans. It is a no-op for this processor.
func (p *CorrelationSpanProcessor) ForceFlush(ctx context.Context) error {
	return nil
}
