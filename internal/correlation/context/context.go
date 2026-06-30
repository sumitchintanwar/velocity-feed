// Package context provides strongly-typed context injection and extraction for correlation.
package context

import (
	"context"

	"go.opentelemetry.io/otel/baggage"
)

const CorrelationIDKey = "correlation_id"

// WithCorrelationID returns a new context with the given Correlation ID
// safely embedded into W3C Baggage, enabling cross-boundary propagation.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	m, err := baggage.NewMember(CorrelationIDKey, id)
	if err != nil {
		return ctx
	}

	b := baggage.FromContext(ctx)
	b, err = b.SetMember(m)
	if err != nil {
		return ctx
	}

	return baggage.ContextWithBaggage(ctx, b)
}

// CorrelationIDFromContext extracts the Correlation ID from the W3C Baggage in the context.
// Returns empty string if not found.
func CorrelationIDFromContext(ctx context.Context) string {
	b := baggage.FromContext(ctx)
	m := b.Member(CorrelationIDKey)
	return m.Value()
}
