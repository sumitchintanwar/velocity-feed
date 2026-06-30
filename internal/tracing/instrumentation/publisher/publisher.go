// Package publisher provides semantic tracing wrappers for the Publisher component.
package publisher

import (
	"context"

	"github.com/sumit/rtmds/internal/tracing/attributes"
	"github.com/sumit/rtmds/internal/tracing/factory"
	"go.opentelemetry.io/otel/trace"
)

// Tracer provides Publisher tracing capabilities.
type Tracer interface {
	StartSerializeSpan(ctx context.Context, topic string) (context.Context, trace.Span)
	StartRedisPublishSpan(ctx context.Context, topic string) (context.Context, trace.Span)
}

type tracerImpl struct {
	f *factory.Factory
}

// NewTracer creates a new Publisher Tracer.
func NewTracer(f *factory.Factory) Tracer {
	return &tracerImpl{f: f}
}

// StartSerializeSpan initiates an Internal span for event serialization.
func (t *tracerImpl) StartSerializeSpan(ctx context.Context, topic string) (context.Context, trace.Span) {
	spanCtx, span := t.f.StartSpanWithKind(ctx, "publisher", "serialize", trace.SpanKindInternal)
	
	span.SetAttributes(
		attributes.Topic(topic),
	)
	
	return spanCtx, span
}

// StartRedisPublishSpan initiates a Producer span for writing an event to Redis.
func (t *tracerImpl) StartRedisPublishSpan(ctx context.Context, topic string) (context.Context, trace.Span) {
	spanCtx, span := t.f.StartSpanWithKind(ctx, "publisher", "redis.publish", trace.SpanKindProducer)
	
	span.SetAttributes(
		attributes.Topic(topic),
		attributes.MessagingSystem("redis"),
	)
	
	return spanCtx, span
}

// StartSerializeSpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartSerializeSpan(ctx context.Context, f *factory.Factory, topic string) (context.Context, trace.Span) {
	return NewTracer(f).StartSerializeSpan(ctx, topic)
}

// StartRedisPublishSpan is a legacy static wrapper.
// Deprecated: Inject the Tracer interface instead.
func StartRedisPublishSpan(ctx context.Context, f *factory.Factory, topic string) (context.Context, trace.Span) {
	return NewTracer(f).StartRedisPublishSpan(ctx, topic)
}
