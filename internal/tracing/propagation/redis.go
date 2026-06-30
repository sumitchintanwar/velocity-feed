// Package propagation provides helpers for extracting and injecting OpenTelemetry
// context across various architectural boundaries.
package propagation

import (
	"context"

	"go.opentelemetry.io/otel"
)

// FastCarrier is a custom zero-allocation OpenTelemetry TextMapCarrier.
// It explicitly stores the two standard W3C Trace Context headers.
// It is fully JSON serializable so it can be embedded directly into message envelopes.
type FastCarrier struct {
	TraceParent string `json:"traceparent,omitempty"`
	TraceState  string `json:"tracestate,omitempty"`
}

func (c *FastCarrier) Get(key string) string {
	switch key {
	case "traceparent":
		return c.TraceParent
	case "tracestate":
		return c.TraceState
	default:
		return ""
	}
}

func (c *FastCarrier) Set(key string, value string) {
	switch key {
	case "traceparent":
		c.TraceParent = value
	case "tracestate":
		c.TraceState = value
	}
}

func (c *FastCarrier) Keys() []string {
	return []string{"traceparent", "tracestate"}
}

// InjectRedis encodes the current trace context from the given context.Context
// into a FastCarrier struct. This struct should be embedded in the Redis pub/sub
// payload as a "trace_headers" field.
func InjectRedis(ctx context.Context) *FastCarrier {
	carrier := &FastCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

// ExtractRedis decodes the trace context from a FastCarrier (typically extracted
// from a Redis pub/sub payload's "trace_headers" field) and injects it into a new context.Context.
func ExtractRedis(ctx context.Context, carrier *FastCarrier) context.Context {
	if carrier == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
