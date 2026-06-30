// Package propagation provides helpers to serialize and deserialize the
// Correlation Framework context (W3C Baggage + Trace Context) across
// asynchronous boundaries like Redis Pub/Sub.
package propagation

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
)

// FastCarrier is a highly-optimized implementation of propagation.TextMapCarrier
// designed for high-frequency trading pipelines. It avoids map heap allocations
// by storing metadata in a pre-allocated flat slice.
type FastCarrier struct {
	pairs []string
}

// NewFastCarrier creates a new FastCarrier with pre-allocated capacity
// for the standard OpenTelemetry Baggage and Trace headers.
func NewFastCarrier() *FastCarrier {
	return &FastCarrier{
		pairs: make([]string, 0, 4),
	}
}

// Get returns the value associated with the passed key.
func (c *FastCarrier) Get(key string) string {
	for i := 0; i < len(c.pairs); i += 2 {
		if c.pairs[i] == key {
			return c.pairs[i+1]
		}
	}
	return ""
}

// Set stores the key-value pair.
func (c *FastCarrier) Set(key, value string) {
	for i := 0; i < len(c.pairs); i += 2 {
		if c.pairs[i] == key {
			c.pairs[i+1] = value
			return
		}
	}
	c.pairs = append(c.pairs, key, value)
}

// Keys lists the keys stored in this carrier.
func (c *FastCarrier) Keys() []string {
	keys := make([]string, 0, len(c.pairs)/2)
	for i := 0; i < len(c.pairs); i += 2 {
		keys = append(keys, c.pairs[i])
	}
	return keys
}

// Reset clears the carrier for reuse without reallocating the underlying slice.
func (c *FastCarrier) Reset() {
	c.pairs = c.pairs[:0]
}

// GlobalPropagator returns the standard OpenTelemetry propagator combination
// (TraceContext + Baggage) used to serialize context metadata.
func GlobalPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

// Inject serializes the current context (including Correlation ID and Trace ID)
// into the provided TextMapCarrier. The resulting carrier can be sent over Redis.
func Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	GlobalPropagator().Inject(ctx, carrier)
}

// Extract deserializes metadata from a TextMapCarrier (received from Redis)
// and returns a new context enriched with the Correlation ID and Trace ID.
func Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return GlobalPropagator().Extract(ctx, carrier)
}
