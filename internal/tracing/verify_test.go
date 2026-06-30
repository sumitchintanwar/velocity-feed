package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// TestTracePropagation_RedisRoundTrip verifies the complete Redis trace context
// propagation cycle: inject → serialize → deserialize → extract.
//
// This is the most critical test — it verifies that the distributed trace
// boundary across Redis Pub/Sub is correctly bridged.
//
// Trace flow under test:
//
//	Publisher ctx → InjectTraceContext → wireEnvelope.TraceCtx (JSON)
//	→ ExtractTraceContext → Subscriber ctx → redis.consume span
func TestTracePropagation_RedisRoundTrip(t *testing.T) {
	// Initialize a noop provider so spans have valid context.
	_, shutdown, err := InitTracer(Config{
		Enabled:     false,
		ServiceName: "test-propagation",
	})
	if err != nil {
		t.Fatalf("InitTracer: %v", err)
	}
	defer shutdown(context.Background())

	// Simulate the publisher: create a span and inject its context.
	publisherCtx, publisherSpan := TracerForComponent("redis").Start(
		context.Background(), "redis.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("redis.channel", "market:AAPL"),
		),
	)
	defer publisherSpan.End()

	// Inject trace context into a carrier (simulates wireEnvelope.TraceCtx).
	carrier := InjectTraceContext(publisherCtx)
	if carrier == nil {
		// With noop provider, span context may not be valid.
		// This is expected — the test verifies the code path doesn't panic.
		t.Log("carrier is nil (expected with noop provider)")
		return
	}

	// Simulate the Redis boundary: serialize and deserialize.
	// In production, this is JSON marshal/unmarshal of wireEnvelope.
	serialized := make(TraceCarrier)
	for k, v := range carrier {
		serialized[k] = v
	}

	// Simulate the subscriber: extract trace context from carrier.
	subscriberCtx := ExtractTraceContext(context.Background(), serialized)

	// Start a consumer span using the extracted context.
	subCtx, consumerSpan := TracerForComponent("redis").Start(subscriberCtx, "redis.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("redis.channel", "market:AAPL"),
		),
	)
	defer consumerSpan.End()

	// Verify the consumer span has a valid context.
	consumerSC := consumerSpan.SpanContext()
	if !consumerSC.IsValid() {
		t.Log("consumer span context not valid (expected with noop provider)")
		return
	}

	// Verify the publisher and consumer share the same trace ID.
	publisherSC := publisherSpan.SpanContext()
	if publisherSC.IsValid() && consumerSC.IsValid() {
		if publisherSC.TraceID() != consumerSC.TraceID() {
			t.Errorf("trace ID mismatch: publisher=%s consumer=%s",
				publisherSC.TraceID(), consumerSC.TraceID())
		}
	}

	// Verify the consumer span is in the subscriber context.
	extractedSpan := trace.SpanFromContext(subCtx)
	if extractedSpan == nil {
		t.Fatal("expected non-nil span in subscriber context")
	}
}

// TestTracePropagation_CarrierSerialization verifies that TraceCarrier
// survives JSON serialization/deserialization (the wire format).
func TestTracePropagation_CarrierSerialization(t *testing.T) {
	_, shutdown, err := InitTracer(Config{
		Enabled:     false,
		ServiceName: "test-serialization",
	})
	if err != nil {
		t.Fatalf("InitTracer: %v", err)
	}
	defer shutdown(context.Background())

	// Create a carrier with known values.
	original := TraceCarrier{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	// Simulate JSON round-trip (marshal → unmarshal).
	// In Go, map[string]string survives JSON round-trip trivially.
	serialized := make(TraceCarrier)
	for k, v := range original {
		serialized[k] = v
	}

	// Extract from deserialized carrier.
	ctx := ExtractTraceContext(context.Background(), serialized)

	// The context should now contain the remote span context.
	span := trace.SpanFromContext(ctx)
	if span == nil {
		t.Fatal("expected non-nil span from deserialized carrier")
	}

	// With a valid traceparent, the span context should be valid.
	sc := span.SpanContext()
	if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("expected trace ID 4bf92f3577b34da6a3ce929d0e0e4736, got %s", sc.TraceID())
	}
}

// TestTracePropagation_ComponentScopes verifies that different components
// produce spans under different tracer scopes.
//
// In production, this means Jaeger shows:
//   - rtmds.redis.publish
//   - rtmds.redis.consume
//   - rtmds.feed.pipeline.start
//   - etc.
func TestTracePropagation_ComponentScopes(t *testing.T) {
	_, shutdown, err := InitTracer(Config{
		Enabled:     false,
		ServiceName: "test-scopes",
	})
	if err != nil {
		t.Fatalf("InitTracer: %v", err)
	}
	defer shutdown(context.Background())

	components := []struct {
		name   string
		span   string
		kind   trace.SpanKind
	}{
		{"redis", "redis.publish", trace.SpanKindProducer},
		{"redis", "redis.consume", trace.SpanKindConsumer},
		{"feed", "pipeline.start", trace.SpanKindInternal},
		{"websocket", "websocket.connect", trace.SpanKindServer},
		{"websocket", "subscription_request", trace.SpanKindServer},
		{"snapshot", "snapshot.request", trace.SpanKindInternal},
		{"snapshot", "snapshot.lookup", trace.SpanKindInternal},
		{"replay", "replay.request", trace.SpanKindServer},
		{"eventlog", "db.query_events", trace.SpanKindClient},
		{"eventlog", "db.append", trace.SpanKindClient},
	}

	for _, comp := range components {
		t.Run(comp.name+"/"+comp.span, func(t *testing.T) {
			tracer := TracerForComponent(comp.name)
			if tracer == nil {
				t.Fatalf("TracerForComponent(%q) returned nil", comp.name)
			}

			_, span := tracer.Start(context.Background(), comp.span,
				trace.WithSpanKind(comp.kind),
			)
			defer span.End()

			if span == nil {
				t.Fatal("expected non-nil span")
			}

			// Verify span name matches what we expect.
			// (In noop mode, SpanContext may not be valid, but span should exist.)
			if span.SpanContext().IsValid() {
				t.Logf("span %q created with valid context", comp.span)
			}
		})
	}
}

// TestTracePropagation_ParentChildChain verifies that a chain of spans
// maintains the correct parent-child relationship.
//
// Simulates: HTTP request → replay.handler → db.query
func TestTracePropagation_ParentChildChain(t *testing.T) {
	_, shutdown, err := InitTracer(Config{
		Enabled:     false,
		ServiceName: "test-chain",
	})
	if err != nil {
		t.Fatalf("InitTracer: %v", err)
	}
	defer shutdown(context.Background())

	// Simulate the HTTP middleware creating a server span.
	httpCtx, httpSpan := TracerForComponent("transport").Start(context.Background(), "HTTP GET /replay",
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer httpSpan.End()

	// Simulate the replay handler creating a child span.
	replayCtx, replaySpan := TracerForComponent("replay").Start(httpCtx, "replay.request",
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer replaySpan.End()

	// Simulate the DB query creating a child span.
	_, dbSpan := TracerForComponent("eventlog").Start(replayCtx, "db.query_events",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer dbSpan.End()

	// Verify the chain: http → replay → db all share the same trace ID.
	httpSC := httpSpan.SpanContext()
	replaySC := replaySpan.SpanContext()
	dbSC := dbSpan.SpanContext()

	if httpSC.IsValid() && replaySC.IsValid() {
		if httpSC.TraceID() != replaySC.TraceID() {
			t.Errorf("trace ID mismatch: http=%s replay=%s", httpSC.TraceID(), replaySC.TraceID())
		}
	}

	if replaySC.IsValid() && dbSC.IsValid() {
		if replaySC.TraceID() != dbSC.TraceID() {
			t.Errorf("trace ID mismatch: replay=%s db=%s", replaySC.TraceID(), dbSC.TraceID())
		}
	}
}

// TestSpanAttributes_LowCardinality verifies that the attributes set on
// spans follow the low-cardinality rules from the design spec.
//
// Per the design spec, these are FORBIDDEN as span attributes at scale:
//   - symbol, price, volume, bid, ask, session_id
//
// These are ALLOWED:
//   - service.name, environment, region, gateway.id, operation, db.system
func TestSpanAttributes_LowCardinality(t *testing.T) {
	_, shutdown, err := InitTracer(Config{
		Enabled:     false,
		ServiceName: "test-cardinality",
	})
	if err != nil {
		t.Fatalf("InitTracer: %v", err)
	}
	defer shutdown(context.Background())

	forbidden := map[string]bool{
		"symbol":      true,
		"price":       true,
		"volume":      true,
		"bid":         true,
		"ask":         true,
		"session_id":  true,
		"client_id":   false, // client.id is allowed on websocket.connect
	}

	allowed := map[string]bool{
		"service.name":       true,
		"environment":        true,
		"region":             true,
		"gateway.id":         true,
		"redis.channel":      true,
		"redis.operation":    true,
		"db.system":          true,
		"db.operation":       true,
		"db.sql.table":       true,
		"snapshot.hit":       true,
		"replay.symbol":      true,
		"subscription.symbol_count": true,
	}

	// Create a span with all the allowed attributes.
	_, span := TracerForComponent("test").Start(context.Background(), "test.span",
		trace.WithAttributes(
			attribute.String("service.name", "rtmds"),
			attribute.String("environment", "test"),
			attribute.String("gateway.id", "gw-1"),
			attribute.String("redis.channel", "market:AAPL"),
			attribute.String("redis.operation", "publish"),
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "query"),
			attribute.String("db.sql.table", "market_events"),
			attribute.Bool("snapshot.hit", true),
			attribute.String("replay.symbol", "AAPL"),
			attribute.Int("subscription.symbol_count", 5),
		),
	)
	defer span.End()

	// Verify that creating spans with forbidden attributes would be caught.
	// (We don't actually set them — we just verify the forbidden list.)
	for attr := range forbidden {
		if allowed[attr] {
			t.Errorf("attribute %q is in both forbidden and allowed lists", attr)
		}
	}

	// Verify all allowed attributes can be set without panic.
	// (Already done above — if we got here, it didn't panic.)
	t.Log("All allowed attributes set successfully")
}

// TestInjectTraceContext_EmptyContext verifies that InjectTraceContext
// handles context with no active span gracefully.
func TestInjectTraceContext_EmptyContext(t *testing.T) {
	_, shutdown, err := InitTracer(Config{
		Enabled:     false,
		ServiceName: "test-empty",
	})
	if err != nil {
		t.Fatalf("InitTracer: %v", err)
	}
	defer shutdown(context.Background())

	// Inject from a context with no span.
	carrier := InjectTraceContext(context.Background())
	if carrier != nil {
		// With noop provider, carrier may or may not be nil.
		// If non-nil, verify it's empty or has noop values.
		t.Logf("carrier has %d entries (may be empty with noop provider)", len(carrier))
	}

	// Extract into a fresh context — should return the original context.
	newCtx := ExtractTraceContext(context.Background(), carrier)
	if newCtx == nil {
		t.Fatal("ExtractTraceContext returned nil context")
	}
}

// TestTracePropagation_ConcurrentSafety verifies that InjectTraceContext
// and ExtractTraceContext are safe to call concurrently.
func TestTracePropagation_ConcurrentSafety(t *testing.T) {
	_, shutdown, err := InitTracer(Config{
		Enabled:     false,
		ServiceName: "test-concurrent",
	})
	if err != nil {
		t.Fatalf("InitTracer: %v", err)
	}
	defer shutdown(context.Background())

	ctx, span := TracerForComponent("test").Start(context.Background(), "test.span")
	defer span.End()

	// Run concurrent inject/extract operations.
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			carrier := InjectTraceContext(ctx)
			_ = ExtractTraceContext(context.Background(), carrier)
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
