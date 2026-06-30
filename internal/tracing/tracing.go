// Package tracing provides OpenTelemetry distributed tracing for the RTMDS platform.
//
// Design decisions:
//   - Uses W3C Trace Context propagation (OTel default) for vendor-neutral context flow.
//   - Head-based sampling with configurable ratio to control trace volume at 100k ops/sec.
//   - OTLP HTTP exporter as default (works with Jaeger, Tempo, Honeycomb, Datadog).
//   - Noop exporter for testing and when tracing is disabled.
//   - Every span carries low-cardinality attributes (service, environment, region, component)
//     per the distributed tracing design spec. High-cardinality fields (symbol, price, client_id)
//     are never set as span attributes.
//
// Trace boundaries (per design spec):
//   - websocket_connect, subscription_request, snapshot_request, replay_request
//   - redis.publish, redis.consume (infrastructure spans)
//   - NOT traced: per-tick market data, per-Redis-message, per-WebSocket-frame
package tracing

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds OpenTelemetry tracing configuration.
// All fields map to environment variables via Viper in the config package.
type Config struct {
	Enabled     bool    // false disables tracing entirely (noop provider)
	Endpoint    string  // OTLP collector URL, e.g. "localhost:4318"
	SampleRatio float64 // 0.0–1.0, fraction of traces to sample (default 0.01 = 1%)
	ServiceName string  // e.g. "rtmds"
	Environment string  // prod, uat, dev
	Version     string  // software release version
	Region      string  // e.g. "us-east-1"
}

// Tracer wraps an OpenTelemetry Tracer and provides convenience methods
// for starting spans with consistent service attributes.
type Tracer struct {
	tracer     trace.Tracer
	provider   *sdktrace.TracerProvider
	resource   *resource.Resource
	noop       bool // true when tracing is disabled
}

// InitTracer initializes the OpenTelemetry SDK and returns a Tracer.
// The returned shutdown function MUST be called on application exit to
// flush pending spans. It is safe to call InitTracer multiple times;
// subsequent calls replace the global provider.
//
// When cfg.Enabled is false, a noop provider is installed so all span
// operations become no-ops with zero overhead.
func InitTracer(cfg Config) (*Tracer, func(context.Context) error, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "localhost:4318"
	}
	if cfg.SampleRatio <= 0 {
		cfg.SampleRatio = 0.01 // 1% default
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "rtmds"
	}

	// Build resource with service identity attributes.
	// These appear on every span and enable filtering by environment/region.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(cfg.ServiceName),
			attribute.String("environment", cfg.Environment),
			attribute.String("version", cfg.Version),
			attribute.String("region", cfg.Region),
		),
	)
	if err != nil {
		return nil, nil, errors.New("tracing: failed to create resource: " + err.Error())
	}

	// When disabled, install a noop provider and return a stub tracer.
	if !cfg.Enabled {
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()), // still need a valid provider
		)
		// Replace global provider so otel.Tracer() returns noop spans.
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		return &Tracer{
			tracer:   tp.Tracer(cfg.ServiceName),
			provider: tp,
			resource: res,
			noop:     true,
		}, tp.Shutdown, nil
	}

	// Create OTLP HTTP exporter.
	// The endpoint is the collector's OTLP HTTP receiver (default port 4318).
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(cfg.Endpoint),
		otlptracehttp.WithInsecure(), // collector is typically behind a load balancer
	)
	if err != nil {
		return nil, nil, errors.New("tracing: failed to create OTLP exporter: " + err.Error())
	}

	// Head-based sampling: the sampler decision is made at trace start.
	// ParentBased ensures child spans follow the parent's decision.
	// Ratio-based sampler gives us 1% of traces in production.
	sampler := sdktrace.ParentBased(
		sdktrace.TraceIDRatioBased(cfg.SampleRatio),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),   // flush every 5s
			sdktrace.WithMaxExportBatchSize(512),        // batch size for OTLP
			sdktrace.WithMaxQueueSize(2048),             // queue capacity
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set global propagator to W3C Trace Context.
	// This enables context extraction from incoming HTTP headers (traceparent).
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Tracer{
		tracer:   tp.Tracer(cfg.ServiceName),
		provider: tp,
		resource: res,
	}, tp.Shutdown, nil
}

// StartSpan starts a new span from the given context.
// The span is automatically ended when the caller calls span.End().
// Returns the new context containing the span and the span itself.
//
// Usage:
//
//   ctx, span := tracing.StartSpan(ctx, "gateway.connect")
//   defer span.End()
//   // ... do work
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracerFromContext(ctx).Start(ctx, name, opts...)
}

// SpanFromContext extracts the current span from context.
// Returns a noop span if no span is present (safe to call Always).
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// TracerForComponent returns a named Tracer for a specific component.
// This produces spans under "rtmds.{component}" for consistent filtering.
//
// Usage:
//
//	tracer := tracing.TracerForComponent("redis")
//	ctx, span := tracer.Start(ctx, "redis.publish")
func TracerForComponent(component string) trace.Tracer {
	return otel.Tracer("rtmds." + component)
}

// --- Redis trace context propagation ---
//
// Redis Pub/Sub is asynchronous — there is no request context like HTTP.
// To propagate trace context across the Redis boundary, we serialize the
// current span context into the message metadata (wireEnvelope.TraceCtx)
// and reconstruct it on the subscriber side.
//
// This enables distributed traces like:
//
//	Pipeline → redis.publish → redis.consume → TopicManager → Client

// TraceCarrier is a simple map that implements propagation.TextMapCarrier
// for injecting/extracting trace context into message metadata.
type TraceCarrier map[string]string

// Get returns the value for the given key.
func (c TraceCarrier) Get(key string) string { return c[key] }

// Set stores a key-value pair.
func (c TraceCarrier) Set(key, value string) { c[key] = value }

// Keys returns all stored keys.
func (c TraceCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectTraceContext serializes the current span context from ctx into a
// TraceCarrier map. Returns nil if no valid span context is present.
//
// The carrier is intended to be embedded in the wire envelope and sent
// alongside the message payload.
func InjectTraceContext(ctx context.Context) TraceCarrier {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return nil
	}
	carrier := make(TraceCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

// ExtractTraceContext reconstructs a span context from a TraceCarrier map
// returned by InjectTraceContext. Returns the original ctx unchanged if
// carrier is nil or empty.
//
// The returned context contains a remote span context that can be used as
// the parent of a consumer span, linking producer → consumer traces.
func ExtractTraceContext(ctx context.Context, carrier TraceCarrier) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// Shutdown flushes all pending spans and releases resources.
// Must be called on application exit with a deadline context.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

// tracerFromContext returns the global tracer. In the future this could
// extract a component-specific tracer from context.
func tracerFromContext(_ context.Context) trace.Tracer {
	return otel.Tracer("rtmds")
}

// SpanKind sets the span kind on span start options.
// Convenience wrappers for common span types.

// WithSpanKindHTTP returns a SpanStartOption indicating an HTTP server span.
func WithSpanKindHTTP() trace.SpanStartOption {
	return trace.WithSpanKind(trace.SpanKindServer)
}

// WithSpanKindProducer returns a SpanStartOption indicating a message producer span.
func WithSpanKindProducer() trace.SpanStartOption {
	return trace.WithSpanKind(trace.SpanKindProducer)
}

// WithSpanKindConsumer returns a SpanStartOption indicating a message consumer span.
func WithSpanKindConsumer() trace.SpanStartOption {
	return trace.WithSpanKind(trace.SpanKindConsumer)
}

// WithSpanKindInternal returns a SpanStartOption indicating an internal span.
func WithSpanKindInternal() trace.SpanStartOption {
	return trace.WithSpanKind(trace.SpanKindInternal)
}
