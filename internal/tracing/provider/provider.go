// Package provider manages the OpenTelemetry global TracerProvider lifecycle.
package provider

import (
	"context"
	"fmt"
	"time"

	"github.com/sumit/rtmds/internal/tracing/attributes"
	"github.com/sumit/rtmds/internal/tracing/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"github.com/sumit/rtmds/internal/tracing/samplers"
)

// Provider holds the configured TracerProvider and ensures graceful shutdown.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// New initializes the OpenTelemetry TracerProvider with an OTLP HTTP exporter.
func New(ctx context.Context, cfg *config.Config) (*Provider, error) {
	// Configure the OTLP HTTP Exporter
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
		otlptracehttp.WithTimeout(5 * time.Second), // Harden exporter timeout
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         true,
			InitialInterval: 1 * time.Second,
			MaxInterval:     10 * time.Second,
			MaxElapsedTime:  30 * time.Second,
		}),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Configure Resource Attributes
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			attributes.Environment(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Configure Parent-Based Probability Sampler wrapped in Custom Rules
	baseSampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRate))
	rulesSampler := samplers.NewRulesBasedSampler(baseSampler)

	// Create the TracerProvider with explicit batching limits for high throughput
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxQueueSize(8192),
			sdktrace.WithMaxExportBatchSize(2048),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(rulesSampler),
	)

	// Set as global provider and register standard context propagators (W3C)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Provider{tp: tp}, nil
}

// Shutdown flushes remaining spans and shuts down the exporter.
func (p *Provider) Shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.tp.Shutdown(ctx)
}
