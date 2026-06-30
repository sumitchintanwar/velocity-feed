// Package samplers provides customized OpenTelemetry sampling strategies.
package samplers

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// RulesBasedSampler is a custom sampler that prioritizes errors and critical
// workflows while falling back to probability sampling for standard operations.
type RulesBasedSampler struct {
	Fallback sdktrace.Sampler
}

// NewRulesBasedSampler creates a sampler that wraps a fallback probability sampler.
func NewRulesBasedSampler(fallback sdktrace.Sampler) sdktrace.Sampler {
	return &RulesBasedSampler{
		Fallback: fallback,
	}
}

// ShouldSample determines if a span should be recorded and exported.
func (s *RulesBasedSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	// 1. Always sample critical startup/recovery flows based on span name or attributes
	if p.Name == "marketdata.recovery.startup" || p.Name == "marketdata.recovery.sync" {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
	}

	// Note: We cannot sample based on "Status=Error" here because ShouldSample
	// is called *before* the span executes and before errors are recorded.
	// To truly sample 100% of errors in Go OTEL, Tail-Based Sampling is required
	// at the OpenTelemetry Collector. This Head-Based sampler falls back to the
	// probability sampler for standard requests.

	return s.Fallback.ShouldSample(p)
}

// Description returns the name of the sampler.
func (s *RulesBasedSampler) Description() string {
	return "RulesBasedSampler{" + s.Fallback.Description() + "}"
}
