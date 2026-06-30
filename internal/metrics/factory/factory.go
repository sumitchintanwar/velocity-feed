// Package factory provides the primary API for creating and registering metrics.
//
// Purpose:
// Centralizes the creation of Prometheus metric vectors and enforces standard
// naming conventions, prefixes, and safe registration into the shared Registry.
package factory

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sumit/rtmds/internal/metrics/config"
	"github.com/sumit/rtmds/internal/metrics/counters"
	"github.com/sumit/rtmds/internal/metrics/gauges"
	"github.com/sumit/rtmds/internal/metrics/histograms"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
	"github.com/sumit/rtmds/internal/metrics/registry"
	"github.com/sumit/rtmds/internal/metrics/summaries"
)

// Factory handles the instantiation and registration of metrics.
type Factory struct {
	reg    *registry.Registry
	cfg    config.Config
	prefix string
}

// New creates a new metrics Factory for a specific component.
// Prefix format: {namespace}_{component}_
func New(reg *registry.Registry, cfg config.Config, component string) *Factory {
	prefix := cfg.Namespace
	if component != "" {
		prefix = fmt.Sprintf("%s_%s", cfg.Namespace, component)
	}
	return &Factory{
		reg:    reg,
		cfg:    cfg,
		prefix: prefix,
	}
}

// buildName constructs the canonical metric name.
func (f *Factory) buildName(name string) string {
	if f.prefix == "" {
		return name
	}
	// Sanitize to prevent double prefixes
	prefixWithUnderscore := f.prefix + "_"
	if len(name) >= len(prefixWithUnderscore) && name[:len(prefixWithUnderscore)] == prefixWithUnderscore {
		return name
	}
	return prefixWithUnderscore + name
}

// NewCounter creates and registers a Counter.
func (f *Factory) NewCounter(name, help string, labels []string) (interfaces.Counter, error) {
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: f.buildName(name),
		Help: help,
	}, labels)

	col, err := f.reg.Register(f.buildName(name), vec)
	if err != nil {
		return nil, err
	}

	return counters.New(col.(*prometheus.CounterVec)), nil
}

// NewGauge creates and registers a Gauge.
func (f *Factory) NewGauge(name, help string, labels []string) (interfaces.Gauge, error) {
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: f.buildName(name),
		Help: help,
	}, labels)

	col, err := f.reg.Register(f.buildName(name), vec)
	if err != nil {
		return nil, err
	}

	return gauges.New(col.(*prometheus.GaugeVec)), nil
}

// NewHistogram creates and registers a Histogram.
func (f *Factory) NewHistogram(name, help string, buckets []float64, labels []string) (interfaces.Histogram, error) {
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    f.buildName(name),
		Help:    help,
		Buckets: buckets,
	}, labels)

	col, err := f.reg.Register(f.buildName(name), vec)
	if err != nil {
		return nil, err
	}

	return histograms.New(col.(*prometheus.HistogramVec)), nil
}

// NewSummary creates and registers a Summary.
func (f *Factory) NewSummary(name, help string, objectives map[float64]float64, labels []string) (interfaces.Summary, error) {
	vec := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       f.buildName(name),
		Help:       help,
		Objectives: objectives,
	}, labels)

	col, err := f.reg.Register(f.buildName(name), vec)
	if err != nil {
		return nil, err
	}

	return summaries.New(col.(*prometheus.SummaryVec)), nil
}
