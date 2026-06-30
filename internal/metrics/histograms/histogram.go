package histograms

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
	"go.opentelemetry.io/otel/trace"
)

type histogramWrapper struct {
	vec    *prometheus.HistogramVec
	metric prometheus.Observer
}

// New constructs a new Histogram wrapping a Prometheus HistogramVec.
func New(vec *prometheus.HistogramVec) interfaces.Histogram {
	return &histogramWrapper{
		vec: vec,
	}
}

func (h *histogramWrapper) Observe(val float64) {
	if h.metric != nil {
		h.metric.Observe(val)
		return
	}
	m, err := h.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Observe(val)
	}
}

func (h *histogramWrapper) ObserveWithContext(ctx context.Context, val float64) {
	span := trace.SpanFromContext(ctx)
	var exemplar prometheus.Labels
	if span.SpanContext().HasTraceID() {
		exemplar = prometheus.Labels{"trace_id": span.SpanContext().TraceID().String()}
	}

	if h.metric != nil {
		if eo, ok := h.metric.(prometheus.ExemplarObserver); ok && exemplar != nil {
			eo.ObserveWithExemplar(val, exemplar)
			return
		}
		h.metric.Observe(val)
		return
	}
	
	m, err := h.vec.GetMetricWithLabelValues()
	if err == nil {
		if eo, ok := m.(prometheus.ExemplarObserver); ok && exemplar != nil {
			eo.ObserveWithExemplar(val, exemplar)
			return
		}
		m.Observe(val)
	}
}

func (h *histogramWrapper) With(labelValues ...string) (res interfaces.Histogram) {
	defer func() {
		if r := recover(); r != nil {
			// Catch panic from Prometheus on label cardinality mismatch
			res = h
		}
	}()

	m := h.vec.WithLabelValues(labelValues...)
	return &histogramWrapper{
		vec:    h.vec,
		metric: m,
	}
}
