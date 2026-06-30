package gauges

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

type gaugeWrapper struct {
	vec    *prometheus.GaugeVec
	metric prometheus.Gauge
}

// New constructs a new Gauge wrapping a Prometheus GaugeVec.
func New(vec *prometheus.GaugeVec) interfaces.Gauge {
	return &gaugeWrapper{
		vec: vec,
	}
}

func (g *gaugeWrapper) Set(val float64) {
	if g.metric != nil {
		g.metric.Set(val)
		return
	}
	m, err := g.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Set(val)
	}
}

func (g *gaugeWrapper) Inc() {
	if g.metric != nil {
		g.metric.Inc()
		return
	}
	m, err := g.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Inc()
	}
}

func (g *gaugeWrapper) Dec() {
	if g.metric != nil {
		g.metric.Dec()
		return
	}
	m, err := g.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Dec()
	}
}

func (g *gaugeWrapper) Add(val float64) {
	if g.metric != nil {
		g.metric.Add(val)
		return
	}
	m, err := g.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Add(val)
	}
}

func (g *gaugeWrapper) Sub(val float64) {
	if g.metric != nil {
		g.metric.Sub(val)
		return
	}
	m, err := g.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Sub(val)
	}
}

func (g *gaugeWrapper) With(labelValues ...string) (res interfaces.Gauge) {
	defer func() {
		if r := recover(); r != nil {
			// Catch panic from Prometheus on label cardinality mismatch
			res = g
		}
	}()

	m := g.vec.WithLabelValues(labelValues...)
	return &gaugeWrapper{
		vec:    g.vec,
		metric: m,
	}
}
