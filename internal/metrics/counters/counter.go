package counters

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

type counterWrapper struct {
	vec    *prometheus.CounterVec
	metric prometheus.Counter
}

// New constructs a new Counter wrapping a Prometheus CounterVec.
func New(vec *prometheus.CounterVec) interfaces.Counter {
	return &counterWrapper{
		vec: vec,
	}
}

func (c *counterWrapper) Inc() {
	if c.metric != nil {
		c.metric.Inc()
		return
	}
	m, err := c.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Inc()
	}
}

func (c *counterWrapper) Add(val float64) {
	if c.metric != nil {
		c.metric.Add(val)
		return
	}
	m, err := c.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Add(val)
	}
}

func (c *counterWrapper) With(labelValues ...string) (res interfaces.Counter) {
	defer func() {
		if r := recover(); r != nil {
			// Catch panic from Prometheus on label cardinality mismatch
			res = c
		}
	}()

	m := c.vec.WithLabelValues(labelValues...)
	return &counterWrapper{
		vec:    c.vec,
		metric: m,
	}
}
