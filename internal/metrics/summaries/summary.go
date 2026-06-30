package summaries

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
)

type summaryWrapper struct {
	vec    *prometheus.SummaryVec
	metric prometheus.Observer
}

// New constructs a new Summary wrapping a Prometheus SummaryVec.
func New(vec *prometheus.SummaryVec) interfaces.Summary {
	return &summaryWrapper{
		vec: vec,
	}
}

func (s *summaryWrapper) Observe(val float64) {
	if s.metric != nil {
		s.metric.Observe(val)
		return
	}
	m, err := s.vec.GetMetricWithLabelValues()
	if err == nil {
		m.Observe(val)
	}
}

func (s *summaryWrapper) With(labelValues ...string) (res interfaces.Summary) {
	defer func() {
		if r := recover(); r != nil {
			// Catch panic from Prometheus on label cardinality mismatch
			res = s
		}
	}()

	m := s.vec.WithLabelValues(labelValues...)
	return &summaryWrapper{
		vec:    s.vec,
		metric: m,
	}
}
