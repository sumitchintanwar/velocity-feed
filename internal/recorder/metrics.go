package recorder

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsRecorded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rtmds_recorder_events_total",
		Help: "The total number of events recorded to the event store",
	}, []string{"symbol"})

	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "rtmds_recorder_batch_size",
		Help:    "Distribution of batch sizes written to storage",
		Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000},
	})
)
