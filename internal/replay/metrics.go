package replay

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ActiveSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rtmds_replay_active_sessions",
		Help: "The current number of active replay sessions",
	})

	EventsPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rtmds_replay_published_events_total",
		Help: "The total number of historical events published during replay",
	}, []string{"symbol"})

	PausesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rtmds_replay_pauses_total",
		Help: "The total number of pause operations executed",
	})

	SeeksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rtmds_replay_seeks_total",
		Help: "The total number of seek operations executed",
	})
)
