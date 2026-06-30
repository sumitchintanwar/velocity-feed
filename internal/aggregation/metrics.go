package aggregation

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TicksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rtmds_aggregation_ticks_processed_total",
		Help: "The total number of ticks processed by the aggregation engine",
	}, []string{"symbol"})

	AggregationsPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rtmds_aggregation_published_total",
		Help: "The total number of OHLC/VWAP windows published",
	}, []string{"type"}) // type="ohlc_1s", "ohlc_1m", "vwap"

	TickProcessingLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "rtmds_aggregation_tick_processing_duration_seconds",
		Help:    "Latency of processing a single tick through all aggregators",
		Buckets: prometheus.DefBuckets,
	})

	ActiveSymbols = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rtmds_aggregation_active_symbols",
		Help: "The current number of active symbols being aggregated in memory",
	})

	DroppedCandles = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rtmds_aggregation_dropped_candles_total",
		Help: "The total number of OHLC candles dropped due to a full publisher channel",
	}, []string{"symbol"})

	LateEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rtmds_aggregation_late_events_total",
		Help: "The total number of ticks rejected due to arriving late (jitter)",
	}, []string{"symbol"})
)
