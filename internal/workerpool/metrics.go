package workerpool

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus instruments for the worker pool.
// Uses aggregate counters (no per-worker labels) to avoid cardinality explosion.
type Metrics struct {
	// TasksReceived counts events accepted into the pool queue.
	TasksReceived prometheus.Counter

	// TasksCompleted counts events successfully processed by workers.
	TasksCompleted prometheus.Counter

	// TasksFailed counts events that panicked during processing.
	TasksFailed prometheus.Counter

	// TasksDropped counts events rejected because the queue was full.
	TasksDropped prometheus.Counter

	// ActiveWorkers tracks the current number of busy workers.
	ActiveWorkers prometheus.Gauge

	// QueueDepth tracks the current number of items waiting in the queue.
	QueueDepth prometheus.Gauge

	// TaskDurationSeconds measures how long each task takes to process.
	TaskDurationSeconds prometheus.Histogram

	// QueueWaitSeconds measures time an event spends waiting in the queue.
	QueueWaitSeconds prometheus.Histogram
}

// NewMetrics creates and registers worker pool metrics on the given registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)

	return &Metrics{
		TasksReceived: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "worker",
			Name:      "tasks_received_total",
			Help:      "Total number of tasks accepted into the worker pool queue.",
		}),

		TasksCompleted: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "worker",
			Name:      "tasks_completed_total",
			Help:      "Total number of tasks successfully processed by workers.",
		}),

		TasksFailed: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "worker",
			Name:      "tasks_failed_total",
			Help:      "Total number of tasks that panicked during processing.",
		}),

		TasksDropped: f.NewCounter(prometheus.CounterOpts{
			Namespace: "rtmds",
			Subsystem: "worker",
			Name:      "tasks_dropped_total",
			Help:      "Total number of tasks dropped because the queue was full.",
		}),

		ActiveWorkers: f.NewGauge(prometheus.GaugeOpts{
			Namespace: "rtmds",
			Subsystem: "worker",
			Name:      "active_workers",
			Help:      "Current number of workers processing a task.",
		}),

		QueueDepth: f.NewGauge(prometheus.GaugeOpts{
			Namespace: "rtmds",
			Subsystem: "worker",
			Name:      "queue_depth",
			Help:      "Current number of tasks waiting in the queue.",
		}),

		TaskDurationSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "rtmds",
			Subsystem: "worker",
			Name:      "task_duration_seconds",
			Help:      "Time in seconds to process a single task.",
			Buckets:   []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		}),

		QueueWaitSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "rtmds",
			Subsystem: "worker",
			Name:      "queue_wait_seconds",
			Help:      "Time in seconds an event waits in the queue before processing.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		}),
	}
}
