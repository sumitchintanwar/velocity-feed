// Package feed provides the Pipeline that connects a Feed Generator to a
// Publisher Service. It continuously reads quotes from the feed and
// publishes them to the bus. The Pipeline depends only on interfaces —
// no concrete implementations are imported.
package feed

import (
	"context"
	"fmt"
	"time"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/pubsub"
	"github.com/sumit/rtmds/internal/sequencer"
	"github.com/sumit/rtmds/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// HeartbeatUpdater is an interface for updating the liveness heartbeat.
type HeartbeatUpdater interface {
	Mark()
}

// Pipeline connects a Feed to a Publisher. It runs the feed's read loop
// and publishes every received quote to the bus. The Pipeline is safe
// for a single Run call; concurrent Run calls are not supported.
type Pipeline struct {
	feed      marketdata.Feed
	publisher pubsub.Publisher
	seq       sequencer.Generator
	log       *log.Logger
	metrics   *platform.Metrics    // optional; nil disables instrumentation
	heartbeat HeartbeatUpdater     // optional; nil disables liveness heartbeat
}

// NewPipeline creates a Pipeline. The metrics parameter is optional —
// pass nil to disable Prometheus instrumentation.
// The heartbeat parameter is optional — pass nil to disable liveness heartbeat.
func NewPipeline(feed marketdata.Feed, publisher pubsub.Publisher, seq sequencer.Generator, l *log.Logger, metrics *platform.Metrics, heartbeat HeartbeatUpdater) *Pipeline {
	return &Pipeline{
		feed:      feed,
		publisher: publisher,
		seq:       seq,
		log:       l,
		metrics:   metrics,
		heartbeat: heartbeat,
	}
}

// Run starts the pipeline. It blocks until:
//   - ctx is cancelled (clean shutdown)
//   - the feed returns an error
//   - the feed channel is closed (feed stopped)
//
// A nil error means the pipeline shut down cleanly.
//
// Trace boundary: "pipeline.start" — a single long-running span covering the
// pipeline's entire lifetime. Lifecycle events (feed errors, shutdown) are
// recorded as span events, NOT separate spans. This avoids tracing every
// market tick (100k+/sec) while preserving operational visibility.
func (p *Pipeline) Run(ctx context.Context) error {
	_, span := tracing.TracerForComponent("feed").Start(ctx, "pipeline.start",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("feed.name", p.feed.Name()),
		),
	)
	defer span.End()

	quotes, err := p.feed.Run(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetAttributes(attribute.String("event", "pipeline_feed_start_error"))
		if p.metrics != nil {
			p.metrics.FeedErrors.WithLabelValues(p.feed.Name(), "start").Inc()
		}
		return fmt.Errorf("pipeline: feed start: %w", err)
	}

	span.AddEvent("pipeline_started")

	p.log.Underlying().Info().Str("feed", p.feed.Name()).Str("event", "pipeline_started").Msg("pipeline started")

	for {
		select {
		case q, ok := <-quotes:
			if !ok {
				span.AddEvent("pipeline_feed_closed")
				p.log.Underlying().Info().Str("event", "pipeline_feed_closed").Msg("pipeline: feed channel closed")
				return nil
			}
			// Assign per-symbol sequence number before publishing.
			q.Seq = p.seq.Next(q.Symbol)
			if p.metrics != nil {
				// Cardinality fix: only label by provider, never by symbol.
				p.metrics.FeedMessagesReceived.
					WithLabelValues(q.Provider).
					Inc()
				// Track data staleness: wall clock minus exchange timestamp.
				p.metrics.DataStaleness.Set(time.Since(q.Timestamp).Seconds())
			}
			// Use context.Background() to break trace propagation on the hot path.
			// The pipeline.start span is long-running (entire process lifetime).
			// Passing its ctx to Publish would create a redis.publish child span
			// for EVERY tick (100k+/sec), overwhelming the OTLP collector.
			// Per-tick Redis publishes are not traced — only client-facing
			// operations (subscription, replay, snapshot) are traced.
			p.publisher.Publish(context.Background(), q)

			// Update liveness heartbeat to detect deadlocks.
			// If this loop deadlocks, the heartbeat goes stale and
			// the Liveness probe fails (see health_check_review.md).
			if p.heartbeat != nil {
				p.heartbeat.Mark()
			}

		case <-ctx.Done():
			span.AddEvent("pipeline_shutting_down")
			p.log.Underlying().Info().Str("event", "pipeline_shutting_down").Msg("pipeline: shutting down")
			return nil
		}
	}
}
