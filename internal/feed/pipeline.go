// Package feed provides the Pipeline that connects a Feed Generator to a
// Publisher Service. It continuously reads quotes from the feed and
// publishes them to the bus. The Pipeline depends only on interfaces —
// no concrete implementations are imported.
package feed

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/pubsub"
)

// Pipeline connects a Feed to a Publisher. It runs the feed's read loop
// and publishes every received quote to the bus. The Pipeline is safe
// for a single Run call; concurrent Run calls are not supported.
type Pipeline struct {
	feed      marketdata.Feed
	publisher pubsub.Publisher
	log       zerolog.Logger
	metrics   *platform.Metrics // optional; nil disables instrumentation
}

// NewPipeline creates a Pipeline. The metrics parameter is optional —
// pass nil to disable Prometheus instrumentation.
func NewPipeline(feed marketdata.Feed, publisher pubsub.Publisher, log zerolog.Logger, metrics *platform.Metrics) *Pipeline {
	return &Pipeline{
		feed:      feed,
		publisher: publisher,
		log:       log,
		metrics:   metrics,
	}
}

// Run starts the pipeline. It blocks until:
//   - ctx is cancelled (clean shutdown)
//   - the feed returns an error
//   - the feed channel is closed (feed stopped)
//
// A nil error means the pipeline shut down cleanly.
func (p *Pipeline) Run(ctx context.Context) error {
	quotes, err := p.feed.Run(ctx)
	if err != nil {
		if p.metrics != nil {
			p.metrics.FeedErrors.WithLabelValues(p.feed.Name(), "start").Inc()
		}
		return fmt.Errorf("pipeline: feed start: %w", err)
	}

	p.log.Info().Str("feed", p.feed.Name()).Msg("pipeline started")

	for {
		select {
		case q, ok := <-quotes:
			if !ok {
				p.log.Info().Msg("pipeline: feed channel closed")
				return nil
			}
			if p.metrics != nil {
				// Cardinality fix: only label by provider, never by symbol.
				p.metrics.FeedMessagesReceived.
					WithLabelValues(q.Provider).
					Inc()
				// Track data staleness: wall clock minus exchange timestamp.
				p.metrics.DataStaleness.Set(time.Since(q.Timestamp).Seconds())
			}
			p.publisher.Publish(ctx, q)

		case <-ctx.Done():
			p.log.Info().Msg("pipeline: shutting down")
			return nil
		}
	}
}
