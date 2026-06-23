// Package feed provides the Pipeline that connects a Feed Generator to a
// Publisher Service. It continuously reads quotes from the feed and
// publishes them to the bus. The Pipeline depends only on interfaces —
// no concrete implementations are imported.
package feed

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/pubsub"
)

// Pipeline connects a Feed to a Publisher. It runs the feed's read loop
// and publishes every received quote to the bus. The Pipeline is safe
// for a single Run call; concurrent Run calls are not supported.
type Pipeline struct {
	feed      marketdata.Feed
	publisher pubsub.Publisher
	log       zerolog.Logger
}

// NewPipeline creates a Pipeline. Both parameters are required.
func NewPipeline(feed marketdata.Feed, publisher pubsub.Publisher, log zerolog.Logger) *Pipeline {
	return &Pipeline{
		feed:      feed,
		publisher: publisher,
		log:       log,
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
			p.publisher.Publish(ctx, q)

		case <-ctx.Done():
			p.log.Info().Msg("pipeline: shutting down")
			return nil
		}
	}
}
