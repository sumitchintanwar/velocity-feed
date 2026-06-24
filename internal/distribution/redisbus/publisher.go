// Package redisbus implements a Redis Pub/Sub backed Publisher and Subscriber
// for distributed market data distribution across multiple gateway instances.
//
// Architecture:
//
//	Pipeline → RedisPublisher → Redis Pub/Sub → RedisSubscriber → Local TopicManager → Clients
//
// Redis acts as the message bus between a single Publisher and N Gateway instances.
// Each Gateway runs its own RedisSubscriber that receives events and routes them
// through a local TopicManager for fan-out to connected WebSocket clients.
package redisbus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
)

var jsonLib = jsoniter.ConfigCompatibleWithStandardLibrary

// ChannelPrefix is the prefix for per-symbol Redis Pub/Sub channels.
// Each symbol gets its own channel: "market:AAPL", "market:MSFT", etc.
const ChannelPrefix = "market:"

// wireEnvelope is the serialization format for events sent over Redis.
type wireEnvelope struct {
	Symbol string             `json:"symbol"`
	Type   string             `json:"type"`
	Raw    jsoniter.RawMessage `json:"raw"`
}

// publishRequest is an internal message queued for async delivery.
type publishRequest struct {
	ctx   context.Context
	event marketdata.MarketEvent
}

// Publisher publishes MarketEvents to Redis Pub/Sub channels.
// It uses an internal worker pool to decouple Publish() from Redis network
// latency, making the hot path truly non-blocking.
//
// Topic-based routing: each symbol gets its own Redis channel
// ("market:AAPL", "market:MSFT"). Gateways only subscribe to channels
// their local clients are interested in, avoiding the single-firehose
// bottleneck.
//
// Safe for concurrent use.
type Publisher struct {
	client *redis.Client
	prefix string // channel prefix, default "market:"
	log    zerolog.Logger

	// Async worker pool.
	queue   chan publishRequest
	workers int
	wg      sync.WaitGroup
	doneCh  chan struct{}

	// Lifecycle.
	closed  atomic.Bool
	dropped atomic.Uint64
}

// PublisherOption configures the Publisher.
type PublisherOption func(*Publisher)

// WithChannelPrefix overrides the default channel prefix ("market:").
func WithChannelPrefix(prefix string) PublisherOption {
	return func(p *Publisher) { p.prefix = prefix }
}

// WithWorkers sets the number of worker goroutines (default: 4).
func WithWorkers(n int) PublisherOption {
	return func(p *Publisher) { p.workers = n }
}

// WithQueueSize sets the internal publish queue size (default: 8192).
func WithQueueSize(n int) PublisherOption {
	return func(p *Publisher) { p.queue = make(chan publishRequest, n) }
}

// NewPublisher creates a Redis-backed Publisher with async workers.
func NewPublisher(client *redis.Client, log zerolog.Logger, opts ...PublisherOption) *Publisher {
	p := &Publisher{
		client:  client,
		prefix:  ChannelPrefix,
		log:     log,
		workers: 4,
		queue:   make(chan publishRequest, 8192),
		doneCh:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	// Start worker pool.
	p.wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go p.worker(i)
	}
	return p
}

// worker reads from the publish queue and sends to Redis.
func (p *Publisher) worker(id int) {
	defer p.wg.Done()

	for req := range p.queue {
		p.doPublish(req.ctx, req.event)
	}
}

// Publish enqueues a MarketEvent for async delivery to Redis.
// The contract is truly non-blocking: serialization happens inline (fast),
// and the network call happens in a background worker. If the internal
// queue is full (all workers busy + buffer saturated), the event is dropped.
func (p *Publisher) Publish(ctx context.Context, event marketdata.MarketEvent) {
	if p.closed.Load() {
		return
	}

	// Serialize inline — this is fast (no network I/O).
	raw, err := jsonLib.Marshal(event)
	if err != nil {
		p.log.Warn().Err(err).Str("symbol", event.EventSymbol()).
			Msg("redis-publisher: failed to marshal event")
		return
	}

	env := wireEnvelope{
		Symbol: event.EventSymbol(),
		Type:   event.EventType(),
		Raw:    raw,
	}

	payload, err := jsonLib.Marshal(env)
	if err != nil {
		p.log.Warn().Err(err).Msg("redis-publisher: failed to marshal envelope")
		return
	}

	// Non-blocking enqueue. If the queue is full, drop the event.
	select {
	case p.queue <- publishRequest{ctx: ctx, event: &marshaledEvent{channel: p.prefix + event.EventSymbol(), payload: payload}}:
	default:
		p.dropped.Add(1)
		if p.dropped.Load()%1000 == 1 {
			p.log.Warn().Uint64("total_dropped", p.dropped.Load()).
				Msg("redis-publisher: queue full, event dropped (rate-limited)")
		}
	}
}

// doPublish performs the actual Redis publish call (called by workers).
func (p *Publisher) doPublish(ctx context.Context, event marketdata.MarketEvent) {
	me, ok := event.(*marshaledEvent)
	if !ok {
		return
	}

	if err := p.client.Publish(ctx, me.channel, me.payload).Err(); err != nil {
		p.dropped.Add(1)
		if p.dropped.Load()%1000 == 1 {
			p.log.Warn().Err(err).Uint64("total_dropped", p.dropped.Load()).
				Msg("redis-publisher: publish failed (rate-limited)")
		}
	}
}

// Close shuts down the publisher: stops accepting new events, drains the
// worker queue, and waits for all in-flight publishes to complete.
func (p *Publisher) Close() {
	if p.closed.Swap(true) {
		return // already closed
	}
	close(p.queue) // signal workers to drain and exit
	p.wg.Wait()   // block until all workers finish
	close(p.doneCh)
}

// Dropped returns the total number of events dropped (queue full or Redis errors).
func (p *Publisher) Dropped() uint64 {
	return p.dropped.Load()
}

// Client returns the underlying Redis client for health checks.
func (p *Publisher) Client() *redis.Client {
	return p.client
}

// Done returns a channel that is closed after all workers have exited.
func (p *Publisher) Done() <-chan struct{} {
	return p.doneCh
}

// ChannelForSymbol returns the Redis channel name for a given symbol.
func (p *Publisher) ChannelForSymbol(symbol string) string {
	return p.prefix + symbol
}

// marshaledEvent is a pre-serialized event passed to workers to avoid
// redundant marshaling.
type marshaledEvent struct {
	channel string
	payload []byte
}

// EventSymbol implements MarketEvent.
func (m *marshaledEvent) EventSymbol() string { return m.channel }

// EventType implements MarketEvent.
func (m *marshaledEvent) EventType() string { return "serialized" }

// NewClient creates a new Redis client from configuration.
func NewClient(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}

// Ping checks Redis connectivity.
func Ping(ctx context.Context, client *redis.Client) error {
	return client.Ping(ctx).Err()
}

// CloseClient closes the Redis connection.
func CloseClient(client *redis.Client) error {
	if client == nil {
		return nil
	}
	return fmt.Errorf("close redis: %w", client.Close())
}
