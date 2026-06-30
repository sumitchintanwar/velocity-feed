package redisbus

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Subscriber listens on Redis Pub/Sub channels and forwards received
// MarketEvents to a local TopicManager. Each gateway instance runs one
// Subscriber that feeds its own TopicManager, enabling horizontal scaling
// without shared state between gateways.
//
// Topic-based routing: the Subscriber dynamically subscribes/unsubscribes
// from per-symbol Redis channels based on local client demand. This avoids
// the single-firehose bottleneck where every gateway processes every event.
//
// Lifecycle:
//
//	Start()          → starts the listener loop
//	Subscribe(sym)   → subscribes to a Redis channel for the given symbol
//	Unsubscribe(sym) → unsubscribes when no local clients need the symbol
//	Stop()           → graceful shutdown, waits for in-flight messages
type Subscriber struct {
	client  *redis.Client
	prefix  string
	tm      topicmanager.Manager
	log     *log.Logger
	cancel  context.CancelFunc
	doneCh  chan struct{}

	// Channel management.
	channels   map[string]struct{} // symbols we're subscribed to
	channelsMu sync.RWMutex

	// PubSub handle for dynamic subscribe/unsubscribe.
	pubsub   *redis.PubSub
	pubsubMu sync.Mutex

	// Lifecycle.
	startOnce sync.Once
	stopOnce  sync.Once
	received  atomic.Uint64

	// Stale data protection: tracks when the last message was received.
	// If no message arrives within StaleThreshold, the OnStale callback
	// is invoked so the gateway can notify or disconnect clients.
	lastMessageAt   atomic.Int64 // unix nanoseconds
	staleOnce       sync.Once
	onStale         func()        // called once when staleness detected
	staleThreshold  time.Duration // max time without messages before declaring stale
}

// SubscriberOption configures the Subscriber.
type SubscriberOption func(*Subscriber)

// WithSubscriberPrefix overrides the default channel prefix ("market:").
func WithSubscriberPrefix(prefix string) SubscriberOption {
	return func(s *Subscriber) { s.prefix = prefix }
}

// WithStaleCallback registers a callback invoked once when no message
// is received for the given duration. Used to broadcast degradation
// notices or disconnect clients during a Redis outage.
func WithStaleCallback(threshold time.Duration, callback func()) SubscriberOption {
	return func(s *Subscriber) {
		s.onStale = callback
		s.staleThreshold = threshold
	}
}

// staleThreshold is the default time without messages before staleness is declared.
var defaultStaleThreshold = 5 * time.Second

// NewSubscriber creates a Redis-backed Subscriber that forwards events to the
// given local TopicManager. Call Start() to begin listening, then
// Subscribe()/Unsubscribe() to manage per-symbol channels.
func NewSubscriber(client *redis.Client, tm topicmanager.Manager, l *log.Logger, opts ...SubscriberOption) *Subscriber {
	s := &Subscriber{
		client:         client,
		prefix:         ChannelPrefix,
		tm:             tm,
		log:            l,
		doneCh:         make(chan struct{}),
		channels:       make(map[string]struct{}),
		staleThreshold: defaultStaleThreshold,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start begins the listener loop. Safe to call multiple times.
func (s *Subscriber) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		ctx, s.cancel = context.WithCancel(ctx)

		// Create the PubSub handle.
		s.pubsubMu.Lock()
		s.pubsub = s.client.Subscribe(ctx)

		// Batch-subscribe to all channels registered before Start was called.
		if len(s.channels) > 0 {
			subs := make([]string, 0, len(s.channels))
			for sym := range s.channels {
				subs = append(subs, s.prefix+sym)
			}
			if err := s.pubsub.Subscribe(ctx, subs...); err != nil {
				s.log.Underlying().Warn().Err(err).Str("event", "initial_subscribe_failed").
					Msg("redis-subscriber: initial subscribe failed")
			}
		}
		s.pubsubMu.Unlock()

		go s.listen(ctx)
	})
}

// Subscribe adds a symbol to the subscription set. The subscriber will
// listen on the corresponding Redis channel ("market:AAPL").
// Optimized for rapid reconnection: updates internal state first (fast),
// then issues the Redis SUBSCRIBE (slow) outside the lock.
func (s *Subscriber) Subscribe(symbol string) {
	s.channelsMu.Lock()
	s.channels[symbol] = struct{}{}
	s.channelsMu.Unlock()

	s.pubsubMu.Lock()
	ps := s.pubsub
	s.pubsubMu.Unlock()

	// Issue Redis SUBSCRIBE outside the lock to avoid blocking other
	// subscribe/unsubscribe calls during thundering herd reconnects.
	if ps != nil {
		ch := s.prefix + symbol
		if err := ps.Subscribe(context.Background(), ch); err != nil {
			s.log.Underlying().Warn().Err(err).Str("symbol", symbol).
				Str("event", "subscribe_failed").
				Msg("redis-subscriber: failed to subscribe")
		}
	}
}

// SubscribeBatch subscribes to multiple symbols in a single Redis
// SUBSCRIBE command. This is significantly faster than calling Subscribe
// in a loop during thundering herd reconnects (1 RTT vs N RTTs).
func (s *Subscriber) SubscribeBatch(symbols []string) {
	if len(symbols) == 0 {
		return
	}

	s.channelsMu.Lock()
	chs := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		if _, ok := s.channels[sym]; !ok {
			s.channels[sym] = struct{}{}
			chs = append(chs, s.prefix+sym)
		}
	}
	s.channelsMu.Unlock()

	if len(chs) == 0 {
		return // all symbols already subscribed
	}

	s.pubsubMu.Lock()
	ps := s.pubsub
	s.pubsubMu.Unlock()

	if ps != nil {
		if err := ps.Subscribe(context.Background(), chs...); err != nil {
			s.log.Underlying().Warn().Err(err).Int("count", len(chs)).
				Str("event", "batch_subscribe_failed").
				Msg("redis-subscriber: batch subscribe failed")
		}
	}
}

// Unsubscribe removes a symbol from the subscription set.
// Optimized: updates internal state first, then issues Redis UNSUBSCRIBE.
func (s *Subscriber) Unsubscribe(symbol string) {
	s.channelsMu.Lock()
	delete(s.channels, symbol)
	s.channelsMu.Unlock()

	s.pubsubMu.Lock()
	ps := s.pubsub
	s.pubsubMu.Unlock()

	if ps != nil {
		ch := s.prefix + symbol
		if err := ps.Unsubscribe(context.Background(), ch); err != nil {
			s.log.Underlying().Warn().Err(err).Str("symbol", symbol).
				Str("event", "unsubscribe_failed").
				Msg("redis-subscriber: failed to unsubscribe")
		}
	}
}

// Stop signals the subscriber to shut down and waits for the listen goroutine
// to exit. Safe to call multiple times.
func (s *Subscriber) Stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		<-s.doneCh
	})
}

// Received returns the total number of events received from Redis.
func (s *Subscriber) Received() uint64 {
	return s.received.Load()
}

// Done returns a channel that is closed when the subscriber has stopped.
func (s *Subscriber) Done() <-chan struct{} {
	return s.doneCh
}

// SubscribedSymbols returns a snapshot of currently subscribed symbols.
func (s *Subscriber) SubscribedSymbols() []string {
	s.channelsMu.RLock()
	defer s.channelsMu.RUnlock()

	syms := make([]string, 0, len(s.channels))
	for sym := range s.channels {
		syms = append(syms, sym)
	}
	return syms
}

// listen is the main loop that reads Redis messages and forwards them.
func (s *Subscriber) listen(ctx context.Context) {
	defer close(s.doneCh)

	ch := s.pubsub.Channel()
	if ch == nil {
		s.log.Underlying().Error().Str("event", "channel_get_failed").Msg("redis-subscriber: failed to get channel")
		return
	}

	s.log.Underlying().Info().Str("event", "redis_listening").Msg("redis-subscriber: listening")

	// Staleness check timer — fires every second to detect Redis outages.
	staleTicker := time.NewTicker(time.Second)
	defer staleTicker.Stop()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				s.log.Underlying().Info().Str("event", "channel_closed").Msg("redis-subscriber: channel closed")
				return
			}
			// Record last message time for staleness detection.
			s.lastMessageAt.Store(time.Now().UnixNano())
			// Use background context for TopicManager.Publish to avoid
			// cancellation during graceful shutdown — we want in-flight
			// messages to complete delivery to local clients.
			s.handleMessage(context.Background(), msg.Payload, msg.Channel)

		case <-staleTicker.C:
			s.checkStaleness()

		case <-ctx.Done():
			s.log.Underlying().Info().Str("event", "redis_shutting_down").Msg("redis-subscriber: shutting down")
			return
		}
	}
}

// checkStaleness detects if no messages have been received for the
// configured threshold. If so, it invokes the OnStale callback once.
func (s *Subscriber) checkStaleness() {
	if s.onStale == nil {
		return
	}
	last := s.lastMessageAt.Load()
	if last == 0 {
		return // no message received yet, not stale
	}
	elapsed := time.Since(time.Unix(0, last))
	if elapsed > s.staleThreshold {
		s.staleOnce.Do(func() {
			s.log.Underlying().Warn().Dur("since_last", elapsed).
				Str("event", "stale_data_detected").
				Msg("redis-subscriber: stale data detected, invoking callback")
			s.onStale()
		})
	}
}

// IsStale returns true if no message has been received for longer than
// the configured threshold. Used by health checks to report degraded state.
func (s *Subscriber) IsStale() bool {
	last := s.lastMessageAt.Load()
	if last == 0 {
		return false
	}
	return time.Since(time.Unix(0, last)) > s.staleThreshold
}

// handleMessage deserializes a Redis message and forwards it to the local
// TopicManager. Creates a "redis.consume" span to trace the consume operation.
// Span attributes follow low-cardinality rules: channel, operation.
//
// Trace propagation: extracts the producer's trace context from the wire
// envelope, making this consumer span a child of the producer span. This
// creates a distributed trace: Pipeline → redis.publish → redis.consume → Client.
func (s *Subscriber) handleMessage(ctx context.Context, payload string, channel string) {
	var env wireEnvelope
	if err := jsonLib.Unmarshal([]byte(payload), &env); err != nil {
		s.log.Underlying().Warn().Err(err).Str("event", "envelope_unmarshal_failed").
			Msg("redis-subscriber: failed to unmarshal envelope")
		return
	}

	// Extract producer trace context from the envelope.
	// This makes the consumer span a child of the producer span,
	// creating a single distributed trace across the Redis boundary.
	ctx = tracing.ExtractTraceContext(ctx, env.TraceCtx)

	ctx, span := tracing.TracerForComponent("redis").Start(ctx, "redis.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("redis.channel", channel),
			attribute.String("redis.operation", "consume"),
		),
	)
	defer span.End()

	// Reconstruct the event from the raw JSON envelope, without parsing the inner marketdata JSON.
	// This maintains the zero-copy optimization for the Gateway.
	event := marketdata.PreEncodedEvent{
		Symbol:     env.Symbol,
		Typ:        env.Type,
		SeqNum:     env.Seq,
		Time:       env.Timestamp,
		EncodedMsg: env.Raw,
	}

	s.received.Add(1)

	s.tm.Publish(ctx, event)
}
