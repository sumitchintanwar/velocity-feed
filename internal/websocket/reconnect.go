package websocket

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

// Backoff configures exponential backoff with jitter.
type Backoff struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	Jitter     float64 // ± fraction, e.g. 0.2 = ±20%
}

// DefaultBackoff returns the recommended production backoff:
// 1s initial, 30s max, 2x multiplier, ±20% jitter.
func DefaultBackoff() Backoff {
	return Backoff{
		Initial:    1 * time.Second,
		Max:        30 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.2,
	}
}

// dialTimeout is the timeout for WebSocket dial operations to prevent
// blocking on blackhole nodes where TCP SYN packets are silently dropped.
const dialTimeout = 5 * time.Second

// Delay returns the delay for the given attempt number (0-indexed).
// attempt=0 → ~1s, attempt=1 → ~2s, attempt=2 → ~4s, ... capped at Max.
func (b Backoff) Delay(attempt int) time.Duration {
	delay := float64(b.Initial) * math.Pow(b.Multiplier, float64(attempt))
	if delay > float64(b.Max) {
		delay = float64(b.Max)
	}
	// Apply ±jitter.
	if b.Jitter > 0 {
		jitter := delay * b.Jitter
		delay = delay - jitter + rand.Float64()*2*jitter //nolint:gosec
	}
	return time.Duration(delay)
}

// ReconnectClient is a client-side WebSocket wrapper that automatically
// reconnects on connection loss and resubscribes to previously subscribed
// topics. It implements the design from AUTOMATIC_RECONNECT_DESIGN.md.
//
// Usage:
//
//	rc := websocket.NewReconnectClient(url, zerolog.Nop(), nil)
//	rc.Subscribe("AAPL", "MSFT")
//	rc.Start()
//	defer rc.Stop()
//	for msg := range rc.Events() {
//	    // handle msg
//	}
type ReconnectClient struct {
	url    string
	dialer *websocket.Dialer
	log    zerolog.Logger

	// Subscription registry — client owns subscription state.
	subMu       sync.RWMutex
	subscriptions map[string]bool

	// Active connection.
	connMu sync.Mutex
	conn   *websocket.Conn

	// Outbound channel for decoded server messages.
	events chan ServerMessage

	// Control signals.
	stopCh chan struct{}
	done   chan struct{}

	// Backoff state.
	attempt atomic.Int64

	// Metrics (optional — nil disables).
	reconnectAttempts   func()
	reconnectSuccess    func()
	reconnectFailures   func()
	resubscriptions     func()

	// backoffFn is overridable for testing.
	backoffFn func(attempt int) time.Duration
}

// ReconnectOption configures a ReconnectClient.
type ReconnectOption func(*ReconnectClient)

// WithBackoff overrides the default backoff strategy.
func WithBackoff(b Backoff) ReconnectOption {
	return func(rc *ReconnectClient) {
		rc.backoffFn = func(attempt int) time.Duration { return b.Delay(attempt) }
	}
}

// WithDialer overrides the default WebSocket dialer.
func WithDialer(d *websocket.Dialer) ReconnectOption {
	return func(rc *ReconnectClient) {
		rc.dialer = d
	}
}

// WithMetrics wires Prometheus counters for reconnect observability.
func WithMetrics(reconnectAttempts, reconnectSuccess, reconnectFailures, resubscriptions func()) ReconnectOption {
	return func(rc *ReconnectClient) {
		rc.reconnectAttempts = reconnectAttempts
		rc.reconnectSuccess = reconnectSuccess
		rc.reconnectFailures = reconnectFailures
		rc.resubscriptions = resubscriptions
	}
}

// NewReconnectClient creates a new ReconnectClient.
func NewReconnectClient(url string, log zerolog.Logger, opts ...ReconnectOption) *ReconnectClient {
	rc := &ReconnectClient{
		url:           url,
		dialer:        websocket.DefaultDialer,
		log:           log,
		subscriptions: make(map[string]bool),
		events:        make(chan ServerMessage, 256),
		stopCh:        make(chan struct{}),
		done:          make(chan struct{}),
	}
	for _, opt := range opts {
		opt(rc)
	}
	if rc.backoffFn == nil {
		b := DefaultBackoff()
		rc.backoffFn = func(attempt int) time.Duration { return b.Delay(attempt) }
	}
	return rc
}

// Subscribe registers symbols for automatic resubscription on reconnect.
// Thread-safe. Can be called before or after Start.
func (rc *ReconnectClient) Subscribe(symbols ...string) {
	rc.subMu.Lock()
	defer rc.subMu.Unlock()
	for _, s := range symbols {
		rc.subscriptions[s] = true
	}
	// If already connected, send subscribe immediately.
	rc.connMu.Lock()
	conn := rc.conn
	rc.connMu.Unlock()
	if conn != nil {
		rc.sendSubscribe(conn, symbols...)
	}
}

// Unsubscribe removes symbols from the local registry and sends an
// unsubscribe message if connected.
func (rc *ReconnectClient) Unsubscribe(symbols ...string) {
	rc.subMu.Lock()
	defer rc.subMu.Unlock()
	for _, s := range symbols {
		delete(rc.subscriptions, s)
	}
	rc.connMu.Lock()
	conn := rc.conn
	rc.connMu.Unlock()
	if conn != nil {
		rc.sendUnsubscribe(conn)
	}
}

// Events returns the channel of decoded server messages.
// Closed when Stop is called.
func (rc *ReconnectClient) Events() <-chan ServerMessage {
	return rc.events
}

// Start begins the connection loop. It blocks until the first connection
// is established and subscriptions are sent, then runs the reconnect loop
// in the background.
func (rc *ReconnectClient) Start() error {
	ready := make(chan error, 1)
	go rc.run(ready)
	return <-ready
}

// Stop gracefully shuts down the client. It closes the stop channel and
// the underlying connection so that readPump unblocks.
func (rc *ReconnectClient) Stop() {
	close(rc.stopCh)
	rc.closeConn()
	<-rc.done
}

// Connected returns true if the client currently has an active connection.
func (rc *ReconnectClient) Connected() bool {
	rc.connMu.Lock()
	defer rc.connMu.Unlock()
	return rc.conn != nil
}

// connectionAlive returns true if the connection exists and its readPump
// is still running (done channel not closed).
func (rc *ReconnectClient) connectionAlive() bool {
	rc.connMu.Lock()
	conn := rc.conn
	rc.connMu.Unlock()
	if conn == nil {
		return false
	}
	// Check if done channel is closed (readPump exited).
	select {
	case <-rc.done:
		return false
	default:
		return true
	}
}

// Symbols returns the currently subscribed symbols.
func (rc *ReconnectClient) Symbols() []string {
	rc.subMu.RLock()
	defer rc.subMu.RUnlock()
	symbols := make([]string, 0, len(rc.subscriptions))
	for s := range rc.subscriptions {
		symbols = append(symbols, s)
	}
	return symbols
}

// ---------- internal ----------

// run is the main reconnect loop. ready is closed (with error or nil)
// after the first connection attempt completes.
func (rc *ReconnectClient) run(ready chan<- error) {
	defer close(rc.done)
	defer close(rc.events)

	firstAttempt := true

	for {
		select {
		case <-rc.stopCh:
			rc.closeConn()
			return
		default:
		}

		delay := rc.backoffFn(int(rc.attempt.Load()))
		if !firstAttempt {
			rc.log.Info().Dur("delay", delay).Int64("attempt", rc.attempt.Load()).
				Msg("reconnect: waiting before next attempt")
			select {
			case <-rc.stopCh:
				rc.closeConn()
				return
			case <-time.After(delay):
			}
		}

		if rc.reconnectAttempts != nil {
			rc.reconnectAttempts()
		}

		conn, err := rc.connect()
		if err != nil {
			rc.log.Warn().Err(err).Int64("attempt", rc.attempt.Load()).
				Msg("reconnect: failed")
			if rc.reconnectFailures != nil {
				rc.reconnectFailures()
			}
			rc.attempt.Add(1)
			if firstAttempt {
				ready <- err
				firstAttempt = false
			}
			continue
		}

		// Success — reset attempt counter.
		rc.attempt.Store(0)
		if rc.reconnectSuccess != nil {
			rc.reconnectSuccess()
		}
		rc.log.Info().Str("url", rc.url).Msg("reconnect: connected")

		// Resubscribe.
		rc.resubscribeAll(conn)

		if firstAttempt {
			ready <- nil
			firstAttempt = false
		}

		// Read loop — blocks until connection drops.
		rc.readPump(conn)
	}
}

// connect establishes a new WebSocket connection and starts the write pump.
func (rc *ReconnectClient) connect() (*websocket.Conn, error) {
	rc.connMu.Lock()
	url := rc.url
	rc.connMu.Unlock()

	dialCtx, dialCancel := context.WithTimeout(context.Background(), dialTimeout)
	defer dialCancel()
	conn, _, err := rc.dialer.DialContext(dialCtx, url, nil)
	if err != nil {
		return nil, err
	}
	rc.connMu.Lock()
	defer rc.connMu.Unlock()
	select {
	case <-rc.stopCh:
		_ = conn.Close()
		return nil, context.Canceled
	default:
		rc.conn = conn
	}
	return conn, nil
}

// closeConn closes the current connection.
func (rc *ReconnectClient) closeConn() {
	rc.connMu.Lock()
	defer rc.connMu.Unlock()
	if rc.conn != nil {
		_ = rc.conn.Close()
		rc.conn = nil
	}
}

// readPump reads messages from the connection and dispatches them.
func (rc *ReconnectClient) readPump(conn *websocket.Conn) {
	defer func() {
		rc.connMu.Lock()
		if rc.conn == conn {
			_ = conn.Close()
			rc.conn = nil
		}
		rc.connMu.Unlock()
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				rc.log.Debug().Msg("reconnect: connection closed normally")
			} else {
				rc.log.Error().Err(err).Msg("reconnect: read error")
			}
			return
		}

		var msg ServerMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			rc.log.Warn().Err(err).Msg("reconnect: invalid message")
			continue
		}

		select {
		case rc.events <- msg:
		case <-rc.stopCh:
			return
		}
	}
}

// resubscribeAll sends subscribe messages for all tracked symbols.
func (rc *ReconnectClient) resubscribeAll(conn *websocket.Conn) {
	rc.subMu.RLock()
	symbols := make([]string, 0, len(rc.subscriptions))
	for s := range rc.subscriptions {
		symbols = append(symbols, s)
	}
	rc.subMu.RUnlock()

	if len(symbols) == 0 {
		rc.log.Debug().Msg("reconnect: no subscriptions to restore")
		return
	}

	rc.sendSubscribe(conn, symbols...)
	if rc.resubscriptions != nil {
		rc.resubscriptions()
	}
	rc.log.Info().Strs("symbols", symbols).Msg("reconnect: resubscribed")
}

// sendSubscribe writes a subscribe message to the connection.
func (rc *ReconnectClient) sendSubscribe(conn *websocket.Conn, symbols ...string) {
	msg := ClientMessage{
		Action:  "subscribe",
		Symbols: symbols,
	}
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(msg); err != nil {
		rc.log.Error().Err(err).Strs("symbols", symbols).Msg("reconnect: failed to send subscribe")
	}
}

// sendUnsubscribe writes an unsubscribe message to the connection.
func (rc *ReconnectClient) sendUnsubscribe(conn *websocket.Conn) {
	msg := ClientMessage{
		Action: "unsubscribe",
	}
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(msg); err != nil {
		rc.log.Error().Err(err).Msg("reconnect: failed to send unsubscribe")
	}
}
