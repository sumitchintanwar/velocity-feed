package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	wscontext "github.com/sumit/rtmds/internal/correlation/websocket"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/replay/engine"
	"github.com/sumit/rtmds/internal/sequencer"
	"github.com/sumit/rtmds/internal/snapshot"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/wal"
	"github.com/sumit/rtmds/pkg/marketdata"
	"github.com/sumit/rtmds/pkg/protocol"
	"go.opentelemetry.io/otel/attribute"
)

// Replayer defines the interface for fetching historical messages from the WAL.
type Replayer interface {
	Replay(resumeFrom uint64) ([]*wal.Message, error)
}

// bufferPool reuses bytes.Buffer instances to reduce GC pressure.
// At 5k clients × 10k msgs/sec, this eliminates millions of allocs/sec.
var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

const (
	// SC1: After this many consecutive drops, disconnect the slow client.
	maxConsecutiveDrops = 100

	// CH2: Maximum symbols a client can subscribe to in one request.
	maxSymbolsPerSubscription = 100

	// SC2: Control channel capacity. Must exceed maxSymbolsPerSubscription (100)
	// to prevent drops when a client receives a confirmation + N snapshots on subscribe.
	controlQueueSize = 128
)

// Client represents a single WebSocket connection. Each client owns:
//   - A read goroutine (reads commands from the socket)
//   - A write goroutine (writes events to the socket)
//   - A topicmanager.Handle for subscription management
//
// The actual outbound queue lives inside the TopicManager's Handle channel.
// Clients are isolated — a slow consumer cannot block other clients
// or the publish hot path.
type Client struct {
	id      string
	conn    *websocket.Conn
	tm      topicmanager.Manager
	snap    *snapshot.Service // optional; nil disables snapshot delivery
	seqVal  *sequencer.Validator
	log     *log.Logger
	metrics *platform.Metrics

	// heartbeat tracks this client's pong timestamps for dead connection detection.
	heartbeat *HeartbeatManager

	// control is a channel for control messages (errors, confirmations)
	// from readPump to writePump. This ensures all socket writes
	// happen from a single goroutine.
	control chan ServerMessage

	// handleMu protects handle from concurrent read/write between
	// readPump and writePump.
	handleMu sync.RWMutex
	handle   topicmanager.Handle

	// subUpdated signals writePump that a new handle or replay channel is available.
	subUpdated chan struct{}

	// cancelCtx cancels the client's context, used by Shutdown to
	// force readPump to exit.
	cancelCtx context.CancelFunc

	// done is closed by the read pump when it exits. writePump
	// selects on this to know when to stop.
	done chan struct{}

	// cancelOnce ensures handle.Cancel() is called at most once.
	cancelOnce sync.Once

	// SC1: consecutive drops counter for slow consumer detection.
	consecutiveDrops atomic.Int64

	// replayCh receives historical events during an active replay session.
	// nil when no replay is active. writePump multiplexes this alongside
	// live market events from handle.C().
	replayCh chan *marketdata.CachedEvent

	// replayMu protects replayCh, activeReplaySession, and replayCancel.
	replayMu sync.RWMutex

	// activeReplaySession tracks the current replay session for pause/resume/stop.
	activeReplaySession *engine.Session

	// replayCancel cancels the context for the active replay session.
	replayCancel context.CancelFunc

	// replay is the handler for old Postgres time-range replay.
	replay *ReplayHandler

	// walReplayer fetches sequential missing messages.
	walReplayer Replayer

	// startReconnectCh signals writePump to begin buffering live messages.
	startReconnectCh chan struct{}

	// reconnectCh communicates the result of a WAL replay asynchronously to writePump.
	reconnectCh chan reconnectResult
	// redirector checks if a symbol subscription should be handled elsewhere.
	redirector Redirector

	// format stores the negotiated subprotocol (JSON, Protobuf, FlatBuffers)
	format protocol.Format

	// serializer is the protocol-specific encoder
	serializer protocol.Serializer

	// isReconnecting tracks whether a WAL reconnect is actively buffering live events.
	isReconnecting atomic.Bool

	// writeMu protects the WebSocket connection from concurrent writes.
	writeMu sync.Mutex
}

type reconnectResult struct {
	msgs      []*wal.Message
	err       error
	resumeSeq map[string]uint64
	duration  time.Duration
}

func newClient(id string, conn *websocket.Conn, tm topicmanager.Manager, snap *snapshot.Service, l *log.Logger, cancelCtx context.CancelFunc, metrics *platform.Metrics, heartbeat *HeartbeatManager, replay *ReplayHandler, wal Replayer, redirector Redirector, format protocol.Format, serializer protocol.Serializer) *Client {
	return &Client{
		id:               id,
		conn:             conn,
		tm:               tm,
		snap:             snap,
		seqVal:           sequencer.NewValidator(),
		log:              l,
		metrics:          metrics,
		heartbeat:        heartbeat,
		control:          make(chan ServerMessage, controlQueueSize),
		subUpdated:       make(chan struct{}, 1),
		cancelCtx:        cancelCtx,
		done:             make(chan struct{}),
		replay:           replay,
		walReplayer:      wal,
		startReconnectCh: make(chan struct{}, 1),
		reconnectCh:      make(chan reconnectResult, 1),
		redirector:       redirector,
		format:           format,
		serializer:       serializer,
	}
}

// ---------- Read Pump ----------

// readPump reads incoming messages from the WebSocket. It blocks until
// the connection is closed or an error occurs. When it returns, the
// write pump will exit via the done channel.
//
// GL3: readPump does NOT close the connection — only writePump does.
func (c *Client) readPump(ctx context.Context) {
	defer func() {
		// Signal write pump to exit.
		close(c.done)
		// GL3: Removed conn.Close() here — only writePump closes the connection.
	}()

	c.conn.SetReadLimit(maxMessageSize)

	if c.heartbeat != nil {
		c.heartbeat.Register(c.id, func() {
			c.cancelAll()
			c.shutdown(ctx)
		}, func() {
			c.sendControl(ServerMessage{Type: "ping", Payload: marketdata.PingMessage{Type: "ping", Timestamp: time.Now().Unix()}})
		})
		defer c.heartbeat.Unregister(c.id)
	}

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Debug(ctx, c.log).Str("event", "client_closed_connection").Msg("client closed connection")
			} else {
				log.Error(ctx, c.log).Err(err).Str("event", "read_error").Msg("read error")
			}
			return
		}

		var cm ClientMessage
		if err := json.Unmarshal(msg, &cm); err != nil {
			log.Warn(ctx, c.log).Err(err).Str("event", "invalid_message").Msg("invalid message")
			c.sendControl(ServerMessage{Type: "error", Payload: "invalid message format"})
			continue
		}

		c.handleMessage(ctx, cm)
	}
}

// handleMessage dispatches a client message to the appropriate action.
func (c *Client) handleMessage(ctx context.Context, cm ClientMessage) {
	switch cm.Action {
	case "subscribe":
		if len(cm.Symbols) == 0 {
			c.sendControl(ServerMessage{Type: "error", Payload: "no symbols provided"})
			return
		}
		// CH2: Enforce per-client subscription limit.
		if len(cm.Symbols) > maxSymbolsPerSubscription {
			c.sendControl(ServerMessage{Type: "error", Payload: "too many symbols (max 100)"})
			return
		}

		// Trace boundary: "subscription_request"
		subCtx, span := wscontext.DeriveMessageContext(ctx, "websocket.subscription_request")
		span.SetAttributes(attribute.Int("subscription.symbol_count", len(cm.Symbols)))
		defer span.End()

		var validSymbols []string
		for _, sym := range cm.Symbols {
			if c.redirector != nil {
				target := c.redirector.RedirectTarget(sym)
				if target != "" {
					// Send redirect control message and skip subscribing locally
					c.sendControl(ServerMessage{
						Type: "redirect",
						Payload: RedirectPayload{
							Symbol: sym,
							URL:    target,
						},
					})
					continue
				}
			}
			validSymbols = append(validSymbols, sym)
		}

		if len(validSymbols) == 0 {
			// All requested symbols were redirected.
			return
		}

		// Cancel previous subscription if any.
		c.handleMu.Lock()
		if c.handle != nil {
			c.handle.Cancel()
			c.metrics.WSActiveSubscriptions.Dec()
		}
		c.handle = c.tm.Subscribe(c.id, validSymbols...)
		c.metrics.WSActiveSubscriptions.Inc()
		c.handleMu.Unlock()

		// Signal writePump to pick up the new handle.
		select {
		case c.subUpdated <- struct{}{}:
		default:
		}
		c.sendControl(ServerMessage{Type: "subscribed", Payload: validSymbols})
		log.Info(ctx, c.log).Strs("symbols", validSymbols).Str("event", "client_subscribed").Msg("subscribed")

		// Send current snapshots before live streaming begins.
		if c.snap != nil {
			cached := c.snap.GetMultiple(subCtx, validSymbols)
			span.SetAttributes(attribute.Int("snapshot.cached_count", len(cached)))
			for _, ce := range cached {
				// We don't send snapshots via control, we enqueue them into replayCh to leverage writePump formatting
				_ = ce // Wait, we can just send as control message, but control messages are ServerMessage.
				// For now, writeJSON will be refactored next, let's keep it as is.
				c.sendControl(ServerMessage{Type: "snapshot", Payload: ce.Event})
			}
		}

	case "unsubscribe":
		c.handleMu.Lock()
		if c.handle != nil {
			c.handle.Cancel()
			c.handle = nil
			c.metrics.WSActiveSubscriptions.Dec()
		}
		c.handleMu.Unlock()
		select {
		case c.subUpdated <- struct{}{}:
		default:
		}
		c.sendControl(ServerMessage{Type: "unsubscribed", Payload: "all"})

	case "replay":
		c.handleReplayAction(ctx, cm)

	case "replay_pause":
		if c.replay == nil {
			c.sendControl(ServerMessage{Type: "error", Payload: "replay not available"})
			return
		}
		if err := c.replay.PauseReplay(c); err != nil {
			c.sendControl(ServerMessage{Type: "error", Payload: err.Error()})
		} else {
			c.sendControl(ServerMessage{Type: "replay_paused"})
		}

	case "replay_resume":
		if c.replay == nil {
			c.sendControl(ServerMessage{Type: "error", Payload: "replay not available"})
			return
		}
		if err := c.replay.ResumeReplay(c); err != nil {
			c.sendControl(ServerMessage{Type: "error", Payload: err.Error()})
		} else {
			c.sendControl(ServerMessage{Type: "replay_resumed"})
		}

	case "replay_stop":
		if c.replay == nil {
			c.sendControl(ServerMessage{Type: "error", Payload: "replay not available"})
			return
		}
		c.replay.StopReplay(c)

	case "reconnect":
		if c.metrics != nil {
			c.metrics.WSReconnectAttemptsTotal.Inc()
			if cm.ReconnectStart > 0 {
				latency := time.Since(time.Unix(0, cm.ReconnectStart))
				c.metrics.WSReconnectLatency.Observe(latency.Seconds())
			}
		}
		if c.walReplayer == nil {
			c.sendControl(ServerMessage{Type: "error", Payload: "WAL replayer not available"})
			return
		}

		// Set reconnecting flag BEFORE subscribing, so writePump buffers any incoming live events immediately.
		c.isReconnecting.Store(true)

		// Resubscribe to live topics
		if len(cm.Symbols) > 0 {
			c.handleMu.Lock()
			if c.handle != nil {
				c.handle.Cancel()
			} else if c.metrics != nil {
				c.metrics.WSActiveSubscriptions.Inc()
			}
			c.handle = c.tm.Subscribe(c.id, cm.Symbols...)
			c.handleMu.Unlock()
			select {
			case c.subUpdated <- struct{}{}:
			default:
			}
		}

		// Dispatch the disk read in the background so readPump doesn't block
		go func(resumeSeq map[string]uint64) {
			var minSeq uint64 = 0
			first := true
			for _, seq := range resumeSeq {
				if first || seq < minSeq {
					minSeq = seq
					first = false
				}
			}
			dbgf := func(format string, args ...any) {
				_ = format
				_ = args // replaced by fmt.Printf below
				// noop in prod
			}
			_ = dbgf
			log.Info(ctx, c.log).Uint64("minSeq", minSeq).Interface("resumeSeq", resumeSeq).Msg("[DEBUG] WAL replay goroutine started")
			startReplay := time.Now()
			msgs, err := c.walReplayer.Replay(minSeq + 1)
			log.Info(ctx, c.log).Int("msgs", len(msgs)).Err(err).Dur("elapsed", time.Since(startReplay)).Msg("[DEBUG] WAL replay goroutine done")

			duration := time.Since(startReplay)
			c.reconnectCh <- reconnectResult{msgs: msgs, err: err, resumeSeq: resumeSeq, duration: duration}
			log.Info(ctx, c.log).Msg("[DEBUG] WAL replay result sent to reconnectCh")
		}(cm.ResumeSeq)

	case "pong":
		if c.metrics != nil {
			c.metrics.WSPongReceivedTotal.Inc()
			if cm.Timestamp > 0 {
				latency := time.Since(time.Unix(0, cm.Timestamp))
				c.metrics.WSPingLatency.Observe(latency.Seconds())
			}
		}
		if c.heartbeat != nil {
			c.heartbeat.RecordPong(c.id)
		}

	default:
		c.sendControl(ServerMessage{Type: "error", Payload: "unknown action: " + cm.Action})
	}
}

// handleReplayAction processes a replay request from the client.
func (c *Client) handleReplayAction(ctx context.Context, cm ClientMessage) {
	if c.replay == nil {
		c.sendControl(ServerMessage{Type: "error", Payload: "replay not available"})
		return
	}

	symbol := cm.Symbol
	if symbol == "" && len(cm.Symbols) > 0 {
		symbol = cm.Symbols[0]
	}
	if symbol == "" {
		c.sendControl(ServerMessage{Type: "error", Payload: "no symbol provided for replay"})
		return
	}

	var start, end time.Time
	var err error

	if cm.Start != "" {
		start, err = time.Parse(time.RFC3339Nano, cm.Start)
		if err != nil {
			c.sendControl(ServerMessage{Type: "error", Payload: "invalid start time: " + err.Error()})
			return
		}
	}
	if cm.End != "" {
		end, err = time.Parse(time.RFC3339Nano, cm.End)
		if err != nil {
			c.sendControl(ServerMessage{Type: "error", Payload: "invalid end time: " + err.Error()})
			return
		}
	}

	req := ReplayRequest{
		Symbol: symbol,
		Start:  start,
		End:    end,
		Speed:  cm.Speed,
	}

	if err := c.replay.StartReplay(ctx, c, req); err != nil {
		c.sendControl(ServerMessage{Type: "error", Payload: "replay failed: " + err.Error()})
	}
}

// ---------- Write Pump ----------

// writePump is the ONLY goroutine that writes to the WebSocket.
// It multiplexes: market events (from handle), control messages (from
// readPump), pings, and shutdown signals.
//
// Uses pre-encoded JSON bytes from *CachedEvent when available,
// eliminating redundant JSON serialization per-client.
//
// GL3: writePump owns the connection and closes it on exit.
//
// [ASYMMETRIC TRACING RULE]: Outbound market data streams (via CachedEvent) MUST NOT
// generate child spans or message contexts. At production scale, generating a span
// per outbound websocket frame will trigger catastrophic Span Explosion (e.g. 10M+
// spans per second). Tracing is limited strictly to inbound control messages (e.g. Subscribe).
// Do NOT add tracing middleware to the eventC loop.
func (c *Client) writePump(ctx context.Context) {
	defer func() {
		// GL3: Only writePump closes the connection.
		_ = c.conn.Close()
	}()

	var eventC <-chan *marketdata.CachedEvent
	var doneC <-chan struct{}
	var replayC <-chan *marketdata.CachedEvent

	// Snapshot handle under read lock.
	c.handleMu.RLock()
	h := c.handle
	c.handleMu.RUnlock()

	if h != nil {
		eventC = h.C()
		doneC = h.Done()
	}

	// Snapshot replay channel.
	c.replayMu.RLock()
	replayC = c.replayCh
	c.replayMu.RUnlock()

	// State for WAL reconnect buffering
	var reconnectBuffer []*marketdata.CachedEvent

	for {
		select {
		case <-c.subUpdated:
			// Handle or replay channel changed — re-snapshot.
			c.handleMu.RLock()
			h := c.handle
			c.handleMu.RUnlock()
			if h != nil {
				eventC = h.C()
				doneC = h.Done()
			} else {
				eventC = nil
				doneC = nil
			}

			c.replayMu.RLock()
			replayC = c.replayCh
			c.replayMu.RUnlock()

		// case <-c.startReconnectCh: is handled in the priority check above

		case res := <-c.reconnectCh:
			log.Info(ctx, c.log).Int("msgs", len(res.msgs)).Err(res.err).Msg("[DEBUG] writePump received reconnectCh result")
			if c.metrics != nil {
				c.metrics.WSReplayDuration.Observe(res.duration.Seconds())
			}
			if res.err != nil {
				if c.metrics != nil {
					c.metrics.WSReconnectFailuresTotal.Inc()
				}
				// Log the error so we can see it!
				log.Error(ctx, c.log).Err(res.err).Msg("replay failed during reconnect")

				// Replay failed (e.g. sequence in future or missing). Send error and flush buffer.
				if err := c.writeJSON(ServerMessage{Type: "error", Payload: res.err.Error()}); err != nil {
					return
				}
				// We flush unconditionally since we failed to validate
				for _, ev := range reconnectBuffer {
					_ = c.writeRaw(ev.JSON)
				}
			} else {
				if c.metrics != nil {
					c.metrics.WSReconnectSuccessTotal.Inc()
					c.metrics.WSResubscriptionsTotal.Add(float64(len(res.resumeSeq)))
				}
				// Replay success
				if err := c.writeJSON(ServerMessage{Type: "reconnected", Payload: "success"}); err != nil {
					return
				}

				expectedSeqMap := res.resumeSeq
				if expectedSeqMap == nil {
					expectedSeqMap = make(map[string]uint64)
				}

				// 1. Stream the historical payload directly
				for _, msg := range res.msgs {
					expectedSeq, ok := expectedSeqMap[msg.Topic]
					if !ok {
						continue // Skip topics this client is not subscribed to
					}
					if msg.Sequence >= expectedSeq {
						if c.format == protocol.FormatJSON {
							// Wrap historical raw payload into a ServerMessage envelope
							msgType := msg.Type
							if msgType == "" {
								msgType = "quote" // fallback for old segments
							}
							wrapped, _ := json.Marshal(ServerMessage{
								Type:    msgType,
								Payload: json.RawMessage(msg.Payload),
							})
							if err := c.writeRaw(wrapped); err != nil {
								return
							}
						} else {
							// Protobuf/FlatBuffers payloads are already serialized events, no envelope needed
							if err := c.writeRaw(msg.Payload); err != nil {
								return
							}
						}
						expectedSeqMap[msg.Topic] = msg.Sequence + 1
					}
				}

				// 2. Flush the concurrent live buffer with deduplication
				for _, cached := range reconnectBuffer {
					if seqEv, ok := cached.Event.(marketdata.SequencedEvent); ok {
						seq := uint64(seqEv.GetSeq())
						expectedSeq := expectedSeqMap[cached.EventSymbol()]
						if seq >= expectedSeq {
							if err := c.writeProtocolEvent(cached); err != nil {
								return
							}
							expectedSeqMap[cached.EventSymbol()] = seq + 1
						}
					} else {
						// Not sequenced, just write it
						if err := c.writeProtocolEvent(cached); err != nil {
							return
						}
					}
				}
			}
			reconnectBuffer = nil
			c.isReconnecting.Store(false)

		case cached, ok := <-eventC:
			if !ok {
				// The topic manager closed our event channel (e.g. slow consumer disconnect).
				// We MUST return here to close the websocket connection, which forces
				// the client to reconnect and resync from the WAL to preserve ordering.
				log.Warn(ctx, c.log).Msg("event channel closed by topic manager, disconnecting client")
				return
			}

			if c.isReconnecting.Load() {
				if len(reconnectBuffer) >= 4096 {
					if c.metrics != nil {
						c.metrics.WSReconnectFailuresTotal.Inc()
					}
					log.Error(ctx, c.log).Msg("reconnect buffer overflow (>= 4096), aborting reconnect")
					_ = c.writeJSON(ServerMessage{Type: "error", Payload: "reconnect buffer overflow"})
					c.isReconnecting.Store(false)
					reconnectBuffer = nil
					return
				}
				reconnectBuffer = append(reconnectBuffer, cached)
				continue
			}

			// Validate ordering when the wrapped event carries a sequence number.
			if seqEv, ok := cached.Event.(marketdata.SequencedEvent); ok {
				seq := seqEv.GetSeq()
				if seq > 0 {
					result, gap := c.seqVal.Validate(cached.EventSymbol(), seq)
					if result == sequencer.OutOfOrder {
						c.metrics.WSSequenceGaps.Inc()
					}
					if gap != nil {
						c.metrics.WSSequenceGaps.Inc()
					}
				}
			}
			// Record end-to-end delivery latency from event timestamp.
			if te, ok := cached.Event.(marketdata.TimestampedEvent); ok {
				latency := time.Since(te.GetTimestamp())
				c.metrics.WSDeliveryLatency.Observe(latency.Seconds())
			}

			if err := c.writeProtocolEvent(cached); err != nil {
				log.Error(ctx, c.log).Err(err).Str("event", "write_error").Msg("write error")
				c.metrics.WebsocketWriteErrorsTotal.Inc()
				return
			}
			c.metrics.MessagesSentTotal.Inc()
			// SC1: Reset drop counter on successful write.
			c.consecutiveDrops.Store(0)
			c.metrics.WSSlowConsumers.Dec()

		case ev, ok := <-replayC:
			if !ok {
				replayC = nil
				continue
			}
			if err := c.writeProtocolEvent(ev); err != nil {
				log.Error(ctx, c.log).Err(err).Str("event", "replay_write_error").Msg("replay write error")
				c.metrics.WebsocketWriteErrorsTotal.Inc()
				return
			}
			c.metrics.MessagesSentTotal.Inc()

		case msg := <-c.control:
			if err := c.writeJSON(msg); err != nil {
				log.Error(ctx, c.log).Err(err).Str("event", "control_write_error").Msg("control write error")
				return
			}

		case <-doneC:
			eventC = nil
			doneC = nil

		case <-c.done:
			return

		case <-ctx.Done():
			return
		}
	}
}

// ---------- Helpers ----------

// writeRaw writes pre-encoded JSON bytes directly to the WebSocket.
// This is the hot path for market events — zero serialization overhead.
// Tracks bytes_sent_total and message_size_bytes for bandwidth observability.
// writeProtocolEvent writes a CachedEvent using the negotiated serializer.
// It leverages pre-encoded JSON if the client negotiated JSON.
func (c *Client) writeProtocolEvent(ce *marketdata.CachedEvent) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.format == protocol.FormatJSON && ce.JSON != nil {
		// Zero allocation fast-path for JSON.
		return c.writeRawLocked(ce.JSON)
	}
	if c.format == protocol.FormatProtobuf && ce.Protobuf != nil {
		// Zero allocation fast-path for Protobuf.
		return c.writeRawLocked(ce.Protobuf)
	}
	if c.format == protocol.FormatFlatBuffers && ce.FlatBuffers != nil {
		// Zero allocation fast-path for FlatBuffers.
		return c.writeRawLocked(ce.FlatBuffers)
	}

	// Dynamic serialization for Protobuf/FlatBuffers.
	b, err := c.serializer.Serialize(ce.Event)
	if err != nil {
		return err
	}

	msgType := websocket.BinaryMessage
	if c.format == protocol.FormatJSON {
		msgType = websocket.TextMessage
	}

	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	err = c.conn.WriteMessage(msgType, b)
	if err != nil {
		if c.metrics != nil {
			c.metrics.WebsocketWriteErrorsTotal.Inc()
		}
	}
	if c.metrics != nil {
		c.metrics.WSBytesSent.Add(float64(len(b)))
		c.metrics.WSMessageSize.Observe(float64(len(b)))
	}
	return err
}

// writeJSON writes a JSON message to the WebSocket with a deadline.
// Uses a pooled buffer to reduce allocations on the hot path.
// Used for control messages (subscribe/unsubscribe confirmations, errors).
func (c *Client) writeJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	w, err := c.conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	err1 := json.NewEncoder(w).Encode(v)
	err2 := w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// writeRawLocked writes pre-encoded JSON bytes directly to the WebSocket.
// Assumes writeMu is already held by the caller.
func (c *Client) writeRawLocked(data []byte) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	msgType := websocket.BinaryMessage
	if c.format == protocol.FormatJSON {
		msgType = websocket.TextMessage
	}
	err := c.conn.WriteMessage(msgType, data)
	if err == nil {
		if c.metrics != nil {
			n := len(data)
			c.metrics.WSBytesSent.Add(float64(n))
			c.metrics.WSMessageSize.Observe(float64(n))
		}
	}
	return err
}

// writeRaw writes pre-encoded JSON bytes directly to the WebSocket.
// This is the hot path for market events — zero serialization overhead.
// Tracks bytes_sent_total and message_size_bytes for bandwidth observability.
func (c *Client) writeRaw(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeRawLocked(data)
}

// sendControl queues a control message for writePump to deliver.
// SC2: Uses blocking send with warning — control messages must not be dropped.
func (c *Client) sendControl(msg ServerMessage) {
	select {
	case c.control <- msg:
	default:
		// SC2: Control channel full — log warning instead of silently dropping.
		c.log.Underlying().Warn().Str("type", msg.Type).Str("event", "control_channel_full").Msg("control channel full, dropping message")
	}
}

// cancelAll cancels the current subscription. Safe for concurrent use.
func (c *Client) cancelAll() {
	c.cancelOnce.Do(func() {
		c.handleMu.Lock()
		h := c.handle
		c.handleMu.Unlock()
		if h != nil {
			h.Cancel()
		}
	})
}

// setReplay activates a replay session on the client. It replaces any
// existing live subscription with the replay channel.
func (c *Client) setReplay(ch chan *marketdata.CachedEvent, session *engine.Session, cancel context.CancelFunc) {
	c.replayMu.Lock()
	c.replayCh = ch
	c.activeReplaySession = session
	c.replayCancel = cancel
	c.replayMu.Unlock()

	// Unsubscribe from live events — replay replaces the data stream.
	c.handleMu.Lock()
	if c.handle != nil {
		c.handle.Cancel()
		c.handle = nil
		c.metrics.WSActiveSubscriptions.Dec()
	}
	c.handleMu.Unlock()

	// Signal writePump to switch to replay channel.
	select {
	case c.subUpdated <- struct{}{}:
	default:
	}
}

// clearReplayIfActive clears the replay state only if the given session
// is still the active one. Returns true if cleanup was performed.
// Used by the background goroutine to avoid clearing a newer replay.
func (c *Client) clearReplayIfActive(session *engine.Session) bool {
	c.replayMu.Lock()
	if c.activeReplaySession != session {
		c.replayMu.Unlock()
		return false
	}
	ch := c.replayCh
	c.replayCh = nil
	c.activeReplaySession = nil
	c.replayCancel = nil
	c.replayMu.Unlock()

	if ch != nil {
		close(ch)
	}

	// Signal writePump that replay channel is gone.
	select {
	case c.subUpdated <- struct{}{}:
	default:
	}

	return true
}

// stopReplay stops the active replay session and clears state.
func (c *Client) stopReplay() {
	c.replayMu.Lock()
	session := c.activeReplaySession
	cancel := c.replayCancel
	c.replayCh = nil
	c.activeReplaySession = nil
	c.replayCancel = nil
	c.replayMu.Unlock()

	if session != nil {
		_ = session.Stop()
	}
	if cancel != nil {
		cancel()
	}
}

// shutdown cancels the client's context and waits for the write pump to exit.
//
// GL1: Sets a read deadline after cancelCtx to force ReadMessage to return
// immediately instead of blocking for up to pongWait (60s).
func (c *Client) shutdown(ctx context.Context) {
	// Execute the disconnect span to trace the duration of the graceful teardown.
	_, span := wscontext.DeriveMessageContext(ctx, "websocket.disconnect")
	defer span.End()

	// Wait for any active historical replay to finish, or until context is done.
	// This ensures clients aren't disconnected in the middle of a replay.
	for {
		c.replayMu.RLock()
		isActiveReplay := c.activeReplaySession != nil
		c.replayMu.RUnlock()

		if !isActiveReplay && !c.isReconnecting.Load() {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			goto forceClose
		}
	}

forceClose:
	// Send goaway message
	_ = c.writeJSON(ServerMessage{Type: "goaway", Payload: map[string]string{"reason": "server_shutdown"}})

	// GL1: Force ReadMessage to return by setting an expired deadline.
	// Without this, readPump can block for up to 60s after shutdown.
	_ = c.conn.SetReadDeadline(time.Now().Add(-time.Second))

	// GL1: Cancel context to signal readPump and writePump.
	c.cancelCtx()

	// Send close frame.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(writeWait)
	}
	c.writeMu.Lock()
	_ = c.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"),
		deadline,
	)
	c.writeMu.Unlock()

	// Wait for write pump to exit.
	select {
	case <-c.done:
	case <-ctx.Done():
	}
}
