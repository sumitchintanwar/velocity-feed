package websocket

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/models"
	"github.com/sumit/rtmds/internal/replay/engine"
	"github.com/sumit/rtmds/internal/replay/scheduler"
	"github.com/sumit/rtmds/pkg/marketdata"
)

// ReplayHandler manages WebSocket replay sessions. It bridges the replay
// engine to WebSocket clients, converting stored events back to market
// events and delivering them through the client's dedicated replay channel.
type ReplayHandler struct {
	replayer *engine.Replayer
	log      *log.Logger
}

// NewReplayHandler creates a handler backed by the given Replayer.
func NewReplayHandler(replayer *engine.Replayer, l *log.Logger) *ReplayHandler {
	return &ReplayHandler{
		replayer: replayer,
		log:      l,
	}
}

// StartReplay initiates a historical replay session for the given client.
// It cancels any active replay on the client first, then starts a new session.
// Events are delivered through the client's replayCh channel.
func (h *ReplayHandler) StartReplay(ctx context.Context, c *Client, req ReplayRequest) error {
	// Cancel any existing replay.
	c.stopReplay()

	if req.Speed <= 0 {
		req.Speed = 1.0
	}

	// Buffered channel so the clock never blocks on a slow WebSocket write.
	ch := make(chan *marketdata.CachedEvent, 1024)

	pub := &replayPublisher{
		ch:  ch,
		log: h.log,
	}

	session, err := h.replayer.ReplayTimeRange(ctx, req.Symbol, req.Start, req.End, req.Speed, pub)
	if err != nil {
		close(ch)
		return err
	}

	// Create a cancellable context for this replay session.
	replayCtx, replayCancel := context.WithCancel(ctx)

	// Capture the current replay generation so the background goroutine
	// can detect if a new replay has replaced this one.
	c.setReplay(ch, session, replayCancel)

	c.sendControl(ServerMessage{
		Type: "replay_started",
		Payload: map[string]any{
			"session_id": session.Status().ID,
			"symbol":     req.Symbol,
			"start":      req.Start,
			"end":        req.End,
			"speed":      req.Speed,
		},
	})

	h.log.Underlying().Info().
		Str("client_id", c.id).
		Str("symbol", req.Symbol).
		Time("start", req.Start).
		Time("end", req.End).
		Float64("speed", req.Speed).
		Str("event", "replay_started").
		Msg("replay: session started")

	// Background goroutine: wait for session completion, notify client, clean up.
	go func() {
		<-replayCtx.Done()
		session.Wait()

		// Only clean up and notify if this session is still the active one.
		if c.clearReplayIfActive(session) {
			c.sendControl(ServerMessage{
				Type: "replay_completed",
				Payload: map[string]any{
					"session_id": session.Status().ID,
					"symbol":     req.Symbol,
					"state":      session.Status().State.String(),
				},
			})

			h.log.Underlying().Info().
				Str("client_id", c.id).
				Str("symbol", req.Symbol).
				Str("event", "replay_completed").
				Msg("replay: session completed")
		}
	}()

	return nil
}

// StopReplay stops the active replay session for the given client.
func (h *ReplayHandler) StopReplay(c *Client) {
	c.stopReplay()
	c.sendControl(ServerMessage{Type: "replay_stopped"})
}

// PauseReplay pauses the active replay session.
func (h *ReplayHandler) PauseReplay(c *Client) error {
	c.replayMu.RLock()
	session := c.activeReplaySession
	c.replayMu.RUnlock()

	if session == nil {
		return errNoActiveReplay
	}
	return session.Pause()
}

// ResumeReplay resumes the paused replay session.
func (h *ReplayHandler) ResumeReplay(c *Client) error {
	c.replayMu.RLock()
	session := c.activeReplaySession
	c.replayMu.RUnlock()

	if session == nil {
		return errNoActiveReplay
	}
	return session.Resume()
}

// ---------- replayPublisher ----------

// replayPublisher implements scheduler.Publisher. It converts stored events
// back to market events and sends them as CachedEvents through a channel.
type replayPublisher struct {
	ch  chan *marketdata.CachedEvent
	log *log.Logger
}

// PublishEvent converts a StoredEvent to a MarketEvent, wraps it in a
// CachedEvent with pre-encoded JSON, and sends it through the channel.
// Non-blocking: drops events if the channel is full to prevent the
// virtual clock from stalling on a slow WebSocket connection.
func (p *replayPublisher) PublishEvent(ev models.StoredEvent) {
	marketEvent, err := reconstructMarketEvent(ev)
	if err != nil {
		p.log.Underlying().Warn().Err(err).
			Str("symbol", ev.Symbol).
			Str("event_type", ev.EventType).
			Str("event", "replay_event_reconstruct_failed").
			Msg("replay: failed to reconstruct event")
		return
	}

	cached := marketdata.NewCachedEvent(marketEvent)

	select {
	case p.ch <- cached:
	default:
		p.log.Underlying().Warn().
			Str("symbol", ev.Symbol).
			Str("event", "replay_event_dropped").
			Msg("replay: event dropped, channel full")
	}
}

// ---------- reconstructMarketEvent ----------

// reconstructMarketEvent rebuilds a MarketEvent from a StoredEvent's payload.
// The payload contains the original JSON-encoded Quote or Bar that was persisted
// by the PersistingPublisher.
func reconstructMarketEvent(ev models.StoredEvent) (marketdata.MarketEvent, error) {
	if len(ev.Payload) == 0 {
		return nil, errNoPayload
	}

	switch ev.EventType {
	case "quote", "trade", "":
		var q marketdata.Quote
		if err := json.Unmarshal(ev.Payload, &q); err != nil {
			return nil, err
		}
		return q, nil
	case "bar":
		var b marketdata.Bar
		if err := json.Unmarshal(ev.Payload, &b); err != nil {
			return nil, err
		}
		return b, nil
	default:
		return nil, errUnknownEventType
	}
}

// ---------- errors ----------

var (
	errNoPayload        = &replayError{"no payload data in stored event"}
	errUnknownEventType = &replayError{"unknown event type"}
	errNoActiveReplay   = &replayError{"no active replay session"}
)

type replayError struct {
	msg string
}

func (e *replayError) Error() string { return e.msg }

// ---------- ReplayRequest ----------

// ReplayRequest holds the parameters for a client-initiated replay.
type ReplayRequest struct {
	Symbol string    `json:"symbol"`
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	Speed  float64   `json:"speed"` // 0 or negative = max speed
}

// ---------- Scheduler publisher guard ----------
var _ scheduler.Publisher = (*replayPublisher)(nil)
