package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/sumit/rtmds/internal/eventlog"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/snapshot"
	"github.com/sumit/rtmds/internal/wal"
	"github.com/sumit/rtmds/pkg/marketdata"
)

// WALRecoverer coordinates startup recovery using the Snapshot engine and the WAL.
type WALRecoverer struct {
	snap *snapshot.Service
	log  wal.Log
	l    *log.Logger
}

// NewWALRecoverer creates a new WAL-based recoverer.
func NewWALRecoverer(snap *snapshot.Service, walLog wal.Log) *WALRecoverer {
	return &WALRecoverer{
		snap: snap,
		log:  walLog,
		l:    log.New(nil, "wal_recovery"),
	}
}

// Recover executes the recovery sequence:
// 1. Load the latest snapshot.
// 2. Determine the latest sequence processed.
// 3. Replay all remaining entries from the WAL into the snapshot.
// 4. Return the highest sequence recovered so the global sequencer can resume.
func (r *WALRecoverer) Recover(ctx context.Context) (uint64, error) {
	// 1. Load snapshot
	if err := r.snap.LoadCheckpoint(); err != nil {
		return 0, fmt.Errorf("failed to load snapshot: %w", err)
	}

	// 2. Get last sequence
	// eventlog.Cursor uses EventID as the sequence
	lastCursor := r.snap.LastCursor()
	lastSeq := uint64(lastCursor.EventID)

	r.l.Underlying().Info().
		Uint64("last_sequence", lastSeq).
		Msg("wal_recovery: snapshot loaded, starting WAL replay")

	// 3. Start buffering live events in snapshot (prevents replay race)
	r.snap.StartBuffering()
	defer r.snap.StopBuffering()

	// 4. Open WAL reader starting at lastSeq + 1
	reader, err := r.log.NewReader(lastSeq + 1)
	if err != nil {
		return 0, fmt.Errorf("failed to open WAL reader: %w", err)
	}
	defer reader.Close()

	var replayed int
	var highestSeq uint64 = lastSeq

	for {
		select {
		case <-ctx.Done():
			return highestSeq, fmt.Errorf("recovery context cancelled after %d events", replayed)
		default:
		}

		msg, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			// Let the caller handle true errors; ErrCorruptedTail is handled inside WAL automatically during open,
			// but if it surfaces during reading, we stop gracefully at the corruption boundary.
			if err == wal.ErrCorruptedTail {
				r.l.Underlying().Warn().Msg("wal_recovery: encountered corrupted tail, stopping replay at safe boundary")
				break
			}
			return highestSeq, fmt.Errorf("failed to read WAL: %w", err)
		}

		// Deserialize payload
		event, err := r.reconstructEvent(msg.Topic, msg.Payload)
		if err != nil {
			r.l.Underlying().Warn().Err(err).Uint64("seq", msg.Sequence).
				Msg("wal_recovery: skipping unparseable payload")
			continue
		}

		// Apply directly to snapshot
		r.snap.Update(event)

		highestSeq = msg.Sequence
		replayed++
	}

	// 5. Update the snapshot cursor with the highest recovered sequence
	newCursor := eventlog.Cursor{
		EventID:   int64(highestSeq),
		Timestamp: time.Now(),
	}
	r.snap.UpdateCursor(newCursor)

	// 6. Mark the snapshot service as ready
	r.snap.MarkReady()

	r.l.Underlying().Info().
		Int("events_replayed", replayed).
		Uint64("highest_sequence", highestSeq).
		Msg("wal_recovery: WAL replay complete")

	return highestSeq, nil
}

// reconstructEvent is a helper to parse the JSON payloads from the WAL back into MarketEvents.
func (r *WALRecoverer) reconstructEvent(topic string, payload []byte) (marketdata.MarketEvent, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty payload")
	}

	// Assuming topic defines the event type roughly, but we can also attempt generic JSON unmarshaling
	var q marketdata.Quote
	if err := json.Unmarshal(payload, &q); err == nil && q.Symbol != "" {
		return q, nil
	}
	var b marketdata.Bar
	if err := json.Unmarshal(payload, &b); err == nil && b.Symbol != "" {
		return b, nil
	}

	return nil, fmt.Errorf("unknown payload format")
}
