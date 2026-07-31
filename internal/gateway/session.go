package gateway

import (
	"context"
	"fmt"

	"github.com/sumit/rtmds/internal/wal"
)

// Replayer defines the interface for fetching historical messages.
type Replayer interface {
	Replay(resumeFrom uint64) ([]*wal.Message, error)
}

// ConnectResponse is sent to the client to confirm or reject their reconnect attempt.
type ConnectResponse struct {
	Success bool
	Error   string
}

// Session handles the lifecycle of a single client connection, seamlessly
// transitioning from historical replay to live streaming using channels.
type Session struct {
	LiveCh     chan *wal.Message    // Channel receiving live broadcasts from the gateway
	SendCh     chan *wal.Message    // Channel pushing messages to the actual client socket
	RespCh     chan ConnectResponse // Channel emitting the result of the validation/replay
	replayer   Replayer
	resumeFrom uint64
}

// NewSession initializes a state machine for handing off a reconnecting client.
func NewSession(replayer Replayer, resumeFrom uint64) *Session {
	return &Session{
		LiveCh:     make(chan *wal.Message, 4096),
		SendCh:     make(chan *wal.Message, 4096),
		RespCh:     make(chan ConnectResponse, 1),
		replayer:   replayer,
		resumeFrom: resumeFrom,
	}
}

// Run executes the state machine for the session until the context is canceled.
func (s *Session) Run(ctx context.Context) error {
	// 1. Buffer to catch live messages published concurrently during the disk replay
	buffer := make([]*wal.Message, 0, 4096)

	type replayResult struct {
		msgs []*wal.Message
		err  error
	}
	replayCh := make(chan replayResult, 1)

	// Fire off the disk read asynchronously
	go func() {
		msgs, err := s.replayer.Replay(s.resumeFrom)
		replayCh <- replayResult{msgs: msgs, err: err}
	}()

	isReplaying := true
	var expectedSeq uint64 = s.resumeFrom

	// 2. Lock-free select loop to multiplex disk replay vs concurrent live stream
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg := <-s.LiveCh:
			if isReplaying {
				buffer = append(buffer, msg)
			} else {
				if msg.Sequence >= expectedSeq {
					s.SendCh <- msg
					expectedSeq = msg.Sequence + 1
				}
			}

		case res := <-replayCh:
			if res.err != nil {
				// Validation failed (missing sequence, future sequence, etc.)
				s.RespCh <- ConnectResponse{Success: false, Error: res.err.Error()}
				return fmt.Errorf("replay validation failed: %w", res.err)
			}

			// Validation succeeded!
			s.RespCh <- ConnectResponse{Success: true}

			// 3. Stream replayed messages
			for _, msg := range res.msgs {
				if msg.Sequence >= expectedSeq {
					s.SendCh <- msg
					expectedSeq = msg.Sequence + 1
				}
			}

			// 4. Flush the buffered live messages
			for _, msg := range buffer {
				if msg.Sequence >= expectedSeq {
					s.SendCh <- msg
					expectedSeq = msg.Sequence + 1
				}
			}

			buffer = nil
			isReplaying = false
		}
	}
}
