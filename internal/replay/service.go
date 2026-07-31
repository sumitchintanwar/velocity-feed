package replay

import (
	"fmt"
	"io"

	"github.com/sumit/rtmds/internal/sequencer"
	"github.com/sumit/rtmds/internal/wal"
)

// Service provides a high-level API to retrieve historical messages.
type Service struct {
	engine    *Engine
	allocator *sequencer.Allocator
}

// NewService creates a new replay service.
func NewService(engine *Engine, allocator *sequencer.Allocator) *Service {
	return &Service{
		engine:    engine,
		allocator: allocator,
	}
}

// Replay fetches all messages from the requested sequence up to the latest
// known committed sequence at the time of the request.
func (s *Service) Replay(resumeFrom uint64) ([]*wal.Message, error) {
	currentSeq := s.allocator.Current()

	// 1. Handle empty log
	if currentSeq == 0 {
		if resumeFrom > 0 {
			return nil, fmt.Errorf("log is empty, cannot replay sequence %d", resumeFrom)
		}
		return nil, nil
	}

	// 2. Validate sequence (future sequence)
	if resumeFrom > currentSeq {
		return nil, fmt.Errorf("requested sequence %d is in the future (current latest: %d)", resumeFrom, currentSeq)
	}

	// 3. Start engine iterator
	it, err := s.engine.Start(resumeFrom, currentSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to start replay engine: %w", err)
	}
	defer it.Close()

	var messages []*wal.Message
	for {
		msg, err := it.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error during replay read: %w", err)
		}

		// Must safely copy payload out of the engine's zero-allocation scratch buffer
		payloadCopy := make([]byte, len(msg.Payload))
		copy(payloadCopy, msg.Payload)

		messages = append(messages, &wal.Message{
			Sequence:  msg.Sequence,
			Timestamp: msg.Timestamp,
			Topic:     msg.Topic,
			Type:      msg.Type,
			Payload:   payloadCopy,
		})
	}

	// 4. Handle missing sequence gracefully
	if len(messages) > 0 && messages[0].Sequence > resumeFrom {
		return nil, fmt.Errorf("missing sequence: requested %d but earliest available in log is %d", resumeFrom, messages[0].Sequence)
	}

	if len(messages) == 0 && resumeFrom <= currentSeq {
		return nil, fmt.Errorf("missing sequence: requested %d could not be found in WAL", resumeFrom)
	}

	// 5. Returns ordered messages
	return messages, nil
}
