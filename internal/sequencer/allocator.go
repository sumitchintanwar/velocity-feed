package sequencer

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/sumit/rtmds/internal/wal"
)

// Allocator manages thread-safe, lock-free sequence numbers.
type Allocator struct {
	sequence atomic.Uint64
}

// NewAllocator creates a new sequence allocator initialized to 0.
func NewAllocator() *Allocator {
	return &Allocator{}
}

// Next atomically increments the sequence counter by 1 and returns the new value.
func (a *Allocator) Next() uint64 {
	return a.sequence.Add(1)
}

// Current returns the current sequence number without modifying it.
func (a *Allocator) Current() uint64 {
	return a.sequence.Load()
}

// Set explicitly forces the sequence counter to a specific value.
func (a *Allocator) Set(val uint64) {
	a.sequence.Store(val)
}

// InitFromWAL sequentially reads the provided WAL file to discover the highest
// recorded sequence number, ensuring the allocator survives restarts without data loss.
func (a *Allocator) InitFromWAL(walPath string) error {
	reader, err := wal.NewReader(walPath)
	if err != nil {
		return fmt.Errorf("failed to open WAL for sequence initialization: %w", err)
	}
	defer reader.Close()

	var maxSeq uint64
	for {
		msg, err := reader.Next()
		if err != nil {
			// EOF or ErrCorruptedTail are acceptable markers for the end of a log.
			if err == io.EOF || errors.Is(err, wal.ErrCorruptedTail) {
				break
			}
			return fmt.Errorf("error scanning WAL for latest sequence: %w", err)
		}

		if msg.Sequence > maxSeq {
			maxSeq = msg.Sequence
		}
	}

	a.Set(maxSeq)
	return nil
}
