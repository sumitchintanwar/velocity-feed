package replay

import (
	"errors"
	"io"

	"github.com/sumit/rtmds/internal/wal"
)

// Iterator provides sequential access to historical messages.
type Iterator interface {
	// Next returns the next message in the replay sequence.
	// Returns io.EOF when the end of the requested replay range is reached.
	// Note: The returned message's Payload points to a shared buffer that will be
	// overwritten on the next call. Copy it if you need to retain it.
	Next() (*wal.Message, error)
	// Close releases resources.
	Close() error
}

// Engine serves historical messages from the Write Ahead Log.
type Engine struct {
	walLog wal.Log
}

// NewEngine creates a new replay engine reading from the specified WAL.
func NewEngine(walLog wal.Log) *Engine {
	return &Engine{walLog: walLog}
}

// Start creates a new iterator starting from (and including) resumeFrom,
// stopping after it yields endSequence. This allows replaying up to the exact
// boundary of what was live when the client connected.
func (e *Engine) Start(resumeFrom uint64, endSequence uint64) (Iterator, error) {
	reader, err := e.walLog.NewReader(resumeFrom)
	if err != nil {
		return nil, err
	}

	it := &walIterator{
		reader:      reader,
		resumeFrom:  resumeFrom,
		endSequence: endSequence,
	}

	// Fast-forward to resumeFrom
	for {
		// This loop is extremely efficient as it avoids GC pressure by heavily reusing
		// the wal.LogReader's internal scratch buffer. No new allocations happen for payloads here.
		msg, err := it.reader.Next()
		if err != nil {
			if errors.Is(err, wal.ErrCorruptedTail) || err == io.EOF {
				it.done = true
				return it, nil // Iterator is instantly exhausted
			}
			it.reader.Close()
			return nil, err
		}

		if msg.Sequence >= resumeFrom {
			it.nextMsg = msg
			break
		}
	}

	return it, nil
}

type walIterator struct {
	reader      wal.Reader
	resumeFrom  uint64
	endSequence uint64
	nextMsg     *wal.Message
	done        bool
}

func (i *walIterator) Next() (*wal.Message, error) {
	if i.done {
		return nil, io.EOF
	}

	// Yield the queued message from the fast-forward phase
	if i.nextMsg != nil {
		msg := i.nextMsg
		i.nextMsg = nil
		if msg.Sequence > i.endSequence {
			i.done = true
			return nil, io.EOF
		}
		return msg, nil
	}

	msg, err := i.reader.Next()
	if err != nil {
		// Torn writes at the tail mean we simply ran out of safe log to replay.
		if errors.Is(err, wal.ErrCorruptedTail) || err == io.EOF {
			i.done = true
			return nil, io.EOF
		}
		return nil, err
	}

	if msg.Sequence > i.endSequence {
		i.done = true
		return nil, io.EOF
	}

	return msg, nil
}

func (i *walIterator) Close() error {
	i.done = true
	return i.reader.Close()
}
