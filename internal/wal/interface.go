package wal

import "time"

// Writer defines the interface for appending messages to the WAL.
type Writer interface {
	// Append writes a message to the end of the log.
	// This writes to the OS buffer, you must call Sync() for strict durability.
	Append(msg *Message) error

	// Sync flushes all pending writes to the physical disk.
	Sync() error

	// Close closes the underlying file descriptor.
	Close() error
}

// Reader defines the interface for sequentially reading the WAL for recovery.
type Reader interface {
	// Next reads the next message from the WAL.
	// Returns io.EOF when the end of the log is reached.
	Next() (*Message, error)

	// Close closes the underlying file descriptor.
	Close() error
}

// Log defines the interface for a segmented write-ahead log.
type Log interface {
	// Append writes a message to the active segment.
	// Returns the sequence number and physical offset of the appended message.
	Append(msg *Message) (seq uint64, offset uint64, err error)

	// NewReader returns an iterator starting from the given sequence.
	NewReader(startSequence uint64) (Reader, error)

	// LastSequence returns the highest sequence number written to the WAL.
	LastSequence() uint64

	// Sync flushes all pending writes to the physical disk.
	Sync() error

	// Close closes all segments and underlying file descriptors.
	Close() error
}

// Segment represents a single Log/Index file pair.
type Segment interface {
	// Append writes a message to the segment.
	Append(msg *Message) (offset uint64, err error)

	// ReadAt returns a reader starting from the given sequence within this segment.
	ReadAt(seq uint64) (Reader, error)

	// BaseSequence returns the base sequence number (the filename) of this segment.
	BaseSequence() uint64

	// Size returns the current physical size in bytes of the log file.
	Size() int64

	// ModTime returns the last modification time of the segment file.
	ModTime() (time.Time, error)

	// Sync flushes pending writes to disk.
	Sync() error

	// Close closes the segment files.
	Close() error

	// Remove deletes the segment files from disk.
	Remove() error
}
