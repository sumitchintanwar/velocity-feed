package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// segment implements the Segment interface for a single log and index file pair.
type segment struct {
	mu      sync.Mutex
	baseSeq uint64
	logFile *LogWriter
	idxFile *IndexFile

	// configuration
	indexIntervalBytes int64
	bytesSinceIndex    int64
	currentSize        int64
}

// OpenSegment opens an existing segment or creates a new one given the base sequence.
func OpenSegment(dir string, baseSeq uint64, indexIntervalBytes int64) (Segment, error) {
	basename := fmt.Sprintf("%020d", baseSeq)
	logPath := filepath.Join(dir, basename+".log")
	idxPath := filepath.Join(dir, basename+".index")

	logWriter, err := NewWriter(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log writer for segment %d: %w", baseSeq, err)
	}

	idxFile, err := NewIndexFile(idxPath)
	if err != nil {
		logWriter.Close()
		return nil, fmt.Errorf("failed to open index for segment %d: %w", baseSeq, err)
	}

	info, err := os.Stat(logPath)
	var currentSize int64
	if err == nil {
		currentSize = info.Size()
	}

	return &segment{
		baseSeq:            baseSeq,
		logFile:            logWriter,
		idxFile:            idxFile,
		indexIntervalBytes: indexIntervalBytes,
		bytesSinceIndex:    0, // On reopen, we could read the last index to find out, but 0 is fine, it will just index the next message.
		currentSize:        currentSize,
	}, nil
}

// Append writes the message to the log file and conditionally updates the index.
func (s *segment) Append(msg *Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := uint64(s.currentSize)

	bytesWritten, err := s.logFile.Append(msg)
	if err != nil {
		return 0, err
	}

	msgSize := int64(bytesWritten)
	s.currentSize += msgSize
	s.bytesSinceIndex += msgSize

	// Write to sparse index if threshold reached or it's the very first message
	if s.bytesSinceIndex >= s.indexIntervalBytes || offset == 0 {
		if err := s.idxFile.Append(msg.Sequence, offset); err != nil {
			return 0, err
		}
		s.bytesSinceIndex = 0
	}

	return offset, nil
}

// ReadAt opens a LogReader starting at the physical offset associated with the requested sequence.
func (s *segment) ReadAt(seq uint64) (Reader, error) {
	idxEntry, err := s.idxFile.Lookup(seq)
	if err != nil {
		// If index lookup fails (e.g., empty index), default to start of file
		idxEntry = IndexEntry{Sequence: s.baseSeq, Offset: 0}
	}

	reader, err := NewReader(s.logFile.file.Name())
	if err != nil {
		return nil, err
	}

	// Seek to the physical offset found in the index
	if _, err := reader.file.Seek(int64(idxEntry.Offset), os.SEEK_SET); err != nil {
		reader.Close()
		return nil, fmt.Errorf("failed to seek segment to offset %d: %w", idxEntry.Offset, err)
	}

	// We must also discard any buffered data in bufio.Reader since we Seek()ed the underlying file
	reader.reader.Reset(reader.file)

	return reader, nil
}

// BaseSequence returns the base sequence of this segment.
func (s *segment) BaseSequence() uint64 {
	return s.baseSeq
}

// Size returns the physical byte size of the .log file.
func (s *segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentSize
}

// ModTime returns the physical modification time of the .log file.
func (s *segment) ModTime() (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := os.Stat(s.logFile.file.Name())
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

// Sync fsyncs both the log and index files.
func (s *segment) Sync() error {
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.idxFile.Sync()
}

// Close closes both log and index files.
func (s *segment) Close() error {
	logErr := s.logFile.Close()
	idxErr := s.idxFile.Close()
	if logErr != nil {
		return logErr
	}
	return idxErr
}

// Remove deletes the segment from disk entirely.
func (s *segment) Remove() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logPath := s.logFile.file.Name()
	idxPath := s.idxFile.file.Name()

	// Safe local close without locking again
	_ = s.logFile.Close()
	_ = s.idxFile.Close()

	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(idxPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// recover scans the segment to find the last valid boundary and truncates torn bytes.
func (s *segment) recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Sync any buffers before reading
	s.logFile.Sync()

	// 2. We could optimize by starting from the last index entry,
	// but for correctness, a full scan of the active segment is perfectly safe and fast.
	reader, err := NewReader(s.logFile.file.Name())
	if err != nil {
		return err
	}
	defer reader.Close()

	var lastValidOffset int64 = 0
	var lastValidSequence uint64 = 0

	for {
		currentOffset, _ := reader.file.Seek(0, os.SEEK_CUR)
		msg, err := reader.Next()
		if err == io.EOF || errors.Is(err, ErrCorruptedTail) {
			break
		}
		if err != nil {
			return fmt.Errorf("unexpected error during recovery scan: %w", err)
		}

		// Determine the exact size of this message
		msgSize := int64(22 + len(msg.Topic) + len(msg.Payload) + 4)
		lastValidOffset = currentOffset + msgSize
		lastValidSequence = msg.Sequence
	}

	// 3. Truncate the log file to the last valid boundary
	if err := s.logFile.file.Truncate(lastValidOffset); err != nil {
		return fmt.Errorf("failed to truncate log file to %d: %w", lastValidOffset, err)
	}

	// 4. Seek the writer to the end of the truncated file so new appends work
	if _, err := s.logFile.file.Seek(lastValidOffset, os.SEEK_SET); err != nil {
		return fmt.Errorf("failed to seek log writer to %d: %w", lastValidOffset, err)
	}
	s.logFile.writer.Reset(s.logFile.file)
	s.currentSize = lastValidOffset

	// 5. Recover the index file by discarding entries beyond the last valid sequence
	if err := s.idxFile.truncateAfter(lastValidSequence); err != nil {
		return fmt.Errorf("failed to truncate index: %w", err)
	}

	return nil
}

// ExtractBaseSequence parses a filename like "00000000000000000000.log" and returns 0.
func ExtractBaseSequence(filename string) (uint64, error) {
	// Strip extension
	name := filename[:len(filename)-len(filepath.Ext(filename))]
	return strconv.ParseUint(name, 10, 64)
}
