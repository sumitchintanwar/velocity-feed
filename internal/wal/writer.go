package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"sync"
	"unsafe"
)

// LogWriter implements the Writer interface for an append-only file WAL.
// It is thread-safe for concurrent appends.
type LogWriter struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	hasher hash.Hash32
}

// NewWriter opens a WAL file for append-only writing.
// If the file does not exist, it will be created.
func NewWriter(path string) (*LogWriter, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	return &LogWriter{
		file: file,
		// Using a buffered writer significantly improves throughput by reducing syscalls.
		writer: bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		hasher: crc32.NewIEEE(),
	}, nil
}

// Append writes the message in binary format to the buffer, appending a CRC32 checksum.
// Returns the exact number of bytes written.
func (w *LogWriter) Append(msg *Message) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 8 (seq) + 8 (ts) + 2 (topic len) + 4 (payload len) = 22 bytes fixed header
	var header [22]byte

	binary.LittleEndian.PutUint64(header[0:8], msg.Sequence)
	binary.LittleEndian.PutUint64(header[8:16], uint64(msg.Timestamp))

	topicLen := len(msg.Topic)
	if topicLen > 65535 {
		return 0, fmt.Errorf("topic length %d exceeds max uint16", topicLen)
	}
	binary.LittleEndian.PutUint16(header[16:18], uint16(topicLen))

	payloadLen := len(msg.Payload)
	binary.LittleEndian.PutUint32(header[18:22], uint32(payloadLen))

	// Compute CRC32
	w.hasher.Reset()
	w.hasher.Write(header[:])
	if topicLen > 0 {
		w.hasher.Write(unsafe.Slice(unsafe.StringData(msg.Topic), topicLen))
	}
	if payloadLen > 0 {
		w.hasher.Write(msg.Payload)
	}
	checksum := w.hasher.Sum32()
	var crcBytes [4]byte
	binary.LittleEndian.PutUint32(crcBytes[:], checksum)

	// Write Header
	if _, err := w.writer.Write(header[:]); err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}

	// Write Topic
	if topicLen > 0 {
		if _, err := w.writer.WriteString(msg.Topic); err != nil {
			return 0, fmt.Errorf("write topic: %w", err)
		}
	}

	// Write Payload
	if payloadLen > 0 {
		if _, err := w.writer.Write(msg.Payload); err != nil {
			return 0, fmt.Errorf("write payload: %w", err)
		}
	}

	// Write Checksum
	if _, err := w.writer.Write(crcBytes[:]); err != nil {
		return 0, fmt.Errorf("write checksum: %w", err)
	}

	totalBytes := 22 + topicLen + payloadLen + 4
	return totalBytes, nil
}

// Sync flushes the userspace buffer and issues an fsync to the OS.
func (w *LogWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// Close flushes, syncs, and closes the underlying file.
func (w *LogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	flushErr := w.writer.Flush()
	syncErr := w.file.Sync()
	closeErr := w.file.Close()

	if flushErr != nil {
		return fmt.Errorf("close flush: %w", flushErr)
	}
	if syncErr != nil {
		return fmt.Errorf("close sync: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close: %w", closeErr)
	}
	return nil
}
