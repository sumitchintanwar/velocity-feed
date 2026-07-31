package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// ErrCorruptedTail is returned when a torn write or corrupted checksum is encountered
// at the end of the log, meaning recovery should safely stop.
var ErrCorruptedTail = errors.New("corrupted log tail")

// ErrSequenceTooOld is returned when a requested sequence has been deleted by the retention policy.
var ErrSequenceTooOld = errors.New("sequence too old")

// LogReader implements the Reader interface for sequentially reading the WAL.
type LogReader struct {
	file   *os.File
	reader *bufio.Reader

	// scratch buffer to avoid allocating new memory for payloads
	scratch []byte
}

// NewReader opens a WAL file for reading.
// Returns an error if the file does not exist.
func NewReader(path string) (*LogReader, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	return &LogReader{
		file:    file,
		reader:  bufio.NewReaderSize(file, 64*1024),
		scratch: make([]byte, 0, 4096),
	}, nil
}

// Next reads the next sequential message from the log.
// Returns io.EOF when the clean end of the log is reached.
// If a torn write (partial bytes or invalid CRC32) is encountered at the end, it will return ErrCorruptedTail.
func (r *LogReader) Next() (*Message, error) {
	var header [22]byte

	// Read exactly 22 bytes for the header
	_, err := io.ReadFull(r.reader, header[:])
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return nil, ErrCorruptedTail
		}
		return nil, fmt.Errorf("read header: %w", err)
	}

	seq := binary.LittleEndian.Uint64(header[0:8])
	ts := int64(binary.LittleEndian.Uint64(header[8:16]))
	topicLen := int(binary.LittleEndian.Uint16(header[16:18]))
	payloadLen := int(binary.LittleEndian.Uint32(header[18:22]))

	totalLen := topicLen + payloadLen

	// Grow scratch buffer if needed
	if cap(r.scratch) < totalLen {
		r.scratch = make([]byte, totalLen)
	}
	r.scratch = r.scratch[:totalLen]

	if totalLen > 0 {
		if _, err := io.ReadFull(r.reader, r.scratch); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, ErrCorruptedTail
			}
			return nil, fmt.Errorf("read topic/payload: %w", err)
		}
	}

	// Read 4 byte CRC32
	var crcBytes [4]byte
	if _, err := io.ReadFull(r.reader, crcBytes[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrCorruptedTail
		}
		return nil, fmt.Errorf("read checksum: %w", err)
	}
	expectedCrc := binary.LittleEndian.Uint32(crcBytes[:])

	// Compute and verify CRC32
	crc := crc32.NewIEEE()
	crc.Write(header[:])
	if totalLen > 0 {
		crc.Write(r.scratch)
	}

	if crc.Sum32() != expectedCrc {
		// Log is corrupted here
		return nil, ErrCorruptedTail
	}

	var topic string
	if topicLen > 0 {
		topic = string(r.scratch[:topicLen])
	}

	var payload []byte
	if payloadLen > 0 {
		payload = r.scratch[topicLen:]
	}

	return &Message{
		Sequence:  seq,
		Timestamp: ts,
		Topic:     topic,
		Payload:   payload,
	}, nil
}

// Close closes the underlying file descriptor.
func (r *LogReader) Close() error {
	return r.file.Close()
}
