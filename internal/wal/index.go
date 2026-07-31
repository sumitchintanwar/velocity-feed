package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// IndexEntry represents a single mapping in the index file.
type IndexEntry struct {
	Sequence uint64
	Offset   uint64
}

// IndexFile manages the sparse index for a WAL segment.
// It maps absolute sequence numbers to physical byte offsets in the .log file.
type IndexFile struct {
	file *os.File
	size int64
}

// NewIndexFile opens or creates an index file.
func NewIndexFile(path string) (*IndexFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat index file: %w", err)
	}

	return &IndexFile{
		file: file,
		size: info.Size(),
	}, nil
}

// Append adds a new index entry.
func (idx *IndexFile) Append(seq uint64, offset uint64) error {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], seq)
	binary.LittleEndian.PutUint64(buf[8:16], offset)

	if _, err := idx.file.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to write index entry: %w", err)
	}
	idx.size += 16
	return nil
}

// Lookup performs a binary search to find the largest sequence number <= targetSeq.
// Returns the index entry or an error if not found (e.g. empty index).
func (idx *IndexFile) Lookup(targetSeq uint64) (IndexEntry, error) {
	if idx.size == 0 {
		return IndexEntry{}, fmt.Errorf("index is empty")
	}

	var best IndexEntry
	var found bool

	low := int64(0)
	high := (idx.size / 16) - 1

	for low <= high {
		mid := low + (high-low)/2
		offset := mid * 16

		var buf [16]byte
		if _, err := idx.file.ReadAt(buf[:], offset); err != nil {
			return IndexEntry{}, fmt.Errorf("failed to read index at %d: %w", offset, err)
		}

		seq := binary.LittleEndian.Uint64(buf[0:8])
		physOffset := binary.LittleEndian.Uint64(buf[8:16])

		if seq == targetSeq {
			return IndexEntry{Sequence: seq, Offset: physOffset}, nil
		}

		if seq < targetSeq {
			best = IndexEntry{Sequence: seq, Offset: physOffset}
			found = true
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if !found {
		// Target is before the first entry, return the first entry
		var buf [16]byte
		if _, err := idx.file.ReadAt(buf[:], 0); err != nil {
			return IndexEntry{}, err
		}
		return IndexEntry{
			Sequence: binary.LittleEndian.Uint64(buf[0:8]),
			Offset:   binary.LittleEndian.Uint64(buf[8:16]),
		}, nil
	}

	return best, nil
}

// Sync flushes the index file to disk.
func (idx *IndexFile) Sync() error {
	return idx.file.Sync()
}

// truncateAfter removes all index entries that have a Sequence > targetSeq.
func (idx *IndexFile) truncateAfter(targetSeq uint64) error {
	// Simple approach: we can just find the physical size by binary search,
	// or we can just read from the beginning since the index is small.
	// For simplicity, we scan it to find where the sequence exceeds targetSeq.

	if _, err := idx.file.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	var buf [16]byte
	var keepCount int64 = 0

	for {
		_, err := io.ReadFull(idx.file, buf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		seq := binary.LittleEndian.Uint64(buf[0:8])
		if seq > targetSeq {
			break
		}
		keepCount++
	}

	keepSize := keepCount * 16
	if err := idx.file.Truncate(keepSize); err != nil {
		return err
	}

	// Seek back to the new end so appends work
	if _, err := idx.file.Seek(keepSize, os.SEEK_SET); err != nil {
		return err
	}
	idx.size = keepSize
	return nil
}

// Close closes the underlying index file.
func (idx *IndexFile) Close() error {
	return idx.file.Close()
}

// Remove deletes the physical index file.
func (idx *IndexFile) Remove() error {
	path := idx.file.Name()
	idx.file.Close()
	return os.Remove(path)
}
