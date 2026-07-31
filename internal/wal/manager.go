package wal

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds configuration for the SegmentManager.
type Config struct {
	Dir                string
	MaxSegmentBytes    int64
	IndexIntervalBytes int64
	RetentionBytes     int64
	RetentionTime      time.Duration
}

// DefaultConfig provides sensible defaults.
var DefaultConfig = Config{
	Dir:                "data/wal",
	MaxSegmentBytes:    1024 * 1024 * 1024,      // 1GB
	IndexIntervalBytes: 4 * 1024,                // 4KB
	RetentionBytes:     10 * 1024 * 1024 * 1024, // 10GB
	RetentionTime:      24 * 7 * time.Hour,      // 7 days
}

// SegmentManager manages multiple segments for the WAL.
type SegmentManager struct {
	mu       sync.RWMutex
	cfg      Config
	segments []Segment
	active   Segment

	lastSeq uint64
	closeCh chan struct{}
}

// NewSegmentManager initializes and recovers the WAL segments.
func NewSegmentManager(cfg Config) (*SegmentManager, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}

	m := &SegmentManager{
		cfg:     cfg,
		closeCh: make(chan struct{}),
	}

	if err := m.loadSegments(); err != nil {
		return nil, err
	}

	go m.retainerLoop()

	return m, nil
}

// loadSegments discovers existing segments and recovers state.
func (m *SegmentManager) loadSegments() error {
	entries, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return fmt.Errorf("read wal dir: %w", err)
	}

	var baseSeqs []uint64
	seqMap := make(map[uint64]bool)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		seq, err := ExtractBaseSequence(entry.Name())
		if err != nil {
			continue
		}
		if !seqMap[seq] {
			seqMap[seq] = true
			baseSeqs = append(baseSeqs, seq)
		}
	}

	sort.Slice(baseSeqs, func(i, j int) bool { return baseSeqs[i] < baseSeqs[j] })

	m.segments = make([]Segment, 0, len(baseSeqs))

	for _, seq := range baseSeqs {
		seg, err := OpenSegment(m.cfg.Dir, seq, m.cfg.IndexIntervalBytes)
		if err != nil {
			return err
		}
		m.segments = append(m.segments, seg)
	}

	if len(m.segments) > 0 {
		if s, ok := m.active.(*segment); ok {
			if err := s.recover(); err != nil {
				return fmt.Errorf("failed to recover active segment: %w", err)
			}
		}
	} else {
		// Create the first segment starting at seq 0
		seg, err := OpenSegment(m.cfg.Dir, 0, m.cfg.IndexIntervalBytes)
		if err != nil {
			return err
		}
		m.segments = append(m.segments, seg)
		m.active = seg
	}

	// Find the highest sequence by reading the last segment
	if len(m.segments) > 0 {
		lastSeg := m.segments[len(m.segments)-1]
		reader, err := lastSeg.ReadAt(lastSeg.BaseSequence())
		if err == nil {
			var highest uint64
			for {
				msg, rErr := reader.Next()
				if rErr != nil {
					break // EOF or error
				}
				highest = msg.Sequence
			}
			reader.Close()
			atomic.StoreUint64(&m.lastSeq, highest)
		}
	}

	return nil
}

// Append appends a message to the active segment and rolls if necessary.
func (m *SegmentManager) Append(msg *Message) (uint64, uint64, error) {
	m.mu.RLock()
	active := m.active

	// Check if roll is needed
	if active.Size() >= m.cfg.MaxSegmentBytes {
		m.mu.RUnlock()

		// Upgrade to full lock to roll
		m.mu.Lock()
		// Double check condition after acquiring lock
		if m.active.Size() >= m.cfg.MaxSegmentBytes {
			if err := m.active.Sync(); err != nil {
				m.mu.Unlock()
				return 0, 0, fmt.Errorf("failed to sync active segment before rolling: %w", err)
			}

			newSeq := msg.Sequence
			newSeg, err := OpenSegment(m.cfg.Dir, newSeq, m.cfg.IndexIntervalBytes)
			if err != nil {
				m.mu.Unlock()
				return 0, 0, fmt.Errorf("failed to open new segment: %w", err)
			}

			m.segments = append(m.segments, newSeg)
			m.active = newSeg
		}
		active = m.active
		m.mu.Unlock()
	} else {
		m.mu.RUnlock()
	}

	offset, err := active.Append(msg)
	if err == nil {
		atomic.StoreUint64(&m.lastSeq, msg.Sequence)
	}
	return msg.Sequence, offset, err
}

// LastSequence returns the highest successfully appended sequence.
func (m *SegmentManager) LastSequence() uint64 {
	return atomic.LoadUint64(&m.lastSeq)
}

// NewReader returns a reader that can span across segments starting at startSequence.
func (m *SegmentManager) NewReader(startSequence uint64) (Reader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.segments) == 0 {
		return nil, fmt.Errorf("no segments available")
	}

	// Binary search to find the segment containing startSequence
	// Find highest segment whose baseSequence <= startSequence
	targetIdx := -1
	for i := len(m.segments) - 1; i >= 0; i-- {
		if m.segments[i].BaseSequence() <= startSequence {
			targetIdx = i
			break
		}
	}

	if targetIdx == -1 {
		// startSequence is older than the oldest segment, which means it was purged by retention!
		return nil, ErrSequenceTooOld
	}

	// Create a MultiSegmentReader
	return &MultiSegmentReader{
		manager:       m,
		segmentIdx:    targetIdx,
		startSequence: startSequence,
		currentReader: nil,
	}, nil
}

// Sync flushes the active segment.
func (m *SegmentManager) Sync() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active.Sync()
}

// Close closes all segments.
func (m *SegmentManager) Close() error {
	close(m.closeCh)
	m.mu.Lock()
	defer m.mu.Unlock()
	var lastErr error
	for _, seg := range m.segments {
		if err := seg.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// retainerLoop runs in the background and removes segments that exceed retention policies.
func (m *SegmentManager) retainerLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case <-ticker.C:
			m.enforceRetention()
		}
	}
}

func (m *SegmentManager) enforceRetention() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.segments) <= 1 {
		return // Never delete the active segment
	}

	var totalSize int64
	for _, seg := range m.segments {
		totalSize += seg.Size()
	}

	var toRemove int
	for i := 0; i < len(m.segments)-1; i++ {
		seg := m.segments[i]

		var shouldDelete bool

		// Check size retention
		if totalSize > m.cfg.RetentionBytes {
			shouldDelete = true
		}

		// Check age retention
		if !shouldDelete && m.cfg.RetentionTime > 0 {
			if modTime, err := seg.ModTime(); err == nil {
				if time.Since(modTime) > m.cfg.RetentionTime {
					shouldDelete = true
				}
			}
		}

		if shouldDelete {
			totalSize -= seg.Size()
			_ = seg.Remove()
			toRemove++
		} else {
			// Segments are ordered by age. If this segment shouldn't be deleted,
			// newer segments definitely shouldn't be deleted based on age.
			// (Though they could based on size, but we only delete oldest first anyway).
			break
		}
	}

	if toRemove > 0 {
		m.segments = m.segments[toRemove:]
	}
}

// MultiSegmentReader implements Reader by crossing segment boundaries.
type MultiSegmentReader struct {
	manager       *SegmentManager
	segmentIdx    int
	startSequence uint64
	currentReader Reader
}

func (r *MultiSegmentReader) Next() (*Message, error) {
	for {
		if r.currentReader == nil {
			r.manager.mu.RLock()
			if r.segmentIdx >= len(r.manager.segments) {
				r.manager.mu.RUnlock()
				return nil, io.EOF
			}
			seg := r.manager.segments[r.segmentIdx]
			r.manager.mu.RUnlock()

			sr, err := seg.ReadAt(r.startSequence)
			if err != nil {
				return nil, err
			}
			r.currentReader = sr
		}

		msg, err := r.currentReader.Next()
		if err == io.EOF || err == ErrCorruptedTail {
			// Check if there is a next segment available
			r.manager.mu.RLock()
			hasNext := r.segmentIdx < len(r.manager.segments)-1
			r.manager.mu.RUnlock()

			if hasNext {
				_ = r.currentReader.Close()
				r.currentReader = nil
				r.segmentIdx++
				// Reset startSequence so the next segment reads from its beginning (0)
				r.startSequence = 0
				continue
			} else {
				// We hit EOF on the active segment (caught up to writer)
				return nil, io.EOF
			}
		}
		if err != nil {
			return nil, err
		}

		// Fast forward if we seeked to a 4KB chunk but haven't reached the exact sequence yet
		if msg.Sequence < r.startSequence {
			continue
		}

		return msg, nil
	}
}

func (r *MultiSegmentReader) Close() error {
	if r.currentReader != nil {
		return r.currentReader.Close()
	}
	return nil
}
