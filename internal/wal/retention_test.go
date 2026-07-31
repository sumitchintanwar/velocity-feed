package wal

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestWALRetention_MaxTotalBytes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_retention_size_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create manager with small max segment size and retention size
	cfg := DefaultConfig
	cfg.Dir = tmpDir
	cfg.MaxSegmentBytes = 500    // Roll frequently
	cfg.IndexIntervalBytes = 100 // Index frequently
	cfg.RetentionBytes = 1500    // Keep max ~3 closed segments + 1 active
	cfg.RetentionTime = 0        // Disable age retention

	mgr, err := NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Write enough data to create roughly 6 segments
	for i := uint64(1); i <= 50; i++ {
		data, _ := json.Marshal(map[string]interface{}{"seq": i, "fill": "some data to pad size out"})
		_, _, err := mgr.Append(&Message{
			Sequence:  i,
			Timestamp: time.Now().UnixNano(),
			Topic:     "test",
			Payload:   data,
		})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Trigger retention manually instead of waiting for the background ticker
	mgr.enforceRetention()

	mgr.mu.RLock()
	segCount := len(mgr.segments)
	mgr.mu.RUnlock()

	// We expect the active segment, plus however many closed segments fit in 1500 bytes.
	// Since each is ~500 bytes, we expect around 3 closed + 1 active = 4 segments total.
	if segCount > 5 {
		t.Errorf("expected size retention to reduce segments, got %d", segCount)
	}

	if segCount == 0 {
		t.Errorf("expected at least active segment, got 0")
	}
}

func TestWALRetention_MaxAge(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_retention_age_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig
	cfg.Dir = tmpDir
	cfg.MaxSegmentBytes = 500
	cfg.IndexIntervalBytes = 100
	cfg.RetentionBytes = 10 * 1024 * 1024 // Large enough to not trigger size retention
	cfg.RetentionTime = 1 * time.Hour     // 1 Hour age retention

	mgr, err := NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Write data to create a few segments
	for i := uint64(1); i <= 20; i++ {
		data, _ := json.Marshal(map[string]interface{}{"seq": i, "pad": "pad"})
		_, _, err := mgr.Append(&Message{
			Sequence:  i,
			Timestamp: time.Now().UnixNano(),
			Topic:     "test",
			Payload:   data,
		})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	mgr.mu.RLock()
	segCountBefore := len(mgr.segments)
	// Get the first segment's file name to artificially backdate it
	firstSegName := mgr.segments[0].(*segment).logFile.file.Name()
	mgr.mu.RUnlock()

	if segCountBefore <= 1 {
		t.Fatalf("expected multiple segments, got %d", segCountBefore)
	}

	// Artificially change the ModTime of the first segment to be 2 hours old
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(firstSegName, oldTime, oldTime); err != nil {
		t.Fatalf("failed to change file time: %v", err)
	}

	// Trigger retention manually
	mgr.enforceRetention()

	mgr.mu.RLock()
	segCountAfter := len(mgr.segments)
	mgr.mu.RUnlock()

	if segCountAfter >= segCountBefore {
		t.Errorf("expected retention to delete the aged segment. Before: %d, After: %d", segCountBefore, segCountAfter)
	}
}

func TestWALRetention_ReplaySafety(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_retention_replay_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig
	cfg.Dir = tmpDir
	cfg.MaxSegmentBytes = 500
	cfg.RetentionBytes = 500 // Very small size retention
	cfg.RetentionTime = 0

	mgr, err := NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Append sequence 1..20
	for i := uint64(1); i <= 20; i++ {
		data, _ := json.Marshal(map[string]interface{}{"seq": i, "pad": "some padding text to increase size"})
		_, _, err := mgr.Append(&Message{
			Sequence:  i,
			Timestamp: time.Now().UnixNano(),
			Topic:     "test",
			Payload:   data,
		})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Trigger retention to delete older segments
	mgr.enforceRetention()

	// Determine the oldest available sequence now
	mgr.mu.RLock()
	oldestBaseSeq := mgr.segments[0].BaseSequence()
	mgr.mu.RUnlock()

	if oldestBaseSeq <= 1 {
		t.Fatalf("expected oldest segment base sequence to be > 1 after retention, got %d", oldestBaseSeq)
	}

	// Attempt to start a reader from sequence 1
	// Since segment containing seq 1 was deleted, this should return ErrSequenceTooOld
	_, err = mgr.NewReader(1)
	if err != ErrSequenceTooOld {
		t.Errorf("expected ErrSequenceTooOld, got %v", err)
	}

	// Attempt to start a reader from the oldest available sequence
	// This should succeed
	reader, err := mgr.NewReader(oldestBaseSeq)
	if err != nil {
		t.Errorf("expected success reading from oldest available %d, got %v", oldestBaseSeq, err)
	}
	if reader != nil {
		reader.Close()
	}
}
