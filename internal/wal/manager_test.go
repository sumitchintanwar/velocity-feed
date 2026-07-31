package wal

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"
)

func TestSegmentManagerWriteRead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")

	cfg := DefaultConfig
	cfg.Dir = dir
	cfg.MaxSegmentBytes = 100   // Force rolling very frequently
	cfg.IndexIntervalBytes = 20 // Force indexing frequently

	manager, err := NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Append 10 messages, which should span multiple segments
	for i := 0; i < 10; i++ {
		msg := &Message{
			Sequence:  uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Topic:     "TEST",
			Payload:   []byte(fmt.Sprintf("payload-%d", i+1)),
		}
		if _, _, err := manager.Append(msg); err != nil {
			t.Fatalf("failed to append msg %d: %v", i+1, err)
		}
	}

	// Force sync and check segment count
	manager.Sync()

	manager.mu.RLock()
	segCount := len(manager.segments)
	manager.mu.RUnlock()

	if segCount <= 1 {
		t.Fatalf("expected multiple segments, got %d", segCount)
	}

	// Read all from start
	reader, err := manager.NewReader(0)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	var count int
	for {
		msg, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to read next at count %d: %v", count, err)
		}

		count++
		if msg.Sequence != uint64(count) {
			t.Fatalf("expected sequence %d, got %d", count, msg.Sequence)
		}
	}

	if count != 10 {
		t.Fatalf("expected 10 messages, got %d", count)
	}
	reader.Close()

	// Test ResumeFrom
	reader5, err := manager.NewReader(5)
	if err != nil {
		t.Fatalf("failed to create reader5: %v", err)
	}
	msg5, err := reader5.Next()
	if err != nil {
		t.Fatalf("failed to read msg5: %v", err)
	}
	if msg5.Sequence != 5 {
		t.Fatalf("expected sequence 5, got %d", msg5.Sequence)
	}
	reader5.Close()

	manager.Close()
}

func TestSegmentManagerRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")

	cfg := DefaultConfig
	cfg.Dir = dir
	cfg.MaxSegmentBytes = 200

	manager1, _ := NewSegmentManager(cfg)
	for i := 0; i < 5; i++ {
		manager1.Append(&Message{
			Sequence:  uint64(i + 1),
			Timestamp: 1000,
			Topic:     "RECOV",
			Payload:   []byte("test"),
		})
	}
	manager1.Sync()
	manager1.Close() // Graceful shutdown

	// Restart
	manager2, err := NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to recover manager: %v", err)
	}

	reader, _ := manager2.NewReader(0)
	var count int
	for {
		_, err := reader.Next()
		if err == io.EOF {
			break
		}
		count++
	}
	if count != 5 {
		t.Fatalf("expected 5 messages after recovery, got %d", count)
	}
	reader.Close()
	manager2.Close()
}
