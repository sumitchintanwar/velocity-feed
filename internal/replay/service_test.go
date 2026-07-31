package replay

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/sequencer"
	"github.com/sumit/rtmds/internal/wal"
)

func TestServiceReplay(t *testing.T) {
	dir, err := os.MkdirTemp("", "service_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Phase 1: Populate WAL with messages 5 to 10 (simulating truncated/compacted log)
	walDir := filepath.Join(dir, "wal")
	cfg := wal.DefaultConfig
	cfg.Dir = walDir
	walLog, err := wal.NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to create wal log: %v", err)
	}
	defer walLog.Close()

	for i := uint64(5); i <= 10; i++ {
		msg := &wal.Message{
			Sequence:  i,
			Timestamp: time.Now().UnixNano(),
			Topic:     "SERVICE_TEST",
			Payload:   []byte("data"),
		}
		if _, _, err := walLog.Append(msg); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	walLog.Sync()

	engine := NewEngine(walLog)
	allocator := sequencer.NewAllocator()
	allocator.Set(10) // Simulate the latest committed sequence is 10

	service := NewService(engine, allocator)

	tests := []struct {
		name          string
		resumeFrom    uint64
		expectedErr   string
		expectedCount int
	}{
		{
			name:          "Valid Replay",
			resumeFrom:    5,
			expectedErr:   "",
			expectedCount: 6, // 5, 6, 7, 8, 9, 10
		},
		{
			name:          "Future Sequence",
			resumeFrom:    11,
			expectedErr:   "requested sequence 11 is in the future",
			expectedCount: 0,
		},
		{
			name:          "Missing Sequence (Truncated)",
			resumeFrom:    3,
			expectedErr:   "missing sequence: requested 3 but earliest available in log is 5",
			expectedCount: 0,
		},
		{
			name:          "Replay From Middle",
			resumeFrom:    8,
			expectedErr:   "",
			expectedCount: 3, // 8, 9, 10
		},
		{
			name:          "Replay Exact Last",
			resumeFrom:    10,
			expectedErr:   "",
			expectedCount: 1, // 10
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgs, err := service.Replay(tt.resumeFrom)

			if tt.expectedErr != "" {
				if err == nil {
					t.Fatalf("Expected error containing %q, got nil", tt.expectedErr)
				}
				// Substring check for expected error
				if !contains(err.Error(), tt.expectedErr[:30]) { // Check prefix
					t.Errorf("Expected error containing %q, got %v", tt.expectedErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if len(msgs) != tt.expectedCount {
				t.Errorf("Expected %d messages, got %d", tt.expectedCount, len(msgs))
			}

			if tt.expectedCount > 0 {
				if msgs[0].Sequence != tt.resumeFrom {
					t.Errorf("Expected first message sequence to be %d, got %d", tt.resumeFrom, msgs[0].Sequence)
				}
				// Verify ordering
				for i := 1; i < len(msgs); i++ {
					if msgs[i].Sequence <= msgs[i-1].Sequence {
						t.Errorf("Messages not ordered correctly: %d came after %d", msgs[i].Sequence, msgs[i-1].Sequence)
					}
				}
			}
		})
	}
}

func TestServiceEmptyLog(t *testing.T) {
	dir, err := os.MkdirTemp("", "service_empty_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	walDir := filepath.Join(dir, "wal")
	cfg := wal.DefaultConfig
	cfg.Dir = walDir
	walLog, _ := wal.NewSegmentManager(cfg)
	defer walLog.Close()

	engine := NewEngine(walLog)
	allocator := sequencer.NewAllocator()
	allocator.Set(0) // Empty log = sequence 0

	service := NewService(engine, allocator)

	// Valid request for empty log
	msgs, err := service.Replay(0)
	if err != nil {
		t.Errorf("Did not expect error for resume 0 on empty log, got %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(msgs))
	}

	// Invalid request for empty log
	_, err = service.Replay(1)
	if err == nil {
		t.Errorf("Expected error for resume 1 on empty log, got nil")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}
