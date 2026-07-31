package replay

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/wal"
)

func TestEngineReplay(t *testing.T) {
	dir, err := os.MkdirTemp("", "engine_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Phase 1: Populate WAL with 10 messages (seq 1 to 10)
	walDir := filepath.Join(dir, "wal")
	cfg := wal.DefaultConfig
	cfg.Dir = walDir
	walLog, err := wal.NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to create wal log: %v", err)
	}
	defer walLog.Close()

	for i := uint64(1); i <= 10; i++ {
		msg := &wal.Message{
			Sequence:  i,
			Timestamp: time.Now().UnixNano(),
			Topic:     "REPLAY_TEST",
			Payload:   []byte("data"),
		}
		if _, _, err := walLog.Append(msg); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	walLog.Sync()

	engine := NewEngine(walLog)

	tests := []struct {
		name          string
		resumeFrom    uint64
		endSequence   uint64
		expectedCount int
		expectedStart uint64
		expectedEnd   uint64
	}{
		{
			name:          "Full Replay",
			resumeFrom:    1,
			endSequence:   10,
			expectedCount: 10,
			expectedStart: 1,
			expectedEnd:   10,
		},
		{
			name:          "Partial Replay Middle",
			resumeFrom:    4,
			endSequence:   7,
			expectedCount: 4, // sequences 4, 5, 6, 7
			expectedStart: 4,
			expectedEnd:   7,
		},
		{
			name:          "Replay Out Of Bounds Resume",
			resumeFrom:    20,
			endSequence:   30,
			expectedCount: 0,
		},
		{
			name:          "Replay Exact Last",
			resumeFrom:    10,
			endSequence:   10,
			expectedCount: 1,
			expectedStart: 10,
			expectedEnd:   10,
		},
		{
			name:          "Replay Until Infinity (Bounded by WAL)",
			resumeFrom:    8,
			endSequence:   100,
			expectedCount: 3, // sequences 8, 9, 10
			expectedStart: 8,
			expectedEnd:   10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it, err := engine.Start(tt.resumeFrom, tt.endSequence)
			if err != nil {
				t.Fatalf("Engine.Start failed: %v", err)
			}
			defer it.Close()

			var readMessages []*wal.Message
			for {
				msg, err := it.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Next failed: %v", err)
				}

				// Deep copy needed because payload is overwritten
				payloadCopy := make([]byte, len(msg.Payload))
				copy(payloadCopy, msg.Payload)

				readMessages = append(readMessages, &wal.Message{
					Sequence:  msg.Sequence,
					Timestamp: msg.Timestamp,
					Topic:     msg.Topic,
					Payload:   payloadCopy,
				})
			}

			if len(readMessages) != tt.expectedCount {
				t.Fatalf("Expected %d messages, got %d", tt.expectedCount, len(readMessages))
			}

			if tt.expectedCount > 0 {
				if readMessages[0].Sequence != tt.expectedStart {
					t.Errorf("Expected first sequence %d, got %d", tt.expectedStart, readMessages[0].Sequence)
				}
				if readMessages[len(readMessages)-1].Sequence != tt.expectedEnd {
					t.Errorf("Expected last sequence %d, got %d", tt.expectedEnd, readMessages[len(readMessages)-1].Sequence)
				}
			}

			// Verify topic and payload are intact
			for _, m := range readMessages {
				if m.Topic != "REPLAY_TEST" {
					t.Errorf("Topic corrupted: %s", m.Topic)
				}
				if string(m.Payload) != "data" {
					t.Errorf("Payload corrupted: %s", string(m.Payload))
				}
			}
		})
	}
}

func TestEngineZeroAllocationPayloads(t *testing.T) {
	// A small verification that if we don't copy the payload, it gets overwritten by the Next() call
	// which proves zero-allocation behavior is working as intended.

	dir, _ := os.MkdirTemp("", "engine_alloc_test")
	defer os.RemoveAll(dir)

	walDir := filepath.Join(dir, "wal")
	cfg := wal.DefaultConfig
	cfg.Dir = walDir
	walLog, _ := wal.NewSegmentManager(cfg)
	defer walLog.Close()

	msg1 := &wal.Message{Sequence: 1, Topic: "T", Payload: []byte("FOO")}
	msg2 := &wal.Message{Sequence: 2, Topic: "T", Payload: []byte("BAR")}
	_, _, _ = walLog.Append(msg1)
	_, _, _ = walLog.Append(msg2)
	walLog.Sync()

	engine := NewEngine(walLog)
	it, _ := engine.Start(1, 10)
	defer it.Close()

	rm1, _ := it.Next()
	// Stash a pointer to rm1's payload slice header
	p1 := rm1.Payload

	if string(p1) != "FOO" {
		t.Fatalf("Expected FOO, got %s", p1)
	}

	_, _ = it.Next()

	// Because the scratch buffer is reused, p1 should now contain BAR (since they are both 3 bytes).
	if string(p1) != "BAR" {
		t.Fatalf("Zero allocation behavior failed, expected payload pointer to be overwritten with BAR, got %s", p1)
	}
}
