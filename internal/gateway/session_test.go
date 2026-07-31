package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/wal"
)

// mockReplayer simulates a slow disk read that returns historical messages.
type mockReplayer struct {
	msgs  []*wal.Message
	delay time.Duration
	err   error
}

func (m *mockReplayer) Replay(resumeFrom uint64) ([]*wal.Message, error) {
	time.Sleep(m.delay)
	if m.err != nil {
		return nil, m.err
	}
	return m.msgs, nil
}

func TestSessionTransition(t *testing.T) {
	// Replay returns sequences 5, 6, 7
	// Live broadcast receives 7, 8, 9 while replay is happening
	// The client should receive exactly 5, 6, 7, 8, 9 with no duplicates of 7.

	replayedMsgs := []*wal.Message{
		{Sequence: 5, Payload: []byte("R5")},
		{Sequence: 6, Payload: []byte("R6")},
		{Sequence: 7, Payload: []byte("R7")},
	}

	replayer := &mockReplayer{
		msgs:  replayedMsgs,
		delay: 50 * time.Millisecond, // Simulate 50ms disk latency
	}

	session := NewSession(replayer, 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run session in background
	go session.Run(ctx)

	// Simulate concurrent Live Publishing DURING replay
	go func() {
		// Wait just a tiny bit to ensure the select loop has started
		time.Sleep(10 * time.Millisecond)

		// 7 overlaps with the end of replay, should be dropped!
		session.LiveCh <- &wal.Message{Sequence: 7, Payload: []byte("L7")}
		session.LiveCh <- &wal.Message{Sequence: 8, Payload: []byte("L8")}
		session.LiveCh <- &wal.Message{Sequence: 9, Payload: []byte("L9")}
	}()

	// Collect output
	var received []*wal.Message
	timer := time.NewTimer(100 * time.Millisecond)

collectLoop:
	for {
		select {
		case msg := <-session.SendCh:
			received = append(received, msg)
			if msg.Sequence == 9 {
				break collectLoop
			}
		case <-timer.C:
			t.Fatal("Timeout waiting for messages")
		}
	}

	if len(received) != 5 {
		t.Fatalf("Expected 5 messages, got %d", len(received))
	}

	for i, msg := range received {
		expectedSeq := uint64(5 + i)
		if msg.Sequence != expectedSeq {
			t.Errorf("Expected sequence %d at index %d, got %d", expectedSeq, i, msg.Sequence)
		}
	}

	// Verify sources to prove it actually transitioned properly
	if string(received[0].Payload) != "R5" {
		t.Errorf("Expected R5, got %s", received[0].Payload)
	}
	if string(received[1].Payload) != "R6" {
		t.Errorf("Expected R6, got %s", received[1].Payload)
	}
	if string(received[2].Payload) != "R7" {
		t.Errorf("Expected R7, got %s", received[2].Payload)
	}
	if string(received[3].Payload) != "L8" {
		t.Errorf("Expected L8, got %s", received[3].Payload)
	}
	if string(received[4].Payload) != "L9" {
		t.Errorf("Expected L9, got %s", received[4].Payload)
	}
}
