package wal

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWALWriteAndRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logPath := filepath.Join(dir, "test.log")

	writer, err := NewWriter(logPath)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	messages := []*Message{
		{
			Sequence:  1,
			Timestamp: time.Now().UnixNano(),
			Topic:     "BTCUSD",
			Payload:   []byte(`{"price": 50000}`),
		},
		{
			Sequence:  2,
			Timestamp: time.Now().UnixNano(),
			Topic:     "ETHUSD",
			Payload:   []byte(`{"price": 3000}`),
		},
		{
			Sequence:  3,
			Timestamp: time.Now().UnixNano(),
			Topic:     "EMPTY",
			Payload:   nil,
		},
	}

	for _, msg := range messages {
		if _, err := writer.Append(msg); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reader, err := NewReader(logPath)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	var readMessages []*Message
	for {
		msg, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		// Copy payload because reader reuses the scratch buffer
		payloadCopy := make([]byte, len(msg.Payload))
		copy(payloadCopy, msg.Payload)
		msg.Payload = payloadCopy

		readMessages = append(readMessages, msg)
	}

	if len(readMessages) != len(messages) {
		t.Fatalf("Expected %d messages, got %d", len(messages), len(readMessages))
	}

	for i, expected := range messages {
		actual := readMessages[i]
		if actual.Sequence != expected.Sequence {
			t.Errorf("Message %d: expected sequence %d, got %d", i, expected.Sequence, actual.Sequence)
		}
		if actual.Topic != expected.Topic {
			t.Errorf("Message %d: expected topic %s, got %s", i, expected.Topic, actual.Topic)
		}
		if !bytes.Equal(actual.Payload, expected.Payload) {
			t.Errorf("Message %d: expected payload %s, got %s", i, expected.Payload, actual.Payload)
		}
	}
}

func TestWALConcurrency(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_concurrency_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logPath := filepath.Join(dir, "concurrent.log")

	writer, err := NewWriter(logPath)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	var wg sync.WaitGroup
	numRoutines := 10
	numMessages := 100

	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numMessages; j++ {
				msg := &Message{
					Sequence:  uint64(routineID*1000 + j),
					Timestamp: time.Now().UnixNano(),
					Topic:     "CONCURRENT",
					Payload:   []byte("test data"),
				}
				if _, err := writer.Append(msg); err != nil {
					t.Errorf("Append failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reader, err := NewReader(logPath)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	count := 0
	for {
		_, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		count++
	}

	expectedTotal := numRoutines * numMessages
	if count != expectedTotal {
		t.Fatalf("Expected %d total messages, got %d", expectedTotal, count)
	}
}

func TestWALCorruptedTail(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_corruption_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logPath := filepath.Join(dir, "corrupted.log")

	writer, err := NewWriter(logPath)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	msg := &Message{
		Sequence:  1,
		Timestamp: time.Now().UnixNano(),
		Topic:     "TEST",
		Payload:   []byte("valid payload"),
	}

	if _, err := writer.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Deliberately truncate the file to simulate a torn write
	// Add a partial second message
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	// Write exactly 10 bytes (a partial header)
	if _, err := f.Write([]byte("1234567890")); err != nil {
		t.Fatalf("Write partial failed: %v", err)
	}
	f.Close()

	reader, err := NewReader(logPath)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	// First message should be readable
	readMsg, err := reader.Next()
	if err != nil {
		t.Fatalf("Expected to read first valid message, got err: %v", err)
	}
	if readMsg.Sequence != 1 {
		t.Fatalf("Expected sequence 1, got %d", readMsg.Sequence)
	}

	// Second message should result in ErrCorruptedTail
	_, err = reader.Next()
	if err != ErrCorruptedTail {
		t.Fatalf("Expected ErrCorruptedTail on torn write, got %v", err)
	}
}
