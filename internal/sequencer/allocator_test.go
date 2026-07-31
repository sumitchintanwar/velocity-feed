package sequencer

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/wal"
)

func TestAllocatorBasic(t *testing.T) {
	a := NewAllocator()

	if curr := a.Current(); curr != 0 {
		t.Errorf("Expected current to be 0, got %d", curr)
	}

	if next := a.Next(); next != 1 {
		t.Errorf("Expected next to be 1, got %d", next)
	}

	if next := a.Next(); next != 2 {
		t.Errorf("Expected next to be 2, got %d", next)
	}

	a.Set(100)
	if curr := a.Current(); curr != 100 {
		t.Errorf("Expected current to be 100, got %d", curr)
	}

	if next := a.Next(); next != 101 {
		t.Errorf("Expected next to be 101, got %d", next)
	}
}

func TestAllocatorConcurrency(t *testing.T) {
	a := NewAllocator()
	var wg sync.WaitGroup

	numRoutines := 50
	incrementsPerRoutine := 1000

	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerRoutine; j++ {
				a.Next()
			}
		}()
	}

	wg.Wait()

	expected := uint64(numRoutines * incrementsPerRoutine)
	if curr := a.Current(); curr != expected {
		t.Errorf("Expected concurrent final count to be %d, got %d", expected, curr)
	}
}

func TestAllocatorInitFromWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "allocator_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logPath := filepath.Join(dir, "init.log")

	// Phase 1: Write sequences up to 500 into the WAL
	writer, err := wal.NewWriter(logPath)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	var maxSeqWritten uint64 = 500
	for i := uint64(1); i <= maxSeqWritten; i++ {
		msg := &wal.Message{
			Sequence:  i,
			Timestamp: time.Now().UnixNano(),
			Topic:     "TEST",
			Payload:   []byte("test"),
		}
		if _, err := writer.Append(msg); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	writer.Sync()
	writer.Close()

	// Phase 2: Initialize Allocator from WAL
	a := NewAllocator()
	if err := a.InitFromWAL(logPath); err != nil {
		t.Fatalf("InitFromWAL failed: %v", err)
	}

	if curr := a.Current(); curr != maxSeqWritten {
		t.Errorf("Expected initialized sequence to be %d, got %d", maxSeqWritten, curr)
	}

	if next := a.Next(); next != maxSeqWritten+1 {
		t.Errorf("Expected next sequence after init to be %d, got %d", maxSeqWritten+1, next)
	}
}
