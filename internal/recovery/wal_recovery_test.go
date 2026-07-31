package recovery

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/eventlog"
	"github.com/sumit/rtmds/internal/snapshot"
	"github.com/sumit/rtmds/internal/wal"
	"github.com/sumit/rtmds/pkg/marketdata"
)

func TestWALRecovery_HappyPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_recovery_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 1. Setup Snapshot Service
	checkpointPath := filepath.Join(tmpDir, "snapshot.json")
	snap := snapshot.New(snapshot.WithCheckpoint(checkpointPath, 1*time.Hour))
	snap.UpdateCursor(eventlog.Cursor{EventID: 5, Timestamp: time.Now()})
	snap.Update(marketdata.Quote{Symbol: "AAPL", Bid: 150.0, Seq: 5})
	if err := snap.Checkpoint(); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	// 2. Setup WAL with messages beyond sequence 5
	walDir := filepath.Join(tmpDir, "wal")
	walLog, err := wal.NewSegmentManager(wal.Config{
		Dir:                walDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
	})
	if err != nil {
		t.Fatalf("failed to init WAL: %v", err)
	}

	for i := uint64(1); i <= 10; i++ {
		q := marketdata.Quote{Symbol: "AAPL", Bid: 150.0 + float64(i), Seq: int64(i)}
		data, _ := json.Marshal(q)
		_, _, err := walLog.Append(&wal.Message{
			Sequence:  i,
			Timestamp: time.Now().UnixNano(),
			Topic:     "quote",
			Payload:   data,
		})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Close WAL to flush buffers and simulate a crash/restart
	walLog.Close()

	// 3. Create a fresh snapshot service and WAL manager (simulating restart)
	snapRestart := snapshot.New(snapshot.WithCheckpoint(checkpointPath, 1*time.Hour))
	walLogRestart, err := wal.NewSegmentManager(wal.Config{
		Dir:                walDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
	})
	if err != nil {
		t.Fatalf("failed to restart WAL: %v", err)
	}
	defer walLogRestart.Close()

	// 4. Recover
	recoverer := NewWALRecoverer(snapRestart, walLogRestart)
	highestSeq, err := recoverer.Recover(context.Background())
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if highestSeq != 10 {
		t.Errorf("expected highest sequence 10, got %d", highestSeq)
	}

	// 5. Verify snapshot state was updated by WAL
	cached := snapRestart.Get(context.Background(), "AAPL")
	if cached == nil {
		t.Fatal("expected AAPL snapshot to exist")
	}
	q, ok := cached.Event.(marketdata.Quote)
	if !ok {
		t.Fatal("expected quote")
	}
	if q.Bid != 160.0 {
		t.Errorf("expected final bid 160.0, got %f", q.Bid)
	}
}

func TestWALRecovery_MissingSnapshot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_recovery_missing_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointPath := filepath.Join(tmpDir, "snapshot.json")
	// Note: We don't save the checkpoint. It is missing.

	walDir := filepath.Join(tmpDir, "wal")
	walLog, err := wal.NewSegmentManager(wal.Config{
		Dir:                walDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
	})
	if err != nil {
		t.Fatalf("failed to init WAL: %v", err)
	}

	q := marketdata.Quote{Symbol: "GOOG", Bid: 2500.0, Seq: 1}
	data, _ := json.Marshal(q)
	walLog.Append(&wal.Message{
		Sequence:  1,
		Timestamp: time.Now().UnixNano(),
		Topic:     "quote",
		Payload:   data,
	})

	// Close to flush
	walLog.Close()

	walLogRestart, err := wal.NewSegmentManager(wal.Config{
		Dir:                walDir,
		MaxSegmentBytes:    1024 * 1024,
		IndexIntervalBytes: 1024,
	})
	if err != nil {
		t.Fatalf("failed to restart WAL: %v", err)
	}
	defer walLogRestart.Close()

	snap := snapshot.New(snapshot.WithCheckpoint(checkpointPath, 1*time.Hour))
	recoverer := NewWALRecoverer(snap, walLogRestart)

	highestSeq, err := recoverer.Recover(context.Background())
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if highestSeq != 1 {
		t.Errorf("expected highest sequence 1, got %d", highestSeq)
	}

	cached := snap.Get(context.Background(), "GOOG")
	if cached == nil {
		t.Fatal("expected GOOG snapshot to exist")
	}
}
