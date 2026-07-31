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

// BenchmarkStartupRecoveryTime measures how quickly the system can boot,
// load a snapshot, and replay a WAL that has 50,000 un-snapshotted messages.
func BenchmarkStartupRecoveryTime(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "recovery_bench")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Pre-build a massive WAL state
	checkpointPath := filepath.Join(tmpDir, "snapshot.json")
	snap := snapshot.New(snapshot.WithCheckpoint(checkpointPath, 1*time.Hour))

	// Start snapshot at sequence 100,000
	var startSeq int64 = 100000
	snap.UpdateCursor(eventlog.Cursor{EventID: startSeq, Timestamp: time.Now()})
	snap.Update(marketdata.Quote{Symbol: "AAPL", Bid: 150.0, Seq: startSeq})
	if err := snap.Checkpoint(); err != nil {
		b.Fatalf("failed to save checkpoint: %v", err)
	}

	walDir := filepath.Join(tmpDir, "wal")
	walLog, err := wal.NewSegmentManager(wal.Config{
		Dir:                walDir,
		MaxSegmentBytes:    10 * 1024 * 1024,
		IndexIntervalBytes: 1024,
	})
	if err != nil {
		b.Fatalf("failed to init WAL: %v", err)
	}

	// Append 50,000 events AFTER the snapshot sequence
	payload, _ := json.Marshal(marketdata.Quote{Symbol: "AAPL", Bid: 150.50, Seq: 0})
	for i := int64(1); i <= 50000; i++ {
		_, _, err := walLog.Append(&wal.Message{
			Sequence:  uint64(startSeq + i),
			Timestamp: time.Now().UnixNano(),
			Topic:     "quote",
			Payload:   payload,
		})
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
	walLog.Close() // Flush and close to prepare for "restart" simulation

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 1. Simulate Boot: Create fresh Snapshot and WAL manager
		snapRestart := snapshot.New(snapshot.WithCheckpoint(checkpointPath, 1*time.Hour))

		// Note: NewSegmentManager parses all segments, verifying base sequences
		walLogRestart, err := wal.NewSegmentManager(wal.Config{
			Dir:                walDir,
			MaxSegmentBytes:    10 * 1024 * 1024,
			IndexIntervalBytes: 1024,
		})
		if err != nil {
			b.Fatalf("failed to restart WAL: %v", err)
		}

		// 2. Execute Recovery
		recoverer := NewWALRecoverer(snapRestart, walLogRestart)
		_, err = recoverer.Recover(context.Background())
		if err != nil {
			b.Fatalf("recovery failed: %v", err)
		}

		// Clean up for the next iteration
		walLogRestart.Close()
	}
}
