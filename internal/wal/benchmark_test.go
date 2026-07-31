package wal

import (
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"
)

// BenchmarkReplayThroughput measures the raw read throughput of the WAL reader.
// It writes a large number of messages to disk, resets the timer, and then
// measures how fast it can sequentially scan and decode them.
func BenchmarkReplayThroughput(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "wal_bench_throughput")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig
	cfg.Dir = tmpDir
	mgr, err := NewSegmentManager(cfg)
	if err != nil {
		b.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	payload := []byte(`{"symbol":"AAPL","bid":150.25,"ask":150.30,"seq":0}`)

	// Pre-fill the WAL with a reasonable number of messages
	numMessages := 100000
	for i := 1; i <= numMessages; i++ {
		_, _, err := mgr.Append(&Message{
			Sequence:  uint64(i),
			Timestamp: time.Now().UnixNano(),
			Topic:     "quote",
			Payload:   payload,
		})
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
	mgr.Sync()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader, err := mgr.NewReader(1)
		if err != nil {
			b.Fatalf("failed to create reader: %v", err)
		}

		count := 0
		for {
			_, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("read error: %v", err)
			}
			count++
		}
		reader.Close()

		if count != numMessages {
			b.Fatalf("expected to read %d messages, got %d", numMessages, count)
		}
	}
}

// BenchmarkSegmentLookup measures how fast the WAL can locate a specific sequence
// using the binary search across multiple segments and the sparse index.
func BenchmarkSegmentLookup(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "wal_bench_lookup")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig
	cfg.Dir = tmpDir
	// Force frequent segment rolling and indexing
	cfg.MaxSegmentBytes = 50 * 1024
	cfg.IndexIntervalBytes = 1024

	mgr, err := NewSegmentManager(cfg)
	if err != nil {
		b.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	payload := make([]byte, 256) // 256 byte payload

	// Create ~50 segments by writing a lot of data
	var maxSeq uint64 = 10000
	for i := uint64(1); i <= maxSeq; i++ {
		_, _, err := mgr.Append(&Message{
			Sequence:  i,
			Timestamp: time.Now().UnixNano(),
			Topic:     "quote",
			Payload:   payload,
		})
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
	mgr.Sync()

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark the time it takes to open a reader exactly at the middle sequence
	targetSeq := maxSeq / 2
	for i := 0; i < b.N; i++ {
		reader, err := mgr.NewReader(targetSeq)
		if err != nil {
			b.Fatalf("failed to create reader: %v", err)
		}

		msg, err := reader.Next()
		if err != nil {
			b.Fatalf("read error: %v", err)
		}
		if msg.Sequence != targetSeq {
			b.Fatalf("expected sequence %d, got %d", targetSeq, msg.Sequence)
		}
		reader.Close()
	}
}

// BenchmarkAppendLatency measures the latency of writing a single message
// to the WAL without a strict fsync (which is how the system currently runs,
// relying on OS buffers for throughput).
func BenchmarkAppendLatency(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "wal_bench_append")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig
	cfg.Dir = tmpDir
	mgr, err := NewSegmentManager(cfg)
	if err != nil {
		b.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	payload, _ := json.Marshal(map[string]string{"symbol": "BTCUSD", "price": "60000"})
	msg := &Message{
		Sequence:  1,
		Timestamp: time.Now().UnixNano(),
		Topic:     "quote",
		Payload:   payload,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg.Sequence = uint64(i + 1)
		_, _, err := mgr.Append(msg)
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
}
