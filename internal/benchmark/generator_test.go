package benchmark

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/wal"
)

func TestDatasetGenerator(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dataset_gen_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	gen := NewGenerator(filepath.Join(tmpDir, "wal"))
	gen.SegmentSize = 1024 * 1024 // 1MB segments for testing

	numMessages := 10000

	// 1. Generate the dataset
	start := time.Now()
	if err := gen.Generate(numMessages); err != nil {
		t.Fatalf("generation failed: %v", err)
	}
	t.Logf("Generated %d messages in %v", numMessages, time.Since(start))

	// 2. Verify it's readable and correctly encoded
	cfg := wal.DefaultConfig
	cfg.Dir = gen.Dir
	mgr, err := wal.NewSegmentManager(cfg)
	if err != nil {
		t.Fatalf("failed to open generated WAL: %v", err)
	}
	defer mgr.Close()

	reader, err := mgr.NewReader(1)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	var count int
	for {
		msg, err := reader.Next()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("read failed at %d: %v", count+1, err)
		}

		count++

		if msg.Sequence != uint64(count) {
			t.Fatalf("expected sequence %d, got %d", count, msg.Sequence)
		}
		if msg.Topic != "quote" {
			t.Fatalf("expected topic quote, got %s", msg.Topic)
		}
		if len(msg.Payload) != 24 {
			t.Fatalf("expected 24 byte binary payload, got %d", len(msg.Payload))
		}

		// Decode binary payload
		// symBytes := msg.Payload[0:8]
		priceBits := binary.LittleEndian.Uint64(msg.Payload[8:16])
		price := math.Float64frombits(priceBits)
		seq := binary.LittleEndian.Uint64(msg.Payload[16:24])

		if price < 100.0 || price > 1000.0 {
			t.Fatalf("price %f out of bounds", price)
		}
		if seq != uint64(count) {
			t.Fatalf("encoded sequence %d doesn't match msg.Sequence %d", seq, msg.Sequence)
		}
	}

	if count != numMessages {
		t.Fatalf("expected to read %d messages, got %d", numMessages, count)
	}
}

func testGenerateScale(t *testing.T, numMessages int) {
	if testing.Short() {
		t.Skip("skipping scale test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "dataset_scale_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	gen := NewGenerator(filepath.Join(tmpDir, "wal"))

	start := time.Now()
	if err := gen.Generate(numMessages); err != nil {
		t.Fatalf("generation failed: %v", err)
	}
	t.Logf("Generated %d messages in %v", numMessages, time.Since(start))
}

func TestGenerate_1Million(t *testing.T) {
	testGenerateScale(t, 1_000_000)
}

func TestGenerate_5Million(t *testing.T) {
	testGenerateScale(t, 5_000_000)
}

func TestGenerate_10Million(t *testing.T) {
	testGenerateScale(t, 10_000_000)
}
