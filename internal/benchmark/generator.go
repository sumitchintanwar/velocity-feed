package benchmark

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/sumit/rtmds/internal/wal"
)

var symbols = []string{"AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "JNJ"}

// Generator handles the creation of massive benchmarking datasets.
type Generator struct {
	Dir         string
	SegmentSize int64
}

// NewGenerator creates a new dataset generator that writes WAL segments to dir.
func NewGenerator(dir string) *Generator {
	return &Generator{
		Dir:         dir,
		SegmentSize: 100 * 1024 * 1024, // 100 MB segments by default
	}
}

// Generate generates exactly numMessages and writes them to the WAL.
// The payload is a highly efficient 24-byte binary encoding:
// [8 bytes symbol padded][8 bytes price float64][8 bytes sequence]
func (g *Generator) Generate(numMessages int) error {
	if err := os.MkdirAll(g.Dir, 0755); err != nil {
		return fmt.Errorf("failed to create benchmark dir: %w", err)
	}

	cfg := wal.DefaultConfig
	cfg.Dir = g.Dir
	cfg.MaxSegmentBytes = g.SegmentSize
	// No retention during generation
	cfg.RetentionBytes = 1024 * 1024 * 1024 * 1024

	mgr, err := wal.NewSegmentManager(cfg)
	if err != nil {
		return fmt.Errorf("failed to create segment manager: %w", err)
	}
	defer mgr.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Preallocate a reusable payload buffer (24 bytes)
	// 8 bytes string + 8 bytes float64 + 8 bytes uint64
	payload := make([]byte, 24)

	var msg wal.Message
	msg.Topic = "quote"

	for i := 1; i <= numMessages; i++ {
		sym := symbols[r.Intn(len(symbols))]
		price := 100.0 + r.Float64()*900.0 // Random price between 100 and 1000

		// Encode payload
		copy(payload[0:8], sym)
		// Pad remaining string bytes with nulls if symbol < 8 chars
		for j := len(sym); j < 8; j++ {
			payload[j] = 0
		}

		binary.LittleEndian.PutUint64(payload[8:16], math.Float64bits(price))
		binary.LittleEndian.PutUint64(payload[16:24], uint64(i))

		msg.Sequence = uint64(i)
		msg.Timestamp = time.Now().UnixNano()
		msg.Payload = payload

		_, _, err := mgr.Append(&msg)
		if err != nil {
			return fmt.Errorf("append failed at msg %d: %w", i, err)
		}
	}

	return mgr.Sync()
}
