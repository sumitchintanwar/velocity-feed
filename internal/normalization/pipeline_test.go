package normalization

import (
	"fmt"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
)

type dummyMapper struct{}

func (m *dummyMapper) Map(raw marketdata.RawMessage) (marketdata.Quote, error) {
	if raw.Payload == nil {
		return marketdata.Quote{}, fmt.Errorf("nil payload")
	}
	return raw.Payload.(marketdata.Quote), nil
}

func TestPipeline_Normalize(t *testing.T) {
	logger := log.New(nil, "test")
	pipe := NewPipeline(&dummyMapper{}, NewDefaultValidator(), logger)

	t.Run("Valid quote passes", func(t *testing.T) {
		raw := marketdata.RawMessage{
			Provider: "test",
			Payload: marketdata.Quote{
				Symbol:    "BTC/USD", // Test symbol normalization
				Type:      marketdata.QuoteTypeTrade,
				Price:     100,
				Volume:    5,
				Timestamp: time.Now(),
			},
		}

		q, err := pipe.Normalize(raw)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if q.Symbol != "BTC-USD" {
			t.Errorf("expected BTC-USD, got %s", q.Symbol)
		}
	})

	t.Run("Invalid quote fails validation", func(t *testing.T) {
		raw := marketdata.RawMessage{
			Provider: "test",
			Payload: marketdata.Quote{
				Symbol:    "BTC/USD",
				Type:      marketdata.QuoteTypeTrade,
				Price:     -10, // Invalid
				Volume:    5,
				Timestamp: time.Now(),
			},
		}

		_, err := pipe.Normalize(raw)
		if err == nil {
			t.Fatalf("expected error for invalid price, got nil")
		}
	})

	t.Run("Mapper error propagates", func(t *testing.T) {
		raw := marketdata.RawMessage{
			Provider: "test",
			Payload:  nil, // dummyMapper will fail on this
		}

		_, err := pipe.Normalize(raw)
		if err == nil {
			t.Fatalf("expected mapping error, got nil")
		}
	})
}
