package normalization

import (
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/marketdata"
)

func TestDefaultValidator(t *testing.T) {
	v := NewDefaultValidator()

	tests := []struct {
		name    string
		quote   marketdata.Quote
		wantErr bool
	}{
		{
			name: "Valid Trade",
			quote: marketdata.Quote{
				Symbol:    "BTC-USD",
				Type:      marketdata.QuoteTypeTrade,
				Price:     50000,
				Volume:    1,
				Timestamp: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "Valid Quote",
			quote: marketdata.Quote{
				Symbol:    "BTC-USD",
				Type:      marketdata.QuoteTypeQuote,
				Price:     50000,
				Timestamp: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "Missing Symbol",
			quote: marketdata.Quote{
				Type:      marketdata.QuoteTypeTrade,
				Price:     100,
				Volume:    10,
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "Zero Price",
			quote: marketdata.Quote{
				Symbol:    "AAPL",
				Type:      marketdata.QuoteTypeTrade,
				Price:     0,
				Volume:    10,
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "Negative Volume for Trade",
			quote: marketdata.Quote{
				Symbol:    "AAPL",
				Type:      marketdata.QuoteTypeTrade,
				Price:     150,
				Volume:    -5,
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "Missing Timestamp",
			quote: marketdata.Quote{
				Symbol: "AAPL",
				Type:   marketdata.QuoteTypeTrade,
				Price:  150,
				Volume: 10,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(&tt.quote)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
