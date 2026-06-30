package metadata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/sumit/rtmds/internal/log"
)

func getTestSeeds() []*Instrument {
	return []*Instrument{
		{
			CanonicalSymbol: "BTC-USD",
			ExchangeSymbol:  "BTCUSD",
			Exchange:        "COINBASE",
			AssetClass:      AssetClassCrypto,
			TradingStatus:   StatusTrading,
		},
		{
			CanonicalSymbol: "AAPL",
			ExchangeSymbol:  "AAPL",
			Exchange:        "NASDAQ",
			AssetClass:      AssetClassEquity,
			TradingStatus:   StatusClosed,
		},
	}
}

func TestCacheAndService(t *testing.T) {
	repo := NewInMemoryRepository(getTestSeeds())
	logger := log.New(nil, "test")
	svc := NewService(repo, logger)

	// Test cache empty before load
	_, err := svc.GetInstrument("BTC-USD")
	if err == nil {
		t.Fatalf("Expected error when cache is empty")
	}

	// Load cache
	if err := svc.LoadCache(context.Background()); err != nil {
		t.Fatalf("Failed to load cache: %v", err)
	}

	// Test Symbol Lookup
	inst, err := svc.GetInstrument("BTC-USD")
	if err != nil || inst.Exchange != "COINBASE" {
		t.Fatalf("Failed to lookup symbol: %v", err)
	}

	// Test Exchange Lookup
	nasdaq := svc.GetInstrumentsByExchange("NASDAQ")
	if len(nasdaq) != 1 || nasdaq[0].CanonicalSymbol != "AAPL" {
		t.Fatalf("Failed exchange lookup")
	}

	// Test AssetClass Lookup
	crypto := svc.GetInstrumentsByAssetClass(AssetClassCrypto)
	if len(crypto) != 1 || crypto[0].CanonicalSymbol != "BTC-USD" {
		t.Fatalf("Failed asset class lookup")
	}
}

func TestAPIEndpoints(t *testing.T) {
	repo := NewInMemoryRepository(getTestSeeds())
	logger := log.New(nil, "test")
	svc := NewService(repo, logger)
	_ = svc.LoadCache(context.Background())

	api := NewAPI(svc)
	r := chi.NewRouter()
	r.Mount("/api/v1/metadata", api.Routes())

	// Test GET Symbol
	req, _ := http.NewRequest("GET", "/api/v1/metadata/symbols/BTC-USD", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

func TestCacheConcurrency(t *testing.T) {
	repo := NewInMemoryRepository(getTestSeeds())
	logger := log.New(nil, "test")
	svc := NewService(repo, logger)
	_ = svc.LoadCache(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spawn writer
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = svc.LoadCache(context.Background())
			}
		}
	}()

	// Spawn readers
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				_, _ = svc.GetInstrument("BTC-USD")
			}
			done <- true
		}()
	}

	// Wait for readers
	for i := 0; i < 100; i++ {
		<-done
	}
}
