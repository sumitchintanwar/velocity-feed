package exchange_test

import (
	"testing"

	"github.com/sumit/rtmds/internal/exchange"
)

func TestRegistry(t *testing.T) {
	exchange.Register("reg_test", func(cfg exchange.AdapterConfig) (exchange.ExchangeAdapter, error) {
		return nil, nil
	})
	
	_, err := exchange.GetFactory("reg_test")
	if err != nil {
		t.Errorf("expected factory, got error: %v", err)
	}
	
	_, err = exchange.GetFactory("not_found")
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}
