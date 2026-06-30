package normalization

import (
	"fmt"
	"strings"

	"golang.org/x/time/rate"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
)

// Pipeline represents a full normalization stage for a specific exchange adapter.
// It applies parsing, mapping, timestamp/symbol normalisation, and validation.
type Pipeline struct {
	mapper    Mapper
	validator Validator
	logger    *log.Logger
	limiter   *rate.Limiter
}

// NewPipeline creates a new Normalization Pipeline for an adapter.
func NewPipeline(mapper Mapper, validator Validator, logger *log.Logger) *Pipeline {
	// 10 errors per second burstable to 20 to prevent log flooding
	limiter := rate.NewLimiter(10, 20)
	return &Pipeline{
		mapper:    mapper,
		validator: validator,
		logger:    logger,
		limiter:   limiter,
	}
}

// Normalize processes a raw message and returns a canonical Quote.
// If the message is invalid, it logs the failure and returns an error.
func (p *Pipeline) Normalize(raw marketdata.RawMessage) (marketdata.Quote, error) {
	// 1. Map (Parse & Field Mapping)
	quote, err := p.mapper.Map(raw)
	if err != nil {
		if p.limiter.Allow() {
			p.logger.Underlying().Warn().Err(err).Str("provider", raw.Provider).Msg("Mapping failed")
		}
		return marketdata.Quote{}, fmt.Errorf("mapping error: %w", err)
	}

	// 2. Symbol Normalization (Canonical form e.g. BTC/USD -> BTC-USD)
	// Fast path to avoid allocations
	needsUpper := false
	needsReplace := false
	for i := 0; i < len(quote.Symbol); i++ {
		if quote.Symbol[i] >= 'a' && quote.Symbol[i] <= 'z' {
			needsUpper = true
		}
		if quote.Symbol[i] == '/' {
			needsReplace = true
		}
	}
	if needsUpper || needsReplace {
		quote.Symbol = strings.ReplaceAll(strings.ToUpper(quote.Symbol), "/", "-")
	}

	// 3. Validation (Structural & Business Rules)
	if p.validator != nil {
		if err := p.validator.Validate(&quote); err != nil {
			if p.limiter.Allow() {
				p.logger.Underlying().Warn().Err(err).Str("symbol", quote.Symbol).Str("provider", raw.Provider).Msg("Validation failed")
			}
			return marketdata.Quote{}, fmt.Errorf("validation error: %w", err)
		}
	}

	return quote, nil
}
