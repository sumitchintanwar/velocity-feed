package normalization

import (
	"fmt"

	"github.com/sumit/rtmds/internal/marketdata"
)

// Rule defines a single validation check for a quote.
type Rule func(*marketdata.Quote) error

// RuleEngine evaluates a series of Rules.
type RuleEngine struct {
	rules []Rule
}

// NewRuleEngine creates a new RuleEngine with the given rules.
func NewRuleEngine(rules ...Rule) *RuleEngine {
	return &RuleEngine{rules: rules}
}

// NewDefaultValidator returns a standard pipeline validator with common rules.
func NewDefaultValidator() *RuleEngine {
	return NewRuleEngine(
		RequireSymbol,
		RequireQuoteType,
		RequirePositivePrice,
		RequireValidVolume,
		RequireTimestamp,
	)
}

// Validate executes all registered rules.
func (e *RuleEngine) Validate(q *marketdata.Quote) error {
	for _, rule := range e.rules {
		if err := rule(q); err != nil {
			return err
		}
	}
	return nil
}

// --- Standard Rules ---

func RequireSymbol(q *marketdata.Quote) error {
	if q.Symbol == "" {
		return fmt.Errorf("missing symbol")
	}
	return nil
}

func RequireQuoteType(q *marketdata.Quote) error {
	if q.Type == "" {
		return fmt.Errorf("missing quote type")
	}
	return nil
}

func RequirePositivePrice(q *marketdata.Quote) error {
	if q.Price <= 0 {
		return fmt.Errorf("invalid price: %f", q.Price)
	}
	return nil
}

func RequireValidVolume(q *marketdata.Quote) error {
	if q.Type == marketdata.QuoteTypeTrade && q.Volume <= 0 {
		return fmt.Errorf("invalid volume for trade: %d", q.Volume)
	}
	return nil
}

func RequireTimestamp(q *marketdata.Quote) error {
	if q.Timestamp.IsZero() {
		return fmt.Errorf("missing timestamp")
	}
	return nil
}
