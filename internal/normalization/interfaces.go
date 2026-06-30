package normalization

import (
	"github.com/sumit/rtmds/internal/marketdata"
)

// Mapper is responsible for transforming an exchange-specific raw payload
// into a canonical marketdata.Quote.
type Mapper interface {
	// Map converts the provider-specific payload into a Quote.
	// Returns an error if the payload is malformed or unrecognized.
	Map(raw marketdata.RawMessage) (marketdata.Quote, error)
}

// Validator applies business and structural rules to a normalized Quote
// to ensure it is safe for downstream distribution.
type Validator interface {
	// Validate checks if a quote meets the platform's requirements.
	// Returns an error explaining why the quote is invalid.
	Validate(q *marketdata.Quote) error
}
