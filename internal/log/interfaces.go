package log

import "context"

// ContextExtractor extracts scoped fields (like correlation_id) from context.
// Implement this interface to provide custom context propagation strategies.
type ContextExtractor interface {
	// ExtractCorrelationID returns the correlation ID from the context.
	// Returns empty string if no correlation ID is present.
	ExtractCorrelationID(ctx context.Context) string
}

// Sanitizer cleans or redacts sensitive data before serialization.
// Implement this interface to provide custom redaction policies
// (e.g., PCI DSS, HIPAA compliance).
type Sanitizer interface {
	// Sanitize cleans sensitive fields from the given string.
	Sanitize(s string) string
}

// defaultContextExtractor extracts correlation IDs via the standard context keys.
type defaultContextExtractor struct{}

// ExtractCorrelationID implements ContextExtractor.
func (d defaultContextExtractor) ExtractCorrelationID(ctx context.Context) string {
	return GetCorrelationID(ctx)
}

// DefaultContextExtractor returns the standard ContextExtractor that uses
// the correlation ID stored via SetCorrelationID.
func DefaultContextExtractor() ContextExtractor {
	return defaultContextExtractor{}
}

// defaultSanitizer implements Sanitizer using the standard redaction logic.
type defaultSanitizer struct{}

// Sanitize implements Sanitizer.
func (d defaultSanitizer) Sanitize(s string) string {
	return SanitizeString(s)
}

// DefaultSanitizer returns the standard Sanitizer that redacts known
// sensitive field patterns (passwords, tokens, API keys).
func DefaultSanitizer() Sanitizer {
	return defaultSanitizer{}
}
