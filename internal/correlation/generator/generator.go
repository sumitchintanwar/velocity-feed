// Package generator provides interfaces and implementations for Correlation ID generation.
package generator

import (
	"github.com/google/uuid"
)

// Generator defines the interface for generating correlation identifiers.
type Generator interface {
	// Generate returns a new, globally unique identifier.
	Generate() string
}

// UUIDv7Generator implements Generator using time-ordered UUIDv7.
type UUIDv7Generator struct{}

// NewUUIDv7Generator creates a new UUIDv7Generator.
func NewUUIDv7Generator() Generator {
	return &UUIDv7Generator{}
}

// Generate creates a new UUIDv7 string. It is safe for concurrent use.
func (g *UUIDv7Generator) Generate() string {
	// UUIDv7 is time-ordered and random, making it excellent for database locality
	id, err := uuid.NewV7()
	if err != nil {
		// Fallback to UUIDv4 if clock sequence fails (extremely rare)
		return uuid.NewString()
	}
	return id.String()
}
