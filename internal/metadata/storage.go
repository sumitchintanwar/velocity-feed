package metadata

import (
	"context"
	"fmt"
	"sync"
)

// Repository defines the interface for persistent metadata storage.
type Repository interface {
	// GetAll retrieves all active instruments.
	GetAll(ctx context.Context) ([]*Instrument, error)
	// Save upserts an instrument into the repository.
	Save(ctx context.Context, instrument *Instrument) error
}

// InMemoryRepository is a thread-safe mock database for local dev and testing.
type InMemoryRepository struct {
	mu   sync.RWMutex
	data map[string]*Instrument
}

// NewInMemoryRepository creates a new InMemoryRepository populated with optional seeds.
func NewInMemoryRepository(seeds []*Instrument) *InMemoryRepository {
	repo := &InMemoryRepository{
		data: make(map[string]*Instrument),
	}
	for _, seed := range seeds {
		repo.data[seed.CanonicalSymbol] = seed
	}
	return repo
}

// GetAll returns all stored instruments.
func (r *InMemoryRepository) GetAll(ctx context.Context) ([]*Instrument, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*Instrument, 0, len(r.data))
	for _, inst := range r.data {
		result = append(result, inst)
	}
	return result, nil
}

// Save stores an instrument by its canonical symbol.
func (r *InMemoryRepository) Save(ctx context.Context, instrument *Instrument) error {
	if instrument == nil || instrument.CanonicalSymbol == "" {
		return fmt.Errorf("invalid instrument data")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[instrument.CanonicalSymbol] = instrument
	return nil
}
