package metadata

import (
	"context"
	"fmt"

	"github.com/sumit/rtmds/internal/log"
)

// Service orchestrates metadata access, wrapping the cache and repository.
type Service struct {
	repo   Repository
	cache  *InstrumentCache
	logger *log.Logger
}

// NewService creates a new metadata service.
func NewService(repo Repository, logger *log.Logger) *Service {
	return &Service{
		repo:   repo,
		cache:  NewInstrumentCache(),
		logger: logger,
	}
}

// LoadCache strictly loads all metadata from the repository into the fast memory cache.
func (s *Service) LoadCache(ctx context.Context) error {
	instruments, err := s.repo.GetAll(ctx)
	if err != nil {
		s.logger.Underlying().Error().Err(err).Msg("Failed to fetch instruments from repository")
		return fmt.Errorf("failed to load instruments: %w", err)
	}

	s.cache.Replace(instruments)
	s.logger.Underlying().Info().Int("count", len(instruments)).Msg("Metadata cache reloaded")
	return nil
}

// GetInstrument retrieves a canonical instrument by symbol from the cache.
func (s *Service) GetInstrument(symbol string) (*Instrument, error) {
	return s.cache.GetBySymbol(symbol)
}

// GetInstrumentsByExchange retrieves all instruments on an exchange.
func (s *Service) GetInstrumentsByExchange(exchange string) []*Instrument {
	return s.cache.GetByExchange(exchange)
}

// GetInstrumentsByAssetClass retrieves all instruments matching an asset class.
func (s *Service) GetInstrumentsByAssetClass(class AssetClass) []*Instrument {
	return s.cache.GetByAssetClass(class)
}
