package storage

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/models"
)

// InMemoryStore provides a fast, thread-safe implementation of EventStore for testing.
type InMemoryStore struct {
	mu     sync.RWMutex
	events map[string][]models.StoredEvent // Partitioned by symbol for fast lookups
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		events: make(map[string][]models.StoredEvent),
	}
}

func (s *InMemoryStore) WriteBatch(ctx context.Context, batch []models.StoredEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ev := range batch {
		s.events[ev.Symbol] = append(s.events[ev.Symbol], ev)
	}

	// Re-sort partitions by Timestamp and SequenceNumber
	for sym := range s.events {
		sort.SliceStable(s.events[sym], func(i, j int) bool {
			if s.events[sym][i].Timestamp.Equal(s.events[sym][j].Timestamp) {
				return s.events[sym][i].SequenceNumber < s.events[sym][j].SequenceNumber
			}
			return s.events[sym][i].Timestamp.Before(s.events[sym][j].Timestamp)
		})
	}

	return nil
}

func (s *InMemoryStore) ReadStream(ctx context.Context, symbol string, start, end time.Time, chunkSize int) (EventIterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partition, ok := s.events[symbol]
	if !ok {
		return &memoryIterator{events: nil}, nil
	}

	var results []models.StoredEvent
	for _, ev := range partition {
		if (ev.Timestamp.Equal(start) || ev.Timestamp.After(start)) && 
		   (ev.Timestamp.Equal(end) || ev.Timestamp.Before(end)) {
			results = append(results, ev)
		}
	}

	return &memoryIterator{
		events:    results, // We copy the references for the iterator
		chunkSize: chunkSize,
		cursor:    0,
	}, nil
}

type memoryIterator struct {
	mu        sync.Mutex
	events    []models.StoredEvent
	chunkSize int
	cursor    int
}

func (m *memoryIterator) Next() ([]models.StoredEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cursor >= len(m.events) {
		return nil, nil // EOF
	}

	end := m.cursor + m.chunkSize
	if end > len(m.events) {
		end = len(m.events)
	}

	batch := m.events[m.cursor:end]
	m.cursor = end
	return batch, nil
}

func (m *memoryIterator) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = nil
	return nil
}

