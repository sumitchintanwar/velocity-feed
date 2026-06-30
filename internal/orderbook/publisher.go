package orderbook

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Publisher defines the contract for emitting L2 events to the downstream gateways.
type Publisher interface {
	PublishIncrement(inc OrderBookIncrement) error
}

// MockPublisher is an in-memory publisher for testing and benchmarking.
type MockPublisher struct {
	mu        sync.Mutex
	Published []OrderBookIncrement
}

func (m *MockPublisher) PublishIncrement(inc OrderBookIncrement) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Published = append(m.Published, inc)
	return nil
}

func (m *MockPublisher) GetPublishedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Published)
}

// RedisPublisher is a placeholder for actual Redis integration.
// It wraps a redis client (e.g. go-redis) and publishes to pub/sub channels.
type RedisPublisher struct {
	// client *redis.Client
}

// NewRedisPublisher constructs a publisher.
func NewRedisPublisher() *RedisPublisher {
	return &RedisPublisher{}
}

func (r *RedisPublisher) PublishIncrement(inc OrderBookIncrement) error {
	payload, err := json.Marshal(inc)
	if err != nil {
		return fmt.Errorf("failed to serialize increment: %w", err)
	}

	// Topic string logic
	topic := "rtmds:l2:" + inc.Symbol

	// Mocking redis publish for now:
	// return r.client.Publish(context.Background(), topic, payload).Err()

	// Simulate success
	_ = payload
	_ = topic
	_ = context.Background()

	return nil
}
