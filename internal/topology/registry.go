package topology

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrGatewayNotFound      = errors.New("gateway not found")
	ErrGatewayAlreadyExists = errors.New("gateway already exists")
)

// GatewayState represents the lifecycle phase of a physical gateway.
type GatewayState string

const (
	StateJoining GatewayState = "JOINING" // Loading WAL/Snapshots, not ready for traffic
	StateOnline  GatewayState = "ONLINE"  // Actively serving WebSocket traffic
	StateLeaving GatewayState = "LEAVING" // Draining connections, handing off partitions
	StateOffline GatewayState = "OFFLINE" // Dead/Shutdown
)

// GatewayMetadata holds information about a physical gateway node.
type GatewayMetadata struct {
	ID        string
	Address   string
	State     GatewayState
	UpdatedAt time.Time
}

// Registry defines the interface for the Control Plane topology store.
type Registry interface {
	Register(ctx context.Context, meta GatewayMetadata) error
	Remove(ctx context.Context, id string) error
	Heartbeat(ctx context.Context, id string) error
	UpdateState(ctx context.Context, id string, state GatewayState) error
	Get(ctx context.Context, id string) (GatewayMetadata, error)
	List(ctx context.Context) ([]GatewayMetadata, error)
}

// MemoryRegistry is an in-memory, thread-safe implementation of the Registry.
// It is useful for testing or single-binary deployments without Redis.
type MemoryRegistry struct {
	mu       sync.RWMutex
	gateways map[string]GatewayMetadata
	ttl      time.Duration
}

// NewMemoryRegistry creates a new in-memory registry.
// Gateways that miss their heartbeat beyond the TTL are considered OFFLINE.
func NewMemoryRegistry(ttl time.Duration) *MemoryRegistry {
	return &MemoryRegistry{
		gateways: make(map[string]GatewayMetadata),
		ttl:      ttl,
	}
}

func (r *MemoryRegistry) Register(ctx context.Context, meta GatewayMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.gateways[meta.ID]; exists {
		return ErrGatewayAlreadyExists
	}

	meta.UpdatedAt = time.Now()
	r.gateways[meta.ID] = meta
	return nil
}

func (r *MemoryRegistry) Remove(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.gateways[id]; !exists {
		return ErrGatewayNotFound
	}
	delete(r.gateways, id)
	return nil
}

func (r *MemoryRegistry) Heartbeat(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	meta, exists := r.gateways[id]
	if !exists {
		return ErrGatewayNotFound
	}

	meta.UpdatedAt = time.Now()
	r.gateways[id] = meta
	return nil
}

func (r *MemoryRegistry) UpdateState(ctx context.Context, id string, state GatewayState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	meta, exists := r.gateways[id]
	if !exists {
		return ErrGatewayNotFound
	}

	meta.State = state
	meta.UpdatedAt = time.Now()
	r.gateways[id] = meta
	return nil
}

func (r *MemoryRegistry) Get(ctx context.Context, id string) (GatewayMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	meta, exists := r.gateways[id]
	if !exists {
		return GatewayMetadata{}, ErrGatewayNotFound
	}

	// Check TTL
	if time.Since(meta.UpdatedAt) > r.ttl {
		meta.State = StateOffline
	}

	return meta, nil
}

func (r *MemoryRegistry) List(ctx context.Context) ([]GatewayMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []GatewayMetadata
	for _, meta := range r.gateways {
		// Update state to offline if TTL expired before returning
		if time.Since(meta.UpdatedAt) > r.ttl {
			meta.State = StateOffline
		}
		list = append(list, meta)
	}
	return list, nil
}
