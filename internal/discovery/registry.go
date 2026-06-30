// Package discovery implements Redis-based service discovery for gateway
// instances. Each gateway registers itself with a TTL key, and periodically
// refreshes the TTL (heartbeat) to signal liveness.
//
// Architecture:
//
//	Gateway Start → Register(key + TTL) → Heartbeat goroutine
//	Gateway Stop  → Deregister(delete key)
//	Gateway Crash → TTL expires → removed automatically
//
// Redis data model (per-gateway keys, not a hash):
//
//	Key:    rtmds:gateways:{id}  (string, JSON value)
//	Set:    rtmds:gateways:active (SET of known gateway IDs)
//	TTL:    30s per key
//
// The active set enables O(N) listing where N = gateway count,
// avoiding SCAN over the entire keyspace.
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sumit/rtmds/internal/log"
)

const (
	// keyPrefix is the Redis key prefix for gateway registrations.
	keyPrefix = "rtmds:gateways:"

	// activeSetKey is the Redis SET tracking all active gateway IDs.
	// SADD on register, SREM on deregister, SMEMBERS on list.
	activeSetKey = "rtmds:gateways:active"

	// defaultTTL is the default time-to-live for a gateway registration.
	// If a gateway fails to heartbeat within this window, its entry is
	// automatically removed by Redis.
	defaultTTL = 30 * time.Second

	// defaultHeartbeatInterval is how often the gateway refreshes its TTL.
	// Must be significantly less than TTL to account for network jitter.
	defaultHeartbeatInterval = 10 * time.Second
)

// GatewayInfo holds metadata about a registered gateway instance.
type GatewayInfo struct {
	ID            string    `json:"id"`
	Addr          string    `json:"addr"`
	Port          int       `json:"port"`
	Status        string    `json:"status"` // "healthy", "degraded"
	StartedAt     time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// Registry provides gateway service discovery operations backed by Redis.
type Registry struct {
	client  *redis.Client
	log     *log.Logger
	ttl     time.Duration
	hbEvery time.Duration

	// cancel stops the heartbeat goroutine on deregistration.
	cancel   context.CancelFunc
	cancelMu sync.Mutex

	// isHealthy is checked by heartbeatLoop before writing to Redis.
	// If nil, assumes healthy. This prevents zombie gateways from
	// holding registrations when the local process is unhealthy.
	isHealthy func() bool
}

// RegistryOption configures the Registry.
type RegistryOption func(*Registry)

// WithTTL overrides the default registration TTL (30s).
func WithTTL(ttl time.Duration) RegistryOption {
	return func(r *Registry) { r.ttl = ttl }
}

// WithHeartbeatInterval overrides the default heartbeat interval (10s).
func WithHeartbeatInterval(d time.Duration) RegistryOption {
	return func(r *Registry) { r.hbEvery = d }
}

// WithHealthCheck sets a local health check function. heartbeatLoop
// calls this before each Redis write — if it returns false, the heartbeat
// is skipped, allowing the TTL to expire and the gateway to de-register
// itself (preventing silent zombies).
func WithHealthCheck(fn func() bool) RegistryOption {
	return func(r *Registry) { r.isHealthy = fn }
}

// NewRegistry creates a Redis-backed service registry.
func NewRegistry(client *redis.Client, l *log.Logger, opts ...RegistryOption) *Registry {
	r := &Registry{
		client:  client,
		log:     l,
		ttl:     defaultTTL,
		hbEvery: defaultHeartbeatInterval,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// keyFor returns the Redis key for a given gateway ID.
func keyFor(id string) string {
	return keyPrefix + id
}

// Register adds a gateway to the registry with a TTL and tracks it
// in the active set. Call StartHeartbeat to begin the background TTL refresh.
func (r *Registry) Register(ctx context.Context, info GatewayInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("registry: marshal gateway info: %w", err)
	}

	// Use a pipeline: SET the key + SADD to the active set atomically.
	pipe := r.client.Pipeline()
	pipe.Set(ctx, keyFor(info.ID), data, r.ttl)
	pipe.SAdd(ctx, activeSetKey, info.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("registry: register %s: %w", info.ID, err)
	}

	r.log.Underlying().Info().
		Str("id", info.ID).
		Str("addr", info.Addr).
		Int("port", info.Port).
		Str("event", "gateway_registered").
		Msg("gateway registered")

	return nil
}

// StartHeartbeat begins the background goroutine that periodically refreshes
// the gateway's TTL. Must be called after Register. Uses the provided context
// for its lifetime (typically the run context, not the startup context).
func (r *Registry) StartHeartbeat(ctx context.Context, info GatewayInfo) {
	r.cancelMu.Lock()
	// Cancel any previous heartbeat.
	if r.cancel != nil {
		r.cancel()
	}
	hbCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.cancelMu.Unlock()

	go r.heartbeatLoop(hbCtx, info)
}

// StopHeartbeat stops the background heartbeat goroutine.
func (r *Registry) StopHeartbeat() {
	r.cancelMu.Lock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.cancelMu.Unlock()
}

// Deregister removes a gateway from the registry and stops its heartbeat.
func (r *Registry) Deregister(ctx context.Context, gatewayID string) error {
	r.cancelMu.Lock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.cancelMu.Unlock()

	// Pipeline: DEL the key + SREM from the active set.
	pipe := r.client.Pipeline()
	pipe.Del(ctx, keyFor(gatewayID))
	pipe.SRem(ctx, activeSetKey, gatewayID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("registry: deregister %s: %w", gatewayID, err)
	}

	r.log.Underlying().Info().Str("id", gatewayID).Str("event", "gateway_deregistered").Msg("gateway deregistered")
	return nil
}

// List returns all currently registered gateways whose TTL has not expired.
// Uses the active set (SMEMBERS) instead of SCAN, giving O(N) performance
// where N = number of gateways, not total Redis keys.
func (r *Registry) List(ctx context.Context) ([]GatewayInfo, error) {
	ids, err := r.client.SMembers(ctx, activeSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("registry: list smembers: %w", err)
	}

	if len(ids) == 0 {
		return []GatewayInfo{}, nil
	}

	// Batch-fetch all values via pipeline MGET.
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = keyFor(id)
	}

	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("registry: list mget: %w", err)
	}

	gateways := make([]GatewayInfo, 0, len(vals))
	for i, val := range vals {
		if val == nil {
			// Key expired between SMEMBERS and MGET — remove stale ID
			// from the active set to prevent repeated lookups.
			r.client.SRem(ctx, activeSetKey, ids[i])
			continue
		}
		raw, ok := val.(string)
		if !ok {
			continue
		}
		var info GatewayInfo
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			r.log.Underlying().Warn().Err(err).Str("event", "registry_malformed_entry").Msg("registry: skipping malformed entry")
			continue
		}
		gateways = append(gateways, info)
	}

	return gateways, nil
}

// Count returns the number of active gateways using SCARD on the active set.
func (r *Registry) Count(ctx context.Context) (int, error) {
	n, err := r.client.SCard(ctx, activeSetKey).Result()
	if err != nil {
		return 0, fmt.Errorf("registry: count scard: %w", err)
	}
	return int(n), nil
}

// Get returns info for a specific gateway by ID, or nil if not found/expired.
func (r *Registry) Get(ctx context.Context, gatewayID string) (*GatewayInfo, error) {
	data, err := r.client.Get(ctx, keyFor(gatewayID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("registry: get %s: %w", gatewayID, err)
	}

	var info GatewayInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, fmt.Errorf("registry: get %s unmarshal: %w", gatewayID, err)
	}

	return &info, nil
}

// heartbeatLoop periodically refreshes the gateway's TTL in the registry.
// If a local health check is configured and it returns false, the heartbeat
// is skipped — allowing the TTL to expire and the gateway to de-register.
func (r *Registry) heartbeatLoop(ctx context.Context, info GatewayInfo) {
	ticker := time.NewTicker(r.hbEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Deep health check: skip heartbeat if local gateway is unhealthy.
			if r.isHealthy != nil && !r.isHealthy() {
			r.log.Underlying().Warn().Str("id", info.ID).
				Str("event", "heartbeat_skipped").
				Msg("heartbeat: skipping — local health check failed")
				continue
			}

			info.LastHeartbeat = time.Now()
			data, err := json.Marshal(info)
			if err != nil {
				r.log.Underlying().Warn().Err(err).Str("event", "heartbeat_marshal_failed").Msg("heartbeat: marshal failed")
				continue
			}
			if err := r.client.Set(ctx, keyFor(info.ID), data, r.ttl).Err(); err != nil {
				r.log.Underlying().Warn().Err(err).Str("id", info.ID).Str("event", "heartbeat_refresh_failed").Msg("heartbeat: refresh failed")
			}
		}
	}
}

// TTL returns the configured registration TTL.
func (r *Registry) TTL() time.Duration {
	return r.ttl
}

// HeartbeatInterval returns the configured heartbeat interval.
func (r *Registry) HeartbeatInterval() time.Duration {
	return r.hbEvery
}
