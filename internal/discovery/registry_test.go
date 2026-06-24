package discovery

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// skipIfNoRedis skips the test if Redis is not reachable.
func skipIfNoRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := "localhost:6379"
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		addr = v
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("skipping: Redis not available at %s: %v", addr, err)
	}
	// Clean up any stale keys from previous runs before the test starts.
	cleanupRedis(t, client)
	t.Cleanup(func() {
		cleanupRedis(t, client)
		client.Close()
	})
	return client
}

func cleanupRedis(t *testing.T, client *redis.Client) {
	t.Helper()
	ctx := context.Background()
	keys, _ := client.Keys(ctx, keyPrefix+"*").Result()
	if len(keys) > 0 {
		client.Del(ctx, keys...)
	}
	client.Del(ctx, activeSetKey)
}

func newTestRegistry(t *testing.T, client *redis.Client) *Registry {
	t.Helper()
	log := zerolog.Nop()
	return NewRegistry(client, log,
		WithTTL(5*time.Second),
		WithHeartbeatInterval(1*time.Second),
	)
}

func TestRegistry_Register(t *testing.T) {
	client := skipIfNoRedis(t)
	r := newTestRegistry(t, client)
	ctx := context.Background()

	info := GatewayInfo{
		ID:            "gateway-test-1",
		Addr:          "10.0.0.1",
		Port:          9091,
		Status:        "healthy",
		StartedAt:     time.Now(),
		LastHeartbeat: time.Now(),
	}

	if err := r.Register(ctx, info); err != nil {
		t.Fatalf("Register: %v", err)
	}

	gateways, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(gateways) != 1 {
		t.Fatalf("expected 1 gateway, got %d", len(gateways))
	}
	if gateways[0].ID != "gateway-test-1" {
		t.Errorf("expected ID gateway-test-1, got %s", gateways[0].ID)
	}
	if gateways[0].Port != 9091 {
		t.Errorf("expected port 9091, got %d", gateways[0].Port)
	}
}

func TestRegistry_Deregister(t *testing.T) {
	client := skipIfNoRedis(t)
	r := newTestRegistry(t, client)
	ctx := context.Background()

	info := GatewayInfo{
		ID:            "gateway-test-2",
		Addr:          "10.0.0.2",
		Port:          9092,
		Status:        "healthy",
		StartedAt:     time.Now(),
		LastHeartbeat: time.Now(),
	}

	if err := r.Register(ctx, info); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := r.Deregister(ctx, "gateway-test-2"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	gateways, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(gateways) != 0 {
		t.Fatalf("expected 0 gateways after deregister, got %d", len(gateways))
	}
}

func TestRegistry_List_Multiple(t *testing.T) {
	client := skipIfNoRedis(t)
	r := newTestRegistry(t, client)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		info := GatewayInfo{
			ID:            "gateway-multi-" + string(rune('0'+i)),
			Addr:          "10.0.0.1",
			Port:          9090 + i,
			Status:        "healthy",
			StartedAt:     time.Now(),
			LastHeartbeat: time.Now(),
		}
		if err := r.Register(ctx, info); err != nil {
			t.Fatalf("Register %d: %v", i, err)
		}
	}

	gateways, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(gateways) != 3 {
		t.Fatalf("expected 3 gateways, got %d", len(gateways))
	}
}

func TestRegistry_Get(t *testing.T) {
	client := skipIfNoRedis(t)
	r := newTestRegistry(t, client)
	ctx := context.Background()

	info := GatewayInfo{
		ID:            "gateway-get-1",
		Addr:          "10.0.0.5",
		Port:          9095,
		Status:        "healthy",
		StartedAt:     time.Now(),
		LastHeartbeat: time.Now(),
	}

	if err := r.Register(ctx, info); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := r.Get(ctx, "gateway-get-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected gateway, got nil")
	}
	if got.ID != "gateway-get-1" {
		t.Errorf("expected ID gateway-get-1, got %s", got.ID)
	}
	if got.Port != 9095 {
		t.Errorf("expected port 9095, got %d", got.Port)
	}

	missing, err := r.Get(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Get nonexistent: %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for nonexistent gateway, got %+v", missing)
	}
}

func TestRegistry_Count(t *testing.T) {
	client := skipIfNoRedis(t)
	r := newTestRegistry(t, client)
	ctx := context.Background()

	count, err := r.Count(ctx)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}

	for i := 1; i <= 5; i++ {
		info := GatewayInfo{
			ID:            "gateway-count-" + string(rune('0'+i)),
			Addr:          "10.0.0.1",
			Port:          9100 + i,
			Status:        "healthy",
			StartedAt:     time.Now(),
			LastHeartbeat: time.Now(),
		}
		if err := r.Register(ctx, info); err != nil {
			t.Fatalf("Register %d: %v", i, err)
		}
	}

	count, err = r.Count(ctx)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5, got %d", count)
	}
}

func TestRegistry_StaleFiltering(t *testing.T) {
	client := skipIfNoRedis(t)
	r := NewRegistry(client, zerolog.Nop(),
		WithTTL(2*time.Second),
		WithHeartbeatInterval(1*time.Second),
	)
	ctx := context.Background()

	info := GatewayInfo{
		ID:            "gateway-stale-1",
		Addr:          "10.0.0.1",
		Port:          9091,
		Status:        "healthy",
		StartedAt:     time.Now(),
		LastHeartbeat: time.Now(),
	}
	if err := r.Register(ctx, info); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Should be visible immediately.
	gateways, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(gateways) != 1 {
		t.Fatalf("expected 1 gateway, got %d", len(gateways))
	}

	// Deregister (removes from key + set).
	r.Deregister(ctx, "gateway-stale-1")

	// Insert stale entry directly with short TTL (bypasses Register
	// so it's NOT in the active set — simulating a zombie that was
	// added before the set-based approach).
	staleInfo := GatewayInfo{
		ID:            "gateway-stale-1",
		Addr:          "10.0.0.1",
		Port:          9091,
		Status:        "healthy",
		StartedAt:     time.Now().Add(-1 * time.Minute),
		LastHeartbeat: time.Now().Add(-10 * time.Second),
	}
	data, _ := json.Marshal(staleInfo)
	client.Set(ctx, keyFor("gateway-stale-1"), data, 1*time.Second)

	// Should NOT be visible — not in the active set.
	gateways, err = r.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(gateways) != 0 {
		t.Fatalf("expected 0 gateways (stale not in set), got %d", len(gateways))
	}
}

func TestRegistry_TTLConfig(t *testing.T) {
	client := skipIfNoRedis(t)
	r := NewRegistry(client, zerolog.Nop(),
		WithTTL(45*time.Second),
		WithHeartbeatInterval(15*time.Second),
	)

	if r.TTL() != 45*time.Second {
		t.Errorf("expected TTL 45s, got %v", r.TTL())
	}
	if r.HeartbeatInterval() != 15*time.Second {
		t.Errorf("expected heartbeat 15s, got %v", r.HeartbeatInterval())
	}
}

func TestRegistry_IndependentTTLs(t *testing.T) {
	client := skipIfNoRedis(t)
	r := NewRegistry(client, zerolog.Nop(),
		WithTTL(5*time.Second),
		WithHeartbeatInterval(1*time.Second),
	)
	ctx := context.Background()

	// Register two gateways — gateway-b gets a shorter TTL (simulating stale entry).
	info1 := GatewayInfo{
		ID:            "gateway-a",
		Addr:          "10.0.0.1",
		Port:          9091,
		Status:        "healthy",
		StartedAt:     time.Now(),
		LastHeartbeat: time.Now(),
	}
	info2 := GatewayInfo{
		ID:            "gateway-b",
		Addr:          "10.0.0.2",
		Port:          9092,
		Status:        "healthy",
		StartedAt:     time.Now(),
		LastHeartbeat: time.Now(),
	}

	r.Register(ctx, info1)
	// gateway-b gets a shorter TTL and is added to the set.
	data, _ := json.Marshal(info2)
	client.Set(ctx, keyFor("gateway-b"), data, 1*time.Second)
	client.SAdd(ctx, activeSetKey, "gateway-b")

	// Both visible initially.
	gateways, _ := r.List(ctx)
	if len(gateways) != 2 {
		t.Fatalf("expected 2 gateways, got %d", len(gateways))
	}

	// Wait for gateway-b's TTL to expire.
	time.Sleep(1500 * time.Millisecond)

	gateways, _ = r.List(ctx)
	if len(gateways) != 1 {
		t.Fatalf("expected 1 gateway after TTL, got %d", len(gateways))
	}
	if gateways[0].ID != "gateway-a" {
		t.Errorf("expected gateway-a, got %s", gateways[0].ID)
	}
}

func TestRegistry_HealthCheck(t *testing.T) {
	client := skipIfNoRedis(t)

	healthy := true
	r := NewRegistry(client, zerolog.Nop(),
		WithTTL(3*time.Second),
		WithHeartbeatInterval(500*time.Millisecond),
		WithHealthCheck(func() bool { return healthy }),
	)
	ctx := context.Background()

	info := GatewayInfo{
		ID:            "gateway-hc-1",
		Addr:          "10.0.0.1",
		Port:          9091,
		Status:        "healthy",
		StartedAt:     time.Now(),
		LastHeartbeat: time.Now(),
	}
	if err := r.Register(ctx, info); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Heartbeat should update timestamps while healthy.
	r.StartHeartbeat(ctx, info)
	time.Sleep(1200 * time.Millisecond)

	got, _ := r.Get(ctx, "gateway-hc-1")
	if got == nil {
		t.Fatal("expected gateway to still exist while healthy")
	}

	// Mark unhealthy — heartbeat should stop refreshing.
	healthy = false
	time.Sleep(2 * time.Second)

	// Key should still exist (TTL hasn't expired yet).
	got, _ = r.Get(ctx, "gateway-hc-1")
	if got == nil {
		t.Fatal("key should still exist within TTL window")
	}

	// Wait for TTL to expire — no heartbeat was sent while unhealthy.
	time.Sleep(1500 * time.Millisecond)

	gateways, _ := r.List(ctx)
	for _, g := range gateways {
		if g.ID == "gateway-hc-1" {
			t.Fatal("gateway should have been removed after unhealthy period")
		}
	}
}
