package topology

import (
	"context"
	"testing"
	"time"
)

func TestMemoryRegistry_Lifecycle(t *testing.T) {
	ctx := context.Background()
	reg := NewMemoryRegistry(5 * time.Second)

	meta := GatewayMetadata{
		ID:      "gateway-1",
		Address: "10.0.0.1:8080",
		State:   StateJoining,
	}

	// 1. Register
	err := reg.Register(ctx, meta)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	// Registering again should fail
	err = reg.Register(ctx, meta)
	if err != ErrGatewayAlreadyExists {
		t.Fatalf("Expected ErrGatewayAlreadyExists, got %v", err)
	}

	// 2. State Update
	err = reg.UpdateState(ctx, "gateway-1", StateOnline)
	if err != nil {
		t.Fatalf("Failed to update state: %v", err)
	}

	fetched, _ := reg.Get(ctx, "gateway-1")
	if fetched.State != StateOnline {
		t.Fatalf("Expected state ONLINE, got %s", fetched.State)
	}

	// 3. Heartbeat
	time.Sleep(10 * time.Millisecond)
	oldUpdate := fetched.UpdatedAt

	err = reg.Heartbeat(ctx, "gateway-1")
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	fetched, _ = reg.Get(ctx, "gateway-1")
	if !fetched.UpdatedAt.After(oldUpdate) {
		t.Fatalf("Heartbeat did not update timestamp")
	}

	// 4. Remove
	err = reg.Remove(ctx, "gateway-1")
	if err != nil {
		t.Fatalf("Failed to remove: %v", err)
	}

	_, err = reg.Get(ctx, "gateway-1")
	if err != ErrGatewayNotFound {
		t.Fatalf("Expected ErrGatewayNotFound after removal, got %v", err)
	}
}

func TestMemoryRegistry_TTL(t *testing.T) {
	ctx := context.Background()
	// Create a registry with a tiny 50ms TTL
	reg := NewMemoryRegistry(50 * time.Millisecond)

	reg.Register(ctx, GatewayMetadata{
		ID:    "gateway-2",
		State: StateOnline,
	})

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	fetched, _ := reg.Get(ctx, "gateway-2")
	if fetched.State != StateOffline {
		t.Fatalf("Expected gateway to be marked OFFLINE due to TTL expiration, got %s", fetched.State)
	}

	// List should also reflect offline
	list, _ := reg.List(ctx)
	if list[0].State != StateOffline {
		t.Fatalf("Expected list to reflect OFFLINE state, got %s", list[0].State)
	}
}
