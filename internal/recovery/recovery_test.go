package recovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecoveryManager_StateTransitions(t *testing.T) {
	rm := New()

	if rm.State() != StateInit {
		t.Fatalf("expected initial state, got %v", rm.State())
	}

	if rm.IsReady() {
		t.Fatal("should not be ready initially")
	}

	rm.SetState(context.Background(),StateLoading, "test")
	if rm.State() != StateLoading {
		t.Fatalf("expected loading state, got %v", rm.State())
	}

	rm.SetState(context.Background(),StateReady, "done")
	if !rm.IsReady() {
		t.Fatal("should be ready after StateReady")
	}
}

func TestRecoveryManager_Dependencies(t *testing.T) {
	rm := New()

	rm.RegisterDependency("redis")
	rm.RegisterDependency("database")

	if rm.AllDependenciesReady() {
		t.Fatal("should not be ready with unregistered deps")
	}

	rm.DependencyReady(context.Background(),"redis")
	if rm.AllDependenciesReady() {
		t.Fatal("should not be ready with only one dep")
	}

	rm.DependencyReady(context.Background(),"database")
	if !rm.AllDependenciesReady() {
		t.Fatal("should be ready when all deps are ready")
	}
}

func TestRecoveryManager_WaitForDependencies(t *testing.T) {
	rm := New()
	rm.RegisterDependency("redis")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- rm.WaitForDependencies(ctx)
	}()

	// Should not be ready yet.
	select {
	case err := <-done:
		t.Fatalf("should still be waiting: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Mark ready.
	rm.DependencyReady(context.Background(),"redis")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for dependencies")
	}
}

func TestRecoveryManager_WaitForDependencies_ContextCancelled(t *testing.T) {
	rm := New()
	rm.RegisterDependency("redis")

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- rm.WaitForDependencies(ctx)
	}()

	// Cancel before marking ready.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out")
	}
}

func TestRecoveryManager_Recover(t *testing.T) {
	rm := New()
	rm.RegisterDependency("redis")
	rm.DependencyReady(context.Background(),"redis")

	var recovered atomic.Bool
	err := rm.Recover(context.Background(), func(ctx context.Context) error {
		recovered.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !recovered.Load() {
		t.Fatal("recovery function was not called")
	}
	if !rm.IsReady() {
		t.Fatal("should be ready after recovery")
	}
}

func TestRecoveryManager_Recover_FunctionError(t *testing.T) {
	rm := New()
	rm.RegisterDependency("redis")
	rm.DependencyReady(context.Background(),"redis")

	err := rm.Recover(context.Background(), func(ctx context.Context) error {
		return errors.New("db unavailable")
	})
	if err == nil {
		t.Fatal("expected error from recovery function")
	}
	if rm.State() != StateFailed {
		t.Fatalf("expected failed state, got %v", rm.State())
	}
}

func TestRecoveryManager_StatePersistence(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "recovery.json")

	rm := New(WithStatePath(statePath))
	rm.RegisterDependency("redis")
	rm.DependencyReady(context.Background(),"redis")

	// Run recovery to trigger state persistence.
	err := rm.Recover(context.Background(), func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Read the persisted file.
	data, readErr := os.ReadFile(statePath)
	if readErr != nil {
		t.Fatalf("failed to read state: %v", readErr)
	}

	if len(data) == 0 {
		t.Fatal("state file is empty")
	}

	// Create a new manager and load state.
	rm2 := New(WithStatePath(statePath))
	rm2.loadState(context.Background())

	if rm2.recoveryCnt != 1 {
		t.Fatalf("expected recovery count 1, got %d", rm2.recoveryCnt)
	}
}

func TestRecoveryManager_Report(t *testing.T) {
	rm := New()
	rm.RegisterDependency("redis")
	rm.DependencyReady(context.Background(),"redis")

	rm.SetState(context.Background(),StateReady, "test")

	report := rm.Report()
	if report.State != "ready" {
		t.Fatalf("expected ready state, got %v", report.State)
	}
	if report.RecoveryCnt != 0 {
		t.Fatalf("expected recovery count 0, got %d", report.RecoveryCnt)
	}
	if !report.Dependencies["redis"] {
		t.Fatal("expected redis dependency to be ready")
	}
}

func TestRecoveryManager_HealthCallback(t *testing.T) {
	var lastState atomic.Value
	rm := New(WithHealthCallback(func(state State, reason string) {
		lastState.Store(state)
	}))

	rm.SetState(context.Background(), StateLoading, "test")

	if lastState.Load() != StateLoading {
		t.Fatal("health callback was not called")
	}
}

func TestRecoveryManager_FullSequence(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "recovery.json")

	rm := New(
		WithStatePath(statePath),
	)

	// Register dependencies.
	rm.RegisterDependency("redis")
	rm.RegisterDependency("database")

	// Track state transitions.
	var transitions []State
	var mu sync.Mutex
	rm.healthCallback = func(state State, reason string) {
		mu.Lock()
		transitions = append(transitions, state)
		mu.Unlock()
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		rm.DependencyReady(context.Background(), "redis")
		rm.DependencyReady(context.Background(), "database")
	}()

	// Run recovery.
	err := rm.Recover(context.Background(), func(ctx context.Context) error {
		// Simulate snapshot recovery.
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if !rm.IsReady() {
		t.Fatal("system should be ready after recovery")
	}

	mu.Lock()
	if len(transitions) < 4 {
		t.Fatalf("expected at least 4 transitions, got %d", len(transitions))
	}

	// Verify order: loading -> connecting -> recovering -> building -> ready
	expected := []State{StateLoading, StateConnecting, StateRecovering, StateBuilding, StateReady}
	for i, s := range expected {
		if i < len(transitions) && transitions[i] != s {
			t.Errorf("transition %d: expected %v, got %v", i, s, transitions[i])
		}
	}
	mu.Unlock()
}

func TestState_String(t *testing.T) {
	tests := []struct {
		state    State
		expected string
	}{
		{StateInit, "init"},
		{StateLoading, "loading"},
		{StateConnecting, "connecting"},
		{StateRecovering, "recovering"},
		{StateBuilding, "building"},
		{StateReady, "ready"},
		{StateFailed, "failed"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("State(%d).String() = %q, want %q", tt.state, got, tt.expected)
		}
	}
}
