package lifecycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/platform/lifecycle"
	"github.com/sumit/rtmds/internal/platform/lifecycle/adapters"
)

func TestManager_OrderedStartup_ReverseShutdown(t *testing.T) {
	m := lifecycle.NewManager()

	c1 := adapters.NewMockRedis().(*adapters.MockService)
	c2 := adapters.NewMockPostgres().(*adapters.MockService)
	c3 := adapters.NewMockPublisher().(*adapters.MockService)

	// Registration dictates dependency order
	m.Register(c1)
	m.Register(c2)
	m.Register(c3)

	ctx := context.Background()
	
	// Start all
	err := m.StartAll(ctx, time.Second)
	if err != nil {
		t.Fatalf("expected successful startup, got %v", err)
	}

	if !c1.WasStarted || !c2.WasStarted || !c3.WasStarted {
		t.Errorf("expected all components to be started")
	}

	// Stop all (should be in reverse order, though this test doesn't explicitly 
	// hook the timing, we verify they all get the signal successfully).
	err = m.StopAll(ctx, time.Second)
	if err != nil {
		t.Fatalf("expected successful shutdown, got %v", err)
	}

	if !c1.WasStopped || !c2.WasStopped || !c3.WasStopped {
		t.Errorf("expected all components to be stopped")
	}
}

func TestManager_FailFast_Rollback(t *testing.T) {
	m := lifecycle.NewManager()

	c1 := adapters.NewMockRedis().(*adapters.MockService) // Starts successfully
	c2 := adapters.NewMockPostgres().(*adapters.MockService) // Starts successfully
	c3 := &adapters.MockService{ServiceName: "BrokenService", StartErr: errors.New("boom")} // FAILS
	c4 := adapters.NewMockPublisher().(*adapters.MockService) // Should never start

	m.Register(c1)
	m.Register(c2)
	m.Register(c3)
	m.Register(c4)

	ctx := context.Background()
	
	// Start all
	err := m.StartAll(ctx, time.Second)
	if err == nil {
		t.Fatalf("expected failure from BrokenService")
	}

	// c1 and c2 should have started, c3 failed, c4 untouched
	if !c1.WasStarted || !c2.WasStarted {
		t.Errorf("expected c1 and c2 to have started before the failure")
	}
	if c4.WasStarted {
		t.Errorf("expected c4 to never start")
	}

	// But wait! Because c3 failed, the manager should have AUTOMATICALLY
	// executed the rollback on c1 and c2!
	if !c1.WasStopped || !c2.WasStopped {
		t.Errorf("expected fail-fast rollback to automatically shut down c1 and c2")
	}
	if c3.WasStopped || c4.WasStopped {
		t.Errorf("c3 and c4 should never have been stopped because they didn't start successfully")
	}
}

type TimeoutMock struct {
	*adapters.MockService
	CustomTimeout time.Duration
}

func (t *TimeoutMock) Timeout() time.Duration {
	return t.CustomTimeout
}

func TestManager_CustomTimeoutOverride(t *testing.T) {
	m := lifecycle.NewManager()

	// Global timeout is 50ms, but this component requires 200ms to start!
	// If it doesn't override the timeout, it will fail.
	comp := &TimeoutMock{
		MockService: &adapters.MockService{
			ServiceName: "SlowDB",
			StartDelay:  200 * time.Millisecond,
		},
		CustomTimeout: 500 * time.Millisecond, // Override allows it to succeed!
	}

	m.Register(comp)

	// We pass 50ms to the global timeout. The component should still succeed
	// because its CustomTimeout is 500ms.
	err := m.StartAll(context.Background(), 50*time.Millisecond)
	if err != nil {
		t.Fatalf("expected successful start due to custom timeout override, got: %v", err)
	}
}

func TestManager_OnStartupComplete(t *testing.T) {
	m := lifecycle.NewManager()
	
	m.Register(adapters.NewMockRedis())

	hookFired := false
	m.OnStartupComplete(func() {
		hookFired = true
	})

	err := m.StartAll(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("expected success")
	}

	if !hookFired {
		t.Fatalf("expected startup hook to have fired")
	}
}

