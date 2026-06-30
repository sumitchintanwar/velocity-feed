package lifecycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/platform/lifecycle"
	"github.com/sumit/rtmds/internal/platform/lifecycle/adapters"
)

func TestComponentGroup_ConcurrentStartup(t *testing.T) {
	// We create 5 components that each take 100ms to start.
	// If they run sequentially, it takes 500ms.
	// If they run concurrently (as a group should), it takes ~100ms.
	comps := make([]lifecycle.Component, 5)
	for i := 0; i < 5; i++ {
		comps[i] = &adapters.MockService{
			ServiceName: "Worker",
			StartDelay:  100 * time.Millisecond,
		}
	}

	group := lifecycle.NewComponentGroup("WorkerPools", comps...)

	start := time.Now()
	err := group.Start(context.Background())
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("expected successful start, got %v", err)
	}

	// 5 * 100ms = 500ms. If it took more than 300ms, it wasn't strictly concurrent.
	if duration > 300*time.Millisecond {
		t.Fatalf("expected concurrent startup (<300ms), but took %v", duration)
	}
}
