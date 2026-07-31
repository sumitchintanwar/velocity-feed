package lifecycle_test

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/platform/lifecycle"
	"github.com/sumit/rtmds/internal/platform/lifecycle/adapters"
)

func TestIntegration_HTTPGracefulDrain(t *testing.T) {
	m := lifecycle.NewManager()

	// 1. Create a slow HTTP handler
	var activeRequests sync.WaitGroup
	handlerEntered := make(chan struct{})
	var handlerEnteredOnce sync.Once

	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		activeRequests.Add(1)
		defer activeRequests.Done()

		handlerEnteredOnce.Do(func() {
			close(handlerEntered)
		})

		// Simulate a long 500ms database transaction
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	srv := &http.Server{
		Addr:    ":9999",
		Handler: mux,
	}

	httpAdapter := adapters.NewHTTPServerAdapter("TestHTTP", srv)
	m.Register(httpAdapter)

	ctx := context.Background()

	// Start the server
	if err := m.StartAll(ctx, 2*time.Second); err != nil {
		t.Fatalf("failed to start http adapter: %v", err)
	}

	// 2. Fire a request asynchronously
	reqErrCh := make(chan error, 1)
	go func() {
		var resp *http.Response
		var err error
		// Retry for up to 2 seconds to allow the server to start listening
		for i := 0; i < 20; i++ {
			resp, err = http.Get("http://localhost:9999/slow")
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			reqErrCh <- err
			return
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			reqErrCh <- err
			return
		}
		reqErrCh <- nil
	}()

	// Wait until the request actually hits the handler, or timeout
	select {
	case <-handlerEntered:
		// Perfect, the request is now safely in-flight
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for request to reach handler")
	}

	// 3. Initiate Shutdown (while request is actively sleeping)
	shutdownErrCh := make(chan error, 1)
	go func() {
		// Shutdown with a 2-second timeout, plenty of time for the 500ms request
		shutdownErrCh <- m.StopAll(ctx, 2*time.Second)
	}()

	// 4. Verify the request finished successfully despite the shutdown trigger!
	if err := <-reqErrCh; err != nil {
		t.Fatalf("in-flight HTTP request was violently terminated: %v", err)
	}

	// 5. Verify the shutdown completed cleanly after the request finished
	if err := <-shutdownErrCh; err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestIntegration_TimeoutEnforcement(t *testing.T) {
	m := lifecycle.NewManager()

	// A service that deadlocks and refuses to shut down
	deadlockingService := &adapters.MockService{
		ServiceName: "Deadlocker",
		StopDelay:   10 * time.Second, // Refuses to shut down quickly
	}

	m.Register(deadlockingService)

	ctx := context.Background()
	if err := m.StartAll(ctx, 1*time.Second); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Trigger shutdown with a STRICT 50ms timeout!
	err := m.StopAll(ctx, 50*time.Millisecond)

	if err == nil {
		t.Fatalf("expected a timeout error from StopAll, but got nil")
	}

	if deadlockingService.WasStopped {
		t.Fatalf("deadlocking service should NOT have completed its stop routine")
	}
}
