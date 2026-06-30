package lifecycle_test

import (
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/platform/lifecycle"
)

func TestSetupSignalHandler(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.Process.Signal is not fully supported on Windows")
	}

	ctx, cancel := lifecycle.SetupSignalHandler()
	defer cancel()

	// Simulate receiving a SIGTERM from Kubernetes
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("could not find process: %v", err)
	}
	
	err = p.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("could not send signal: %v", err)
	}

	// Wait for the signal handler to intercept and cancel the context
	select {
	case <-ctx.Done():
		// Success! The context was cancelled by the signal.
	case <-time.After(time.Second):
		t.Fatal("expected context to be cancelled upon SIGTERM, but it timed out")
	}
}
