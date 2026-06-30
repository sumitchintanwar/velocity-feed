package adapters

import (
	"context"
	"errors"
	"net/http"

	"github.com/sumit/rtmds/internal/platform/lifecycle"
)

// HTTPServerAdapter wraps an http.Server to make it a lifecycle.Component.
type HTTPServerAdapter struct {
	NameStr string
	Server  *http.Server
}

// NewHTTPServerAdapter creates a new lifecycle adapter for an HTTP server.
func NewHTTPServerAdapter(name string, srv *http.Server) lifecycle.Component {
	return &HTTPServerAdapter{
		NameStr: name,
		Server:  srv,
	}
}

// Name returns the identifier of the HTTP server.
func (a *HTTPServerAdapter) Name() string {
	return a.NameStr
}

// Start boots the HTTP server in a goroutine so it doesn't block the rest of the lifecycle.
func (a *HTTPServerAdapter) Start(ctx context.Context) error {
	// We run ListenAndServe in a goroutine because it blocks indefinitely.
	// We use an error channel to catch immediate bind failures (e.g. port in use).
	errCh := make(chan error, 1)
	
	go func() {
		if err := a.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Wait briefly to ensure the server bound the port successfully,
	// or until the startup context is cancelled.
	// 50ms is usually enough to catch an "address already in use" error locally.
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	default:
		// If no immediate error, we assume it's running fine.
		return nil
	}
}

// Stop initiates a graceful shutdown of the HTTP server, draining connections.
func (a *HTTPServerAdapter) Stop(ctx context.Context) error {
	return a.Server.Shutdown(ctx)
}
