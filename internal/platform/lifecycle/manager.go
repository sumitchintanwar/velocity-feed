package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Manager orchestrates the lifecycle of registered components.
type Manager struct {
	mu           sync.Mutex
	components   []Component
	started      []Component // tracks components that successfully started (for rollback)
	startupHooks []func()
}

// NewManager creates a new lifecycle manager.
func NewManager() *Manager {
	return &Manager{
		components:   make([]Component, 0),
		started:      make([]Component, 0),
		startupHooks: make([]func(), 0),
	}
}

// OnStartupComplete registers a callback that will be executed once all 
// components have successfully started.
func (m *Manager) OnStartupComplete(hook func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startupHooks = append(m.startupHooks, hook)
}

// Register adds a component to the lifecycle manager. 
// Registration order dictates Startup order. Shutdown will be strictly in reverse (LIFO).
func (m *Manager) Register(c Component) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.components = append(m.components, c)
}

// StartAll iterates through registered components and starts them sequentially.
// If any component fails to start, it halts the boot process and rolls back (Stops) 
// any components that had already successfully started.
func (m *Manager) StartAll(globalCtx context.Context, timeoutPerComponent time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, comp := range m.components {
		// Use custom timeout if provided by the component
		effTimeout := timeoutPerComponent
		if tc, ok := comp.(TimeoutComponent); ok {
			effTimeout = tc.Timeout()
		}

		ctx, cancel := context.WithTimeout(globalCtx, effTimeout)
		
		errCh := make(chan error, 1)
		go func(c Component) {
			errCh <- c.Start(ctx)
		}(comp)

		var startErr error
		select {
		case startErr = <-errCh:
		case <-ctx.Done():
			startErr = fmt.Errorf("component %s startup timed out: %w", comp.Name(), ctx.Err())
		}
		
		cancel()

		if startErr != nil {
			// Fail-Fast: Rollback components that already started
			_ = m.rollbackUnlocked(globalCtx, timeoutPerComponent)
			return fmt.Errorf("failed to start component %s: %w", comp.Name(), startErr)
		}

		m.started = append(m.started, comp)
	}

	// Trigger all hooks sequentially after a successful boot
	for _, hook := range m.startupHooks {
		hook()
	}

	return nil
}

// StopAll stops all successfully started components in reverse order (LIFO).
func (m *Manager) StopAll(globalCtx context.Context, timeoutPerComponent time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rollbackUnlocked(globalCtx, timeoutPerComponent)
}

// rollbackUnlocked executes the LIFO shutdown. Must be called with m.mu locked.
func (m *Manager) rollbackUnlocked(globalCtx context.Context, timeoutPerComponent time.Duration) error {
	var finalErr error

	// Iterate backwards over strictly what successfully started
	for i := len(m.started) - 1; i >= 0; i-- {
		comp := m.started[i]
		
		// Use custom timeout if provided by the component
		effTimeout := timeoutPerComponent
		if tc, ok := comp.(TimeoutComponent); ok {
			effTimeout = tc.Timeout()
		}

		ctx, cancel := context.WithTimeout(globalCtx, effTimeout)
		
		errCh := make(chan error, 1)
		go func(c Component) {
			errCh <- c.Stop(ctx)
		}(comp)

		var stopErr error
		select {
		case stopErr = <-errCh:
		case <-ctx.Done():
			stopErr = fmt.Errorf("component %s shutdown timed out: %w", comp.Name(), ctx.Err())
		}
		
		cancel()

		if stopErr != nil {
			// We record the error but continue shutting down the rest of the stack
			if finalErr == nil {
				finalErr = stopErr
			} else {
				finalErr = fmt.Errorf("%v; %v", finalErr, stopErr)
			}
		}
	}
	
	// Clear the started list since everything is shut down
	m.started = m.started[:0]

	return finalErr
}
