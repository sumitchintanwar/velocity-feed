package lifecycle

import (
	"context"
	"time"
)

// Component defines the interface that any lifecycle-managed service must implement.
type Component interface {
	// Name returns the identifier of the component for logging and debugging.
	Name() string
	// Start initializes the component. It should block until the component is fully ready.
	// The provided context bounds the startup time.
	Start(ctx context.Context) error
	// Stop shuts down the component gracefully. It should block until all resources are released.
	// The provided context bounds the shutdown time.
	Stop(ctx context.Context) error
}

// TimeoutComponent extends Component to provide a custom execution timeout
// bounding its Startup and Shutdown phases, overriding the Manager's global blanket timeout.
type TimeoutComponent interface {
	Component
	Timeout() time.Duration
}

// FuncComponent is a helper adapter to wrap inline functions as a Component.
type FuncComponent struct {
	ComponentName string
	StartFunc     func(ctx context.Context) error
	StopFunc      func(ctx context.Context) error
}

func (f *FuncComponent) Name() string {
	return f.ComponentName
}

func (f *FuncComponent) Start(ctx context.Context) error {
	if f.StartFunc != nil {
		return f.StartFunc(ctx)
	}
	return nil
}

func (f *FuncComponent) Stop(ctx context.Context) error {
	if f.StopFunc != nil {
		return f.StopFunc(ctx)
	}
	return nil
}
