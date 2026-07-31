package websocket

import (
	"errors"
	"sync"
	"sync/atomic"
)

// LifecycleState represents the current state of the Gateway.
type LifecycleState string

const (
	LifecycleStarting LifecycleState = "Starting"
	LifecycleHealthy  LifecycleState = "Healthy"
	LifecycleDraining LifecycleState = "Draining"
	LifecycleOffline  LifecycleState = "Offline"
)

var (
	// ErrInvalidStateTransition is returned when an invalid state transition is attempted.
	ErrInvalidStateTransition = errors.New("invalid state transition")
)

// LifecycleObserver is a callback function for state transitions.
type LifecycleObserver func(from, to LifecycleState)

// Lifecycle manages the state machine of the Gateway.
// It is designed to be highly concurrent, with lock-free reads of the current state.
type Lifecycle struct {
	state     atomic.Value
	mu        sync.Mutex
	observers []LifecycleObserver
}

// NewLifecycle creates a new Lifecycle initialized to StateStarting.
func NewLifecycle() *Lifecycle {
	l := &Lifecycle{}
	l.state.Store(LifecycleStarting)
	return l
}

// State returns the current lifecycle state without taking any locks.
func (l *Lifecycle) State() LifecycleState {
	return l.state.Load().(LifecycleState)
}

// Observe registers a callback to be invoked on successful state transitions.
// Observers should not block, as they are invoked synchronously during the transition.
func (l *Lifecycle) Observe(observer LifecycleObserver) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.observers = append(l.observers, observer)
}

// Transition attempts to change the lifecycle state.
// It validates the transition and invokes observers if successful.
func (l *Lifecycle) Transition(to LifecycleState) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	from := l.State()
	if from == to {
		return nil // No-op
	}

	if !l.isValidTransition(from, to) {
		return ErrInvalidStateTransition
	}

	l.state.Store(to)

	for _, obs := range l.observers {
		obs(from, to)
	}

	return nil
}

// isValidTransition enforces the allowed state machine pathways.
func (l *Lifecycle) isValidTransition(from, to LifecycleState) bool {
	switch from {
	case LifecycleStarting:
		// Can transition to Healthy when fully initialized, or Offline if startup fails.
		return to == LifecycleHealthy || to == LifecycleOffline
	case LifecycleHealthy:
		// Can transition to Draining for graceful shutdown, or Offline for immediate crash/stop.
		return to == LifecycleDraining || to == LifecycleOffline
	case LifecycleDraining:
		// Must transition to Offline after draining completes.
		return to == LifecycleOffline
	case LifecycleOffline:
		// Terminal state.
		return false
	default:
		return false
	}
}
