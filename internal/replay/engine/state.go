package engine

import "fmt"

// SessionState represents the explicit state machine of a replay session.
type SessionState int

const (
	StateCreated SessionState = iota
	StateInitializing
	StateRunning
	StatePaused
	StateSeeking
	StateCompleted
	StateDestroyed
)

func (s SessionState) String() string {
	switch s {
	case StateCreated:
		return "Created"
	case StateInitializing:
		return "Initializing"
	case StateRunning:
		return "Running"
	case StatePaused:
		return "Paused"
	case StateSeeking:
		return "Seeking"
	case StateCompleted:
		return "Completed"
	case StateDestroyed:
		return "Destroyed"
	default:
		return "Unknown"
	}
}

// IsValidTransition reports whether the transition from → to is permitted.
//
// Implemented as a switch rather than a package-level mutable map to prevent
// accidental modification of global state machine behaviour at runtime or in tests.
func IsValidTransition(from, to SessionState) bool {
	switch from {
	case StateCreated:
		return to == StateInitializing || to == StateDestroyed
	case StateInitializing:
		return to == StateRunning || to == StateDestroyed
	case StateRunning:
		return to == StatePaused || to == StateSeeking || to == StateCompleted || to == StateDestroyed
	case StatePaused:
		return to == StateRunning || to == StateSeeking || to == StateDestroyed
	case StateSeeking:
		return to == StateRunning || to == StateDestroyed
	case StateCompleted:
		return to == StateDestroyed
	default:
		return false
	}
}

// InvalidStateTransitionError is returned when a control command violates the state machine.
type InvalidStateTransitionError struct {
	From SessionState
	To   SessionState
}

func (e *InvalidStateTransitionError) Error() string {
	return fmt.Sprintf("invalid replay state transition from %s to %s", e.From, e.To)
}
