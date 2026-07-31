package websocket

import (
	"testing"
)

func TestLifecycle_InitialState(t *testing.T) {
	lc := NewLifecycle()
	if state := lc.State(); state != LifecycleStarting {
		t.Errorf("expected initial state to be %s, got %s", LifecycleStarting, state)
	}
}

func TestLifecycle_ValidTransitions(t *testing.T) {
	tests := []struct {
		name     string
		sequence []LifecycleState
	}{
		{"Normal Flow", []LifecycleState{LifecycleStarting, LifecycleHealthy, LifecycleDraining, LifecycleOffline}},
		{"Early Crash", []LifecycleState{LifecycleStarting, LifecycleOffline}},
		{"Crash from Healthy", []LifecycleState{LifecycleStarting, LifecycleHealthy, LifecycleOffline}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := NewLifecycle()
			// Start at index 1 because index 0 is LifecycleStarting
			for i := 1; i < len(tt.sequence); i++ {
				to := tt.sequence[i]
				if err := lc.Transition(to); err != nil {
					t.Errorf("failed valid transition to %s: %v", to, err)
				}
				if state := lc.State(); state != to {
					t.Errorf("expected state to be %s, got %s", to, state)
				}
			}
		})
	}
}

func TestLifecycle_InvalidTransitions(t *testing.T) {
	lc := NewLifecycle() // starting

	if err := lc.Transition(LifecycleDraining); err != ErrInvalidStateTransition {
		t.Errorf("expected ErrInvalidStateTransition, got %v", err)
	}

	_ = lc.Transition(LifecycleHealthy)

	if err := lc.Transition(LifecycleStarting); err != ErrInvalidStateTransition {
		t.Errorf("expected ErrInvalidStateTransition, got %v", err)
	}

	_ = lc.Transition(LifecycleOffline)

	if err := lc.Transition(LifecycleHealthy); err != ErrInvalidStateTransition {
		t.Errorf("expected ErrInvalidStateTransition, got %v", err)
	}
}

func TestLifecycle_Observer(t *testing.T) {
	lc := NewLifecycle()

	observedTransitions := 0
	lc.Observe(func(from, to LifecycleState) {
		observedTransitions++
		if from == LifecycleStarting && to != LifecycleHealthy {
			t.Errorf("unexpected transition in observer")
		}
	})

	_ = lc.Transition(LifecycleHealthy)

	if observedTransitions != 1 {
		t.Errorf("expected 1 observation, got %d", observedTransitions)
	}
}

func TestLifecycle_NoOpTransition(t *testing.T) {
	lc := NewLifecycle()

	observedTransitions := 0
	lc.Observe(func(from, to LifecycleState) {
		observedTransitions++
	})

	// Transition to current state
	if err := lc.Transition(LifecycleStarting); err != nil {
		t.Errorf("expected nil error for no-op transition, got %v", err)
	}

	if observedTransitions != 0 {
		t.Errorf("expected 0 observations for no-op transition, got %d", observedTransitions)
	}
}
