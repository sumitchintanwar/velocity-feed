package chaos

import (
	"context"
	"fmt"
)

// ValidationResult represents the outcome of an automated verification step.
type ValidationResult struct {
	Success bool
	Reason  string
}

// Validator defines a mechanism to observe the system and assert a specific condition.
type Validator interface {
	// Assert returns true if the system state matches expectations (e.g., metric dropped, log emitted).
	Assert(ctx context.Context) (ValidationResult, error)
	Name() string
}

// Experiment defines the lifecycle of a chaos engineering fault injection.
type Experiment interface {
	Name() string
	
	// Setup prepares the environment for the experiment.
	Setup(ctx context.Context) error
	
	// InjectFault triggers the failure condition (e.g., stopping a container).
	InjectFault(ctx context.Context) error
	
	// Validate executes all provided validators to ensure the system behaved correctly during the fault.
	Validate(ctx context.Context, validators []Validator) []ValidationResult
	
	// Teardown MUST restore the environment to a healthy state, even if earlier steps panicked.
	Teardown(ctx context.Context) error
}

// Result aggregates the outcome of an entire experiment run.
type Result struct {
	ExperimentName   string
	SetupError       error
	InjectionError   error
	TeardownError    error
	ValidationErrors []error
	Validations      []ValidationResult
}

// Passed evaluates if the experiment was a complete success.
func (r *Result) Passed() bool {
	if r.SetupError != nil || r.InjectionError != nil || r.TeardownError != nil || len(r.ValidationErrors) > 0 {
		return false
	}
	for _, v := range r.Validations {
		if !v.Success {
			return false
		}
	}
	return true
}

func (r *Result) String() string {
	return fmt.Sprintf("[%s] Passed: %t", r.ExperimentName, r.Passed())
}
