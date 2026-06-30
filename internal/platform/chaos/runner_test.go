package chaos_test

import (
	"context"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/sumit/rtmds/internal/platform/chaos"
)

type mockExperiment struct {
	setupErr    error
	injectErr   error
	teardownErr error
	
	setupCalled    bool
	injectCalled   bool
	teardownCalled bool
}

func (m *mockExperiment) Name() string { return "Mock Experiment" }
func (m *mockExperiment) Setup(ctx context.Context) error {
	m.setupCalled = true
	return m.setupErr
}
func (m *mockExperiment) InjectFault(ctx context.Context) error {
	m.injectCalled = true
	return m.injectErr
}
func (m *mockExperiment) Validate(ctx context.Context, validators []chaos.Validator) []chaos.ValidationResult {
	// Not used in new Runner logic (Runner polls directly)
	return nil
}
func (m *mockExperiment) Teardown(ctx context.Context) error {
	m.teardownCalled = true
	return m.teardownErr
}

type mockValidator struct {
	success bool
	count   int
}
func (m *mockValidator) Name() string { return "Mock Validator" }
func (m *mockValidator) Assert(ctx context.Context) (chaos.ValidationResult, error) {
	m.count++
	// Simulate metric arriving on the second poll
	if m.success && m.count >= 2 {
		return chaos.ValidationResult{Success: true, Reason: "mock passed"}, nil
	}
	return chaos.ValidationResult{Success: false, Reason: "mock waiting"}, nil
}

func TestRunner_RunSuite(t *testing.T) {
	logger := zap.NewNop()
	runner := chaos.NewRunner(logger)

	exp1 := &mockExperiment{}
	exp2 := &mockExperiment{}

	experiments := map[chaos.Experiment][]chaos.Validator{
		exp1: {&mockValidator{success: true}},
		exp2: {&mockValidator{success: true}},
	}

	results := runner.RunSuite(context.Background(), experiments)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	for _, res := range results {
		if !res.Passed() {
			t.Errorf("expected experiment %s to pass", res.ExperimentName)
		}
	}
	
	if !exp1.teardownCalled || !exp2.teardownCalled {
		t.Error("expected all experiments to be torn down")
	}
}

func TestRunner_PollingValidationFailure(t *testing.T) {
	logger := zap.NewNop()
	runner := chaos.NewRunner(logger)

	exp := &mockExperiment{}
	// This validator will never succeed
	validators := []chaos.Validator{&mockValidator{success: false}}

	// We can't easily test the 30s timeout without delaying the test suite by 30s.
	// We'll run it directly to check failure state is captured correctly if run fails
	// In reality we should make the timeout configurable on the Runner for tests.
	// For this test, we just assume the context is cancelled early.
	
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sigChan := make(chan os.Signal) // mock empty channel
	res := runner.Run(ctx, exp, validators, sigChan)

	if res.Passed() {
		t.Error("expected experiment to fail due to validation timeout")
	}
	if !exp.teardownCalled {
		t.Error("teardown must execute")
	}
}
