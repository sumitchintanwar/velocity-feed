package chaos

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// Runner executes chaos experiments safely.
type Runner struct {
	logger *zap.Logger
}

// NewRunner creates a new chaos execution engine.
func NewRunner(logger *zap.Logger) *Runner {
	return &Runner{logger: logger}
}

// RunSuite executes a batch of experiments sequentially.
func (r *Runner) RunSuite(ctx context.Context, experiments map[Experiment][]Validator) []Result {
	var results []Result

	// Set up OS signal trapping
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	for exp, validators := range experiments {
		// Run in a select to abort the suite if an interrupt occurred
		select {
		case <-sigChan:
			r.logger.Fatal("Received interrupt signal. Suite aborted. Ensure manual environment check.")
			return results
		default:
		}

		res := r.Run(ctx, exp, validators, sigChan)
		results = append(results, *res)
	}

	signal.Stop(sigChan)
	return results
}

// Run executes a single experiment with guaranteed teardown constraints, intercepting signals.
func (r *Runner) Run(ctx context.Context, exp Experiment, validators []Validator, sigChan <-chan os.Signal) *Result {
	res := &Result{
		ExperimentName: exp.Name(),
	}

	r.logger.Info("starting chaos experiment", zap.String("experiment", exp.Name()))

	// 1. Setup
	if err := exp.Setup(ctx); err != nil {
		r.logger.Error("experiment setup failed", zap.Error(err))
		res.SetupError = err
		_ = exp.Teardown(context.Background())
		return res
	}

	// Guarantee Teardown
	defer func() {
		if err := exp.Teardown(context.Background()); err != nil {
			r.logger.Error("experiment teardown failed - SYSTEM MAY BE UNSTABLE", zap.Error(err))
			res.TeardownError = err
		} else {
			r.logger.Info("experiment teardown completed successfully")
		}
	}()

	// Watch for interrupt during injection/validation
	abortCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-sigChan:
			r.logger.Warn("INTERRUPT RECEIVED. Forcing teardown of active experiment.")
			cancel() // Triggers early exit in validation/injection
		case <-abortCtx.Done():
		}
	}()

	// 2. Inject Fault
	r.logger.Info("injecting fault")
	if err := exp.InjectFault(abortCtx); err != nil {
		r.logger.Error("fault injection failed", zap.Error(err))
		res.InjectionError = err
		return res
	}

	// 3. Validate with Polling
	r.logger.Info("validating system state with polling (timeout: 30s)")
	res.Validations = r.pollValidators(abortCtx, validators, 30*time.Second)

	return res
}

func (r *Runner) pollValidators(ctx context.Context, validators []Validator, timeout time.Duration) []ValidationResult {
	deadline := time.Now().Add(timeout)
	
	// Create a slice to track which validations have succeeded
	passed := make([]bool, len(validators))
	results := make([]ValidationResult, len(validators))
	allPassed := false

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			r.logger.Warn("Validation aborted early due to context cancellation")
			return results
		default:
		}

		allPassed = true
		for i, v := range validators {
			if passed[i] {
				continue
			}

			res, err := v.Assert(ctx)
			if err != nil {
				// Don't log spam, just record it
				results[i] = ValidationResult{Success: false, Reason: err.Error()}
				allPassed = false
			} else if res.Success {
				passed[i] = true
				results[i] = res
				r.logger.Info("Validator passed", zap.String("validator", v.Name()))
			} else {
				results[i] = res
				allPassed = false
			}
		}

		if allPassed {
			break
		}

		// Sleep before retrying
		time.Sleep(2 * time.Second)
	}

	if !allPassed {
		r.logger.Warn("Validation polling timed out")
	}

	return results
}
