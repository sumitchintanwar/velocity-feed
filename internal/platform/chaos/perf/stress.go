package perf

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/sumit/rtmds/internal/platform/chaos"
)

// CPUStressExperiment locks operating system threads to artificially exhaust CPU quota.
type CPUStressExperiment struct {
	Workers  int
	Duration time.Duration
	stopCh   chan struct{}
}

func (e *CPUStressExperiment) Name() string {
	return "Performance: CPU Exhaustion"
}

func (e *CPUStressExperiment) Setup(ctx context.Context) error {
	e.stopCh = make(chan struct{})
	return nil
}

func (e *CPUStressExperiment) InjectFault(ctx context.Context) error {
	for i := 0; i < e.Workers; i++ {
		go func() {
			// Spin lock
			for {
				select {
				case <-e.stopCh:
					return
				default:
					// Burn CPU cycles
					runtime.Gosched()
				}
			}
		}()
	}
	return nil
}

func (e *CPUStressExperiment) Validate(ctx context.Context, validators []chaos.Validator) []chaos.ValidationResult {
	time.Sleep(e.Duration)
	var results []chaos.ValidationResult
	for _, v := range validators {
		res, err := v.Assert(ctx)
		if err != nil {
			results = append(results, chaos.ValidationResult{Success: false, Reason: fmt.Sprintf("validator error: %v", err)})
		} else {
			results = append(results, res)
		}
	}
	return results
}

func (e *CPUStressExperiment) Teardown(ctx context.Context) error {
	close(e.stopCh)
	return nil
}
