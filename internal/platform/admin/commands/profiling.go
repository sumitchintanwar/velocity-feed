package commands

import (
	"context"
	"runtime"
)

// SetProfilingRatesCommand dynamically updates the Go runtime profiling rates.
// It allows enabling or disabling lock contention profiling without a restart.
type SetProfilingRatesCommand struct {
	MutexFraction int
	BlockRate     int
}

func (c *SetProfilingRatesCommand) Execute(ctx context.Context) error {
	if c.MutexFraction >= 0 {
		runtime.SetMutexProfileFraction(c.MutexFraction)
	}
	
	if c.BlockRate >= 0 {
		runtime.SetBlockProfileRate(c.BlockRate)
	}
	
	return nil
}

func (c *SetProfilingRatesCommand) Name() string {
	return "SetProfilingRates"
}
