package propagation

import (
	"context"
	"fmt"

	tracecontext "github.com/sumit/rtmds/internal/tracing/context"
	"github.com/sumit/rtmds/internal/tracing/factory"
)

// RunWorkerTask executes a background task within a safely branched trace context.
// It creates a new child span for the task, executes the task function, automatically
// records any returned error to the span, and ends the span.
//
// Because it uses context.Branch under the hood, the task will not be killed if the
// parent context (e.g. an HTTP request) is cancelled.
func RunWorkerTask(ctx context.Context, f *factory.Factory, component, operation string, task func(context.Context) error) {
	// Branch the context to detach cancellation but preserve trace metadata
	branchedCtx := tracecontext.Branch(ctx)

	// Spawn the child span for the background worker
	spanCtx, span := f.StartSpan(branchedCtx, component, operation)
	defer span.End()

	// Intercept any fatal panics
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("fatal worker panic: %v", r)
			f.RecordError(span, err)
			span.End() // Explicitly end before re-panicking
			panic(r)
		}
	}()

	// Execute the background business logic
	err := task(spanCtx)

	// Record any failure
	f.RecordError(span, err)
}
