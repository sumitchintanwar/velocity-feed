package propagation

import (
	"context"
)

// WrapWorkerFunc is a utility that safely captures the active execution context
// (including the Correlation ID and Trace Context) and passes it into an
// asynchronous worker function.
//
// It explicitly severs the cancellation tree from the parent context using
// context.WithoutCancel, ensuring that if the parent HTTP request disconnects,
// the background worker does not crash mid-execution.
func WrapWorkerFunc(ctx context.Context, fn func(context.Context)) func() {
	detachedCtx := context.WithoutCancel(ctx)
	return func() {
		fn(detachedCtx)
	}
}
