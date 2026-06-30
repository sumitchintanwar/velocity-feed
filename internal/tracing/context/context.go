// Package context provides tracing context propagation utilities.
package context

import (
	"context"
)

// Branch safely clones a context for background worker propagation.
// It preserves W3C trace identifiers and Baggage values from the parent,
// but detaches the cancellation signal. If the parent context (e.g. HTTP request)
// is cancelled, the returned context will NOT be cancelled.
//
// This MUST be used whenever spawning a background goroutine from an active trace.
func Branch(parent context.Context) context.Context {
	return context.WithoutCancel(parent)
}
