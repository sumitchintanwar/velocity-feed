package websocket

import (
	"context"

	"github.com/sumit/rtmds/internal/tracing"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

// NewConnectionContext bridges an incoming HTTP request context to a long-lived
// WebSocket connection context. It explicitly severs the HTTP timeout/cancellation
// tree while preserving the W3C Baggage (Correlation ID) and Trace Context.
//
// This guarantees that if the client keeps the WebSocket open but the initial
// HTTP Upgrade request context expires, the WebSocket connection context is
// not cancelled, yet observability continuity is maintained.
func NewConnectionContext(reqCtx context.Context) (context.Context, context.CancelFunc) {
	// 1. Sever the HTTP timeout/cancellation tree.
	detachedCtx := context.WithoutCancel(reqCtx)

	// 2. Create a new cancellation root dedicated specifically to the lifespan
	// of the WebSocket session.
	connCtx, cancel := context.WithCancel(detachedCtx)

	// 3. Ensure Baggage and Trace Context are explicitly carried over if they exist.
	// context.WithoutCancel preserves values, but we defensively ensure standard
	// OpenTelemetry metadata is correctly structured for the new root.
	if spanCtx := trace.SpanContextFromContext(reqCtx); spanCtx.IsValid() {
		connCtx = trace.ContextWithSpanContext(connCtx, spanCtx)
	}

	if b := baggage.FromContext(reqCtx); b.Len() > 0 {
		connCtx = baggage.ContextWithBaggage(connCtx, b)
	}

	return connCtx, cancel
}

// DeriveMessageContext creates a short-lived execution context for processing a
// single inbound or outbound WebSocket frame. It generates a child OpenTelemetry
// span linked to the persistent Connection Context.
//
// The caller MUST call span.End() when message processing completes.
func DeriveMessageContext(connCtx context.Context, operation string) (context.Context, trace.Span) {
	return tracing.TracerForComponent("websocket").Start(
		connCtx,
		operation,
		trace.WithSpanKind(trace.SpanKindServer),
	)
}
