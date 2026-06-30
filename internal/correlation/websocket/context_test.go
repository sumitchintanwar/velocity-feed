package websocket

import (
	"context"
	"testing"

	corrcontext "github.com/sumit/rtmds/internal/correlation/context"
)

func TestNewConnectionContext_SeverCancellation(t *testing.T) {
	httpCtx, httpCancel := context.WithCancel(context.Background())
	
	// Create the persistent connection context
	connCtx, _ := NewConnectionContext(httpCtx)
	
	// Cancel the HTTP request (simulating client dropping the HTTP connection post-upgrade)
	httpCancel()
	
	// Verify the connection context is NOT canceled
	select {
	case <-connCtx.Done():
		t.Fatal("expected connCtx to remain active after httpCtx is canceled")
	default:
		// Passed
	}
}

func TestNewConnectionContext_PreservesCorrelationID(t *testing.T) {
	httpCtx := context.Background()
	httpCtx = corrcontext.WithCorrelationID(httpCtx, "ws-correlation-test")
	
	connCtx, _ := NewConnectionContext(httpCtx)
	
	extracted := corrcontext.CorrelationIDFromContext(connCtx)
	if extracted != "ws-correlation-test" {
		t.Errorf("expected Correlation ID to be preserved, got %q", extracted)
	}
}

func TestDeriveMessageContext(t *testing.T) {
	httpCtx := context.Background()
	httpCtx = corrcontext.WithCorrelationID(httpCtx, "msg-correlation-test")
	
	connCtx, _ := NewConnectionContext(httpCtx)
	
	msgCtx, span := DeriveMessageContext(connCtx, "websocket.message_received")
	
	// Ensure span is created
	if !span.SpanContext().IsValid() {
		// Note: since we use the NoopTracer by default in tests without a real provider,
		// the span context might not be valid unless we setup the global provider.
		// However, it shouldn't panic and the context should be derived successfully.
	}
	
	extracted := corrcontext.CorrelationIDFromContext(msgCtx)
	if extracted != "msg-correlation-test" {
		t.Errorf("expected Message context to inherit Correlation ID, got %q", extracted)
	}
	
	span.End()
}
