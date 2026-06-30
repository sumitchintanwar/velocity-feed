package audit

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestZapAuditLogger_Record(t *testing.T) {
	// Setup zap observer to capture logs in memory
	core, logs := observer.New(zap.InfoLevel)
	zapLogger := zap.New(core)
	
	auditLogger := NewZapAuditLogger(zapLogger)

	event := Event{
		Timestamp:     time.Now(),
		UserIdentity:  "admin-user",
		SourceIP:      "192.168.1.100",
		RequestID:     "req-123",
		CorrelationID: "corr-456",
		Action:        "PausePublisher",
		TargetService: "publisher",
		Outcome:       OutcomeSuccess,
		Duration:      10 * time.Millisecond,
	}

	auditLogger.Record(context.Background(), event)

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Message != "administrative action audited" {
		t.Errorf("expected message 'administrative action audited', got '%s'", entry.Message)
	}

	fields := entry.ContextMap()
	if fields["user_identity"] != "admin-user" {
		t.Errorf("expected user_identity 'admin-user', got '%v'", fields["user_identity"])
	}
	if fields["action"] != "PausePublisher" {
		t.Errorf("expected action 'PausePublisher', got '%v'", fields["action"])
	}
	if fields["outcome"] != string(OutcomeSuccess) {
		t.Errorf("expected outcome '%s', got '%v'", OutcomeSuccess, fields["outcome"])
	}
}
