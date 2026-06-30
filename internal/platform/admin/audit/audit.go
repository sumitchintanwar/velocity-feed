// Package audit provides immutable, structured audit logging for administrative actions.
package audit

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// Outcome represents the result of an administrative action.
type Outcome string

const (
	OutcomeSuccess Outcome = "SUCCESS"
	OutcomeFailure Outcome = "FAILURE"
)

// Event represents a single, immutable audit log entry.
type Event struct {
	Timestamp     time.Time `json:"timestamp"`
	UserIdentity  string    `json:"user_identity"`
	SourceIP      string    `json:"source_ip"`
	RequestID     string    `json:"request_id"`
	CorrelationID string    `json:"correlation_id"`
	Action        string    `json:"action"`
	TargetService string    `json:"target_service"`
	Outcome       Outcome   `json:"outcome"`
	Duration      time.Duration `json:"duration"`
	ErrorMessage  string    `json:"error_message,omitempty"`
}

// Logger defines the interface for recording audit events.
type Logger interface {
	// Record emits an immutable audit event to the underlying storage (e.g. stdout via Zap).
	Record(ctx context.Context, event Event)
}

// ZapAuditLogger implements Logger using a structured zap.Logger.
type ZapAuditLogger struct {
	logger *zap.Logger
}

// NewZapAuditLogger creates a new audit logger that writes structured JSON to the provided zap logger.
// The zap logger should be configured to emit to a dedicated audit stream in production.
func NewZapAuditLogger(logger *zap.Logger) *ZapAuditLogger {
	return &ZapAuditLogger{
		logger: logger.Named("audit"),
	}
}

// Record emits the audit event as a structured log.
func (l *ZapAuditLogger) Record(ctx context.Context, event Event) {
	fields := []zap.Field{
		zap.Time("timestamp", event.Timestamp),
		zap.String("user_identity", event.UserIdentity),
		zap.String("source_ip", event.SourceIP),
		zap.String("request_id", event.RequestID),
		zap.String("correlation_id", event.CorrelationID),
		zap.String("action", event.Action),
		zap.String("target_service", event.TargetService),
		zap.String("outcome", string(event.Outcome)),
		zap.Duration("duration", event.Duration),
	}

	if event.ErrorMessage != "" {
		fields = append(fields, zap.String("error_message", event.ErrorMessage))
	}

	// Always log at INFO level so it is never filtered out by log-level settings.
	l.logger.Info("administrative action audited", fields...)
}
