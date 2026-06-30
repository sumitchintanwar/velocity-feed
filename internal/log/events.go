package log

import (
	"regexp"
)

// eventPattern enforces the noun_action naming convention for event fields.
// Events must be lowercase alphanumeric with a single underscore separator.
// Examples: gateway_started, client_connected, replay_completed
// Counter-examples: GatewayStarted (PascalCase), started (no noun), client_connected_v2 (extra parts)
var eventPattern = regexp.MustCompile(`^[a-z][a-z0-9]*_[a-z][a-z0-9]*$`)

// ValidateEventName checks that an event name follows the noun_action convention.
// Returns nil if valid, or an error describing the violation.
func ValidateEventName(name string) error {
	if !eventPattern.MatchString(name) {
		return &InvalidEventError{Name: name}
	}
	return nil
}

// InvalidEventError is returned when an event name violates the naming convention.
type InvalidEventError struct {
	Name string
}

func (e *InvalidEventError) Error() string {
	return "event name must be lowercase noun_action format (e.g., gateway_started): got " + e.Name
}

// LogEntry represents the mandatory schema for every log entry.
// This struct is a reference for serialization and schema validation;
// the hot path uses zerolog's chained API to avoid allocations.
type LogEntry struct {
	Timestamp     string `json:"time"`
	Level         string `json:"level"`
	Service       string `json:"service"`
	Component     string `json:"component"`
	Event         string `json:"event"`
	CorrelationID string `json:"correlation_id,omitempty"`
	TraceID       string `json:"trace_id,omitempty"`       // W3C trace ID for distributed tracing
	SpanID        string `json:"span_id,omitempty"`        // W3C span ID for distributed tracing
	InstanceID    string `json:"instance_id"`
	Message       string `json:"message"`
}

// RequiredFields lists the mandatory field names for schema validation.
// Note: zerolog's .Timestamp() method uses the field name "time".
var RequiredFields = []string{
	"time",
	"level",
	"service",
	"component",
	"event",
	"instance_id",
	"message",
}
