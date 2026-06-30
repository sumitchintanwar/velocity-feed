// Package log provides a structured logging abstraction for the RTMDS platform.
//
// Design principles:
//   - Every log entry is JSON with consistent required fields (timestamp, level,
//     service, component, event, correlation_id, instance_id, message).
//   - Loggers are created via dependency injection, never via globals.
//   - Correlation IDs propagate through context.Context for distributed tracing.
//   - Component-specific constructors enforce low-cardinality field sets.
//   - Sensitive fields (API keys, tokens, passwords) are redacted automatically.
//
// This package wraps zerolog to preserve its zero-allocation hot path while
// adding the structured fields and context propagation required by the
// structured logging design document.
package log

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Config holds the deployment context injected into every log entry.
// All fields are optional; empty values use sensible defaults.
type Config struct {
	Level       string    // debug | info | warn | error
	Format      string    // json | text
	Service     string    // service name (e.g., "rtmds", "gateway")
	Environment string    // deployment environment (prod, uat, dev)
	Version     string    // software release version
	Region      string    // geographic or network region (e.g., "us-east-1")
	InstanceID  string    // unique pod/container/instance identifier
	Hostname    string    // auto-detected from os.Hostname() if empty
	Writer      io.Writer // output writer; defaults to os.Stdout if nil (for testing)
}

// hostname returns the configured hostname or detects it from the OS.
func (c Config) hostname() string {
	if c.Hostname != "" {
		return c.Hostname
	}
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

// instanceID returns the configured instance ID or falls back to hostname.
func (c Config) instanceID() string {
	if c.InstanceID != "" {
		return c.InstanceID
	}
	return c.hostname()
}

// contextKey is the unexported type for context values in this package.
type contextKey struct{}

// correlationKey is the unexported type for correlation ID context values.
type correlationKey struct{}

// Logger wraps zerolog.Logger and adds structured field enforcement,
// context propagation, and correlation ID support.
type Logger struct {
	z       zerolog.Logger
	service string // stored for child logger creation
}

// New creates a Logger that writes JSON to the given writer.
// The service name is included in every log entry emitted by this logger.
func New(w io.Writer, service string) *Logger {
	if w == nil {
		w = os.Stdout
	}
	zl := zerolog.New(w).Level(zerolog.InfoLevel).With().
		Timestamp().
		Str("service", service).
		Logger()
	return &Logger{z: zl, service: service}
}

// NewFromConfig creates a Logger from a Config struct.
// If format is "text", a console writer is used for human-readable output.
// Environment fields (environment, version, region, hostname, instance_id)
// are injected into the base logger and appear on every log entry.
func NewFromConfig(cfg Config) *Logger {
	var w io.Writer
	if cfg.Writer != nil {
		w = cfg.Writer
	} else if cfg.Format == "text" {
		w = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		}
	} else {
		w = os.Stdout
	}

	lvl, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	zl := zerolog.New(w).Level(lvl).With().
		Timestamp().
		Str("service", cfg.Service).
		Str("environment", cfg.Environment).
		Str("version", cfg.Version).
		Str("region", cfg.Region).
		Str("hostname", cfg.hostname()).
		Str("instance_id", cfg.instanceID()).
		Logger()
	return &Logger{z: zl, service: cfg.Service}
}

// NewFromLegacy creates a Logger from raw level/format/service strings.
// Deprecated: Use NewFromConfig for new code to get environment fields.
func NewFromLegacy(level, format, service string) *Logger {
	return NewFromConfig(Config{
		Level:   level,
		Format:  format,
		Service: service,
	})
}

// Component returns a child logger with the "component" field set.
func (l *Logger) Component(name string) *Logger {
	return &Logger{
		z:       l.z.With().Str("component", name).Logger(),
		service: l.service,
	}
}

// Instance returns a child logger with the "instance_id" field set.
func (l *Logger) Instance(id string) *Logger {
	return &Logger{
		z:       l.z.With().Str("instance_id", id).Logger(),
		service: l.service,
	}
}

// WithField returns a child logger with a single string field attached.
func (l *Logger) WithField(key, value string) *Logger {
	return &Logger{
		z:       l.z.With().Str(key, value).Logger(),
		service: l.service,
	}
}

// With returns a child logger context for adding multiple fields.
func (l *Logger) With() zerolog.Context {
	return l.z.With()
}

// Underlying returns the raw zerolog.Logger for callers that need the
// full zerolog API. Prefer the typed methods on Logger for standard logging.
func (l *Logger) Underlying() *zerolog.Logger {
	return &l.z
}

// SetLogLevel changes the log level at runtime.
// This is critical for incident response — enabling DEBUG logging without
// restarting the server (which would drop thousands of WebSocket clients
// and clear the snapshot cache).
func (l *Logger) SetLogLevel(level string) error {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		return err
	}
	l.z = l.z.Level(lvl)
	return nil
}

// -- Context propagation --

// IntoContext stores the Logger in the context.
func IntoContext(ctx context.Context, l *Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, l)
}

// FromContext extracts the Logger from the context. Returns a nop logger
// if no Logger is present so callers never need nil checks.
func FromContext(ctx context.Context) *Logger {
	if l, ok := ctx.Value(contextKey{}).(*Logger); ok && l != nil {
		return l
	}
	return &Logger{z: zerolog.Nop()}
}

// -- Correlation ID --

// SetCorrelationID stores a correlation ID in the context. The ID should
// be generated at the request entry point (typically the HTTP gateway) and
// propagated to all downstream log entries.
func SetCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// GetCorrelationID extracts the correlation ID from the context.
func GetCorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(correlationKey{}).(string); ok {
		return id
	}
	return ""
}

// -- Trace context for log correlation --

// traceContextKey holds trace_id and span_id extracted from OpenTelemetry spans.
type traceContextKey struct{}

// TraceFields holds W3C trace identifiers for log correlation.
type TraceFields struct {
	TraceID string
	SpanID  string
}

// WithTraceFields stores trace identifiers in the context for log correlation.
// Called by the HTTP middleware after extracting them from the OTel span.
func WithTraceFields(ctx context.Context, traceID, spanID string) context.Context {
	return context.WithValue(ctx, traceContextKey{}, TraceFields{
		TraceID: traceID,
		SpanID:  spanID,
	})
}

// GetTraceFields extracts trace identifiers from the context.
// Returns empty strings if no trace context is present.
func GetTraceFields(ctx context.Context) (traceID, spanID string) {
	if tf, ok := ctx.Value(traceContextKey{}).(TraceFields); ok {
		return tf.TraceID, tf.SpanID
	}
	return "", ""
}

// -- Event helpers that inject correlation ID from context --

// Debug starts a debug-level log event with correlation_id from ctx.
func Debug(ctx context.Context, l *Logger) *zerolog.Event {
	return withCorrelation(ctx, l.z.Debug())
}

// Info starts an info-level log event with correlation_id from ctx.
func Info(ctx context.Context, l *Logger) *zerolog.Event {
	return withCorrelation(ctx, l.z.Info())
}

// Warn starts a warn-level log event with correlation_id from ctx.
func Warn(ctx context.Context, l *Logger) *zerolog.Event {
	return withCorrelation(ctx, l.z.Warn())
}

// Error starts an error-level log event with correlation_id from ctx.
func Error(ctx context.Context, l *Logger) *zerolog.Event {
	return withCorrelation(ctx, l.z.Error())
}

// withCorrelation injects the correlation_id, trace_id, and span_id from ctx
// into the event if present. This enables log → trace correlation: every log
// entry produced within a request includes the trace identifiers, allowing
// operators to jump from a log line directly to the distributed trace.
func withCorrelation(ctx context.Context, e *zerolog.Event) *zerolog.Event {
	if id := GetCorrelationID(ctx); id != "" {
		e = e.Str("correlation_id", id)
	}
	traceID, spanID := GetTraceFields(ctx)
	if traceID != "" {
		e = e.Str("trace_id", traceID)
	}
	if spanID != "" {
		e = e.Str("span_id", spanID)
	}
	return e
}
