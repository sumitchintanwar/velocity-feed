# Future Possibilities & Technical Debt

This document tracks deferred architecture decisions, potential improvements, and technical debt items for future consideration.

## 1. Strict API Enforcement of Log Event Names

**Context:** The `LogEntry` schema defines `event` as a required field (using a strict `noun_action` format like `gateway_started`). However, the current logging implementation relies on convention and test-time validation (`ValidateEventName()`) rather than compile-time or API-level enforcement.

**Deferred Decision:** We skipped wrapping the `log.Info()` and `log.Debug()` helpers to enforce passing an `event` string parameter.
* **Reason:** Modifying the signature (e.g., to `func (l *Logger) Info(ctx context.Context, event string, msg string)`) would require rewriting all existing log call sites across the codebase, introducing unnecessary risk and churn right now.
* **Current Mitigation:** Test-time validation via `ValidateEventName()` is sufficient to catch violations during development. Queryability by the `event` field in Splunk/Elasticsearch is already enforced by convention.

**Future Action:** 
When the project undergoes a larger refactoring phase, consider migrating the logging API to strictly require the `event` field in the method signature. This guarantees 100% schema compliance for our downstream log aggregators and prevents unstructured `.Msg("...")` calls.

## 2. Distributed Tracing Implementation (OpenTelemetry)

**Context:** We have added `trace_id` and `span_id` as `omitempty` fields to the `LogEntry` struct. The schema is now ready for tracing without breaking existing callers.

**Future Action:**
* Implement the OpenTelemetry (OTel) SDK across all services (Gateway Cluster, Snapshot Service, Replay API).
* Implement W3C trace context extraction and injection in the HTTP middleware and Redis Pub/Sub message metadata.
* Ensure all log entries automatically pull the `trace_id` and `span_id` from the `context.Context` to enable seamless correlation between metrics, logs, and traces.
