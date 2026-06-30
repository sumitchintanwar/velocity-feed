# Production-Grade OpenTelemetry Architecture Design
## Distributed Market Data Platform

### Author

Principal Engineer – Distributed Systems Observability

---

# Goal

Design a production-grade **OpenTelemetry Architecture** for a distributed real-time market data platform that provides end-to-end distributed tracing with minimal overhead.

The tracing framework should integrate seamlessly with the existing observability stack:

- Structured Logging
- Context-Aware Logging
- Prometheus Metrics
- Business Metrics
- Runtime Metrics

The framework must support:

- Distributed tracing
- Automatic context propagation
- Dependency injection
- OTLP exporters
- Jaeger visualization
- Configurable sampling
- Reusable instrumentation
- Low-overhead execution
- Future extensibility

---

# Current Platform

```text
Exchange Adapter Framework

↓

Market Data Normalization

↓

Publisher Service

↓

Redis Pub/Sub

↓

Topic Manager

↓

Gateway Cluster

↓

Replay API

↓

Snapshot Service

↓

Recovery Manager

↓

Structured Logging

↓

Context-Aware Logging

↓

Prometheus Metrics

↓

Business Metrics

↓

Runtime Metrics

↓

Kubernetes
```

---

# Objectives

```text
Centralized Tracing Package

OpenTelemetry SDK

Dependency Injection

Automatic Context Propagation

Jaeger Support

OTLP Exporters

Low Allocation

Configurable Sampling

Extensible Instrumentation

Production Ready
```

---

# 1. Overall Architecture

The tracing framework should be a shared infrastructure library responsible for initializing OpenTelemetry, managing trace providers, propagating context, and exporting telemetry.

```text
                  Configuration
                         │
                         ▼

                 Tracing Factory
                         │
                         ▼

                 Tracer Provider
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼

 Publisher      Gateway        Replay Service

        ▼                ▼                ▼

 Component Tracers   Component Tracers   Component Tracers

        └────────────────┼────────────────┘
                         ▼

               OTLP Export Pipeline

                         ▼

          OpenTelemetry Collector

                ├──────────────┐
                ▼              ▼

             Jaeger      Future Backends
```

---

# 2. Design Philosophy

Tracing should answer questions that logs and metrics cannot:

```text
Where did latency occur?

Which service introduced delay?

Which downstream dependency failed?

How long did each stage take?

Which request path caused the issue?
```

Tracing complements logs and metrics but should never replace them.

---

# 3. Architecture Responsibilities

The tracing framework is responsible for:

```text
Tracer Initialization

Tracer Provider Lifecycle

Context Propagation

Span Creation

Sampling

Exporter Configuration

Resource Attributes

Dependency Injection
```

It is **not** responsible for:

```text
Business Logic

Logging

Metrics

Alerting

Visualization
```

---

# 4. Package Structure

```text
internal/

    tracing/

        provider/

        factory/

        config/

        context/

        propagation/

        exporters/

        samplers/

        middleware/

        attributes/

        lifecycle/

        interfaces/

    observability/
```

---

## Responsibilities

### provider/

Creates and manages the global Tracer Provider.

---

### factory/

Creates reusable component tracers.

---

### config/

Stores tracing configuration.

Example:

```text
Exporter

Sampling

Service Name

Environment

Collector Endpoint
```

---

### context/

Manages trace context propagation.

---

### propagation/

Implements OpenTelemetry context propagation across:

- HTTP
- WebSocket
- Background workers
- Redis messaging

---

### exporters/

Configures:

```text
OTLP gRPC

OTLP HTTP

Jaeger
```

Future exporters can be added without changing application code.

---

### samplers/

Provides configurable sampling strategies.

---

### middleware/

Shared tracing middleware for:

```text
HTTP

WebSocket

Replay

Background Workers
```

---

### attributes/

Defines standardized span attributes.

---

### lifecycle/

Owns initialization and graceful shutdown.

---

### interfaces/

Abstract tracer interfaces for testing.

---

# 5. Trace Lifecycle

```text
Application Starts

↓

Initialize Tracer Provider

↓

Register Exporter

↓

Create Component Tracers

↓

Inject Tracers

↓

Application Running

↓

Request Creates Root Span

↓

Child Spans Created

↓

Spans Exported

↓

Shutdown Flushes Buffers
```

Tracer providers are initialized once per process.

---

# 6. Dependency Injection

Bootstrap sequence:

```text
Configuration

↓

Tracer Provider

↓

Tracing Factory

↓

Component Tracers

↓

Inject Into Services
```

Services never create tracers directly.

Benefits:

```text
Loose Coupling

Consistent Configuration

Easy Testing

Centralized Lifecycle
```

---

# 7. Context Propagation

Context is the backbone of distributed tracing.

Flow:

```text
Incoming Request

↓

Tracing Middleware

↓

Extract Context

↓

Create Span

↓

Inject Updated Context

↓

Pass Context Through Services

↓

Export Trace
```

Every downstream operation receives the same context.

---

# 8. Propagation Across Components

## HTTP

```text
Client

↓

Gateway

↓

Replay API

↓

Snapshot Service
```

Trace context is propagated using W3C Trace Context headers.

---

## WebSocket

Connection context is established during handshake.

Subsequent messages inherit the connection's tracing context.

---

## Redis Pub/Sub

Publisher:

```text
Create Span

↓

Inject Trace Context

↓

Publish Message
```

Subscriber:

```text
Receive Message

↓

Extract Context

↓

Create Consumer Span
```

This links producer and consumer spans across asynchronous boundaries.

---

## Background Workers

Worker execution flow:

```text
Parent Context

↓

Task Queue

↓

Worker

↓

Child Span
```

Workers should continue traces whenever task metadata contains trace context.

---

# 9. Span Hierarchy

Example request:

```text
HTTP Request

├── Authentication

├── Publisher

│     ├── Validation

│     ├── Redis Publish

│     └── Metrics Update

├── Topic Manager

├── Snapshot Update

└── Response
```

General guidelines:

- Root span per request or background task.
- Child spans for major operations.
- Avoid creating spans for trivial function calls.

---

# 10. Standard Span Attributes

Every span should include:

```text
service.name

service.version

deployment.environment

component

operation

instance.id
```

Optional business attributes:

```text
exchange

topic

asset_class

gateway

worker_pool
```

Avoid unbounded values such as:

```text
request_id

trace_id

connection_id

client_id

symbol
```

unless they are required and bounded.

---

# 11. Sampling Strategy

Tracing every request is rarely practical in high-throughput systems.

Recommended strategies:

## Development

```text
100% Sampling
```

Used for debugging.

---

## Production

Use parent-based probability sampling.

Typical sampling rates:

```text
0.1%

0.5%

1%

5%
```

Critical operations may override default sampling.

Examples:

- Recovery
- Startup
- Replay failures
- Snapshot corruption

---

# 12. Export Pipeline

Recommended architecture:

```text
Application

↓

OTLP Exporter

↓

OpenTelemetry Collector

├───────────────┐

▼               ▼

Jaeger     Future Backends
```

Advantages:

- Centralized processing
- Retry handling
- Batching
- Backend independence

Applications should export only to the collector, not directly to Jaeger.

---

# 13. Performance Considerations

Tracing must remain lightweight.

Design principles:

```text
Batch Span Export

Asynchronous Export

Minimal Allocations

Context Reuse

Configurable Sampling

Bounded Attributes
```

Instrumentation should never become a bottleneck for market data processing.

---

# 14. Error Handling

Spans should capture:

```text
Errors

Timeouts

Retries

Dependency Failures
```

Error information should be recorded as span status and structured attributes.

Do not duplicate large log messages inside spans.

---

# 15. Metric and Log Correlation

Tracing should integrate with existing observability.

Example workflow:

```text
Alert Fires

↓

Grafana Dashboard

↓

Relevant Trace

↓

Associated Logs

↓

Root Cause Analysis
```

Logs should include:

```text
trace_id

span_id
```

Metrics should be correlated through exemplars where supported.

---

# 16. Extensibility

Future tracing modules:

```text
FIX Adapter Tracing

Order Book Tracing

Aggregation Tracing

Recorder Tracing

Metadata Service Tracing

Market Data Quality Tracing
```

New components should plug into the tracing factory without modifying existing services.

---

# 17. Common Mistakes

## Tracing Everything

Creating spans for every function introduces unnecessary overhead.

Instrument service boundaries and meaningful operations.

---

## Excessive Attributes

Avoid storing large payloads or highly variable values.

Keep span attributes bounded and reusable.

---

## Ignoring Asynchronous Context

Failing to propagate context across Redis or worker queues breaks distributed traces.

Always carry context through asynchronous workflows.

---

## Over-Sampling

High sampling rates increase CPU, memory, and storage costs.

Tune sampling based on traffic and operational needs.

---

## Creating Tracers Everywhere

Tracer instances should be created centrally and injected into services.

---

## Exporting Directly to Multiple Backends

Applications should export once to an OpenTelemetry Collector.

The collector is responsible for fan-out.

---

# 18. Production Best Practices

- Use a single Tracer Provider per process.
- Use dependency injection for tracer access.
- Prefer parent-based sampling.
- Propagate context across every service boundary.
- Batch span exports.
- Flush spans during graceful shutdown.
- Keep span names stable.
- Standardize attributes across services.
- Correlate traces with logs and metrics.

---

# 19. Package Ownership

| Package | Responsibility |
|----------|----------------|
| provider | Tracer Provider lifecycle |
| factory | Component tracer creation |
| propagation | Context propagation |
| middleware | HTTP/WebSocket instrumentation |
| samplers | Sampling configuration |
| exporters | OTLP/Jaeger exporters |
| attributes | Shared span attributes |
| lifecycle | Startup and shutdown |

Ownership should be explicit and non-overlapping.

---

# 20. How Production Trading Systems Organize Distributed Tracing

Large financial institutions typically organize tracing as a shared platform capability rather than an application concern.

Common characteristics include:

```text
Single Tracer Provider Per Process

Shared Tracing Library

Dependency Injection

Automatic Context Propagation

Standard Span Attributes

OpenTelemetry Collector

OTLP Exporters

Parent-Based Sampling

Asynchronous Export

Cross-Service Correlation

Integration with Logs and Metrics
```

Tracing is primarily used for:

- Latency investigations
- Dependency analysis
- Production debugging
- Performance regression detection
- Capacity planning

Business events themselves are usually monitored through metrics and logs, while traces provide execution-path visibility.

---

# 21. Recommended Rollout Plan

## Phase 1

```text
Tracing Package

Tracer Provider

OTLP Export

Jaeger Integration

HTTP Middleware
```

---

## Phase 2

```text
WebSocket Instrumentation

Redis Context Propagation

Background Worker Tracing

Replay Tracing

Snapshot Tracing
```

---

## Phase 3

```text
Advanced Sampling

Custom Span Processors

Exemplars

Cross-Cluster Tracing

Performance Optimization
```

---

# Final Recommendation

Build a centralized OpenTelemetry architecture as shared platform infrastructure.

Architecture:

```text
Configuration

↓

Tracing Factory

↓

Tracer Provider

↓

Reusable Component Tracers

↓

Dependency Injection

↓

OTLP Exporter

↓

OpenTelemetry Collector

↓

Jaeger

↓

Future Trace Backends
```

Core principles:

```text
Single Tracer Provider

Reusable Tracers

Dependency Injection

Automatic Context Propagation

Parent-Based Sampling

Low Overhead

Bounded Attributes

Asynchronous Export
```

Operational capabilities:

```text
End-to-End Distributed Tracing

HTTP Tracing

WebSocket Tracing

Redis Propagation

Background Worker Tracing

Replay Visibility

Snapshot Visibility

Cross-Service Correlation

Latency Analysis

Production-Grade Performance
```

This architecture provides a scalable, low-overhead distributed tracing foundation that integrates cleanly with logging and metrics, and reflects the OpenTelemetry deployment patterns commonly adopted by institutional trading platforms, electronic exchanges, and large-scale distributed financial systems.