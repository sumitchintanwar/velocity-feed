# Production-Grade WebSocket Correlation & Context Propagation Framework
## Distributed Market Data Platform

**Author:** Principal Software Engineer – Distributed Trading Infrastructure

---

# Goal

Design a production-grade **WebSocket Correlation and Context Propagation Framework** that extends the existing Correlation ID Framework to support persistent, bidirectional WebSocket connections.

Unlike HTTP, where each request is independent, WebSocket connections remain active for long periods and transport thousands or millions of messages. The framework must provide end-to-end observability while minimizing latency and memory overhead.

The design integrates seamlessly with the platform's existing observability infrastructure:

- Structured Logging (Zap)
- Context-Aware Logging
- Prometheus Metrics
- OpenTelemetry
- Correlation ID Framework
- W3C Trace Context
- W3C Baggage
- Redis Context Propagation
- Worker Context Propagation

---

# Current Architecture

```text
                Exchange Adapter
                       │
                       ▼
               Feed Generator
                       │
                       ▼
                  Publisher
                       │
                       ▼
                 Topic Manager
                       │
                       ▼
                 Redis Pub/Sub
                       │
        ┌──────────────┴──────────────┐
        ▼                             ▼
   Gateway A                     Gateway B
        │                             │
        ▼                             ▼
  WebSocket Clients            WebSocket Clients
```

---

# Objectives

```text
Connection-Scoped Context

Message-Scoped Context

Automatic Correlation

Automatic Trace Context

Automatic Baggage Propagation

Minimal Allocations

Thread Safe

Dependency Injection

Middleware Driven

Production Ready
```

---

# 1. Overall Architecture

```text
                   HTTP Upgrade
                        │
                        ▼

             WebSocket Handshake Middleware

                        │

                Create Connection Context

                        │

      ┌─────────────────┼──────────────────┐
      ▼                 ▼                  ▼

 Reader Loop      Writer Loop       Heartbeat Loop

      │                 │                  │

      ▼                 ▼                  ▼

Message Context    Outbound Context   Connection Health

      │

      ▼

Business Logic

      │

      ▼

Redis

Workers

Replay

Snapshot

Recovery
```

The connection owns long-lived context, while every message derives a lightweight child context.

---

# 2. Connection Lifecycle

```text
TCP Connect

↓

HTTP Upgrade

↓

Authenticate

↓

Correlation Middleware

↓

Connection Context Created

↓

Read / Write Loop

↓

Heartbeat

↓

Business Messages

↓

Graceful Shutdown

↓

Context Cancelled

↓

Connection Closed
```

The connection context exists only for the lifetime of the WebSocket session.

---

# 3. Context Lifecycle

Two context scopes exist.

## Connection Context

Created once.

Contains:

```text
Correlation ID

Trace Context

Baggage

Connection Metadata

Logger

Cancellation

Authentication Context
```

---

## Message Context

Created for every inbound or outbound message.

Derived from:

```text
Connection Context

↓

New Message Context

↓

New Child Span

↓

Business Handler

↓

Disposed
```

Message contexts are intentionally short-lived.

---

# 4. Handshake Flow

```text
HTTP Upgrade Request

↓

Extract Trace Context

↓

Extract Baggage

↓

Extract Correlation ID

↓

Validate

↓

Generate Missing Values

↓

Authenticate

↓

Create Connection Context

↓

Store Connection Context

↓

Upgrade Complete
```

After the handshake, no further protocol-level negotiation is required for context initialization.

---

# 5. Connection-Scoped vs Message-Scoped Context

## Connection Context

Lifetime:

```text
Several Minutes

Hours

Possibly Days
```

Stores:

- Connection metadata
- Authentication state
- Base logger
- Base trace context
- Base baggage
- Cancellation token

Never mutated after creation.

---

## Message Context

Created per message.

Stores:

- Message span
- Request ID (optional)
- Message timestamp
- Operation metadata
- Handler-specific values

Destroyed immediately after processing.

This prevents memory growth over long-lived sessions.

---

# 6. Span Hierarchy

Connection establishment creates a root span.

Example:

```text
WebSocket Connection

├── Authentication

├── Subscribe

├── Subscribe

├── Heartbeat

├── Publish Message

├── Publish Message

├── Unsubscribe

├── Replay Request

├── Snapshot Request

└── Disconnect
```

Each message becomes a child span.

Background tasks initiated by messages continue the same trace.

---

# 7. Correlation Strategy

Every connection receives one Correlation ID.

```text
Connection

↓

Correlation ID

↓

Every Message

↓

Redis

↓

Workers

↓

Replay

↓

Snapshot

↓

Recovery
```

Messages should not generate new Correlation IDs.

Instead:

```text
Connection Correlation ID

+

Optional Request ID

+

Child Span
```

This preserves a single business workflow identity while allowing fine-grained tracing.

---

# 8. Inbound and Outbound Propagation

## Inbound

```text
Client

↓

WebSocket Frame

↓

Message Context

↓

Business Handler

↓

Redis

↓

Workers
```

---

## Outbound

```text
Redis

↓

Gateway

↓

Message Context

↓

WebSocket Frame

↓

Client
```

If downstream consumers understand trace metadata, it may be embedded in protocol-specific message envelopes.

For browser clients, propagation is typically limited to server-side observability unless the application protocol explicitly carries metadata.

---

# 9. Worker Interaction

A WebSocket message may enqueue asynchronous work.

Flow:

```text
Message Context

↓

Capture Context

↓

Worker Queue

↓

Worker Restores Context

↓

Execute

↓

Complete
```

Workers inherit:

```text
Correlation ID

Trace Context

Baggage

Logger Context

Cancellation
```

Retries continue using the same context.

---

# 10. Error Handling

Errors should preserve context.

Pipeline:

```text
Handler Error

↓

Logger

↓

Trace

↓

Metrics

↓

Client Response
```

Every error includes:

```text
Correlation ID

Trace ID

Span ID

Connection ID

Operation

Component
```

The connection remains active unless the error is terminal.

---

# 11. Connection Termination

Shutdown sequence:

```text
Stop Reads

↓

Drain Writes

↓

Cancel Context

↓

Finish Active Spans

↓

Flush Logs

↓

Release Resources

↓

Close Socket
```

Cancellation propagates automatically to background tasks that depend on the connection context.

---

# 12. Reconnect Behavior

A reconnect represents a new transport session.

```text
Disconnect

↓

Reconnect

↓

New TCP Connection

↓

New Handshake

↓

New Connection Context
```

Generate:

- New Connection ID
- New Trace Root
- New Correlation ID (default)

If the application protocol supports session resumption, a previously issued Correlation ID may be reused under controlled conditions. This should be an explicit application-level decision rather than automatic behavior.

---

# 13. Middleware Composition

Recommended order:

```text
Panic Recovery

↓

Connection Context

↓

Correlation Middleware

↓

Trace Middleware

↓

Logger Middleware

↓

Metrics Middleware

↓

Authentication

↓

Business Handler
```

Message middleware:

```text
Message Context

↓

Child Span

↓

Request Timer

↓

Business Logic

↓

Metrics

↓

Logging
```

---

# 14. Dependency Injection

Startup:

```text
Configuration

↓

Logger

↓

Tracer

↓

Metrics

↓

Correlation Framework

↓

WebSocket Middleware Factory

↓

Gateway
```

Handlers receive only the derived context.

They remain unaware of propagation mechanics.

---

# 15. Performance Considerations

The framework must support tens of thousands of concurrent connections.

Guidelines:

```text
Immutable Connection Context

Lightweight Message Context

No Reflection

No Global Locks

Shared Logger

Shared Tracer

Minimal Allocations

Efficient Context Derivation
```

Avoid:

- Per-message logger creation
- Per-message baggage parsing
- Repeated metadata validation
- Large context objects

---

# 16. Common Implementation Mistakes

## Creating New Correlation IDs Per Message

This destroys workflow continuity.

Use one Correlation ID per connection or logical session.

---

## Reusing Message Contexts

Message contexts are short-lived.

Never cache or reuse them across messages.

---

## Long-Lived Spans

Do not leave a single span open for the lifetime of the connection.

Instead:

```text
Connection Span

+

Child Message Spans
```

---

## Mutable Shared Context

Connection context should be immutable after initialization.

---

## Manual Context Copying

Propagation belongs in middleware and infrastructure.

Business handlers should never copy Correlation IDs or trace metadata manually.

---

## Leaking Context After Disconnect

Always cancel the connection context and release associated resources when the session ends.

---

# 17. Security Considerations

Validate all client-provided observability metadata.

Best practices:

- Validate incoming Trace Context.
- Validate Baggage size and key/value limits.
- Reject malformed Correlation IDs.
- Limit total metadata size to prevent abuse.
- Never trust client-provided identity without authentication.
- Do not expose internal topology through propagated metadata.
- Avoid including sensitive information in baggage.

The server remains authoritative for context management.

---

# 18. How Production Trading Platforms Propagate Context

Institutional trading platforms typically model WebSocket observability around **session context plus message context**.

Architecture:

```text
Connection Context

↓

Message Context

↓

Business Logic

↓

Redis

↓

Workers

↓

Replay

↓

Snapshot

↓

Logging

Tracing

Metrics
```

Common characteristics:

- One immutable context per connection.
- Lightweight derived context per message.
- Automatic propagation through asynchronous boundaries.
- Child spans for message processing.
- Context-aware logging by default.
- Shared middleware for all gateways.
- Explicit cancellation on disconnect.
- Minimal allocation to preserve low-latency message handling.
- No observability logic embedded in business handlers.

This approach scales to large gateway clusters while maintaining complete observability across persistent connections.

---

# Recommended Package Structure

```text
internal/

    websocket/

        middleware/
        connection/
        context/
        propagation/
        correlation/
        tracing/
        logging/
        metrics/
        handshake/
        heartbeat/
        lifecycle/

    observability/

    correlation/
```

Each package owns a single responsibility, enabling reuse across gateway instances and future transports.

---

# Rollout Plan

## Phase 1

```text
Handshake Middleware

Connection Context

Correlation Assignment

Trace Context Extraction

Baggage Extraction
```

---

## Phase 2

```text
Message Context

Child Span Creation

Logger Integration

Metrics Integration

Worker Propagation
```

---

## Phase 3

```text
Replay Integration

Snapshot Integration

Recovery Integration

Advanced Session Resumption

Performance Tuning
```

---

# Final Recommendation

Implement a dedicated **WebSocket Correlation and Context Propagation Framework** based on immutable connection-scoped context and lightweight message-scoped context.

Architecture:

```text
HTTP Upgrade

↓

Handshake Middleware

↓

Connection Context

↓

Message Context

↓

Business Logic

↓

Redis

Workers

Replay

Snapshot

Recovery

↓

Logging

Tracing

Metrics
```

Core principles:

```text
One Connection Context Per Session

One Message Context Per Frame

Automatic Correlation

Automatic Trace Propagation

Automatic Baggage Propagation

Immutable Shared State

Child Span Hierarchy

Dependency Injection

Low Allocation

Minimal Latency
```

Operational capabilities:

```text
End-to-End WebSocket Correlation

Parent-Child Span Relationships

Automatic Context Propagation

Redis and Worker Continuity

Replay and Snapshot Integration

Graceful Connection Shutdown

Safe Reconnection Handling

Production-Grade Performance

Consistent Observability Across Persistent Connections
```

This architecture closely aligns with the gateway designs used in institutional trading systems, where long-lived client sessions are treated as stable execution contexts and each market data operation is instrumented as an independent child activity, providing comprehensive observability without compromising throughput or latency.