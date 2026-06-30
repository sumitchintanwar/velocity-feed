# Distributed Tracing Design
## Real-Time Market Data Platform

### Author

Principal Observability Engineer

### Goal

Design a production-grade distributed tracing strategy for a real-time market data platform using OpenTelemetry.

Architecture:

```text
                 Feed Generator
                        ↓

                    Publisher
                        ↓

                 Redis Pub/Sub
                        ↓

      +----------------------------------+
      |                                  |
      ▼                                  ▼

 Snapshot Service                Gateway Cluster
                                        ↓

                                   WebSocket Clients
                                        ↓

                                   Replay API
```

Requirements:

```text
OpenTelemetry

Distributed Tracing

Latency Visibility

Request Correlation

Production Scalability
```

---

# 1. Why Distributed Tracing Exists

Metrics answer:

```text
How Many?
```

Logs answer:

```text
What Happened?
```

Tracing answers:

```text
Where Was Time Spent?
```

---

Example:

Client reports:

```text
Replay Took 3 Seconds
```

Metrics may show:

```text
Replay Latency Increased
```

Logs may show:

```text
Replay Request Received
```

But tracing shows:

```text
Gateway: 5ms

Replay API: 20ms

Database Query: 2950ms

Serialization: 25ms
```

---

Tracing provides:

```text
End-To-End Visibility
```

across distributed services.

---

# 2. OpenTelemetry Architecture

Recommended architecture:

```text
Application
      ↓

OpenTelemetry SDK
      ↓

OTel Collector
      ↓

Tracing Backend
```

Examples:

```text
Jaeger

Tempo

Honeycomb

Datadog

New Relic
```

---

# 3. Core Tracing Concepts

---

## Trace

Represents:

```text
One Logical Request
```

Example:

```text
Replay Request
```

from start to finish.

---

## Span

Represents:

```text
One Unit Of Work
```

inside a trace.

---

Example:

```text
Database Query

Redis Publish

Snapshot Lookup
```

---

# 4. Trace Boundaries

One of the most important design decisions.

Not everything should create a trace.

---

Recommended trace boundaries:

```text
Replay Request

Snapshot Request

Subscription Request

Connection Establishment

Recovery Workflow
```

---

Avoid tracing:

```text
Every Market Tick
```

---

Reason:

```text
10,000+ Updates/Sec
```

would create enormous trace volume.

---

# 5. What Should NOT Be Traced

Avoid:

```text
Per Market Update

Per Price Tick

Per Redis Message

Per WebSocket Send
```

---

Reasons:

```text
Storage Explosion

Collector Overload

Expensive Analysis

Low Signal Value
```

---

Instead trace:

```text
User Flows

Operational Workflows

Recovery Events
```

---

# 6. Trace Hierarchy

Example:

```text
Replay Request
```

creates:

```text
Trace
```

with multiple spans.

---

Hierarchy:

```text
Replay Request
│
├── Gateway Receive
│
├── Replay API
│
│   ├── Validate Request
│   ├── Query Event Log
│   └── Build Response
│
└── Gateway Response
```

---

This provides:

```text
End-To-End Latency Breakdown
```

---

# 7. Recommended Root Spans

Create root spans for:

```text
websocket_connect

snapshot_request

replay_request

subscription_request

recovery_workflow
```

---

These represent:

```text
Business Operations
```

rather than infrastructure details.

---

# 8. Span Naming Convention

Use:

```text
verb.object
```

Examples:

```text
gateway.connect

gateway.subscribe

replay.fetch

snapshot.load

redis.publish

redis.consume
```

---

Benefits:

```text
Consistent Search

Cleaner Dashboards
```

---

# 9. Context Propagation

Distributed traces only work if context flows between services.

---

Each trace contains:

```text
Trace ID

Span ID

Parent Span ID
```

---

Example:

```text
Gateway
```

creates:

```text
Trace ID
```

---

Replay API receives:

```text
Same Trace ID
```

---

Result:

```text
Single End-To-End Trace
```

---

# 10. OpenTelemetry Context Flow

Example:

```text
Gateway
      ↓

Replay API
      ↓

Database Layer
```

All services share:

```text
Trace ID
```

---

Without propagation:

```text
Independent Traces
```

which are almost useless.

---

# 11. Context Propagation Strategy

Use:

```text
W3C Trace Context
```

standard.

---

Benefits:

```text
Vendor Neutral

OpenTelemetry Native

Industry Standard
```

---

Recommended headers:

```text
traceparent

tracestate
```

---

# 12. Correlation Between Logs And Traces

Every log entry should include:

```text
trace_id

span_id
```

---

Benefits:

```text
Jump From Log

→

Trace

→

Root Cause
```

---

Critical for production debugging.

---

# 13. Redis Tracing Challenges

Redis Pub/Sub is asynchronous.

Unlike HTTP:

```text
No Built-In Request Context
```

---

This creates tracing challenges.

---

# 14. Redis Trace Strategy

When publishing:

```text
Trace Context
```

should be attached to:

```text
Message Metadata
```

---

Flow:

```text
Publisher
      ↓

Redis Message
      ↓

Gateway
```

---

Gateway extracts:

```text
Trace Context
```

and continues the trace.

---

# 15. Redis Span Hierarchy

Example:

```text
Replay Recovery
│
├── Replay Fetch
│
├── Redis Publish
│
└── Gateway Consume
```

---

Benefits:

```text
Latency Visibility Across Async Boundaries
```

---

# 16. Redis Attributes

Useful span attributes:

```text
redis.channel

redis.operation

redis.server

message_size
```

---

Avoid:

```text
Price

Volume

Tick Data
```

as attributes.

---

High cardinality is dangerous.

---

# 17. Gateway Tracing

Gateways are critical observation points.

Trace:

```text
Connection Establishment

Subscription Requests

Snapshot Requests

Replay Requests

Recovery Workflows
```

---

Avoid:

```text
Every Market Message
```

---

# 18. Gateway Trace Example

```text
Client Connect
│
├── Authenticate
│
├── Subscribe
│
├── Snapshot Fetch
│
└── Subscription Active
```

---

Useful for:

```text
Client Onboarding Latency
```

---

# 19. Replay API Tracing

Replay is one of the most valuable trace targets.

---

Trace:

```text
Replay Request
│
├── Validation
│
├── Query Event Log
│
├── Deserialize Events
│
├── Build Response
│
└── Send Response
```

---

Benefits:

```text
Identify Slow Queries

Find Serialization Bottlenecks

Detect Large Responses
```

---

# 20. Snapshot Service Tracing

Trace:

```text
Snapshot Request
```

---

Hierarchy:

```text
Snapshot Request
│
├── Lookup Symbol
├── Build Snapshot
└── Return Snapshot
```

---

Useful when:

```text
Snapshot Latency Spikes
```

---

# 21. Trace Attributes

Recommended low-cardinality attributes:

```text
service.name

component

environment

region

gateway_id

operation
```

---

Avoid:

```text
symbol

price

client_id

session_id

request_payload
```

at scale.

---

# 22. Sampling Strategy

Sampling is mandatory.

Tracing everything is impossible.

---

Example:

```text
10,000 Updates/Sec

50,000 Clients
```

---

Unsampled tracing becomes:

```text
Massive Cost

Storage Explosion
```

---

# 23. Head-Based Sampling

Decision made:

```text
At Trace Start
```

Example:

```text
Sample 1%
```

---

Advantages:

```text
Simple

Fast

Low Overhead
```

---

Disadvantages:

```text
Rare Errors May Be Missed
```

---

# 24. Tail-Based Sampling

Decision made:

```text
After Trace Completes
```

---

Can keep:

```text
Errors

Slow Requests

Large Replays
```

---

Advantages:

```text
Higher Signal Quality
```

---

Disadvantages:

```text
More Infrastructure Cost
```

---

# 25. Recommended Sampling Strategy

Production recommendation:

```text
Head Sampling

1-5%
```

for normal traffic.

---

Always keep:

```text
Errors

Timeouts

Slow Requests

Recovery Workflows
```

using tail-based rules.

---

Hybrid approach:

```text
Best Signal-To-Cost Ratio
```

---

# 26. Common Tracing Mistakes

---

## Mistake 1

Tracing Every Tick

Example:

```text
AAPL Update

MSFT Update

NVDA Update
```

---

Result:

```text
Collector Overload

Huge Costs

Noisy Data
```

---

# 27. Mistake 2

Missing Context Propagation

Problem:

```text
Gateway Trace

Replay Trace

Database Trace
```

appear unrelated.

---

Result:

```text
Broken Visibility
```

---

# 28. Mistake 3

High Cardinality Attributes

Bad attributes:

```text
symbol

client_id

price

volume

session_id
```

---

Problems:

```text
Storage Explosion

Slow Queries

Backend Instability
```

---

# 29. Mistake 4

Tracing Internal Loops

Example:

```text
Every Subscriber

Every Publish

Every Buffer Write
```

---

Usually unnecessary.

Focus on:

```text
User-Facing Operations
```

---

# 30. Mistake 5

Ignoring Logs And Metrics

Tracing alone is insufficient.

Observability stack:

```text
Metrics
+
Logs
+
Traces
```

must work together.

---

# 31. Recommended Trace Taxonomy

Top-level operations:

```text
websocket_connect

subscription_request

snapshot_request

replay_request

client_recovery

gateway_reconnect

redis_reconnect
```

---

Infrastructure spans:

```text
redis.publish

redis.consume

db.query

snapshot.lookup
```

---

# 32. Recommended Architecture

```text
                 Applications
                        │
                        ▼

               OpenTelemetry
                        │
                        ▼

                OTel Collector
                        │
                        ▼

               Trace Backend

                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼

      Logs          Metrics         Traces
```

---

# Final Recommendation

Use OpenTelemetry with:

```text
W3C Trace Context

Trace ID Propagation

Log Correlation

Hybrid Sampling
```

Trace:

```text
Replay Requests

Snapshot Requests

Connection Flows

Recovery Workflows
```

Do NOT trace:

```text
Every Market Tick

Every Redis Message

Every WebSocket Frame
```

Use span hierarchies such as:

```text
Replay Request
      ↓

Replay API
      ↓

Database Query
      ↓

Response Build
```

and propagate trace context across:

```text
Gateway

Redis

Replay API

Snapshot Service
```

This approach provides:

```text
End-To-End Latency Visibility

Request Correlation

Scalable Trace Volume

Production-Grade Observability
```

while remaining practical for a high-throughput distributed market data platform.