# Production-Grade Health, Readiness & Liveness Framework

## Distributed Market Data Platform

**Author:** Principal Software Engineer – Platform Reliability

---

# Table of Contents

1. Purpose
2. Design Goals
3. Overall Architecture
4. Health Check Lifecycle
5. Health vs Readiness vs Liveness
6. Package Structure
7. Dependency Injection Strategy
8. Health Check Categories
9. Concurrent Execution Model
10. Timeout Handling
11. Failure Handling
12. Graceful Degradation
13. Kubernetes Integration
14. Docker Integration
15. Performance Considerations
16. Operational Dashboards & Alerting
17. Common Production Mistakes
18. Future Extensibility
19. Trading System Best Practices
20. Final Recommendation

---

# 1. Purpose

The Health Framework provides a standardized mechanism for determining whether a service is:

* Alive
* Ready to receive traffic
* Operating correctly
* Degraded
* Recovering
* Unavailable

The framework serves multiple consumers:

* Kubernetes
* Docker
* Load Balancers
* Monitoring Systems
* Alerting Systems
* Operations Dashboards
* SRE Teams

---

# 2. Design Goals

The framework should provide:

* Reusable architecture
* Consistent behavior across services
* Dependency injection support
* Low overhead
* Concurrent execution
* Configurable timeouts
* Graceful degradation
* Extensible health checks
* Fast response times
* Operational visibility

---

# 3. Overall Architecture

```text
                Service

                    │

                    ▼

            Health Manager

                    │

    ┌───────────────┼───────────────┐

    ▼               ▼               ▼

 Liveness      Readiness       Health

    │               │               │

    ▼               ▼               ▼

 Registered     Registered     Registered
 Checks          Checks         Checks

    │               │               │

    └───────────────┼───────────────┘

                    ▼

            Health Result

                    ▼

            HTTP Endpoints

        /live
        /ready
        /health
```

The Health Manager acts as the central coordinator.

---

# 4. Health Check Lifecycle

```text
Service Startup

↓

Register Checks

↓

Start Service

↓

Probe Request

↓

Execute Checks

↓

Aggregate Results

↓

Determine Status

↓

Return Response

↓

Monitoring Systems Consume Result
```

Checks should execute independently and produce structured results.

---

# 5. Health vs Readiness vs Liveness

---

## Liveness

Question:

```text
Is the process alive?
```

Purpose:

Detect deadlocked or crashed services.

Examples:

* Main loop active
* Event loop active
* Worker supervisor active

Should NOT:

* Check Redis
* Check PostgreSQL
* Check external systems

If liveness fails:

```text
Restart Process
```

---

## Readiness

Question:

```text
Can this instance safely receive traffic?
```

Examples:

* Redis available
* PostgreSQL available
* Required worker pools healthy
* Topic Manager initialized

If readiness fails:

```text
Remove From Load Balancer
```

Process remains running.

---

## Health

Question:

```text
How healthy is the service overall?
```

Health supports:

```text
Healthy
Degraded
Unhealthy
```

Includes:

* Dependencies
* Runtime state
* Internal services
* Queue depth
* Resource pressure

---

# 6. Package Structure

```text
internal/platform/health

├── manager
│
├── checker
│
├── registry
│
├── liveness
│
├── readiness
│
├── health
│
├── result
│
├── status
│
├── middleware
│
├── handlers
│
├── config
│
└── probes
```

---

## manager

Coordinates execution.

Responsibilities:

* Execute checks
* Aggregate results
* Determine final status

---

## checker

Defines reusable health check contracts.

---

## registry

Stores registered checks.

Supports:

* Dynamic registration
* Service-specific checks

---

## probes

Reusable infrastructure probes.

Examples:

```text
Redis
PostgreSQL
Filesystem
Network
```

---

# 7. Dependency Injection Strategy

Health Manager should be injected into services.

```text
Application

↓

Health Manager

↓

Checks

↓

Dependencies
```

Checks receive dependencies through constructors.

Benefits:

* Testability
* Isolation
* Reusability

---

# 8. Health Check Categories

---

## Infrastructure Checks

### Redis

Validate:

* Connectivity
* Command execution
* Latency

---

### PostgreSQL

Validate:

* Connectivity
* Query execution
* Pool availability

---

### Filesystem

Validate:

* Disk accessibility
* Read capability
* Write capability

---

### Network

Validate:

* Required endpoints reachable
* DNS resolution

---

## Application Checks

### Publisher

Validate:

* Publish pipeline active
* Worker pools healthy
* Internal queues operational

---

### Topic Manager

Validate:

* Subscription registry healthy
* Routing operational

---

### Replay Service

Validate:

* Replay workers active
* Event log accessible

---

### Snapshot Service

Validate:

* Snapshot cache healthy
* Update pipeline active

---

### Recovery Manager

Validate:

* Recovery scheduler operational
* Recovery queue healthy

---

## Runtime Checks

### Goroutines

Monitor:

```text
Count
Growth Rate
Leak Detection
```

---

### Memory

Monitor:

```text
Heap Usage
Allocation Rate
Pressure
```

---

### Queue Depth

Monitor:

```text
Publisher Queues
Gateway Queues
Replay Queues
```

---

### Worker Pools

Validate:

```text
Active Workers
Idle Workers
Queue Backlog
```

---

# 9. Concurrent Execution Model

Checks should execute concurrently.

```text
Probe Request

↓

Spawn Check Tasks

↓

Parallel Execution

↓

Collect Results

↓

Aggregate Status
```

Benefits:

* Lower latency
* Faster responses
* Independent execution

Slow checks should not block healthy checks.

---

# 10. Timeout Handling

Every check should have:

```text
Individual Timeout
```

Example categories:

| Check Type | Timeout    |
| ---------- | ---------- |
| Runtime    | Very Short |
| Redis      | Short      |
| PostgreSQL | Short      |
| Filesystem | Short      |
| Network    | Moderate   |

If timeout occurs:

```text
Check Fails
```

The entire endpoint must never hang.

---

# 11. Failure Handling

Failures should be isolated.

Example:

```text
Redis Down

↓

Redis Check Fails

↓

Readiness Fails

↓

Liveness Remains Healthy
```

Avoid cascading failures.

Health checks must never crash the service.

---

# 12. Graceful Degradation

Not every dependency failure should trigger full outage.

Example:

```text
Replay Service Down

↓

Market Data Still Flows

↓

Health = Degraded

↓

Readiness = Healthy
```

Severity should match operational impact.

Recommended states:

```text
Healthy

Degraded

Unhealthy
```

---

# 13. Kubernetes Integration

---

## Liveness Probe

Endpoint:

```text
/live
```

Purpose:

Restart dead processes.

---

## Readiness Probe

Endpoint:

```text
/ready
```

Purpose:

Control traffic routing.

---

## Startup Probe

Endpoint:

```text
/ready
```

Used during:

* Snapshot loading
* Recovery
* Cache warming

---

## Benefits

```text
Automatic Restarts

Traffic Isolation

Rolling Deployments

Safer Recovery
```

---

# 14. Docker Integration

Expose:

```text
/live
/ready
/health
```

Docker health checks should use:

```text
/ready
```

for operational readiness.

---

# 15. Performance Considerations

Health endpoints must be lightweight.

Recommendations:

### Cache Expensive Results

Avoid:

```text
Database Query Every Request
```

Use:

```text
Periodic Refresh
```

---

### Avoid Blocking Operations

Checks should never block request threads.

---

### Limit Execution Time

Use strict deadlines.

---

### Minimize Allocations

Health endpoints are called frequently.

Optimize:

* Object reuse
* Result aggregation

---

# 16. Operational Dashboards & Alerting

Health results should feed:

### Grafana

Panels:

```text
Healthy
Degraded
Unavailable
```

---

### Prometheus

Metrics:

```text
Health Status

Readiness Status

Dependency Status
```

---

### Alerting

Alerts:

```text
Redis Unhealthy

Replay Degraded

Recovery Failed

Publisher Unhealthy
```

---

# 17. Common Production Mistakes

---

## Dependency Checks Inside Liveness

Wrong:

```text
Redis Failure

↓

Liveness Failure

↓

Restart Storm
```

---

## Slow Health Checks

Health endpoints should remain fast.

---

## Blocking Readiness

Avoid large initialization tasks in request path.

---

## No Timeouts

Can cause probe hangs.

---

## Excessive Health Checks

Health checks should be lightweight.

---

# 18. Future Extensibility

Future checks:

```text
Exchange Adapters

Market Data Normalization

Aggregation Engine

Order Book Engine

Recorder

FIX Gateways

Kafka

NATS
```

Framework should support dynamic registration.

---

# 19. Trading System Best Practices

Large trading platforms typically:

### Separate Concerns

```text
Liveness

Readiness

Health
```

Strictly independent.

---

### Dependency-Aware Readiness

Traffic only reaches healthy instances.

---

### Graceful Degradation

Minor failures do not trigger service restarts.

---

### Fast Checks

Health responses generally return within milliseconds.

---

### Centralized Framework

All services share the same health package.

This improves operational consistency.

---

# 20. Final Recommendation

Implement a centralized Health Framework with:

* Shared health package
* Dependency injection
* Concurrent execution
* Timeout protection
* Graceful degradation
* Kubernetes integration
* Docker integration
* Prometheus integration
* Dashboard integration
* Alert integration

Recommended endpoints:

```text
/live

/ready

/health
```

Recommended status model:

```text
Healthy

Degraded

Unhealthy
```

Recommended architecture:

```text
Health Manager

↓

Registered Checks

↓

Concurrent Execution

↓

Result Aggregation

↓

Probe Endpoints
```

This design mirrors the approach used by large-scale trading and market data systems, where rapid failure detection, controlled traffic routing, and graceful degradation are critical to maintaining platform reliability under both normal operation and market stress conditions.
