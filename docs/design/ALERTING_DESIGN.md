````md
# Production Alert Rules & Operational Runbooks Design
## Production-Grade Distributed Market Data Platform

**Author:** Principal Site Reliability Engineer – Production Operations

---

# Table of Contents

1. Purpose
2. Design Goals
3. Alert Rule Architecture
4. Rule Organization
5. Threshold Selection Strategy
6. Alert Metadata & Annotations
7. Infrastructure Alert Rules
8. Platform Alert Rules
9. Market Data Alert Rules
10. Performance Alert Rules
11. Business Alert Rules
12. Operational Runbook Design
13. Dashboard Integration
14. Escalation Workflow
15. Operational Response Workflow
16. Common Mistakes
17. Production Best Practices
18. Final Recommendation

---

# 1. Purpose

Production alerts should detect operational issues early while remaining actionable and trustworthy.

Every alert should answer:

- What failed?
- How severe is it?
- Who owns it?
- Where do I investigate?
- How do I recover?
- When should it escalate?

An alert without a clear action should not exist in production.

---

# 2. Design Goals

The alerting framework should provide:

- High signal-to-noise ratio
- Low false-positive rate
- Clear ownership
- Actionable notifications
- Consistent severity model
- Dashboard integration
- Runbook integration
- Automated escalation
- Minimal operator guesswork

---

# 3. Alert Rule Architecture

```text
Prometheus Metrics
        │
        ▼
Recording Rules
        │
        ▼
Alert Rules
        │
        ▼
Alertmanager
        │
        ▼
Grouping
        │
        ▼
Routing
        │
        ▼
Notification
        │
        ▼
Runbook
        │
        ▼
Dashboard
        │
        ▼
Logs & Traces
```

Use recording rules to precompute expensive expressions and keep alert evaluations fast.

---

# 4. Rule Organization

Organize rules by operational domain.

```text
alerts/
├── infrastructure/
│   ├── cpu
│   ├── memory
│   ├── disk
│   └── network
│
├── platform/
│   ├── publisher
│   ├── gateway
│   ├── redis
│   └── postgres
│
├── market-data/
│   ├── throughput
│   ├── sequence
│   ├── replay
│   ├── snapshot
│   └── recovery
│
├── performance/
│   ├── latency
│   ├── runtime
│   ├── queues
│   └── backpressure
│
└── business/
    ├── clients
    ├── subscriptions
    └── replay
```

This mirrors service ownership and simplifies maintenance.

---

# 5. Threshold Selection Strategy

Thresholds should be:

- Based on historical baselines
- Sustained for a minimum duration
- Reviewed regularly
- Different for warning and critical levels

Avoid alerting on short-lived spikes.

Typical strategy:

- **Warning:** sustained degradation requiring investigation.
- **Critical:** customer-impacting condition requiring immediate action.

---

# 6. Alert Metadata & Annotations

Every alert should include:

- Alert name
- Severity
- Summary
- Detailed description
- Service
- Environment
- Cluster
- Owner
- Dashboard link
- Runbook link
- Suggested first steps

Example metadata:

```text
Alert Name
Severity
Service
Environment
Owner
Summary
Description
Dashboard
Runbook
```

This enables operators to move directly from notification to diagnosis.

---

# 7. Infrastructure Alert Rules

## CPU

Alert on sustained CPU saturation.

Severity:

- Warning
- Critical

Recovery:

- Identify hot processes
- Check deployment changes
- Scale if necessary

---

## Memory

Detect sustained memory pressure.

Recovery:

- Inspect heap usage
- Review recent releases
- Check for leaks
- Scale if required

---

## Disk

Monitor:

- Capacity
- I/O latency
- Free space

Recovery:

- Clean old data
- Expand storage
- Verify WAL growth

---

## Network

Monitor:

- Packet loss
- Interface saturation
- Error rates
- Connection failures

Recovery:

- Validate node networking
- Check cloud networking
- Review recent changes

---

# 8. Platform Alert Rules

## Publisher Unavailable

Alert when publisher instances become unavailable.

Runbook:

- Check pod health
- Inspect logs
- Validate Redis connectivity
- Restart if necessary

---

## Gateway Unavailable

Recovery:

- Verify pod status
- Confirm readiness
- Validate load balancer
- Check WebSocket accept rate

---

## Redis Unavailable

Recovery:

- Verify health
- Confirm replication
- Inspect latency
- Validate client connectivity

---

## PostgreSQL Unavailable

Recovery:

- Check connections
- Verify storage
- Review replication
- Confirm write availability

---

# 9. Market Data Alert Rules

## Throughput Drop

Detect unexpected decreases in market data flow.

Investigate:

- Feed Generator
- Exchange connectivity
- Publisher
- Redis
- Gateways

---

## Sequence Gaps

Critical integrity alert.

Investigate:

- Publisher ordering
- Redis delivery
- Exchange adapter
- Recovery pipeline

---

## Dropped Messages

Detect increasing message loss.

Possible causes:

- Slow consumers
- Queue overflow
- Backpressure
- Gateway overload

---

## Replay Failures

Monitor replay success rate.

Recovery:

- Check PostgreSQL
- Validate replay workers
- Inspect storage latency

---

## Snapshot Failures

Monitor snapshot generation.

Recovery:

- Validate snapshot cache
- Check storage
- Review memory pressure

---

## Recovery Failures

Detect unsuccessful recovery attempts.

Investigate:

- Replay
- Snapshot
- Ordering
- Redis availability

---

# 10. Performance Alert Rules

## Latency

Monitor:

- Feed latency
- Publisher latency
- Redis latency
- Gateway latency
- End-to-end latency

---

## GC Pauses

Alert on prolonged garbage collection pauses.

Investigate:

- Heap growth
- Allocation rate
- Recent releases

---

## Goroutine Explosion

Detect abnormal goroutine growth.

Recovery:

- Inspect leaks
- Review worker pools
- Examine blocked goroutines

---

## Queue Buildup

Monitor internal queue depth.

Recovery:

- Check downstream services
- Increase capacity if appropriate
- Investigate slow consumers

---

## Backpressure

Alert when publishers or gateways are throttling.

Investigate:

- Client performance
- Queue utilization
- Redis throughput

---

# 11. Business Alert Rules

## Client Disconnect Spikes

Monitor unexpected increases in disconnects.

Possible causes:

- Gateway failures
- Network instability
- Client deployments

---

## Subscription Spikes

Detect abnormal subscription growth.

Investigate:

- New deployments
- Abuse
- Customer events

---

## Abnormal Replay Activity

Detect unusual replay demand.

Potential causes:

- Recovery events
- Client misuse
- Operational incidents

---

# 12. Operational Runbook Design

Every runbook should follow the same structure.

## Overview

Purpose and business impact.

## Symptoms

Observable signs.

## Possible Causes

Most likely root causes.

## Diagnostics

- Dashboards
- Metrics
- Logs
- Traces

## Recovery Steps

Step-by-step recovery actions.

## Verification

How to confirm recovery.

## Escalation

When and how to escalate.

## Related Documentation

- Architecture
- Service docs
- Incident history

---

# 13. Dashboard Integration

Every alert should link directly to:

- Platform Operations Dashboard
- Market Data Operations Dashboard
- Service-specific dashboard
- Relevant trace
- Relevant logs
- Runbook

Operators should reach diagnostic information with minimal navigation.

---

# 14. Escalation Workflow

```text
Alert Triggered

↓

Primary On-Call

↓

Acknowledged?

↓

Yes

↓

Investigate

↓

Resolved

OR

Escalate

↓

Secondary On-Call

↓

Engineering Lead

↓

Incident Commander
```

Escalation timing should depend on alert severity.

---

# 15. Operational Response Workflow

```text
Alert

↓

Dashboard

↓

Metrics

↓

Trace

↓

Logs

↓

Root Cause

↓

Mitigation

↓

Recovery

↓

Verification

↓

Incident Closure
```

Use the same workflow for all services to reduce cognitive load during incidents.

---

# 16. Common Mistakes

Avoid:

- Alerting on transient spikes
- Missing ownership
- No runbook
- Duplicate alerts
- High-cardinality alert labels
- Static thresholds that ignore normal workload variation
- Alerting on symptoms instead of root causes
- Failing to review noisy alerts

---

# 17. Production Best Practices

- Define clear service ownership.
- Keep alerts actionable.
- Base thresholds on historical data.
- Use recording rules for expensive calculations.
- Group related alerts.
- Inhibit downstream alerts when upstream dependencies fail.
- Review alert quality after every incident.
- Test runbooks regularly.
- Link alerts directly to dashboards, traces, and logs.
- Continuously tune thresholds as workloads evolve.

Large financial institutions emphasize **high-confidence alerts** that drive rapid, consistent operational responses rather than generating excessive notifications.

---

# 18. Final Recommendation

Adopt a production-grade alerting strategy built around:

- Domain-based alert organization
- Baseline-driven thresholds
- Rich alert annotations
- Standardized runbooks
- Dashboard-first investigation
- Clear ownership
- Structured escalation
- Continuous alert tuning

Every alert should be **actionable**, **owned**, **linked to diagnostics**, and **supported by a documented recovery procedure**. By integrating Prometheus, Alertmanager, Grafana, logs, and traces into a unified operational workflow, the market data platform can achieve rapid incident detection, efficient troubleshooting, and resilient production operations while maintaining a low false-positive rate and a high signal-to-noise ratio.
````
