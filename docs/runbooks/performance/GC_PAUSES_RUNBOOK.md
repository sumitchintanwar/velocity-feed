# Incident Runbook: HighGCPauses

**Severity Classification:** WARNING
**Domain:** PERFORMANCE

## 1. Description
GC pauses are increasing.

## 2. Business Impact
Micro-stutters in processing.

## 3. Possible Causes
- Memory leak

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `rate(go_gc_duration_seconds_sum[5m]) > 0.05`
- **Related Dashboards:** [/d/platform-operations](/d/platform-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=performance`

## 5. Investigation Steps
- Profile heap.

## 6. Recovery Steps
- Restart process.

## 7. Escalation SLA
This is a capacity/degradation warning. Log a Jira ticket. If unresolved within **2 hours**, escalate during normal business hours.

## 8. Verification
Ensure the `rate(go_gc_duration_seconds_sum[5m]) > 0.05` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
