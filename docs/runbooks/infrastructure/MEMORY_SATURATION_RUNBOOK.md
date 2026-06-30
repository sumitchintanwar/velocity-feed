# Incident Runbook: HighMemoryUsage

**Severity Classification:** WARNING
**Domain:** INFRASTRUCTURE

## 1. Description
Memory > 85%.

## 2. Business Impact
Risk of OOM.

## 3. Possible Causes
- Leak

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes * 100 > 85`
- **Related Dashboards:** [/d/platform-operations](/d/platform-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=infrastructure`

## 5. Investigation Steps
- Check `docker stats`.

## 6. Recovery Steps
- Restart container.

## 7. Escalation SLA
This is a capacity/degradation warning. Log a Jira ticket. If unresolved within **2 hours**, escalate during normal business hours.

## 8. Verification
Ensure the `(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes * 100 > 85` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
