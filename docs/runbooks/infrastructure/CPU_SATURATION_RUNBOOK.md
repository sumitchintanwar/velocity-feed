# Incident Runbook: HighCPULoad

**Severity Classification:** WARNING
**Domain:** INFRASTRUCTURE

## 1. Description
Node CPU load is > 90%.

## 2. Business Impact
Potential throttling.

## 3. Possible Causes
- Traffic surge

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `100 - (avg by (instance) (rate(node_cpu_seconds_total{mode='idle'}[5m])) * 100) > 90`
- **Related Dashboards:** [/d/platform-operations](/d/platform-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=infrastructure`

## 5. Investigation Steps
- Check `top`.

## 6. Recovery Steps
- Scale out.

## 7. Escalation SLA
This is a capacity/degradation warning. Log a Jira ticket. If unresolved within **2 hours**, escalate during normal business hours.

## 8. Verification
Ensure the `100 - (avg by (instance) (rate(node_cpu_seconds_total{mode='idle'}[5m])) * 100) > 90` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
