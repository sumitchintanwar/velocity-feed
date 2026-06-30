# Incident Runbook: GatewayQuorumDegraded

**Severity Classification:** HIGH
**Domain:** PLATFORM

## 1. Description
Less than 70% of Gateway instances are healthy.

## 2. Business Impact
Remaining Gateways may become saturated, causing dropped messages.

## 3. Possible Causes
- Node failure
- OOMKill
- Network partition

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `(count(up{job='rtmds-gateway'} == 1) / count(up{job='rtmds-gateway'})) < 0.70`
- **Related Dashboards:** [/d/platform-operations](/d/platform-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=rtmds-gateway`

## 5. Investigation Steps
- Check Gateway logs for panics.

## 6. Recovery Steps
- Scale up new Gateway instances.
- Restart crashed instances.

## 7. Escalation SLA
If this issue is not mitigated within **15 minutes** of the alert firing, escalate to the **Platform Operations Team**.

## 8. Verification
Ensure the `(count(up{job='rtmds-gateway'} == 1) / count(up{job='rtmds-gateway'})) < 0.70` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
