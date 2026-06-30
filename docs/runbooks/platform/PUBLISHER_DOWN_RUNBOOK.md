# Incident Runbook: PublisherQuorumDegraded

**Severity Classification:** HIGH
**Domain:** PLATFORM

## 1. Description
Less than 70% of Publisher instances are healthy.

## 2. Business Impact
Potential data loss if the remaining publishers cannot handle the exchange load.

## 3. Possible Causes
- Deadlock
- OOMKill

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `(count(up{job='rtmds-publisher'} == 1) / count(up{job='rtmds-publisher'})) < 0.70`
- **Related Dashboards:** [/d/platform-operations](/d/platform-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=rtmds-publisher`

## 5. Investigation Steps
- Check Publisher logs.

## 6. Recovery Steps
- Scale out Publishers.

## 7. Escalation SLA
If this issue is not mitigated within **15 minutes** of the alert firing, escalate to the **Platform Operations Team**.

## 8. Verification
Ensure the `(count(up{job='rtmds-publisher'} == 1) / count(up{job='rtmds-publisher'})) < 0.70` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
