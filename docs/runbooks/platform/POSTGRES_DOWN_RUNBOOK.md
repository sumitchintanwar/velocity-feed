# Incident Runbook: PostgresDown

**Severity Classification:** CRITICAL
**Domain:** PLATFORM

## 1. Description
Postgres instance is unreachable.

## 2. Business Impact
Historical replay and gap recovery will fail.

## 3. Possible Causes
- OOMKill
- Disk Full

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `postgres_up == 0 or absent(postgres_up)`
- **Related Dashboards:** [/d/platform-operations](/d/platform-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=postgres`

## 5. Investigation Steps
- Check container status.

## 6. Recovery Steps
- Restart postgres.

## 7. Escalation SLA
If this issue is not mitigated within **5 minutes** of the alert firing, immediately escalate to the **Tier 2 Engineering Lead**.

## 8. Verification
Ensure the `postgres_up == 0 or absent(postgres_up)` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
