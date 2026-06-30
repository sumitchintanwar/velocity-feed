# Incident Runbook: RedisDown

**Severity Classification:** CRITICAL
**Domain:** PLATFORM

## 1. Description
Redis instance has been down for 1 minute. Market data pipeline is halted.

## 2. Business Impact
Total system failure. Zero market data is flowing to clients.

## 3. Possible Causes
- OOMKill
- Hardware failure

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `redis_up == 0 or absent(redis_up)`
- **Related Dashboards:** [/d/platform-operations](/d/platform-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=redis`

## 5. Investigation Steps
- Check container status: `docker ps`

## 6. Recovery Steps
- Restart the container: `docker restart rtmds-redis`

## 7. Escalation SLA
If this issue is not mitigated within **5 minutes** of the alert firing, immediately escalate to the **Tier 2 Engineering Lead**.

## 8. Verification
Ensure the `redis_up == 0 or absent(redis_up)` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
