# Incident Runbook: MassClientDisconnects

**Severity Classification:** CRITICAL
**Domain:** BUSINESS

## 1. Description
>1000 clients have disconnected in 5 minutes.

## 2. Business Impact
Loss of connectivity.

## 3. Possible Causes
- Gateway OOMKill

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `sum(rate(gateway_client_disconnects_total[5m])) > 1000`
- **Related Dashboards:** [/d/market-data-operations](/d/market-data-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=rtmds-gateway`

## 5. Investigation Steps
- Verify Gateway node health.

## 6. Recovery Steps
- Prepare for reconnect storm.

## 7. Escalation SLA
If this issue is not mitigated within **5 minutes** of the alert firing, immediately escalate to the **Tier 2 Engineering Lead**.

## 8. Verification
Ensure the `sum(rate(gateway_client_disconnects_total[5m])) > 1000` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
