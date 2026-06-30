# Incident Runbook: SlowConsumersDetected

**Severity Classification:** HIGH
**Domain:** BUSINESS

## 1. Description
Clients are failing to read from the TCP socket, causing backpressure.

## 2. Business Impact
Gateway buffer fills up, potentially degrading performance for all clients.

## 3. Possible Causes
- Client side processing issue
- Malicious actor

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `sum(gateway_slow_consumers_total) > 50`
- **Related Dashboards:** [/d/market-data-operations](/d/market-data-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=rtmds-gateway`

## 5. Investigation Steps
- Check Gateway logs to identify the specific IP of the slow consumer.

## 6. Recovery Steps
- Surgically disconnect the offending client IP via the Gateway Admin API.
- Do NOT restart the entire Gateway cluster.

## 7. Escalation SLA
If this issue is not mitigated within **15 minutes** of the alert firing, escalate to the **Platform Operations Team**.

## 8. Verification
Ensure the `sum(gateway_slow_consumers_total) > 50` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
