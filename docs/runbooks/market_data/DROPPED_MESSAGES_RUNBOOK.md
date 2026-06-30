# Incident Runbook: MessagesDropped

**Severity Classification:** HIGH
**Domain:** MARKET_DATA

## 1. Description
Internal queues full.

## 2. Business Impact
Clients lose data.

## 3. Possible Causes
- Slow consumers

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `sum(rate(gateway_messages_dropped_total[2m])) > 10`
- **Related Dashboards:** [/d/market-data-operations](/d/market-data-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=market_data`

## 5. Investigation Steps
- Check backpressure.

## 6. Recovery Steps
- Scale Gateways.

## 7. Escalation SLA
If this issue is not mitigated within **15 minutes** of the alert firing, escalate to the **Platform Operations Team**.

## 8. Verification
Ensure the `sum(rate(gateway_messages_dropped_total[2m])) > 10` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
