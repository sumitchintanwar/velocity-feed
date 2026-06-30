# Incident Runbook: ZeroThroughputAnomaly

**Severity Classification:** CRITICAL
**Domain:** MARKET_DATA

## 1. Description
Publisher is not sending any messages.

## 2. Business Impact
Clients receive NO tick data.

## 3. Possible Causes
- Exchange disconnect

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `sum(rate(publisher_messages_sent_total[5m])) == 0`
- **Related Dashboards:** [/d/market-data-operations](/d/market-data-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=market_data`

## 5. Investigation Steps
- Check logs.

## 6. Recovery Steps
- Restart publisher.

## 7. Escalation SLA
If this issue is not mitigated within **5 minutes** of the alert firing, immediately escalate to the **Tier 2 Engineering Lead**.

## 8. Verification
Ensure the `sum(rate(publisher_messages_sent_total[5m])) == 0` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
