# Incident Runbook: SequenceGapDetected

**Severity Classification:** CRITICAL
**Domain:** MARKET_DATA

## 1. Description
Data integrity loss.

## 2. Business Impact
Clients see fragmented orderbooks.

## 3. Possible Causes
- UDP drop

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `sum(rate(publisher_sequence_gaps_total[2m])) > 0`
- **Related Dashboards:** [/d/market-data-operations](/d/market-data-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=market_data`

## 5. Investigation Steps
- Check UDP stats.

## 6. Recovery Steps
- Increase OS UDP buffer.

## 7. Escalation SLA
If this issue is not mitigated within **5 minutes** of the alert firing, immediately escalate to the **Tier 2 Engineering Lead**.

## 8. Verification
Ensure the `sum(rate(publisher_sequence_gaps_total[2m])) > 0` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
