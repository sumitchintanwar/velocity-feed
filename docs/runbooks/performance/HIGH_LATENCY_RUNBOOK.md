# Incident Runbook: HighEndToEndLatency

**Severity Classification:** HIGH
**Domain:** PERFORMANCE

## 1. Description
Latency has spiked > 50% above the 1-hour moving average.

## 2. Business Impact
Traders will execute on stale pricing.

## 3. Possible Causes
- Redis CPU saturation
- Gateway GC pause
- Network congestion

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `histogram_quantile(0.99, sum(rate(end_to_end_latency_seconds_bucket[5m])) by (le)) > (avg_over_time(histogram_quantile(0.99, sum(rate(end_to_end_latency_seconds_bucket[5m])) by (le))[1h:5m]) * 1.5)`
- **Related Dashboards:** [/d/market-data-operations](/d/market-data-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=rtmds-gateway`

## 5. Investigation Steps
- Check 'Pipeline Flow' graph on Market Data Dashboard.
- Check Gateway CPU usage.

## 6. Recovery Steps
- If Gateway CPU is 100%, scale out Gateways.
- If Redis is bottlenecked, verify no background RDB saves are blocking the main thread.

## 7. Escalation SLA
If this issue is not mitigated within **15 minutes** of the alert firing, escalate to the **Platform Operations Team**.

## 8. Verification
Ensure the `histogram_quantile(0.99, sum(rate(end_to_end_latency_seconds_bucket[5m])) by (le)) > (avg_over_time(histogram_quantile(0.99, sum(rate(end_to_end_latency_seconds_bucket[5m])) by (le))[1h:5m]) * 1.5)` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
