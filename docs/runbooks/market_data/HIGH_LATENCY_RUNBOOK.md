# Incident Runbook: High End-to-End Latency

**Severity Classification:** CRITICAL / HIGH
**Domain:** MARKET_DATA

## 1. Overview
Market data delivery is severely delayed across the pipeline. P99 latency has exceeded 50ms.

## 2. Symptoms
- Tick-to-Client latency spikes on the dashboard.
- Traders report stale pricing.

## 3. Diagnostics (Actionable Intelligence)
**Dashboard Link:** [Market Data Operations](/d/market-data-operations)
**Prometheus Query:**
```promql
histogram_quantile(0.99, sum(rate(end_to_end_latency_seconds_bucket[5m])) by (le)) > 0.05
```

## 4. Immediate Recovery Steps (Mitigation)
1. Check 'Pipeline Flow' graph on Market Data Dashboard. If lines diverge, Redis is the bottleneck.
2. Check Gateway CPU usage. If CPU is at 100%, scale up Gateway instances.
3. Check for severe backpressure in Gateway logs.

## 5. Escalation SLA
If this issue is not mitigated within **5 minutes** of the alert firing, immediately escalate to the **Tier 2 Engineering Lead**.

## 6. Verification
Ensure the `histogram_quantile(0.99, sum(rate(end_to_end_latency_seconds_bucket[5m])) by (le)) > 0.05` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
