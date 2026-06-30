# Incident Runbook: DiskSpaceRunningOut

**Severity Classification:** CRITICAL
**Domain:** INFRASTRUCTURE

## 1. Description
Node is critically low on disk space.

## 2. Business Impact
PostgreSQL will halt writes, causing complete system paralysis for historical replay.

## 3. Possible Causes
- Log rotation failure
- Postgres WAL buildup
- Unbounded local cache

## 4. Diagnostics & Observability
- **Related Metrics (PromQL):** `(node_filesystem_avail_bytes{fstype!~'tmpfs|ramfs'} / node_filesystem_size_bytes{fstype!~'tmpfs|ramfs'} * 100) < 10`
- **Related Dashboards:** [/d/platform-operations](/d/platform-operations)
- **Related Traces:** Search Jaeger for tags `error=true` and `service=infrastructure`

## 5. Investigation Steps
- Find large directories: `du -sh /* | sort -hr | head -n 10`.
- Check if Docker volumes are orphaned: `docker system df`.

## 6. Recovery Steps
- Prune docker images: `docker system prune -a`.
- Expand EBS volume size in AWS/GCP console.

## 7. Escalation SLA
If this issue is not mitigated within **5 minutes** of the alert firing, immediately escalate to the **Tier 2 Engineering Lead**.

## 8. Verification
Ensure the `(node_filesystem_avail_bytes{fstype!~'tmpfs|ramfs'} / node_filesystem_size_bytes{fstype!~'tmpfs|ramfs'} * 100) < 10` metric returns to nominal baselines (0 or within SLA) for at least 3 consecutive minutes before closing the incident.
