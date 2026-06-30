# Recovery Guide

**Visual Diagram:** [Recovery Workflow Diagram](../diagrams/operations/RECOVERY_WORKFLOW_DIAGRAM.md)

**Purpose:** To define disaster recovery and data restoration procedures.
**Intended Audience:** Disaster Recovery (DR) Teams, SREs.
**Maintenance Strategy:** Must be tested quarterly in a staging environment. Update if backup technology (e.g., pgBackRest) changes.

---

## 1. Catastrophic PostgreSQL Loss (Event Store Failure)

If the primary PostgreSQL cluster corrupts or is lost, the platform loses its ability to serve Snapshots and Replays. **Live data routing via Redis will continue unaffected.**

### Recovery Steps:
1. **Promote the Replica:** If utilizing streaming replication, immediately promote the hot standby to primary.
   ```bash
   kubectl exec -it <standby-pod> -- repmgr standby promote
   ```
2. **Point-In-Time Recovery (PITR):** If the data was logically corrupted (e.g., a bad schema migration), execute a PITR using pgBackRest to a timestamp 5 minutes prior to the incident.
3. **Verify Integrity:** Query the `events` table to ensure sequence continuity.

## 2. Redis Cluster Failure

If the Redis Pub/Sub cluster crashes, live data dissemination completely halts. 

### Recovery Steps:
1. **Automated Failover:** Redis Sentinel should detect the master failure within 5 seconds and promote a replica. The `Gateway` and `Publisher` Go services are configured with robust connection-retry logic and will automatically reconnect to the new master within milliseconds.
2. **Manual Intervention:** If Sentinel fails (Split-Brain scenario), force a failover manually.
   ```bash
   redis-cli -p 26379 sentinel failover rtmds-master
   ```
3. **Client Remediation:** Once Redis recovers, all connected WebSocket clients will detect a sequence gap. The Gateway will instruct clients to automatically hit the `Replay API` to fetch the ticks missed during the 5-second Redis failover window.
