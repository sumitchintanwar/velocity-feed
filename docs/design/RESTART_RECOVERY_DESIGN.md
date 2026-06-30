# Restart Recovery Design
## Real-Time Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Design a restart recovery strategy for a distributed market data platform.

Current architecture:

```text
                 Feed Generator
                        ↓

                    Publisher
                        ↓

              Persistent Event Log
                        ↓

                 Redis Pub/Sub
                        ↓

      +----------------------------------+
      |                                  |
      ▼                                  ▼

 Snapshot Service                 Gateway Pool
```

Requirements:

```text
Restore Snapshots

Restore Subscriptions Where Practical

Recover Cleanly After Crashes

Minimize Downtime

Avoid Data Corruption
```

---

# 1. Why Restart Recovery Matters

In distributed systems, crashes are expected.

Common causes:

```text
Process Crash

Server Reboot

Container Restart

Deployment Rollout

OOM Kill

Network Failure
```

The goal is not preventing crashes.

The goal is ensuring predictable recovery.

---

# 2. Recovery Principles

A production-grade recovery strategy should provide:

```text
Deterministic Startup

No Manual Intervention

Fast Recovery

Minimal Data Loss

Clear Operational State
```

---

# 3. Recovery Scope

Different components require different recovery strategies.

| Component | Recovery Required |
|------------|------------------|
| Feed Generator | No |
| Publisher | Minimal |
| Redis | Reconnect |
| Snapshot Service | Yes |
| Event Log | Automatic |
| Gateway | Partial |
| WebSocket Clients | Client Reconnect |

---

# 4. Recovery Philosophy

Not all state should be recovered.

State categories:

```text
Persistent State

Recoverable State

Ephemeral State
```

---

## Persistent State

Stored durably.

Examples:

```text
Market Events

Snapshots

Configuration
```

Must survive restarts.

---

## Recoverable State

Can be rebuilt.

Examples:

```text
Redis Subscriptions

Caches

Indexes
```

---

## Ephemeral State

Should not be restored.

Examples:

```text
TCP Connections

WebSocket Sessions

In-Memory Buffers
```

---

# 5. Startup Sequence Overview

Recommended startup sequence:

```text
Start Process
      ↓

Load Configuration
      ↓

Connect Dependencies
      ↓

Recover Persistent State
      ↓

Build In-Memory State
      ↓

Start Data Consumption
      ↓

Accept Traffic
```

---

# 6. Dependency Validation

Before serving traffic:

Verify:

```text
Database Available

Redis Available

Storage Accessible
```

---

Avoid:

```text
Accepting Clients
Before Dependencies Are Ready
```

---

## Dependency Unavailability During Startup

If a required dependency (e.g., the Event Log database) is unavailable at startup:

```text
1. Enter Retry Loop
   with exponential backoff
   (initial: 1s, max: 30s)
      ↓

2. Report Health as "Not Ready"
      ↓

3. Do NOT accept any client traffic
      ↓

4. Once dependency is available,
   resume normal startup sequence
```

The service must **never** crash-loop or enter an undefined state due to a
temporarily unavailable dependency. It must remain in a stable "Not Ready"
state until all dependencies are reachable.

---

# 7. Snapshot Recovery

Snapshots are the most important recoverable state.

---

## Option 1

Rebuild From Event Log

```text
Event Log
      ↓

Replay All Events
      ↓

Reconstruct Snapshot
```

---

Advantages:

```text
Always Correct
```

---

Disadvantages:

```text
Slow Recovery
```

especially after months of data.

---

## Option 2

Persist Snapshot State

Recommended.

```text
Snapshot
      ↓

Periodic Checkpoint
      ↓

Disk
```

---

Recovery:

```text
Load Snapshot File
```

↓

```text
Continue Processing
```

---

Advantages:

```text
Fast Startup

Simple Recovery
```

---

# 8. Snapshot Checkpointing

Periodically persist:

```text
HashMap<Symbol, Snapshot>
```

Example:

```text
Every 30 Seconds

Every 60 Seconds
```

---

## Atomic Checkpoint Writes

Checkpoints must be written atomically to prevent corruption if the process crashes
mid-write. The required pattern is:

```text
1. Write to temporary file  (snapshot_new.tmp)
      ↓

2. fsync() the temporary file to durable storage
      ↓

3. atomically rename  (rename("snapshot_new.tmp", "snapshot.dat"))
      ↓

4. fsync() the parent directory
```

---

## Dual Checkpoint Retention

Keep the **last two** checkpoints on disk:

```text
snapshot.dat       (latest)

snapshot.prev      (previous)
```

If `snapshot.dat` is corrupted or unreadable, fall back to `snapshot.prev`.
If both are corrupted, perform a full replay from the Event Log.

---

Benefits:

```text
Fast Recovery

Bounded Recovery Window

Crash-Safe Writes

Corruption Tolerance
```

---

# 9. Snapshot Recovery Flow

**Critical:** The Snapshot Service must avoid the "Live vs. Replay" overlap race.
Replaying events from the database takes time. If the service only subscribes to the
live Redis stream *after* replay finishes, it will miss events that arrived *during*
replay. The correct flow is:

## Subscribe-First Recovery Pattern

```text
1. Subscribe to Live Redis Stream
      ↓

2. Buffer Incoming Live Events
   (or record the sequence number of the first live event)
      ↓

3. Load Last Checkpoint
      ↓

4. Determine Checkpoint Sequence Number
      ↓

5. Replay Missing Events from Database
   (up to the sequence number captured in step 2)
      ↓

6. Apply Buffered Live Events
      ↓

7. Mark Snapshot Service Ready
```

This guarantees **zero dropped events** during recovery.

---

# 10. Replay-Based Catch-Up

Example:

Checkpoint:

```text
Sequence 1,000,000
```

Crash occurs.

---

Current event log:

```text
Sequence 1,002,500
```

---

Recovery:

```text
Replay

1,000,001

→

1,002,500
```

---

Benefits:

```text
Fast

Accurate

Minimal Replay Volume
```

---

# 11. Gateway Recovery

Gateways contain:

```text
WebSocket Connections

Topic Registries

Client Buffers
```

Most of this state is ephemeral.

---

## Recommendation

Do NOT restore:

```text
WebSocket Connections
```

because they no longer exist.

---

Recovery strategy:

```text
Restart Gateway
      ↓

Reconnect To Redis
      ↓

Accept New Connections
```

---

# 12. Subscription Recovery

A common question:

```text
Should Gateway Subscriptions Be Persisted?
```

---

For market data platforms:

Usually:

```text
No
```

---

Reason:

Subscriptions belong to clients.

Clients should:

```text
Reconnect

Re-Authenticate

Re-Subscribe
```

after disconnect.

---

# 13. Why Client-Driven Recovery Is Better

Example:

```text
Gateway Crash
```

---

Affected clients reconnect.

---

Each client sends:

```text
Subscribe AAPL

Subscribe MSFT

Subscribe NVDA
```

---

Gateway rebuilds state naturally.

---

Benefits:

```text
Simple

Reliable

Stateless Gateways
```

---

# 14. Alternative: Persist Subscription State

Possible approach:

```text
Client ID
      ↓

Subscription List
```

stored externally.

---

Example:

```text
Client 123

AAPL

MSFT

TSLA
```

---

Benefits:

```text
Automatic Restoration
```

---

Problems:

```text
Extra Complexity

State Synchronization

Stale Registrations
```

---

Usually unnecessary.

---

# 15. Redis Recovery

Redis restart:

```text
Redis Offline
```

↓

```text
Connections Lost
```

---

Recovery:

```text
Publisher Reconnects

Gateways Reconnect

Subscriptions Recreated
```

---

System should automatically:

```text
Re-Subscribe To Channels
```

without operator intervention.

---

# 16. Event Log Recovery

Database recovery is usually handled by:

```text
PostgreSQL
```

internally.

---

Platform responsibility:

```text
Reconnect

Resume Writes

Resume Reads
```

---

Avoid:

```text
Manual Recovery Procedures
```

for common failures.

---

# 17. Recovery Ordering

Critical rule:

```text
State First

Traffic Second
```

---

Bad startup:

```text
Accept Clients
      ↓

Recover State
```

---

Good startup:

```text
Recover State
      ↓

Validate Recovery
      ↓

Accept Clients
```

---

# 18. Health Check Integration

Gateway should report:

```text
Not Ready
```

until:

```text
Redis Connected

Snapshot Loaded

Recovery Complete
```

---

Only then:

```text
Ready
```

---

This prevents:

```text
Traffic During Recovery
```

---

## Dynamic Health Downgrades

Health checks are not only a startup mechanism. A running Gateway must
**dynamically downgrade** its health status if it loses connectivity to its
upstream data source (e.g., during a network partition).

```text
Gateway Running (Ready)
      ↓

Network Partition Detected
(Redis connection lost or heartbeat timeout)
      ↓

Health Downgraded to "Not Ready"
      ↓

Proactively Disconnect All WebSocket Clients
      ↓

Clients Forced to Reconnect to Healthy Gateway
```

Gateways must **never** silently hang or continue accepting new clients
when they cannot receive upstream data. Active disconnection is preferred
over silent degradation because it forces clients to find a healthy
Gateway rather than remaining connected to a degraded one.

---

# 19. Failure Scenario: Gateway Crash

Before:

```text
Gateway 2

5000 Clients
```

---

Crash:

```text
Gateway 2 Offline
```

---

Recovery:

```text
Restart
```

↓

```text
Reconnect Redis
```

↓

```text
Ready
```

↓

```text
Clients Reconnect
```

↓

```text
Subscriptions Rebuilt
```

---

# 20. Failure Scenario: Snapshot Service Crash

Before:

```text
Latest Checkpoint

Sequence 1,000,000
```

---

Crash.

---

Recovery:

```text
Load Checkpoint
```

↓

```text
Replay Missing Events
```

↓

```text
Current Snapshot Restored
```

---

# 21. Failure Scenario: Publisher Crash

If the Publisher crashes mid-publish, it must know where it left off to avoid
generating duplicate events.

## Sequence Number Ownership

The **Feed Generator** assigns monotonically increasing sequence numbers to
each event at creation time. The Publisher does **not** generate sequence
numbers — it only publishes events that already carry a sequence number.

## Recovery Flow

```text
1. Publisher Reconnects to Redis
      ↓

2. Reads the last published sequence number
   from Redis (or from the Event Log)
      ↓

3. Resumes publishing from the next sequence number
      ↓

4. Continues normal operation
```

## Duplicate Event Handling

If the Publisher crashes after writing an event to the Event Log but before
acknowledging it, the same event may be published twice on recovery. The
system relies on **sequence number deduplication**:

- Consumers (Snapshot Service, Gateways) must track the last processed
  sequence number.
- Any event with a sequence number **less than or equal to** the last
  processed number is silently discarded.

This guarantees **exactly-once processing** semantics even in the presence
of duplicate publishes.

---

# 22. Failure Scenario: Full Cluster Restart

System startup:

```text
Database
      ↓

Redis
      ↓

Snapshot Service
      ↓

Publisher
      ↓

Gateways
```

---

Benefits:

```text
Dependencies Ready

State Recovered

Traffic Accepted Last
```

---

# 23. Operational Tradeoffs

## Full Event Replay

Advantages:

```text
Perfect Accuracy
```

Disadvantages:

```text
Slow Startup
```

---

## Snapshot Checkpoints

Advantages:

```text
Fast Recovery
```

Disadvantages:

```text
Additional Storage
```

---

## Persisted Subscriptions

Advantages:

```text
Automatic Restoration
```

Disadvantages:

```text
Complexity

State Drift
```

---

## Client Re-Subscription

Advantages:

```text
Simple

Stateless Gateways
```

Disadvantages:

```text
Slightly Longer Client Recovery
```

---

# 24. Recommended Recovery Strategy

## Snapshot Service

```text
Subscribe to Live Redis Stream
      ↓

Buffer Incoming Live Events
      ↓

Load Checkpoint
      ↓

Replay Missing Events (up to buffered sequence)
      ↓

Apply Buffered Events
      ↓

Mark Ready
```

---

## Publisher

```text
Reconnect Redis
      ↓

Read Last Published Sequence Number
      ↓

Resume Publishing from Next Sequence
```

Note: Sequence numbers are assigned by the Feed Generator, not the Publisher.
Consumers must deduplicate by discarding events with sequence numbers
less than or equal to the last processed number.

---

## Gateway

```text
Reconnect Redis

Accept Clients

Rebuild Subscriptions
From Client Requests
```

---

## Clients

```text
Reconnect

Request Snapshot

Replay Missing Events

Re-Subscribe
```

---

# Recommended Recovery Architecture

```text
                 Startup
                    ↓

           Dependency Validation
                    ↓

      Subscribe to Live Redis Stream
                    ↓

           Buffer Live Events
                    ↓

            Snapshot Recovery
                    ↓

           Replay Missing Events
                    ↓

          Apply Buffered Events
                    ↓

             Redis Reconnect
                    ↓

              Gateway Ready
                    ↓

             Client Reconnect
                    ↓

           Subscription Rebuild
```

---

# Final Recommendation

Use:

```text
Checkpointed Snapshots (Atomic Writes)
+
Subscribe-First Recovery Pattern
+
Replay-Based Catch-Up
+
Sequence Number Deduplication
+
Stateless Gateways
+
Client-Driven Re-Subscription
```

Recovery sequence:

```text
Subscribe to Live Stream
      ↓

Buffer Live Events
      ↓

Load Snapshot
      ↓

Replay Missing Events
      ↓

Apply Buffered Events
      ↓

Reconnect Redis
      ↓

Mark Service Ready
      ↓

Accept Traffic
```

Do **not** persist:

```text
WebSocket Connections

Gateway Buffers

In-Memory Subscriber Lists
```

Instead allow clients to:

```text
Reconnect

Request Snapshot

Replay Gaps

Re-Subscribe
```

This provides the best balance between:

```text
Simplicity

Reliability

Fast Recovery

Operational Maintainability
```

while preserving a clean, horizontally scalable architecture.