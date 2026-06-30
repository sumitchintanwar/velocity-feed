# ADR 0002: PostgreSQL as the Immutable Event Store

**Date:** 2026-06-28
**Status:** Approved
**Purpose:** To document the decision to use PostgreSQL for historical market data retention instead of Cassandra, InfluxDB, or Kafka.
**Intended Audience:** Architects, Data Engineers.
**Maintenance Strategy:** Immutable historical record.

## Context
Since we decided to use Redis Pub/Sub for live routing (ADR 0001), we required a parallel system to durably store every tick. This datastore is explicitly used by the `Replay` API to reconstruct missed messages, and the `Snapshot` API to calculate current state.

## Options Considered
1. **Apache Cassandra:** Excellent write throughput for time-series data, but complex to operate and requires significant JVM tuning.
2. **InfluxDB:** Great for metrics, but lacks strict transactional guarantees required for financial auditing.
3. **PostgreSQL:** ACID compliant, rock-solid reliability, natively supports JSONB for extensible market tick payloads, but vertical write scaling has limits.

## Decision
We decided to adopt **PostgreSQL** as the immutable event store.

## Consequences
- **Positive:** We inherit decades of operational tooling, backup strategies (pgBackRest), and JSONB indexing capabilities which make querying by sequence ID extremely efficient.
- **Negative:** PostgreSQL cannot handle 1,000,000 writes/sec naively.
- **Mitigation:** The `Publisher` service implements an asynchronous batch-writer. It queues incoming ticks in memory and executes bulk `INSERT` statements to PostgreSQL, easily absorbing the I/O cost while keeping the live Redis publish path sub-millisecond.
