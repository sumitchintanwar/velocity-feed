# Grafana Dashboard Architecture — Implementation Guide

**Document ID:** ARCH-GRAFANA-001
**Author:** Senior Site Reliability Engineer — Platform Observability
**Version:** 1.1
**Date:** 2026-06-27
**Last Updated:** Staff SRE Architecture Review (v1.1)

---

## 1. Architecture Overview

The Grafana Dashboard Architecture follows a hierarchical, folder-based organization
that mirrors the operational workflow of a distributed market data platform. Each layer
progressively increases technical detail, enabling operators to move from executive-level
health checks to deep diagnostics within a few clicks.

### 1.1 Layer Model

```
Executive (00)    ← C-suite, trading desk leads
    ↓
Platform (10)     ← SRE, platform engineers
    ↓
Services (20)     ← Service-specific teams
    ↓
Infrastructure (30) ← Infrastructure, DevOps
    ↓
Reliability (50)  ← SRE, incident responders
    ↓
Performance (60)  ← Performance engineers
```

### 1.2 Folder Structure

```
grafana/
├── provisioning/
│   ├── datasources/
│   │   └── prometheus.yml          # Prometheus datasource config
│   └── dashboards/
│       └── dashboards.yml          # Provisioning provider config
├── dashboards/
│   ├── 00-executive/
│   │   └── platform-overview.json  # Executive: Platform health
│   ├── 10-platform/
│   │   └── trading-overview.json   # Trading: Business KPIs
│   ├── 20-services/
│   │   ├── publisher/
│   │   │   └── publisher.json      # Publisher service details
│   │   ├── gateway/
│   │   │   └── gateway.json        # Gateway service details
│   │   ├── replay/
│   │   │   └── replay.json         # Replay service details
│   │   └── snapshot/
│   │       └── snapshot.json       # Snapshot service details
│   ├── 30-infrastructure/
│   │   ├── redis/
│   │   │   └── redis.json          # Redis infrastructure
│   │   └── kubernetes/
│   │       └── kubernetes.json     # Kubernetes cluster
│   ├── 50-reliability/
│   │   └── recovery.json           # Recovery operations
│   └── 60-performance/
│       └── runtime.json            # Go runtime metrics
├── shared/
│   └── variables.json              # Reusable variable definitions
└── README.md                       # This file
```

### 1.3 Naming Convention

All dashboards follow: `<Category> - <Service> - <Purpose>`

| Dashboard | Title | Folder |
|-----------|-------|--------|
| platform-overview | Platform - Overview | 00 Executive |
| trading-overview | Trading - Overview | 10 Platform |
| publisher | Service - Publisher | 20 Services |
| gateway | Service - Gateway | 20 Services |
| redis | Infrastructure - Redis | 30 Infrastructure |
| replay | Service - Replay | 20 Services |
| snapshot | Service - Snapshot | 20 Services |
| recovery | Reliability - Recovery | 50 Reliability |
| kubernetes | Infrastructure - Kubernetes | 30 Infrastructure |
| runtime | Performance - Runtime | 60 Performance |

---

## 2. Shared Variables

All dashboards reuse the same variable definitions for consistency:

| Variable | Label | Query | Purpose |
|----------|-------|-------|---------|
| `datasource` | Data Source | `label_values(prometheus_build_info, datasource)` | Select Prometheus instance |
| `environment` | Environment | `label_values(http_requests_total, environment)` | Filter by deployment env |
| `cluster` | Cluster | `label_values(http_requests_total{environment="$environment"}, cluster)` | Filter by cluster |
| `namespace` | Namespace | `label_values(http_requests_total{cluster="$cluster"}, namespace)` | Filter by K8s namespace |
| `service` | Service | `label_values(http_requests_total{namespace="$namespace"}, service)` | Filter by service |
| `instance` | Instance | `label_values(http_requests_total{service="$service"}, instance)` | Filter by instance |
| `gateway` | Gateway | `label_values(gateway_connections_active, gateway_id)` | Filter by gateway |
| `region` | Region | `label_values(http_requests_total, region)` | Filter by region |
| `exchange` | Exchange | `label_values(publisher_messages_total, exchange)` | Filter by exchange |

### Variable Cascading Order

```
datasource → environment → cluster → namespace → service → instance
```

Each downstream variable filters on the parent variable's selection.

---

## 3. Panel Layout Standard

Every dashboard follows the same panel layout order:

```
Row 1: Overview     (stat panels — current values)
Row 2: Traffic      (time series — rate/request metrics)
Row 3: Latency      (heatmap/time series — duration metrics)
Row 4: Errors       (time series — error rates, counts)
Row 5: Resources    (time series — CPU, memory, goroutines)
Row 6: Dependencies (status — Redis, PostgreSQL health)
Row 7: Detailed     (tables — per-instance breakdown)
```

This layout ensures operators always find the information they need in the same location
regardless of which dashboard they are viewing.

---

## 4. Dashboard Ownership

| Dashboard | Owner | Contact |
|-----------|-------|---------|
| Platform Overview | SRE | #sre-platform |
| Trading Overview | Market Data Engineering | #market-data-eng |
| Publisher | Publisher Team | #publisher-eng |
| Gateway | Gateway Team | #gateway-eng |
| Redis | Infrastructure Team | #infrastructure |
| Replay | Replay Team | #replay-eng |
| Snapshot | Snapshot Team | #snapshot-eng |
| Recovery | SRE | #sre-reliability |
| Kubernetes | Infrastructure Team | #infrastructure |
| Runtime | Performance Engineering | #performance-eng |

---

## 5. Drill-Down Navigation

```
Platform Overview
  ├──→ Service - Publisher
  │      └──→ Performance - Runtime
  ├──→ Service - Gateway
  │      └──→ Infrastructure - Kubernetes
  ├──→ Infrastructure - Redis
  ├──→ Reliability - Recovery
  └──→ Trading - Overview
         └──→ Service - Replay
                └──→ Service - Snapshot
```

Each dashboard includes `links` JSON that provides clickable navigation to related
dashboards. Variables and time range are propagated on navigation.

---

## 6. Implementation Notes

- All dashboards use Prometheus as the sole datasource
- Panel queries use `sum()` aggregation by default (not raw series)
- Recording rules are recommended for expensive aggregations
- Default time range: last 1 hour
- Default refresh: 30 seconds
- No high-cardinality labels (request_id, correlation_id) in queries
- Thresholds set at: Info (green), Warning (yellow), Error (red)
- Units: requests/s, seconds (latency), bytes (memory), count (goroutines)

---

## 7. Dashboard Specifications

### 7.1 Platform Overview (`platform-overview`)
**Folder:** `00-executive`
**Purpose:** Executive-level view of platform health for C-suite and trading desk leads.

**Panels:**
- Overview Row: Pipeline status, messages/sec, P99 latency, error rate, active connections
- Traffic Row: Request rate by service, request rate by status code
- Latency Row: Platform-wide latency percentiles (P50/P95/P99), latency by service
- Errors Row: 5xx errors by service, recovery events over time
- Dependencies Row: Redis status, Redis pub/sub rate, active gateways, goroutine count

**Expected Operational Use:** First-look dashboard when issues arise. If any stat turns red, drill into the relevant service dashboard.

---

### 7.2 Trading Overview (`trading-overview`)
**Folder:** `10-platform`
**Purpose:** Business KPIs and trading metrics for Market Data Engineering.

**Panels:**
- Overview Row: Total messages processed, unique symbols, active publishers
- Traffic Row: Message throughput by exchange, message type distribution
- Latency Row: End-to-end latency percentiles, processing time by stage
- Errors Row: Normalization errors, exchange-specific failures
- Quality Row: Data quality metrics, gap detection

**Expected Operational Use:** Monitor trading data flow quality. Investigate drops in message throughput or spikes in normalization errors.

---

### 7.3 Publisher (`publisher`)
**Folder:** `20-services/publisher`
**Purpose:** Detailed Publisher service metrics for Publisher Team.

**Panels:**
- Overview Row: Publish rate, publish latency P99, publish failures, Redis publish rate
- Traffic Row: Publish rate over time (per instance), Redis publish rate over time
- Latency Row: Publish latency percentiles, latency heatmap
- Errors Row: Publish failures over time, HTTP errors by status
- Resources Row: Goroutines, memory usage, GC duration

**Expected Operational Use:** Monitor publisher throughput and Redis integration health. Investigate publish failures and high latency.

---

### 7.4 Gateway (`gateway`)
**Folder:** `20-services/gateway`
**Purpose:** WebSocket gateway metrics for Gateway Team.

**Panels:**
- Overview Row: Active connections, message rate, connection errors/sec, disconnections/sec
- Traffic Row: Connections over time per instance, message rate over time per instance
- Latency Row: Message processing latency percentiles, latency heatmap
- Errors Row: Connection errors by type, HTTP errors by status
- Resources Row: Goroutines, memory usage, GC duration

**Expected Operational Use:** Monitor WebSocket connection health. Investigate connection drops or high message latency.

---

### 7.5 Redis (`redis`)
**Folder:** `30-infrastructure/redis`
**Purpose:** Redis cache and pub/sub health for Infrastructure Team.

**Panels:**
- Overview Row: Redis status, memory used, connected clients, pub/sub messages/sec, commands/sec
- Traffic Row: Memory usage over time, command rate by type
- Latency Row: Command latency percentiles, slow queries
- Errors Row: Error count over time, rejected connections
- Memory Row: Memory usage %, fragmentation ratio, evicted keys

**Expected Operational Use:** Monitor Redis memory pressure and command latency. Alert on memory > 80% or fragmentation > 1.5.

---

### 7.6 Replay (`replay`)
**Folder:** `20-services/replay`
**Purpose:** Historical data replay metrics for Replay Team.

**Panels:**
- Overview Row: Queue depth, replay rate, recovery progress %, replay errors/sec
- Traffic Row: Queue depth over time (per instance), replay rate over time (per instance)
- Latency Row: Replay latency percentiles, time since last replay
- Errors Row: Replay errors by type, gap detection events
- Progress Row: Recovery progress %, ETA to catchup, messages replayed

**Expected Operational Use:** Monitor catch-up progress after outages. Alert if queue depth grows unbounded or replay stalls.

---

### 7.7 Snapshot (`snapshot`)
**Folder:** `20-services/snapshot`
**Purpose:** Order book snapshot service for Snapshot Team.

**Panels:**
- Overview Row: Snapshot rate, avg snapshot size, snapshot freshness, generation errors/sec
- Traffic Row: Snapshot rate over time, snapshot size over time
- Latency Row: Generation latency percentiles, time since last snapshot per symbol
- Errors Row: Snapshot errors by type, serialization errors
- Storage Row: Redis storage used, active snapshots, symbols tracked

**Expected Operational Use:** Monitor snapshot generation cadence. Alert if snapshots become stale (indicating order book issues).

---

### 7.8 Recovery (`recovery`)
**Folder:** `50-reliability`
**Purpose:** Recovery operations and failover metrics for SRE Team.

**Panels:**
- Overview Row: Recovery operations/sec, active failovers, MTTR, recovery success rate
- Recovery Events Row: Recovery operations over time, recovery failures by reason
- Failover Row: Failover events over time, failover duration percentiles
- Availability Row: Service availability %, upstream dependency failures
- Health Row: Platform health, degraded services, last incident duration

**Expected Operational Use:** During incidents, monitor recovery progress and failover health. Post-incident, review MTTR and success rates.

---

### 7.9 Kubernetes (`kubernetes`)
**Folder:** `30-infrastructure/kubernetes`
**Purpose:** Kubernetes cluster health for Infrastructure Team.

**Panels:**
- Overview Row: Cluster status, CPU usage %, memory usage %, pod restarts/sec, pending pods
- Pods Row: Pod count by phase, pod restarts over time by namespace
- Resources Row: CPU usage over time by namespace, memory usage over time by namespace
- Nodes Row: Node CPU usage per node, node memory usage per node
- Deployments Row: Deployment replicas (desired vs available), HPA status

**Expected Operational Use:** Monitor cluster capacity and pod health. Alert if CPU > 95%, memory > 90%, or pending pods > 0.

---

### 7.10 Runtime (`runtime`)
**Folder:** `60-performance`
**Purpose:** Go runtime metrics for Performance Engineering.

**Panels:**
- Overview Row: Total goroutines, total heap, GC pause P99, GC rate/min
- Memory Row: Heap allocation over time, heap live set
- GC Row: GC duration percentiles, GC duration heatmap
- Goroutines Row: Goroutine count by service, goroutine creation rate
- Threads Row: OS threads, goroutines blocked on syscall
- Details Row: Stack memory, heap objects, allocation rate

**Expected Operational Use:** Detect goroutine leaks, memory pressure, and GC issues causing latency spikes. Cross-reference with service dashboards during performance investigations.

---

## 8. Provisioning

All dashboards are provisioned via Grafana file provisioning:

```yaml
# dashboards.yml
apiVersion: 1
providers:
  - name: 'RTMDS Executive'
    folder: '00 Executive'
    type: file
    options:
      path: /var/lib/grafana/dashboards/00-executive
  - name: 'RTMDS Platform'
    folder: '10 Platform'
    type: file
    options:
      path: /var/lib/grafana/dashboards/10-platform
  - name: 'RTMDS Services'
    folder: '20 Services'
    type: file
    options:
      path: /var/lib/grafana/dashboards/20-services
      foldersFromFilesStructure: true
  - name: 'RTMDS Infrastructure'
    folder: '30 Infrastructure'
    type: file
    options:
      path: /var/lib/grafana/dashboards/30-infrastructure
      foldersFromFilesStructure: true
  - name: 'RTMDS Reliability'
    folder: '50 Reliability'
    type: file
    options:
      path: /var/lib/grafana/dashboards/50-reliability
  - name: 'RTMDS Performance'
    folder: '60 Performance'
    type: file
    options:
      path: /var/lib/grafana/dashboards/60-performance
```

---

## 9. Infrastructure-as-Code

This directory is fully declarative:
- Dashboards: JSON files versioned in git
- Provisioning: YAML configuration
- Variables: Shared JSON definitions

To update a dashboard:
1. Edit the relevant JSON file
2. Commit to git
3. Restart Grafana or use API to reload

Do not use Grafana UI to make permanent changes — they will be lost on next provision.

---

## 10. Staff SRE Review Improvements (v1.1)

Following the Staff SRE Architecture Review for 100,000 messages/sec production environment,
the following justified improvements were applied:

### 10.1 Critical Business Metrics Added (HIGH Priority)

**Platform Overview** — Added Data Integrity section:
- **Max Data Staleness**: Feed timestamp vs wall clock lag. At 100k msgs/sec, staleness > 5s indicates downstream issues
- **Sequence Gaps/sec**: Out-of-order or missing sequence numbers. Triggers replay. Critical for market data accuracy
- **Queue Saturation %**: Worker pool queue utilization. **Leading indicator** of dropped messages — alert before drops occur

**Platform Overview** — Added Database Health section:
- **PostgreSQL Status**: EventLog DB connectivity. DOWN = replay unavailable, clients cannot recover
- **DB Active Connections**: Connection pool utilization. Near-max = query failures
- **DB Slow Queries/sec**: Slow query rate. High rate = need for index optimization
- **DB Query Latency P99**: EventLog query latency. High latency impacts replay performance

### 10.2 Leading Indicator Metrics Added (HIGH Priority)

**Publisher** — Added Queue Health section:
- **Queue Utilization %**: Queue depth / capacity as percentage. Alert at 70% yellow, 90% red
- **Queue Depth vs Capacity**: Absolute values over time. When depth approaches capacity, messages will be dropped

**Replay** — Added Queue Health section:
- **Queue Utilization %**: Replay queue saturation. Growing queue = catch-up scenario
- **Queue Depth vs Capacity**: Shows actual vs max capacity over time

### 10.3 Dependency Health Added (HIGH Priority)

**Replay** — Added Database section:
- **EventLog DB Status**: PostgreSQL health for replay API
- **DB Slow Queries/sec**: Query performance monitoring
- **DB Query Latency P99**: Critical for replay performance
- **DB Connection Pool Usage**: Pool saturation monitoring

**Replay** — Added Cache section:
- **Snapshot Cache Hit Ratio**: Hit ratio stat panel
- **Cache Hits vs Misses Over Time**: Time series showing cache efficiency
- **Cache Hit Ratio Over Time**: Per-instance ratio. **Cold cache = DB crushing during replay spike**

### 10.4 Visualization Improvements (MEDIUM Priority)

**Gateway** — Fixed "Connections Per Gateway":
- Changed from stat panel (shows sum) to **bar gauge** showing per-gateway breakdown
- Immediately identifies uneven load distribution or gateway-specific issues

**Gateway** — Fixed "Disconnect Reasons":
- Changed from generic "Connection Errors Over Time" to **Disconnect Reasons** breakdown
- Shows: Client closed, Heartbeat Timeout, Server Error
- **Makes root cause actionable** — route to correct team based on reason

### 10.5 What Was Already Adequate

- Latency heatmaps already existed in Publisher, Gateway dashboards
- Redis Memory Usage % threshold already set correctly at 80% red
- Panel layout standard already consistent across dashboards

### 10.6 Alert Design Notes

Based on review recommendations:
- Queue saturation alerts should fire at 70% (yellow), 90% (red) — before messages drop
- Cache hit ratio < 50% (yellow), < 10% (red) indicates cold cache DB pressure
- Data staleness > 1s (yellow), > 5s (red) indicates downstream issues
- DB connection pool > 50% (yellow), > 90% (red) indicates capacity issues
