# Kubernetes Deployment Design
## Distributed Real-Time Market Data Platform

### Author

Principal Platform Engineer

### Goal

Design a production-grade Kubernetes deployment architecture for a distributed market data platform.

Architecture:

```text
                 Feed Generator
                        ↓

                    Publisher
                        ↓

                 Redis Pub/Sub
                        ↓

        +--------------------------------+
        |                                |
        ▼                                ▼

  Gateway Replica 1               Gateway Replica N
        │                                │
        └──────────────┬─────────────────┘
                       │
                 WebSocket Clients

                        │
                        ▼

                  Replay Service
                        │
                        ▼

                  PostgreSQL

         Prometheus  ←──────── Metrics ───────┐
               ↓                              │
            Grafana ──────────────────────────┘
```

Objectives:

```text
High Availability

Horizontal Scalability

Zero-Downtime Deployments

Operational Simplicity

Cloud-Native Design
```

---

# 1. Kubernetes Deployment Philosophy

Each application component should run independently.

Each service owns:

```text
Its Lifecycle

Its Scaling

Its Configuration

Its Health Checks
```

Recommended Kubernetes resources:

```text
Deployment

StatefulSet

Service

Ingress

ConfigMap

Secret

Horizontal Pod Autoscaler
```

---

# 2. Recommended Kubernetes Architecture

```text
                        Internet
                           │
                           ▼

                     Ingress Controller
                           │
                    WebSocket Support
                           │
                           ▼

                    Gateway Service
                           │
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼

    Gateway Pod      Gateway Pod      Gateway Pod

         │                 │                 │
         └─────────────────┼─────────────────┘
                           │
                           ▼

                     Redis Service
                           │
                           ▼

                    Redis StatefulSet
                           │
                           ▼

                  Replay Service
                           │
                           ▼

                PostgreSQL StatefulSet

────────────────────────────────────────────

                Prometheus Service
                        │
                        ▼

                 Prometheus Pod
                        │
                        ▼

                  Grafana Service
                        │
                        ▼

                  Grafana Pod
```

---

# 3. Deployments

Deployments manage stateless applications.

Recommended Deployments:

```text
Gateway

Replay Service

Snapshot Service

Publisher

Feed Generator

Prometheus

Grafana
```

Benefits:

```text
Rolling Updates

Replica Management

Automatic Recovery

Easy Scaling
```

---

# 4. Stateful Applications

Some services require persistent identity and storage.

Recommended StatefulSets:

```text
Redis

PostgreSQL
```

Reasons:

```text
Persistent Volumes

Stable Network Identity

Ordered Startup

Reliable Recovery
```

Deployments are generally unsuitable for databases.

---

# 5. Replica Strategy

Recommended replicas:

| Component | Initial Replicas |
|------------|-----------------:|
| Gateway | 3 |
| Replay Service | 2 |
| Snapshot Service | 2 |
| Publisher | 2 |
| Feed Generator | 1 |
| Prometheus | 1 |
| Grafana | 1 |
| Redis | 1 (or HA later) |
| PostgreSQL | 1 (or HA later) |

Multiple gateway replicas provide high availability for WebSocket clients.

---

# 6. Services

Services provide stable networking.

Recommended Services:

```text
Gateway Service

Redis Service

PostgreSQL Service

Replay Service

Snapshot Service

Prometheus Service

Grafana Service
```

Benefits:

```text
Stable DNS

Load Balancing

Pod Replacement Transparency
```

Pods communicate using service names instead of IP addresses.

---

# 7. Service Communication

```text
Gateway
     │
     ▼

Redis Service

     │
     ▼

Replay Service

     │
     ▼

PostgreSQL Service
```

Monitoring:

```text
Prometheus

↓

Gateway Metrics

Redis Metrics

PostgreSQL Metrics
```

---

# 8. Ingress

Ingress provides external access into the cluster.

Public endpoints:

```text
Gateway

Grafana (Optional)
```

Private services:

```text
Redis

PostgreSQL

Replay Service

Snapshot Service

Prometheus
```

remain internal.

---

# 9. WebSocket Support

The Ingress Controller must support:

```text
Persistent Connections

Connection Upgrades

Long Timeouts
```

Since WebSockets are long-lived, idle timeout values must be configured appropriately.

---

# 10. Load Balancing

Client flow:

```text
Internet

↓

Ingress

↓

Gateway Service

↓

Gateway Pods
```

Kubernetes distributes new connections across gateway replicas.

Existing WebSocket sessions remain attached to their assigned pod.

---

# 11. ConfigMaps

ConfigMaps store non-sensitive configuration.

Examples:

```text
Application Ports

Worker Counts

Log Levels

Feature Flags

Timeouts

Environment Names
```

Benefits:

```text
Versioned

Easy Updates

Environment Specific
```

---

# 12. Secrets

Sensitive information belongs in Secrets.

Examples:

```text
Database Password

Redis Password

TLS Certificates

API Keys

Authentication Secrets
```

Secrets should never appear in:

```text
Container Images

Git Repositories

ConfigMaps

Application Logs
```

---

# 13. Configuration Flow

```text
ConfigMap

↓

Application Configuration

↓

Startup Validation

↓

Ready
```

Secrets are injected separately during startup.

---

# 14. Autoscaling

Stateless services should scale horizontally.

Recommended autoscaling targets:

```text
Gateway

Replay Service

Publisher
```

Avoid autoscaling:

```text
Redis

PostgreSQL
```

These require dedicated scaling strategies.

---

# 15. Autoscaling Metrics

Horizontal Pod Autoscaler (HPA) can scale based on:

```text
CPU Utilization

Memory Utilization

Custom Metrics
```

For market data platforms, custom metrics often provide better scaling signals.

Examples:

```text
Active Connections

Messages Per Second

Worker Queue Depth

Publish Latency
```

---

# 16. Gateway Scaling

Scale gateway replicas when:

```text
Connection Count Increases

CPU Utilization Rises

Publish Latency Increases

Queue Depth Grows
```

Benefits:

```text
Improved Throughput

Reduced Latency

Higher Availability
```

---

# 17. Rolling Updates

Deployment sequence:

```text
New Pod Created

↓

Startup Probe

↓

Readiness Probe

↓

Traffic Shifted

↓

Old Pod Terminated
```

Clients continue using healthy gateway replicas throughout the deployment.

---

# 18. Zero-Downtime Deployment

Recommended flow:

```text
Gateway Replica 1

↓

Gateway Replica 2

↓

Gateway Replica 3
```

Each pod is updated individually after becoming healthy.

This minimizes disruption for WebSocket traffic.

---

# 19. Pod Lifecycle

```text
Scheduled

↓

Container Starts

↓

Startup Probe

↓

Readiness Probe

↓

Traffic Enabled

↓

Liveness Monitoring

↓

Graceful Shutdown
```

---

# 20. Graceful Shutdown

During termination:

```text
Readiness Fails

↓

Stop Accepting New Connections

↓

Drain Existing WebSocket Sessions

↓

Finish Outstanding Work

↓

Terminate
```

This prevents abrupt client disconnects during deployments.

---

# 21. Resource Requests and Limits

Every workload should define:

```text
CPU Requests

CPU Limits

Memory Requests

Memory Limits
```

Benefits:

```text
Predictable Scheduling

Cluster Stability

Fair Resource Allocation
```

---

# 22. Persistent Storage

Persistent Volumes should be attached to:

```text
Redis

PostgreSQL

Prometheus

Grafana
```

Gateway pods remain stateless.

---

# 23. Health Checks

Each pod should expose:

```text
Startup Probe

Liveness Probe

Readiness Probe
```

Readiness determines traffic routing.

Liveness determines restart behavior.

---

# 24. Monitoring Integration

Prometheus should scrape metrics from:

```text
Gateway

Replay Service

Snapshot Service

Redis

PostgreSQL
```

Grafana visualizes cluster health and application metrics.

---

# 25. Logging Strategy

Containers should emit:

```text
Structured JSON Logs

Standard Output

Standard Error
```

Cluster-level log collection can forward logs to centralized storage.

---

# 26. Failure Recovery

Example:

```text
Gateway Pod Fails

↓

Deployment Detects Failure

↓

Replacement Pod Created

↓

Startup Probe

↓

Readiness Probe

↓

Traffic Restored
```

No manual intervention should be required.

---

# 27. High Availability

Recommended availability strategy:

```text
Multiple Gateway Replicas

Multiple Replay Replicas

Multiple Snapshot Replicas

Persistent Redis

Persistent PostgreSQL
```

Avoid single points of failure where practical.

---

# 28. Common Kubernetes Mistakes

## Running Databases as Deployments

Databases require:

```text
Stable Identity

Persistent Storage
```

Use StatefulSets instead.

---

## Exposing Internal Services

Never expose:

```text
Redis

PostgreSQL
```

through Ingress.

Keep them cluster-internal.

---

## Missing Readiness Probes

Without readiness probes:

```text
Traffic

↓

Uninitialized Pods
```

leading to failed requests.

---

## Missing Resource Limits

Unlimited resource consumption may destabilize the cluster.

Always define requests and limits.

---

## Stateful Gateways

Gateway pods should not permanently own application state.

Externalize state whenever possible.

---

# 29. Kubernetes Resource Summary

| Component | Resource Type |
|------------|---------------|
| Gateway | Deployment |
| Replay Service | Deployment |
| Snapshot Service | Deployment |
| Publisher | Deployment |
| Feed Generator | Deployment |
| Redis | StatefulSet |
| PostgreSQL | StatefulSet |
| Prometheus | Deployment |
| Grafana | Deployment |

---

# 30. Complete Deployment Architecture

```text
                    Internet
                        │
                        ▼

                Ingress Controller
                        │
                        ▼

                Gateway Service
                        │
       ┌────────────────┼────────────────┐
       ▼                ▼                ▼

 Gateway Pod      Gateway Pod      Gateway Pod

                        │
                        ▼

                  Redis Service
                        │
                        ▼

               Redis StatefulSet
                        │
                        ▼

               Replay Deployment
                        │
                        ▼

            PostgreSQL StatefulSet

────────────────────────────────────────────

              Prometheus Deployment
                        │
                        ▼

               Grafana Deployment
```

---

# 31. Operational Deployment Workflow

```text
Container Image Built

↓

Push To Registry

↓

Deployment Updated

↓

New Pods Created

↓

Startup Probe

↓

Readiness Probe

↓

Traffic Shifted

↓

Old Pods Removed

↓

Monitoring Confirms Stability
```

---

# Final Recommendation

Deploy stateless components as:

```text
Deployments
```

```text
Gateway

Replay Service

Snapshot Service

Publisher

Feed Generator

Prometheus

Grafana
```

Deploy stateful components as:

```text
StatefulSets
```

```text
Redis

PostgreSQL
```

Use:

```text
Services

Ingress

ConfigMaps

Secrets

Horizontal Pod Autoscalers
```

Implement:

```text
Rolling Updates

Health Probes

Persistent Volumes

Graceful Shutdown

Structured Logging

Prometheus Monitoring
```

Follow cloud-native principles:

```text
Stateless Compute

Externalized Configuration

Persistent State

Automatic Recovery

Horizontal Scaling
```

This architecture provides:

```text
High Availability

Zero-Downtime Deployments

Operational Simplicity

Elastic Scaling

Production-Grade Reliability
```

and closely reflects Kubernetes deployment patterns used in modern distributed trading platforms, market data systems, and other high-throughput, low-latency services.