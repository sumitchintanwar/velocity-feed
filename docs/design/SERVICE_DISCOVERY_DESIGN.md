# Service Discovery Design
## Distributed Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Design a service discovery mechanism for a distributed market data platform supporting:

- Gateway registration
- Dynamic scaling
- Health awareness
- Automatic instance discovery
- Future horizontal growth

Target architecture:

```text
                    Feed Generator
                           ↓

                       Publisher
                           ↓

                    Redis Pub/Sub
                           ↓

     +-----------+-----------+-----------+
     |           |           |           |
     ↓           ↓           ↓           ↓

 Gateway 1   Gateway 2   Gateway 3   Gateway N
```

Requirements:

```text
Dynamic Registration

Automatic Discovery

Health Monitoring

Minimal Operational Complexity
```

---

# 1. What Is Service Discovery?

Service discovery allows services to locate each other automatically.

Without service discovery:

```text
Gateway 1
IP: 10.0.1.5

Gateway 2
IP: 10.0.1.6

Gateway 3
IP: 10.0.1.7
```

must be configured manually.

This becomes difficult as infrastructure grows.

---

## Goal

Allow services to ask:

```text
Where are all healthy gateways?
```

instead of:

```text
Gateway 1 = 10.0.1.5

Gateway 2 = 10.0.1.6

Gateway 3 = 10.0.1.7
```

---

# 2. Why Service Discovery Matters

Consider:

```text
5 Gateway Instances
```

running today.

Tomorrow:

```text
10 Gateway Instances
```

after scaling.

Static configurations become:

```text
Outdated

Error Prone

Difficult To Maintain
```

A discovery system automatically tracks changes.

---

# 3. Service Discovery Responsibilities

A discovery system should provide:

```text
Registration

Deregistration

Health Status

Instance Lookup
```

---

## Registration

When a gateway starts:

```text
Gateway #7
```

registers itself.

---

## Deregistration

When a gateway shuts down:

```text
Gateway #7
```

removes itself.

---

## Health Monitoring

Unhealthy gateways should disappear automatically.

---

## Lookup

Consumers should be able to ask:

```text
List Healthy Gateways
```

and receive an up-to-date answer.

---

# 4. Static Configuration

## Architecture

```text
config.yaml

gateways:
  - 10.0.1.5
  - 10.0.1.6
  - 10.0.1.7
```

Every service reads the configuration.

---

## Advantages

Very simple.

No extra infrastructure.

Easy to understand.

---

## Disadvantages

Manual updates required.

Scaling requires deployment changes.

No health awareness.

No automatic recovery.

---

## Example Problem

Gateway:

```text
10.0.1.6
```

crashes.

Configuration still contains:

```text
10.0.1.6
```

Traffic may continue targeting a dead instance.

---

## Verdict

Good for:

```text
Local Development

Early Prototypes
```

Poor for production systems.

---

# 5. Redis-Based Registry

## Architecture

Redis stores active instances.

Example:

```text
gateway:1
gateway:2
gateway:3
```

Each gateway registers itself.

---

## Registration Flow

Gateway startup:

```text
Register
```

↓

```text
gateway:5
```

↓

```text
Redis
```

---

## Health Tracking

Use TTL-based registration.

Example:

```text
gateway:5
TTL = 30s
```

Gateway periodically refreshes.

---

If gateway dies:

```text
TTL Expires
```

↓

```text
Registration Removed
```

---

## Advantages

Simple.

Uses infrastructure already present.

Easy implementation.

Low operational cost.

---

## Disadvantages

Limited discovery features.

No advanced routing.

No built-in service mesh support.

Health checking is basic.

---

## Verdict

Excellent intermediate solution.

---

# 6. Consul

## Architecture

```text
Gateway
      ↓

Consul Agent
      ↓

Consul Cluster
```

Services register with Consul.

---

## Features

Built-in:

```text
Health Checks

DNS Discovery

Service Registry

Key-Value Store
```

---

## Example

Query:

```text
market-gateway
```

returns:

```text
Gateway 1

Gateway 2

Gateway 3
```

---

## Advantages

Purpose-built for service discovery.

Strong health checking.

Widely used.

Mature ecosystem.

---

## Disadvantages

Additional infrastructure.

Operational overhead.

More complexity.

---

## Verdict

Excellent for large distributed systems.

May be excessive initially.

---

# 7. Kubernetes Service Discovery

## Architecture

```text
Pods
      ↓

Kubernetes Service
      ↓

DNS Discovery
```

Example:

```text
market-gateway.default.svc.cluster.local
```

automatically resolves healthy pods.

---

## Registration

Automatic.

No custom logic required.

---

## Health Awareness

Built-in:

```text
Readiness Probes

Liveness Probes
```

Unhealthy pods are removed automatically.

---

## Scaling

Example:

```text
5 Pods
      ↓
10 Pods
```

Discovery updates automatically.

---

## Advantages

Zero custom discovery code.

Automatic scaling support.

Native health awareness.

Production proven.

---

## Disadvantages

Requires Kubernetes.

More infrastructure complexity.

Not useful outside Kubernetes.

---

## Verdict

Best option when deploying on Kubernetes.

---

# 8. Health Awareness

Health awareness is critical.

A discovery system must distinguish:

```text
Healthy

Unhealthy
```

instances.

---

## Example

Gateway:

```text
Gateway 4
```

experiences:

```text
CPU Saturation

Memory Exhaustion

Redis Disconnect
```

The discovery system should:

```text
Remove Gateway 4
```

from available instances.

---

# 9. Dynamic Scaling

A key requirement.

Example:

```text
5 Gateways
```

during normal load.

---

Traffic increases:

```text
50000 Connections
```

---

Scale to:

```text
10 Gateways
```

Discovery updates automatically.

No configuration changes required.

---

# 10. Comparison

| Feature | Static Config | Redis Registry | Consul | Kubernetes |
|----------|---------------|---------------|---------|------------|
| Simplicity | Excellent | Excellent | Moderate | Moderate |
| Dynamic Discovery | No | Yes | Yes | Yes |
| Health Awareness | No | Basic | Excellent | Excellent |
| Scaling Support | Poor | Good | Excellent | Excellent |
| Operational Cost | Very Low | Very Low | Medium | Medium |
| Production Readiness | Low | Medium | High | High |
| Infrastructure Required | None | Redis | Consul Cluster | Kubernetes |

---

# 11. Market Data Platform Requirements

Current system:

```text
Feed Generator

Publisher

Redis Pub/Sub

Gateway Instances
```

Current scale target:

```text
50,000 Connections
```

Expected growth:

```text
Additional Gateways

Additional Services

Additional Regions
```

---

# 12. Recommended Evolution Path

## Phase 1

```text
Static Configuration
```

Development only.

---

## Phase 2

```text
Redis Registry
```

Production v1.

Provides:

```text
Gateway Registration

Basic Health Awareness

Dynamic Discovery
```

using infrastructure already present.

---

## Phase 3

If Kubernetes is adopted:

```text
Kubernetes Service Discovery
```

becomes primary mechanism.

No custom registry needed.

---

## Phase 4

For very large multi-service deployments:

```text
Consul
```

may become appropriate.

---

# Recommended Architecture

```text
                   Redis Registry
                          ↑
                          │

 Gateway 1 ───────────────┤

 Gateway 2 ───────────────┤

 Gateway 3 ───────────────┤

 Gateway N ───────────────┘

                          ↓

                 Healthy Gateways
```

Gateway startup:

```text
Register
```

Gateway shutdown:

```text
Deregister
```

Gateway failure:

```text
TTL Expiration
```

removes stale entries automatically.

---

# Final Recommendation

For a market data platform currently using:

```text
Redis Pub/Sub
+
Multiple Gateway Instances
```

the simplest practical approach is:

```text
Redis-Based Service Registry
```

because it provides:

```text
Dynamic Registration

Basic Health Awareness

Automatic Discovery

Minimal Operational Overhead

No Additional Infrastructure
```

Use:

```text
Gateway Registration
+
TTL Heartbeats
+
Redis Registry
```

today.

If the platform later migrates to Kubernetes:

```text
Kubernetes Service Discovery
```

should replace the custom registry entirely.

This provides the best balance between:

```text
Simplicity

Scalability

Operational Cost

Production Readiness
```

for the current stage of the project.