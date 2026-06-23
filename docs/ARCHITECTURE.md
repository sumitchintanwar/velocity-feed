# Real-Time Market Data Distribution System

## Week 1 Architecture & Foundation Design

### Author

Principal Engineer Design Review

### Status

Week 1 Design Baseline

---

# 1. Overview

## Goal

Build a production-grade real-time market data distribution platform capable of receiving market events from exchanges and distributing them to internal and external consumers with low latency, high throughput, and horizontal scalability.

The system will evolve into:

- Exchange Feed Simulation
- Topic-Based Subscriptions
- WebSocket Distribution
- Redis Pub/Sub Integration
- Event Replay
- Persistent Storage
- Horizontal Scaling
- Monitoring and Alerting

Week 1 focuses on establishing a maintainable architecture and engineering foundation.

---

# 2. Core Design Principles

## Clean Architecture

Business logic must not depend on infrastructure.

Dependencies flow inward:

```text
Infrastructure
      ↓
Application
      ↓
Domain
```

This allows:

- Easier testing
- Infrastructure replacement
- Reduced coupling
- Long-term maintainability

---

## Interface First Design

Every external dependency should be hidden behind an interface.

Examples:

- Market data publisher
- Message broker
- Storage engine
- WebSocket hub

Benefits:

- Easier mocking
- Easier testing
- Future extensibility

---

## Event Driven Foundation

The entire platform revolves around events.

Examples:

- Price Update
- Trade Event
- Subscription Request
- Replay Request

This prepares the system for:

- Kafka
- Redis
- NATS
- Multi-region replication

without architectural changes.

---

# 3. High-Level Architecture

```text
                +-------------------+
                | Feed Simulator    |
                +---------+---------+
                          |
                          v

                +-------------------+
                | Market Data Core  |
                +---------+---------+
                          |
         +----------------+----------------+
         |                                 |
         v                                 v

+------------------+          +----------------------+
| Subscription     |          | Distribution Layer   |
| Manager          |          |                      |
+------------------+          +----------+-----------+
                                         |
                                         v

                              +----------------------+
                              | WebSocket Clients    |
                              +----------------------+

Future:

          +--------------------+
          | Redis Pub/Sub      |
          +--------------------+

          +--------------------+
          | Persistence Layer  |
          +--------------------+

          +--------------------+
          | Replay Service     |
          +--------------------+
```

---

# 4. Project Structure

```text
market-data-system/

├── cmd/
│
├── internal/
│   ├── domain/
│   ├── application/
│   ├── infrastructure/
│   └── interfaces/
│
├── pkg/
│
├── configs/
│
├── deployments/
│
├── scripts/
│
├── test/
│
├── docs/
│
├── .github/
│
└── docker/
```

---

# 5. Folder Explanations

## cmd/

Contains application entrypoints.

Example future services:

```text
cmd/
├── server/
├── simulator/
├── replay/
└── worker/
```

Why:

- Separate binaries
- Independent deployment
- Clear startup logic

---

## internal/

Contains all application code.

Protected from external imports by Go.

This is the heart of the system.

---

## internal/domain/

Contains pure business concepts.

Examples:

```text
market event
symbol
subscription
topic
```

Responsibilities:

- Domain models
- Domain rules
- Business invariants

Must have:

- Zero infrastructure knowledge
- Zero Redis knowledge
- Zero WebSocket knowledge

Why:

Keeps business logic stable.

---

## internal/application/

Contains use cases.

Examples:

```text
publish market update
subscribe to symbol
unsubscribe
process feed event
```

Responsibilities:

- Orchestrate workflows
- Coordinate domain and infrastructure

Why:

Business workflows remain independent from transport layers.

---

## internal/infrastructure/

Contains implementation details.

Examples:

```text
redis
websocket
storage
logging
metrics
```

Responsibilities:

- Redis clients
- Database clients
- WebSocket implementations
- Monitoring adapters

Why:

Infrastructure changes should not affect business logic.

---

## internal/interfaces/

Contains transport adapters.

Examples:

```text
http handlers
websocket handlers
grpc handlers
```

Responsibilities:

- Convert external requests
- Call application layer

Why:

Separates transport concerns from business logic.

---

## pkg/

Reusable libraries.

Only place code here if:

- Generic
- Reusable
- Independent

Examples:

```text
ringbuffer
eventbus
retry
```

Why:

Avoid accidental sharing of internal business logic.

---

## configs/

Configuration templates.

Examples:

```text
local.yaml
docker.yaml
production.yaml
```

Why:

Centralized configuration management.

---

## deployments/

Deployment artifacts.

Examples:

```text
docker-compose
kubernetes manifests
helm charts
```

Why:

Infrastructure-as-code separation.

---

## scripts/

Developer tooling.

Examples:

```text
run-local
test-all
lint
generate
```

Why:

Repeatable developer workflows.

---

## test/

Cross-package tests.

Examples:

```text
integration tests
end-to-end tests
load tests
```

Why:

Avoid mixing system tests with unit tests.

---

## docs/

Architecture documentation.

Examples:

```text
ADRs
sequence diagrams
api specs
```

Why:

Knowledge retention.

---

## .github/

CI/CD pipelines.

Examples:

```text
build
test
lint
security scans
```

Why:

Automated quality gates.

---

## docker/

Docker assets.

Examples:

```text
Dockerfile
docker-compose
development images
```

Why:

Container concerns isolated from source code.

---

# 6. Interface Design

Week 1 should define interfaces only.

## Feed Source Interface

Purpose:

Abstract exchange connectivity.

Future implementations:

- Simulator
- NSE
- Binance
- Coinbase

---

## Publisher Interface

Purpose:

Publish market events.

Future implementations:

- In-memory
- Redis
- Kafka
- NATS

---

## Subscription Repository Interface

Purpose:

Manage active subscriptions.

Future implementations:

- Memory
- Redis
- Distributed cache

---

## Client Connection Interface

Purpose:

Represent connected consumers.

Future implementations:

- WebSocket
- gRPC stream
- TCP stream

---

## Event Store Interface

Purpose:

Persist events.

Future implementations:

- PostgreSQL
- ClickHouse
- Kafka log
- Object storage

---

## Replay Interface

Purpose:

Retrieve historical events.

Future implementations:

- Database replay
- Redis stream replay
- Kafka replay

---

## Metrics Interface

Purpose:

Hide monitoring implementation.

Future implementations:

- Prometheus
- OpenTelemetry

---

# 7. Dependency Choices

## Router

Recommendation:

Gin

Reason:

- Mature ecosystem
- Fast
- Widely adopted
- Easy middleware support

Alternative:

- Chi

---

## WebSockets

Recommendation:

Gorilla WebSocket

Reason:

- Battle-tested
- Large community
- Stable API

---

## Logging

Recommendation:

Uber Zap

Reason:

- Structured logging
- High performance
- Production standard

---

## Configuration

Recommendation:

Viper

Reason:

- Environment support
- Config files
- Future secret integration

---

## Validation

Recommendation:

go-playground/validator

Reason:

- Industry standard
- Extensive validation support

---

## Metrics

Recommendation:

Prometheus Client

Reason:

- Industry standard
- Excellent Go support

---

## Testing

Recommendation:

Testify

Reason:

- Assertions
- Mocking support
- Cleaner tests

---

## Redis

Recommendation:

go-redis

Reason:

- Most widely used Redis library
- Production proven

---

# 8. Testing Strategy

## Unit Tests

Target:

Application and domain layers.

Requirements:

- No network calls
- No Redis
- No WebSockets

Coverage Goal:

80%+

---

## Integration Tests

Target:

Infrastructure implementations.

Examples:

- Redis integration
- WebSocket integration

Requirements:

Run against real containers.

---

## Contract Tests

Target:

Interface compliance.

Purpose:

Ensure implementations satisfy expected behavior.

Example:

```text
MemoryPublisher
RedisPublisher
KafkaPublisher
```

must pass identical test suites.

---

## End-to-End Tests

Target:

Entire system.

Flow:

```text
Simulator
→ Publish Event
→ Subscription Manager
→ Client
```

Verify:

Message delivery correctness.

---

## Load Testing

Future.

Tools:

- k6
- Vegeta

Metrics:

- Throughput
- Latency
- Memory
- CPU

---

# 9. Docker Strategy

## Development Container

Purpose:

Consistent local environment.

Contains:

- Go
- Air
- Lint tools

---

## Application Container

Multi-stage build.

Stages:

```text
Builder
    ↓
Minimal Runtime Image
```

Benefits:

- Smaller image
- Faster deployments
- Better security

---

## Local Development Compose

Week 1:

```text
app
redis
```

Future:

```text
app
redis
prometheus
grafana
postgres
```

---

## Container Design Principles

### One Process Per Container

Examples:

```text
market-data-server
redis
prometheus
```

Avoid:

Multiple unrelated services in one container.

---

### Immutable Containers

No runtime modifications.

Build once.

Deploy everywhere.

---

# 10. Week 1 Deliverables

## Must Complete

### Architecture

- Folder structure established
- Dependency boundaries defined
- Domain layer created
- Application layer created

### Interfaces

- Feed source interfaces
- Publisher interfaces
- Subscription interfaces
- Client interfaces
- Metrics interfaces

### Tooling

- Docker setup
- CI pipeline
- Linting
- Formatting
- Testing framework

### Documentation

- Architecture document
- ADR template
- Development guide

---

# 11. Success Criteria

At the end of Week 1:

- Architecture is production-ready
- Interfaces are stable
- Dependency boundaries are enforced
- Unit testing framework is operational
- Docker environment works consistently
- Future Redis, replay, and scaling features can be added without restructuring the repository

The primary objective of Week 1 is not feature delivery. The objective is creating an architecture that remains maintainable when the system grows from a single-node simulator into a distributed market data platform.
