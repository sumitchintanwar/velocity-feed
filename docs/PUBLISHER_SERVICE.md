# Publisher Service Design

## Real-Time Market Data Distribution System

### Version

Week 1 Architecture Design

### Purpose

The Publisher Service is responsible for receiving market updates from upstream producers (feed simulators, exchange connectors, replay services) and distributing them to internal consumers.

It acts as the central event distribution component of the platform.

The design must:

* Receive market updates
* Publish internally
* Remain transport-agnostic
* Support future Redis integration
* Support future Kafka/NATS integration
* Follow interface-driven design principles

No infrastructure dependency should leak into business logic.

---

# 1. Problem Statement

Market data enters the system from a source:

```text
Feed Generator
      ↓
Market Update
      ↓
Publisher Service
```

The Publisher Service should not know:

* Whether subscribers are WebSockets
* Whether Redis exists
* Whether Kafka exists
* Whether data is persisted

Its responsibility is only to distribute events.

---

# 2. Architectural Role

## Position in the System

```text
Feed Generator
      ↓

Publisher Service
      ↓

+-------------------+
| Internal Event Bus|
+-------------------+
      ↓

Subscription Manager
WebSocket Layer
Persistence Layer
Replay Service
Redis Publisher
```

The Publisher Service becomes the central fan-out mechanism.

---

# 3. Core Responsibilities

## Receive Updates

Accept market events from producers.

Examples:

```text
AAPL  185.22
MSFT  452.14
BTCUSD 67850.25
```

The service should validate basic event structure before publication.

---

## Publish Updates

Distribute updates to registered consumers.

Examples:

```text
Subscription Manager

WebSocket Gateway

Persistence Service

Redis Adapter
```

All consumers receive updates through a common abstraction.

---

## Decouple Producers and Consumers

Producer should not know:

```text
Who receives updates
How many receivers exist
What transport is used
```

Consumer should not know:

```text
Where updates originated
```

This creates loose coupling.

---

## Fan-Out Distribution

One update may be consumed by many services.

Example:

```text
AAPL Update
      ↓

Subscription Manager

WebSocket Layer

Persistence Layer

Metrics Layer
```

Publisher handles distribution.

---

# 4. Design Principles

## Dependency Inversion

High-level modules should not depend on infrastructure.

Bad:

```text
Publisher
    ↓
Redis Client
```

Good:

```text
Publisher
    ↓
Publisher Interface
    ↓
Redis Implementation
```

---

## Open/Closed Principle

Publisher should be open for extension.

Publisher should not require modification when adding:

```text
Redis

Kafka

NATS

Replay
```

New adapters plug in through interfaces.

---

## Single Responsibility

Publisher is responsible only for:

```text
Distributing Events
```

Not:

```text
Persistence

Authentication

WebSockets

Storage
```

---

# 5. Interface Design

## Market Event Abstraction

Represents a market update.

Conceptually contains:

```text
Symbol

Price

Timestamp

Sequence Number
```

Publisher operates on events rather than concrete transports.

---

## Publisher Interface

Purpose:

Defines how events are distributed.

Responsibilities:

```text
Accept Event

Publish Event

Notify Subscribers
```

The application layer depends on this abstraction.

---

## Subscriber Interface

Purpose:

Defines anything capable of consuming events.

Examples:

```text
WebSocket Adapter

Redis Adapter

Replay Recorder

Metrics Consumer
```

Each implementation receives updates through a common contract.

---

## Registration Interface

Purpose:

Manage active subscribers.

Responsibilities:

```text
Register Subscriber

Remove Subscriber

List Subscribers
```

Allows dynamic system composition.

---

# 6. Internal Abstractions

## Event Bus

Central distribution mechanism.

Conceptually:

```text
Publisher
      ↓
Event Bus
      ↓
Subscribers
```

Responsibilities:

* Event routing
* Fan-out
* Decoupling

The Event Bus should not know subscriber details.

---

## Subscriber Registry

Maintains active subscribers.

Example:

```text
WebSocket Consumer

Persistence Consumer

Redis Consumer
```

Publisher queries the registry when distributing events.

---

## Event Dispatcher

Responsible for delivering events to subscribers.

Responsibilities:

```text
Fan-Out

Retry Policy

Failure Isolation
```

Benefits:

Keeps Publisher small and focused.

---

# 7. Event Flow

## Market Update Lifecycle

```text
Feed Generator
      ↓

Publisher Service
      ↓

Event Bus
      ↓

Subscriber Registry
      ↓

Dispatch Event
      ↓

Consumers
```

---

## Example Flow

```text
AAPL Update
      ↓

Publisher
      ↓

Event Bus
      ↓

Subscriber Registry
      ↓

WebSocket Consumer

Persistence Consumer

Metrics Consumer
```

All consumers receive the same event.

---

# 8. Future Redis Integration

## Design Goal

Redis should be an implementation detail.

Publisher should never contain logic such as:

```text
Publish To Redis
```

That couples business logic to infrastructure.

---

## Recommended Architecture

```text
Publisher
      ↓

Subscriber Interface
      ↓

Redis Subscriber
```

Redis becomes just another consumer.

---

## Benefits

Adding Redis requires:

```text
New Adapter
```

Only.

No Publisher changes.

No Application changes.

No Domain changes.

---

# 9. Failure Isolation

## Problem

One subscriber may fail.

Example:

```text
Redis Down
```

while:

```text
WebSockets Healthy
```

Without isolation:

```text
Entire Publishing Pipeline Stops
```

---

## Design Requirement

Subscriber failures must be isolated.

Example:

```text
Redis Failure
      ↓
Logged
      ↓
Other Consumers Continue
```

Publisher remains healthy.

---

# 10. Scalability Considerations

## Current Scope

Week 1:

```text
Single Process

In-Memory Event Bus
```

---

## Future Scope

Redis Integration:

```text
Publisher
      ↓
Redis Adapter
      ↓
Multiple Services
```

---

## Horizontal Scale

Future:

```text
Publisher Instance 1

Publisher Instance 2

Publisher Instance 3
```

connected through:

```text
Redis

Kafka

NATS
```

No architectural redesign required.

---

# 11. Observability

Publisher should expose metrics.

## Throughput

```text
events_received_total

events_published_total
```

---

## Subscriber Metrics

```text
active_subscribers

subscriber_failures
```

---

## Performance Metrics

```text
publish_latency

dispatch_latency
```

---

## Queue Metrics

```text
queue_depth

backpressure_events
```

---

# 12. Responsibilities Matrix

| Component           | Responsibility               |
| ------------------- | ---------------------------- |
| Publisher Service   | Accept and distribute events |
| Event Bus           | Internal routing             |
| Subscriber Registry | Track consumers              |
| Dispatcher          | Fan-out delivery             |
| Redis Adapter       | Publish to Redis             |
| WebSocket Adapter   | Push to clients              |
| Persistence Adapter | Store events                 |
| Metrics Adapter     | Collect statistics           |

---

# 13. Week 1 Deliverables

## Interfaces

* Publisher abstraction
* Subscriber abstraction
* Registry abstraction
* Dispatcher abstraction

## Components

* Publisher Service design
* Event Bus design
* Subscriber Registry design

## Documentation

* Event flow diagrams
* Interface contracts
* Extension strategy

---

# Success Criteria

A successful Publisher Service design:

* Receives market updates
* Publishes internally
* Is transport-agnostic
* Supports fan-out distribution
* Supports future Redis integration
* Supports future Kafka/NATS integration
* Follows dependency inversion principles
* Requires no architectural changes when new consumers are added

The Publisher Service should be the stable core of the event distribution layer, while infrastructure integrations remain replaceable adapters around it.
