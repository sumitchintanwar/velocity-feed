# System Architecture

## Real-Time Market Data Distribution System

---

## High-Level Architecture

```
Feed Generator (Simulator)
         │
         │  Run(ctx) → <-chan Quote
         ▼
    ┌─────────┐
    │ Pipeline │  Connects Feed → Publisher
    └────┬────┘
         │
         │  for q := range quotes { publisher.Publish(ctx, q) }
         ▼
   Publisher Service (MemoryBus)
         │
         │  Publish(ctx, Quote) — non-blocking fan-out
         ▼
    ┌────────────┐
    │ Subscribers │
    └─────┬──────┘
          │
    ┌─────┴──────────────────────┐
    │             │              │
    ▼             ▼              ▼
WebSocket    Persistence     Redis/Kafka
```

---

## Component Inventory

| Package | Component | Responsibility |
|---------|-----------|----------------|
| `internal/marketdata` | `Feed` interface | Abstracts upstream data sources |
| `internal/marketdata/simulator` | `Simulator` | Fake feed for dev/testing |
| `internal/pubsub` | `Bus` interface | Abstracts event distribution |
| `internal/pubsub` | `MemoryBus` | In-memory fan-out implementation |
| `internal/feed` | `Pipeline` | Connects Feed → Publisher |
| `internal/websocket` | `Client` | WebSocket client handler |
| `internal/transport` | `Router` | HTTP routes + middleware |
| `internal/platform` | `Logger`, `Metrics` | Cross-cutting concerns |
| `internal/app` | `App` | DI root, lifecycle management |

---

## Interface Map

### Feed (`internal/marketdata/feed.go`)

```go
type Feed interface {
    Name() string
    Subscribe(symbols ...string) error
    Unsubscribe(symbols ...string) error
    Run(ctx context.Context) (<-chan Quote, error)
}
```

Implementations: `Simulator`, future `Alpaca`, `Polygon`, `Binance`

### Publisher (`internal/pubsub/pubsub.go`)

```go
type Publisher interface {
    Publish(ctx context.Context, q Quote)
}
```

### Subscription (`internal/pubsub/pubsub.go`)

```go
type Subscription interface {
    C() <-chan Quote
    Cancel()
}
```

### Bus (`internal/pubsub/pubsub.go`)

```go
type Bus interface {
    Publisher
    Subscribe(id string, symbols ...string) Subscription
}
```

Implementations: `MemoryBus`, future `RedisBus`, `KafkaBus`

---

## Dependency Graph

```
Config
  ↓
Logger, Metrics
  ↓
Bus (MemoryBus)
  ↓
Feed (Simulator)
  ↓
Pipeline
  ↓
Transport (Router)
  ↓
App
```

All arrows point inward. Domain types (`marketdata.Quote`) have zero imports.

---

## Data Flow

1. **Simulator** generates `Quote` structs at configurable intervals
2. **Pipeline** reads from the feed channel and calls `Bus.Publish()`
3. **MemoryBus** fans out to all subscribers registered for that symbol
4. **WebSocket Client** reads from its subscription channel and serializes to JSON
5. **Client** receives the quote over the wire

---

## Concurrency Model

| Component | Goroutines | Synchronization |
|-----------|-----------|-----------------|
| Simulator | 1 (ticker loop) | `sync.RWMutex` on symbols/prices |
| Pipeline | 1 (caller of `Run`) | `context.Context` for cancellation |
| MemoryBus | 0 (lock-free hot path) | `sync.RWMutex` + `atomic.Bool` per entry |
| WebSocket Client | 2 (readPump + writePump) | `chan` for subscription signals |

---

## Overflow Policy

All channels use non-blocking sends:

```
select {
case ch <- q:    // delivered
default:         // dropped — slow consumer loses events
}
```

This prevents a single slow consumer from blocking the hot path. Drops are counted via `atomic.Uint64` per subscriber and exposed through Prometheus metrics.

---

## File Structure

```
internal/
├── marketdata/
│   ├── types.go          # Quote, Bar, ServerMessage
│   ├── feed.go           # Feed interface
│   ├── clock.go          # Clock interface
│   └── simulator/
│       ├── simulator.go      # Simulator implementation
│       └── simulator_test.go
├── pubsub/
│   ├── pubsub.go         # Publisher, Subscription, Bus interfaces
│   ├── memory.go         # MemoryBus implementation
│   └── memory_test.go
├── feed/
│   ├── pipeline.go       # Feed → Publisher connector
│   └── pipeline_test.go
├── distribution/
│   └── hub.go            # Legacy Hub (replaced by MemoryBus)
├── websocket/
│   └── client.go         # WebSocket client handler
├── transport/
│   ├── router.go         # HTTP router
│   └── middleware.go      # Logging, metrics middleware
├── platform/
│   ├── logger.go         # Zerolog factory
│   └── metrics.go        # Prometheus instruments
├── config/
│   └── config.go         # Viper configuration
└── app/
    └── app.go            # DI root, lifecycle
```

---

## Testing Strategy

| Layer | Package | Approach |
|-------|---------|----------|
| Unit | `simulator`, `pubsub`, `feed` | Mock dependencies, verify behavior |
| Integration | `test/integration` | Full HTTP + WebSocket path |
| Unit | `test/unit` | Hub concurrent safety |

---

## Extension Points

| Future Component | How to Add |
|------------------|-----------|
| Redis distribution | Implement `Bus` interface → `RedisBus` |
| Kafka ingestion | Implement `Feed` interface → `KafkaFeed` |
| Exchange feed | Implement `Feed` interface → `AlpacaFeed` |
| Persistence | Add as subscriber to `MemoryBus` |
| Replay | Implement `Feed` interface reading from storage |
