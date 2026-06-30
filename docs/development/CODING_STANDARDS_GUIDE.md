# Go Coding Standards

**Purpose:** To define strict engineering conventions for the RTMDS Go codebase.
**Intended Audience:** Software Engineers.
**Maintenance Strategy:** Update when adopting new major Go versions or refactoring core project patterns.

---

## 1. Dependency Injection (DI)
Never use global state (`var DB *sql.DB`). All dependencies (loggers, database connections, Redis clients) must be injected explicitly into struct constructors.
```go
// GOOD
func NewPublisher(logger *zap.Logger, db *sql.DB) *Publisher { ... }

// BAD
func HandlePublish() { globalDB.Exec(...) } // Do not do this.
```

## 2. Interface Definitions
Define interfaces where they are **used**, not where they are implemented. This allows for trivial mocking during unit tests.
```go
// Defined in the Gateway package, implemented by the Redis package
type MessageSubscriber interface {
    Subscribe(ctx context.Context, channel string) (<-chan []byte, error)
}
```

## 3. Concurrency Safety
- Always run `go test -race`.
- Use `sync.Mutex` or `sync.RWMutex` to protect shared maps (like WebSocket client registries).
- Avoid spawning `goroutines` without a bounding mechanism (like a `context.Context` cancellation or a WaitGroup) to prevent memory leaks.

## 4. Error Handling
- Wrap errors with context: `fmt.Errorf("failed to parse tick %s: %w", tickID, err)`.
- Never swallow errors. If an error is truly ignorable, log it explicitly at the `WARN` or `DEBUG` level.

## 5. Performance (Hot Paths)
The core market data routing loop is a hot path.
- Minimize heap allocations. Reuse structs using `sync.Pool` for JSON unmarshaling.
- Avoid reflection (`reflect` package) in the routing tier.
