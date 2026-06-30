# Real-Time Market Data System (RTMDS)

A high-throughput, low-latency WebSocket server that ingests normalised market-data quotes from pluggable feed providers and fans them out to connected clients.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                          RTMDS Server                        │
│                                                              │
│  ┌──────────┐   quotes   ┌─────────────┐   fan-out          │
│  │   Feed   │──────────►│     Hub     │──────────► [WS Clients]
│  │ (adapter)│           │(distribution)│                     │
│  └──────────┘           └─────────────┘                     │
│       ▲                        ▲                             │
│  marketdata.Feed          subscribe/                         │
│  interface                unsubscribe                        │
│                                                              │
│  ┌──────────────────────────────────────────────────┐        │
│  │   HTTP Router (chi)                              │        │
│  │   GET /health   GET /ready   GET /ws   GET /metrics │     │
│  └──────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────┘
```

## Directory Layout

```
.
├── api/                    # OpenAPI 3.1 spec
├── cmd/server/             # main() entry point
├── deployments/            # Prometheus / Grafana configs
├── internal/
│   ├── app/                # DI root — wires all components
│   ├── config/             # Viper-backed config loading
│   ├── distribution/       # In-process fan-out Hub
│   ├── marketdata/         # Domain types + Feed interface
│   │   └── simulator/      # Fake feed for dev/tests
│   ├── platform/           # Logger + Prometheus metrics
│   ├── transport/          # chi router + middleware
│   └── websocket/          # Per-client WS handler
├── pkg/client/             # Public Go client SDK
├── scripts/                # Dev helper scripts
└── test/
    ├── integration/        # End-to-end httptest tests
    └── unit/               # Package-level unit tests
```

## Quick Start

### Local (no Docker)

```bash
# 1. Download dependencies
go mod download

# 2. Run with simulator feed
make run

# 3. Subscribe to quotes (requires pip install websocket-client)
python scripts/ws_client.py --symbols AAPL TSLA MSFT
```

### Docker Compose (full stack)

```bash
make docker-up
# Server    → http://localhost:8080
# Prometheus → http://localhost:9090
# Grafana   → http://localhost:3000  (admin / admin)
```

## Configuration

All settings can be provided as environment variables with the `RTMDS_` prefix:

| Variable | Default | Description |
|---|---|---|
| `RTMDS_SERVER_PORT` | `8080` | HTTP listen port |
| `RTMDS_REDIS_ADDR` | `localhost:6379` | Redis address |
| `RTMDS_FEED_SYMBOLS` | _(empty)_ | Comma-separated symbols |
| `RTMDS_LOG_LEVEL` | `info` | `debug\|info\|warn\|error` |
| `RTMDS_LOG_FORMAT` | `json` | `json\|text` |
| `RTMDS_METRICS_ENABLED` | `true` | Enable `/metrics` |

Or pass a YAML/TOML file: `./bin/rtmds --config config.yaml`

## Makefile Targets

```
make build            Compile binary → ./bin/rtmds
make run              Run locally
make test             All tests (race detector + coverage)
make test-unit        Unit tests only
make test-integration Integration tests only
make lint             golangci-lint
make fmt              gofmt
make docker-up        Start full Docker Compose stack
make docker-down      Tear down stack
make clean            Remove build artifacts
make help             Show all targets
```

## WebSocket API

Connect to `ws://localhost:8080/ws`, then send JSON commands:

```json
// Subscribe
{ "action": "subscribe", "symbols": ["AAPL", "TSLA"] }

// Unsubscribe
{ "action": "unsubscribe", "symbols": ["AAPL"] }
```

Received events:

```json
{
  "type": "quote",
  "payload": {
    "symbol": "AAPL",
    "type": "trade",
    "price": 182.34,
    "bid": 182.33,
    "ask": 182.35,
    "volume": 4500,
    "timestamp": "2024-11-01T14:30:00.123Z",
    "provider": "simulator"
  }
}
```

See [`api/openapi.yaml`](api/openapi.yaml) for the full API specification.

## Documentation & Operations

RTMDS maintains comprehensive production documentation:
- **[Onboarding Guide](docs/onboarding/ONBOARDING_GUIDE.md):** Repository tour and architecture reading order.
- **[Operations Manual](docs/operations/OPERATIONS_MANUAL.md):** Routine maintenance, scaling, and startup/shutdown procedures.
- **[Security Guide](docs/security/SECURITY_GUIDE.md):** Authentication, entitlements, and secret management.
- **[Contributing Guide](CONTRIBUTING.md):** PR workflow and contribution guidelines.
- **[Sequence Diagrams](docs/diagrams/SEQUENCE_DIAGRAMS.md):** Visual flow of pub/sub and replay workflows.

## Adding a Real Feed Provider

1. Create `internal/marketdata/<provider>/feed.go`
2. Implement `marketdata.Feed` interface (`Name`, `Subscribe`, `Unsubscribe`, `Run`)
3. Wire it in `internal/app/app.go` — replace the simulator

## License

MIT
