# Go Client Library Implementation Plan

The goal is to implement a simple Go client library for the RTMDS system that automatically negotiates protocols (JSON, Protocol Buffers, FlatBuffers) and exposes `Connect()`, `Subscribe()`, `Receive()`, and `Reconnect()` methods.

## User Review Required

### The "Internal" Package Problem
Currently, the market data structs (like `marketdata.Quote` and `marketdata.MarketEvent`) and the serialization logic (`protocol.Serializer`) reside in the `internal/` directory:
- `internal/marketdata`
- `internal/protocol`

Go enforces strict import rules: external users of your client library **cannot** import packages inside an `internal/` directory. If our client library exposes a `Receive()` method that returns an `internal/marketdata.MarketEvent`, external users won't be able to import `internal/marketdata` to type-assert the interface into concrete types (e.g. `*marketdata.Quote`). They will be stuck with an unusable interface.

## Proposed Changes

To build a production-grade, consumable Go client library, I propose the following refactoring and additions:

### 1. Refactor Domain Types to `pkg/`
We must move the shared domain types and protocol interfaces from `internal/` to `pkg/` so they are accessible to external consumers of the client library.

#### [NEW] [pkg/marketdata/...](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/pkg/marketdata)
Move the contents of `internal/marketdata` to `pkg/marketdata`.

#### [NEW] [pkg/protocol/...](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/pkg/protocol)
Move the contents of `internal/protocol` to `pkg/protocol`. 

### 2. Update Import Paths
Run a project-wide search-and-replace to update all internal imports from `github.com/sumit/rtmds/internal/marketdata` to `github.com/sumit/rtmds/pkg/marketdata` and similarly for `protocol`. This guarantees the system still compiles and functions exactly as before.

### 3. Implement the Client Library
#### [MODIFY] [pkg/client/client.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/pkg/client/client.go)
Rewrite the existing `pkg/client` to meet your exact specifications:
- **`Connect(url string, formats ...protocol.Format) (*Client, error)`**: Negotiates the subprotocol. It takes the list of supported formats as variadic arguments (defaulting to Protobuf, Flatbuffers, JSON) and uses `gorilla/websocket` with `dialer.Subprotocols`.
- **`Subscribe(symbols ...string) error`**: Maintains the existing fat-client subscription state.
- **`Receive() (marketdata.MarketEvent, error)`**: Uses the `pkg/protocol.Registry` to select the deserializer matching the negotiated protocol, reads the WebSocket bytes, and returns the strongly-typed `MarketEvent`.
- **`Reconnect()`**: Manually triggers a reconnect loop (in addition to automatic reconnects if connection drops), retaining subscriptions.

### 4. Integration Tests
#### [MODIFY] [pkg/client/client_test.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/pkg/client/client_test.go)
Add tests utilizing `httptest.Server` and a mocked or real `Gateway` instance to verify that `Connect`, `Subscribe`, `Receive`, and `Reconnect` successfully work end-to-end, and correctly deserialize binary streams back into `pkg/marketdata.Quote`.

## Verification Plan

### Automated Tests
- Run `go test -v ./pkg/client` to verify end-to-end flow over Protobuf and JSON.
- Run `go test ./...` to ensure no internal packages were broken during the `internal/` -> `pkg/` migration.

### Manual Verification
Ensure external mock files can successfully import `pkg/marketdata` and type assert events yielded from `client.Receive()`.
