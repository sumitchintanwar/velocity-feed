# Full Protocol Integration Plan

This plan details the final steps to fully integrate the newly built serialization layer (JSON, Protocol Buffers, FlatBuffers), protocol negotiation, and client SDK into the existing websocket gateway. 

## Modified Files
1. **`internal/websocket/gateway.go`**
   - **Changes**: Add `protocol.NewFlatBuffersSerializer()` to the gateway's `protocol.Registry`.
   - **Why**: The JSON and Protobuf serializers are already registered, but FlatBuffers was left pending until implementation was complete.

2. **`pkg/client/client.go`**
   - **Changes**: Add `protocol.NewFlatBuffersSerializer()` to the client's internal registry. Update the default format list to include FlatBuffers if no specific formats are requested.
   - **Why**: Allows the Go client SDK to negotiate and deserialize FlatBuffers payload natively.

## New Packages / Files
3. **`internal/websocket/protocol_integration_test.go`**
   - **Purpose**: A full end-to-end integration test suite.
   - **Flow**: It will spin up an in-memory test instance of the `Gateway`. It will use the `pkg/client` SDK to connect three separate clients — one enforcing JSON, one Protobuf, and one FlatBuffers. It will verify that market events broadcasted by the gateway are correctly received and deserialized by all three clients.

## Integration Flow
1. **Client Connects**: The client SDK dials the websocket endpoint, setting the `Sec-WebSocket-Protocol` HTTP header to its preferred formats (e.g. `v1.flatbuffers.rtmds, v1.protobuf.rtmds, v1.json.rtmds`).
2. **Server Negotiates**: The `Gateway` reads the header, uses `protocol.Negotiator` to find the highest-priority matching protocol, and sets the response header.
3. **Serializer Selected**: Both client and server use the negotiated string to retrieve the correct `protocol.Serializer` from their respective `protocol.Registry`.
4. **Publishing**: When a market event arrives at the gateway, the gateway calls `Serialize()` on the matched serializer and writes the bytes to the socket.
5. **Consumption**: The client reads the bytes, calls `Deserialize()`, and pushes the typed `MarketEvent` to the application channel.

## Deliverables
- **Code Modifications**: Minimal lines added to `gateway.go` and `client.go`.
- **Integration Tests**: The `protocol_integration_test.go` file.
- **Benchmark Documentation**: I will produce a finalized `protocol_benchmark_documentation.md` summarizing the latency, throughput, and memory results.
- **Migration Notes**: I will produce a `migration_notes.md` detailing how existing JSON clients are affected and how to migrate to the new formats.

> [!IMPORTANT]
> Since the gateway currently has `JSON` and `Protobuf` integrated, the changes to the core files will be extremely minimal (under 5 lines of code). The bulk of the work will be writing the integration test suite and documentation. 

Does this plan look good to proceed?
