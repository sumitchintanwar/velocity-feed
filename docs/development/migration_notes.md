# Protocol Migration Notes

The RTMDS Gateway now supports a multi-protocol architecture (JSON, Protocol Buffers, FlatBuffers). This document outlines how existing JSON clients are affected and how to migrate to the binary formats for improved performance.

## Impact on Existing Clients (Backward Compatibility)

**There is NO breaking change for existing clients.**

By default, the RTMDS Gateway acts as a standard JSON WebSocket API if no specific protocol is requested. Existing clients that connect to `ws://.../ws` without providing a `Sec-WebSocket-Protocol` header will be automatically routed to the JSON serializer. No code changes are required for these clients.

## Migrating to Binary Protocols

To take advantage of reduced bandwidth (Protobuf) or zero-copy high throughput (FlatBuffers), clients must opt-in via WebSocket subprotocol negotiation.

### 1. Using the Go SDK (Recommended)

If you are using the official RTMDS Go SDK (`pkg/client`), update to the latest version. The SDK handles negotiation automatically and falls back gracefully.

```go
// Connects with FlatBuffers preference, then Protobuf, then JSON
c, err := client.Connect(wsURL, client.DefaultOptions())

// Force Protobuf only:
c, err := client.Connect(wsURL, client.DefaultOptions(), protocol.FormatProtobuf)
```

### 2. Standard WebSocket Clients (JS/Python/C++)

If you are implementing a raw WebSocket client, you must provide the `Sec-WebSocket-Protocol` header during the HTTP Upgrade phase.

**Supported Protocols:**
- `v1.flatbuffers.rtmds`
- `v1.protobuf.rtmds`
- `v1.json.rtmds`

**Example in JavaScript:**
```javascript
// The browser WebSocket API accepts a second argument for subprotocols
const ws = new WebSocket("ws://gateway:8080/ws", ["v1.protobuf.rtmds", "v1.json.rtmds"]);

ws.onmessage = async (event) => {
    if (ws.protocol === "v1.protobuf.rtmds") {
        // Decode Protobuf binary from event.data (ArrayBuffer)
    } else {
        // Decode JSON text from event.data
    }
};
```

## Known Limitations

- **Control Messages**: Currently, control messages (like errors, subscription confirmations) are always sent as JSON, regardless of the negotiated protocol. Client implementations must be prepared to handle `MessageText` frames for control messages, and `MessageBinary` frames for the actual market events when using Protobuf or FlatBuffers.
