# Protocol Evolution Strategy

This document outlines the strategy for evolving the real-time market data protocol over time without breaking existing clients. It applies to all supported wire formats (JSON, Protocol Buffers, FlatBuffers).

---

## 1. Version Negotiation

Version negotiation is handled at the transport layer before any business logic is executed. This keeps the schema clean of versioning metadata.

### Subprotocol Handshake
Clients declare their preferred format and version during the WebSocket upgrade using the `Sec-WebSocket-Protocol` header:

```http
GET /stream HTTP/1.1
Sec-WebSocket-Protocol: v2.protobuf.rtmds, v1.protobuf.rtmds, v1.json.rtmds
```

The gateway negotiates the highest mutually supported version.
> [!NOTE]
> Major versions (`v1`, `v2`) imply **breaking changes**. Minor versions are not negotiated; they are handled via backward-compatible schema extensions.

---

## 2. Compatibility Guarantees

We guarantee **Forward** and **Backward** compatibility within a single major version.

* **Backward Compatibility**: New gateways can read messages from old clients. (Old clients don't break when the server upgrades).
* **Forward Compatibility**: Old clients can read messages from new gateways. (Old clients ignore new fields they don't understand).

### Rules for Safe Evolution

To maintain compatibility within a major version (e.g., `v1`):

> [!IMPORTANT]
> **Permitted (Safe)**
> * Add new optional fields.
> * Add new enum values (if clients are designed to ignore unknown enums).
> * Add new event types (using Protobuf `oneof` or FlatBuffers `union`).

> [!WARNING]
> **Prohibited (Breaking)**
> * Removing or renaming existing fields.
> * Changing the data type of an existing field (e.g., `int32` to `string`).
> * Reusing a tag number (Protobuf) or struct offset (FlatBuffers) previously used by a deleted field.
> * Making an optional field required.

---

## 3. Handling Fields Safely

### Supporting Future Fields
* **Protobuf**: All fields in `proto3` are inherently optional. When adding a new field, simply assign it a new, unique field number.
* **FlatBuffers**: Add new fields **only to the end** of a `table` definition. Never insert fields in the middle, as this changes the memory offsets for all subsequent fields.
* **JSON**: Add new keys. Clients must be written to gracefully ignore unknown keys during deserialization.

### Deprecating Fields
When a field is no longer needed, it cannot be physically removed from the schema without risking a break.

1. **Mark as Deprecated**: Annotate the schema.
   * Protobuf: `string old_symbol = 1 [deprecated = true];`
   * FlatBuffers: `old_symbol: string (deprecated);`
2. **Reserve the ID**: Prevent future developers from accidentally reusing the identifier.
   * Protobuf: `reserved 1; reserved "old_symbol";`
3. **Stop Populating**: The gateway stops writing data to the field.
4. **Client Migration**: Clients migrate to newer fields. The deprecated field remains in the schema forever (or until a `v2` major version cut).

---

## 4. Schema Migration & Best Practices

Production systems at scale (like gRPC services at Google or Kafka streams at Confluent) rely on strict discipline to avoid "schema rot."

### 4.1. Protobuf `oneof` for Extensibility
Instead of adding loose fields for new event types, use a wrapper envelope with a `oneof` (or FlatBuffers `union`). This makes it structurally clear when new event types are added, and old clients will silently drop the `oneof` variants they don't know how to route.

```protobuf
message MarketEvent {
  // Common fields...
  oneof payload {
    Tick tick = 10;
    Snapshot snapshot = 11;
    // Safely add new payloads here:
    // OrderImbalance imbalance = 12;
  }
}
```

### 4.2. Avoid "Catch-all" Maps Unless Necessary
While adding a `map<string, string> metadata` field seems like an easy way to evolve a schema without recompiling, it defeats the purpose of strong typing, causes high serialization overhead, and makes it impossible to track field usage or deprecation. Use explicit, strongly-typed fields whenever possible.

### 4.3. The "Ignore Unknown" Rule
**All** clients must be explicitly configured to ignore unknown fields.
* **JSON**: Ensure the JSON parser does not panic on unknown keys (e.g., standard `encoding/json` ignores by default, but strict modes must be disabled).
* **Protobuf**: Protobuf handles this natively by preserving unknown fields in the byte stream.
* **FlatBuffers**: Handles this natively via table offsets.

### 4.4. Enum Evolution Danger
Adding values to an `enum` is safe on the wire, but can crash strongly-typed clients (like C++ or Java) if they use exhaustive `switch` statements without a `default` case.
> [!TIP]
> Always define a `UNKNOWN = 0;` value as the first enum element, and instruct client developers to implement a `default: // handle unknown` branch in all enum `switch` blocks.

### 4.5. Major Version Cuts (The "Clean Slate")
When technical debt accumulates (too many deprecated fields, fundamentally wrong data types), a new major version is cut (`v2`).
1. Define the new `v2` schema from scratch.
2. The gateway implements both `v1` and `v2` serializers.
3. Clients negotiate `v2.protobuf.rtmds` in the WebSocket header.
4. Once all clients have migrated, the `v1` serializer and schemas are deleted from the codebase.
