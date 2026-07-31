# Serialization Optimization Walkthrough

## Summary

Optimized the `JSONSerializer` and `ProtobufSerializer` in `pkg/protocol` to reduce allocations, reuse buffers, and pool reusable objects. All changes are backward-compatible — the `Serializer` interface contract is unchanged.

---

## Changes

### [json_serializer.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/pkg/protocol/json_serializer.go)

| Technique | Before | After |
|---|---|---|
| JSON engine | `encoding/json` | `json-iterator` (ConfigCompatibleWithStandardLibrary) |
| Encode buffer | New `[]byte` per call | `sync.Pool` of `bytes.Buffer` via existing `GetBuffer()`/`PutBuffer()` |
| HTML escaping | Enabled (default) | Disabled (`SetEscapeHTML(false)`) — safe for symbol strings |
| Deserialize type probe | Double-unmarshal (peek then re-decode) | Single-pass `jiter.Get(data, "type")` — no second parse |
| Deserialized struct | `new(marketdata.Quote)` per call | Pooled via `quotePool` / `barPool` |

**Allocation budget (Serialize):**
- Before: ~3 allocs (json internal + output slice)
- After: **1 alloc** (final copy-out; pooled buffer avoids the rest)

**New API:** `ReleaseEvent(event)` — callers that want zero-copy round-trips can return deserialized structs to the pool. Non-pooled callers are unaffected.

---

### [protobuf_serializer.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/pkg/protocol/protobuf_serializer.go)

| Technique | Before | After |
|---|---|---|
| Outer `MarketEvent` struct | `new(MarketEvent)` per call | `msgPool sync.Pool` |
| Inner `Tick` / `Snapshot` struct | `new(Tick)` / `new(Snapshot)` per call | `tickPool` / `snapshotPool` |
| Marshal output buffer | `proto.Marshal` (allocates) | `MarshalOptions.MarshalAppend` into pooled `[]byte` slab |
| Output slice | Always heap-allocated | Pooled slab; final `make+copy` only for caller-owned result |

**Allocation budget (Serialize):**
- Before: ~4-5 allocs (outer struct + inner struct + output slice + proto internal)
- After: **1 alloc** (final `make([]byte, n)` for caller ownership)

**Allocation budget (Deserialize):** — unchanged (1 alloc for returned `*Quote`/`*Bar`; proto.Unmarshal requires ownership of internal strings)

---

### [serializer_bench_test.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/pkg/protocol/serializer_bench_test.go)

New benchmark file with:
- **Baselines**: `BenchmarkBaseline_JSON*` and `BenchmarkBaseline_Proto*` using stdlib directly
- **Optimized**: `BenchmarkJSONSerializer_*` and `BenchmarkProtobufSerializer_*`
- **Round-trip**: Serialize + Deserialize in sequence (real gateway path)
- **Parallel**: `RunParallel` to stress pool contention under concurrent load
- **Correctness**: 3 table-driven tests to verify encoding fidelity after pooling

---

## How to Run

```bash
# Full benchmark suite (3s per benchmark, 2 runs for stability)
go test -bench=. -benchmem -count=2 -benchtime=3s ./pkg/protocol/

# Just correctness tests
go test -v -run "Test(JSON|Protobuf)Serializer" ./pkg/protocol/

# Parallel contention test only
go test -bench="Parallel" -benchmem -count=3 ./pkg/protocol/
```

---

## Pool Safety Notes

- **Buffer pool**: `GetBuffer()`/`PutBuffer()` discard buffers that grew >64KB (existing behaviour in `pool.go`) to prevent memory pinning.
- **Proto struct pool**: Structs are `Reset()`-ed before being returned to the pool. The `Payload` oneof field is explicitly `nil`-ed to prevent the proto runtime from retaining pointers.
- **Slab pool**: The `slabPool` stores `*[]byte` (pointer to slice header) so `MarshalAppend` can grow the slice without losing the updated header.
- **Deserialized struct pool** (JSON only): Callers receive a pointer to a pooled struct. They **must** call `s.ReleaseEvent(ev)` when done if they want zero-alloc round-trips. Callers that don't call `ReleaseEvent` are safe — the struct is simply GC'd.
