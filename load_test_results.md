# Serialization Load Test Results

**Tool**: `cmd/serload/main.go`
**Scale**: 100k, 500k, 1M serialized market messages
**Environment**: Local (Intel Core i5-1135G7 @ 2.40GHz, Windows)
**Methodology**:
- Generate `count` dummy MarketEvents (80% Quotes, 20% Bars)
- Force GC (`runtime.GC()`)
- Serialize all events into memory, measure time, throughput, memory, allocs
- Force GC (`runtime.GC()`)
- Deserialize all events from memory, measure time, throughput, memory, allocs

---

## Benchmark Output

```text
===============================================================
              Serialization Load Test Results
===============================================================
Count      | Format       | Operation       | Duration   | Msgs/sec     | Mem (MB)     | Allocs/op 
--------------------------------------------------------------------------------------------------
100k       | JSON         | Serialize       | 103ms      | 967,071      | 63.66        | 7         
100k       | JSON         | Deserialize     | 163ms      | 613,839      | 48.52        | 20        

100k       | Protobuf     | Serialize       | 41ms       | 2,422,058    | 7.99         | 2         
100k       | Protobuf     | Deserialize     | 45ms       | 2,200,966    | 32.20        | 5         

100k       | FlatBuffers  | Serialize       | 27ms       | 3,713,689    | 13.28        | 1         
100k       | FlatBuffers  | Deserialize     | 13ms       | 7,501,932    | 13.13        | 2         

--------------------------------------------------------------------------------------------------
500k       | JSON         | Serialize       | 458ms      | 1,090,514    | 318.10       | 7         
500k       | JSON         | Deserialize     | 790ms      | 633,311      | 242.43       | 20        

500k       | Protobuf     | Serialize       | 193ms      | 2,593,743    | 39.68        | 2         
500k       | Protobuf     | Deserialize     | 225ms      | 2,221,228    | 161.00       | 5         

500k       | FlatBuffers  | Serialize       | 133ms      | 3,766,952    | 66.38        | 1         
500k       | FlatBuffers  | Deserialize     | 56ms       | 8,904,355    | 65.63        | 2         

--------------------------------------------------------------------------------------------------
1M         | JSON         | Serialize       | 845ms      | 1,183,849    | 636.20       | 7         
1M         | JSON         | Deserialize     | 1.587s     | 630,089      | 484.89       | 20        

1M         | Protobuf     | Serialize       | 454ms      | 2,202,887    | 79.35        | 2         
1M         | Protobuf     | Deserialize     | 431ms      | 2,318,391    | 322.00       | 5         

1M         | FlatBuffers  | Serialize       | 267ms      | 3,741,315    | 132.75       | 1         
1M         | FlatBuffers  | Deserialize     | 127ms      | 7,898,277    | 131.26       | 2         

--------------------------------------------------------------------------------------------------
```

## Key Observations

1. **Peak Throughput (FlatBuffers)**
   - Serialization scales linearly and sustains **~3.7 Million messages/sec**.
   - Deserialization sustains an incredible **~7.9 Million messages/sec** (thanks to zero-copy offset reads).

2. **Memory Footprint at 1M Messages**
   - **JSON**: Serialization consumed **636 MB** of memory, driven by large text size and escape-analysis heap objects.
   - **Protobuf**: Serialization consumed just **79 MB**, reflecting its highly efficient varint size compression.
   - **FlatBuffers**: Serialization consumed **132 MB**, larger than Protobuf due to alignment padding but yielding huge speed wins.

3. **Performance Degredation (JSON)**
   - JSON deserializing 1 Million messages took **1.58 seconds** compared to FlatBuffers' **0.127 seconds**. The 20 allocs/op in JSON puts heavy strain on the garbage collector at 1M+ scales.
