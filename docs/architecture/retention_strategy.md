# Production Log-Retention Strategy

A robust retention strategy ensures that the Write Ahead Log (WAL) bounds its disk usage without compromising the system’s ability to recover on restart or serve historical replays to clients. This document outlines the strategy for integrating age-based and size-based retention with the existing Snapshot and Replay engines.

## Core Retention Triggers

To prevent disk exhaustion, the retention manager (a background goroutine) evaluates the segment list on a fixed interval against two policies.

### 1. Size-Based Retention
- **Policy**: `MaxTotalBytes` (e.g., 50 GB).
- **Mechanism**: The manager calculates the total size of all closed segments. If the sum exceeds the threshold, it identifies the oldest segment(s) to purge until the total size falls back below the threshold.
- **Goal**: Guaranteed disk space bounding. Essential for preventing `No space left on device` outages.

### 2. Age-Based Retention
- **Policy**: `MaxAge` (e.g., 7 Days).
- **Mechanism**: The manager inspects the `ModTime` of the closed `.log` files. Any segment older than the threshold is purged.
- **Goal**: Compliance with data storage regulations and minimizing unnecessary state accumulation for fast-moving symbol feeds.

---

## The Deletion Algorithm

The `Background Retainer` executes the following algorithm safely without blocking live appends:

1. **Locking**: Acquire `m.mu.RLock()` to get a stable copy of the `m.segments` slice, then release the lock. The active segment (the last item) is **never** considered for deletion.
2. **Evaluation**: Iterate from the oldest segment `m.segments[0]` forward.
   - Sum the sizes to determine if `MaxTotalBytes` is exceeded.
   - Check file metadata to see if `MaxAge` is exceeded.
3. **Safety Check (Snapshot Compatibility)**: 
   - **Crucial Requirement**: The system *must never* delete a segment that contains sequences newer than the oldest available snapshot. Doing so would create a non-recoverable gap.
   - **Check**: Compare the segment's `BaseSequence` against the sequence of the latest successful Snapshot `S`. If the segment contains messages that haven't been snapshotted yet, **abort deletion** for that segment.
4. **Execution**:
   - Acquire `m.mu.Lock()`.
   - Remove the target segments from the `m.segments` slice.
   - Release `m.mu.Unlock()`.
   - Call `os.Remove()` on the `.log` and `.index` files on disk (done outside the lock to prevent latency spikes).

---

## Replay Safety

When a segment is deleted from disk, any `ReplayClient` currently streaming from that segment will hit an `os.ErrClosed` or `syscall.ENOENT`.

**Mitigation**:
- The `MultiSegmentReader` must detect if a segment is missing.
- When an old segment is requested by a client (`ResumeFrom: N`), but `N` is less than the `BaseSequence` of the oldest retained segment, the `SegmentManager` returns an explicit `ErrSequenceTooOld`.
- The Gateway catches `ErrSequenceTooOld` and automatically disconnects the client with an HTTP 416 (Range Not Satisfiable) or WebSocket Close Code `1008`, instructing the client to fetch a fresh Snapshot before reconnecting.

---

## Compaction Options

If standard deletion deletes too much historical data, the system can utilize log compaction.

| Strategy | Mechanism | Best For |
| :--- | :--- | :--- |
| **Drop Deletion (Current)** | Completely deletes whole segment files based on time/size. | High throughput, fixed disk limits, easy implementation. |
| **Key-Based Compaction** | A background job reads closed segments and rewrites them, keeping only the *latest* sequence for each `Symbol`. | State-machine workloads where intermediate state doesn't matter (e.g., Latest Quote). |
| **Tiered Storage** | Old segments are compressed (e.g., gzip/zstd) and moved to S3/GCS. The local WAL is deleted. | Infinite retention requirements, analytical backfilling. |

> [!TIP]
> For a Real-Time Market Data System, **Drop Deletion** paired with our **Snapshot Engine** is the optimal choice. The Snapshot guarantees we always have the latest state, making the historical intermediate ticks safely disposable.

---

## Recovery Implications & Tradeoffs

### Data Loss vs. Disk Space
- **Aggressive Retention**: Small `MaxTotalBytes` keeps disks cheap but forces clients that disconnect for even a few minutes to fetch full Snapshots (expensive CPU/Bandwidth) instead of performing a quick WAL delta-replay.
- **Lenient Retention**: Large WAL sizes allow clients to easily catch up from weekend disconnects without pulling heavy Snapshots, but drastically increases storage costs.

### Snapshot Race Condition
- **Tradeoff**: If the Snapshot engine crashes and fails to write checkpoints for 3 days, the Retention Manager might want to delete 3 days of logs due to `MaxTotalBytes`. 
- **Implication**: If it obeys `MaxTotalBytes` strictly, it deletes un-snapshotted data, destroying state forever. If it obeys the Snapshot Compatibility rule strictly, the disk fills up and crashes the server.
- **Production Choice**: Disk exhaustion crashes all services. **Always prioritize `MaxTotalBytes` over Snapshot safety**. It is better to lose 3 days of historical state (which can be rebuilt from the upstream exchange) than to take down the entire distributed routing tier due to a full disk.
