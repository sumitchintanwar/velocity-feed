# Glossary & Domain Taxonomy

**Purpose:** To establish a unified organizational vocabulary for the RTMDS platform.
**Intended Audience:** All Engineering and Product teams.
**Maintenance Strategy:** Update when a new major architectural concept or business entity is introduced.

---

| Term | Definition |
|------|------------|
| **Tick** | A single market event (e.g., a trade execution or an order book update) containing price, volume, and an instrument identifier. |
| **Instrument** | A financial asset being traded (e.g., `AAPL`, `BTC/USD`). Used synonymously with "Symbol". |
| **Sequence ID** | A monotonically increasing integer assigned to every single tick by the Publisher. It is the core mechanism used by consumers to detect network drops. |
| **Sequence Gap** | A scenario where a consumer receives a tick with a Sequence ID that skips a number (e.g., receiving `105` immediately after `103`). This triggers a Replay request. |
| **Snapshot** | The aggregated, point-in-time state of an instrument. Instead of replaying an entire day's worth of ticks, a client fetches a Snapshot to instantly bootstrap their local state. |
| **Replay** | The process of querying the PostgreSQL Event Store for a specific range of Sequence IDs to recover missed ticks. |
| **Feed Generator** | An upstream, external system (or internal simulator) that acts as the source of truth for market data, pushing raw ticks into the Publisher. |
| **Hot-Key Starvation** | A Redis anti-pattern where a single highly volatile instrument (e.g., `TSLA`) overwhelms a single Redis shard. Mitigated by the Topic Manager's consistent hashing ring. |
