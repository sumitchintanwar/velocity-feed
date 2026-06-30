# API Reference & Contracts

**Purpose:** To strictly define the JSON payloads, WebSocket protocols, and REST endpoints exposed by the platform.
**Intended Audience:** Consumer Teams, Integration Engineers.
**Maintenance Strategy:** Update synchronously with any Pull Request that mutates an externally facing payload schema.

---

## 1. WebSocket Live Feed (`/ws`)

### Connection URL
`ws://gateway.rtmds.internal/ws`

### Subscription Protocol
To receive live data, a client must send a JSON subscription message after connecting.

**Request:**
```json
{
  "action": "subscribe",
  "instruments": ["AAPL", "BTC/USD"]
}
```

**Response (Acknowledgment):**
```json
{
  "status": "success",
  "message": "subscribed to 2 instruments"
}
```

### Market Tick Payload (The Contract)
All ticks emitted by the Gateway will strictly adhere to the following schema:

```json
{
  "type": "trade",
  "instrument": "AAPL",
  "price": 150.25,
  "volume": 100,
  "sequence_id": 10452,
  "timestamp": "2026-06-28T14:32:01.005Z",
  "trace_id": "8a3c9f2b..." 
}
```
*Note: Consumer teams SHOULD parse the `sequence_id` to detect missing packets.*

---

## 2. Snapshot API

**Endpoint:** `GET /api/v1/snapshot/{instrument}`
**Purpose:** Fetches the aggregated, current state of an instrument so clients don't have to process the entire day's history.

**Response (HTTP 200):**
```json
{
  "instrument": "AAPL",
  "current_price": 150.25,
  "total_volume_today": 1504200,
  "last_sequence_id": 10452
}
```

---

## 3. Replay API

**Endpoint:** `GET /api/v1/replay/{instrument}?start={seq}&end={seq}`
**Purpose:** Recovers a specific sequence of missed ticks.

**Response (HTTP 200):**
```json
{
  "instrument": "AAPL",
  "ticks": [
    {
      "type": "trade",
      "price": 150.23,
      "volume": 50,
      "sequence_id": 10450
    },
    {
      "type": "trade",
      "price": 150.24,
      "volume": 10,
      "sequence_id": 10451
    }
  ]
}
```
