-- 001_create_market_events.sql
-- Creates the append-only market_events table for persistent event logging.

CREATE TABLE IF NOT EXISTS market_events (
    event_id    BIGSERIAL       PRIMARY KEY,
    timestamp   TIMESTAMPTZ     NOT NULL,
    symbol      VARCHAR(16)     NOT NULL,
    event_type  VARCHAR(32)     NOT NULL,
    price       DOUBLE PRECISION,
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    volume      BIGINT,
    exchange    VARCHAR(32),
    provider    VARCHAR(64),
    raw_data    JSONB
);

-- Compound index for QueryBySymbol: covers (symbol, timestamp, event_id)
-- so PostgreSQL can satisfy WHERE and ORDER BY without a sort step.
CREATE INDEX IF NOT EXISTS idx_market_events_symbol_ts  ON market_events (symbol, timestamp, event_id);

-- Optimizes QueryLatest: filters by symbol, orders by event_id DESC.
CREATE INDEX IF NOT EXISTS idx_market_events_symbol_event_id  ON market_events (symbol, event_id);
