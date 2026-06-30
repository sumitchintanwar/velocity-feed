package eventlog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// PostgresRepository implements Repository using PostgreSQL.
// It uses an append-only write pattern with bulk inserts for throughput.
type PostgresRepository struct {
	db     *sql.DB
	log    *log.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// PostgresConfig holds PostgreSQL connection settings.
type PostgresConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	User         string        `mapstructure:"user"`
	Password     string        `mapstructure:"password"`
	DBName       string        `mapstructure:"dbname"`
	SSLMode      string        `mapstructure:"sslmode"`
	MaxOpenConns int           `mapstructure:"max_open_conns"`
	MaxIdleConns int           `mapstructure:"max_idle_conns"`
	MaxLifetime  time.Duration `mapstructure:"max_lifetime"`
}

// DSN returns the PostgreSQL connection string.
func (c PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode,
	)
}

// NewPostgresRepository creates a new PostgreSQL-backed event log repository.
func NewPostgresRepository(cfg PostgresConfig, l *log.Logger) (*PostgresRepository, error) {
	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("eventlog: open database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.MaxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("eventlog: ping database: %w", err)
	}

	repoCtx, cancelRepo := context.WithCancel(context.Background())
	repo := &PostgresRepository{db: db, log: l, ctx: repoCtx, cancel: cancelRepo}
	go repo.partitionManager()
	return repo, nil
}

// NewPostgresRepositoryFromDB creates a repository from an existing sql.DB.
// Useful for testing.
func NewPostgresRepositoryFromDB(db *sql.DB, l *log.Logger) *PostgresRepository {
	ctx, cancel := context.WithCancel(context.Background())
	repo := &PostgresRepository{db: db, log: l, ctx: ctx, cancel: cancel}
	go repo.partitionManager()
	return repo
}

// maxRetries is the maximum number of retries for transient errors.
const maxRetries = 3

// preparedEvent holds a pre-serialized event for bulk insert.
type preparedEvent struct {
	store *StoredEvent
	raw   []byte
}

// isRetryable returns true if the error is transient and worth retrying.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Connection reset, broken pipe, too many connections — all retryable.
	return strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "too many connections") ||
		strings.Contains(err.Error(), "connection refused")
}

// Append persists a single market event. Retries on transient errors.
// Creates a "db.append" span to trace the insert operation.
func (r *PostgresRepository) Append(ctx context.Context, event *StoredEvent) (int64, error) {
	ctx, span := tracing.TracerForComponent("eventlog").Start(ctx, "db.append",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "insert"),
			attribute.String("db.sql.table", "market_events"),
		),
	)
	defer span.End()

	rawJSON := event.RawData
	if rawJSON == nil {
		rawJSON = []byte("null")
	}

	var eventID int64
	var err error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
		}

		err = r.db.QueryRowContext(ctx,
			`INSERT INTO market_events (timestamp, symbol, event_type, price, bid, ask, volume, exchange, provider, raw_data)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			 RETURNING event_id`,
			event.Timestamp, event.Symbol, event.EventType,
			event.Price, event.Bid, event.Ask, event.Volume,
			event.Exchange, event.Provider, rawJSON,
		).Scan(&eventID)

		if err == nil {
			span.SetAttributes(attribute.Int64("db.row_count", 1))
			return eventID, nil
		}
		if !isRetryable(err) {
			break
		}
	}

	span.RecordError(err)
	return 0, fmt.Errorf("eventlog: append: %w", err)
}

// AppendBatch persists multiple events in a single transaction using bulk INSERT.
// Retries on transient errors. Creates a "db.append_batch" span.
func (r *PostgresRepository) AppendBatch(ctx context.Context, events []*StoredEvent) ([]int64, error) {
	if len(events) == 0 {
		return nil, nil
	}

	ctx, span := tracing.TracerForComponent("eventlog").Start(ctx, "db.append_batch",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "bulk_insert"),
			attribute.String("db.sql.table", "market_events"),
			attribute.Int("db.batch_size", len(events)),
		),
	)
	defer span.End()

	prepared := make([]preparedEvent, len(events))
	for i, event := range events {
		rawJSON := event.RawData
		if rawJSON == nil {
			rawJSON = []byte("null")
		}
		prepared[i] = preparedEvent{store: event, raw: rawJSON}
	}

	var ids []int64
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
		}

		var err error
		ids, err = r.insertBatch(ctx, prepared)
		if err == nil {
			span.SetAttributes(attribute.Int("db.row_count", len(ids)))
			return ids, nil
		}
		if !isRetryable(err) {
			break
		}
	}

	retryErr := fmt.Errorf("all %d retries exhausted", maxRetries)
	span.RecordError(retryErr)
	return nil, fmt.Errorf("eventlog: append batch: %w", retryErr)
}

// insertBatch performs a single bulk INSERT within a transaction.
func (r *PostgresRepository) insertBatch(ctx context.Context, prepared []preparedEvent) ([]int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("eventlog: begin tx: %w", err)
	}
	defer tx.Rollback()

	// Build bulk INSERT: INSERT INTO ... VALUES ($1,...,$10), ($11,...,$20), ...
	valueStrings := make([]string, len(prepared))
	valueArgs := make([]interface{}, 0, len(prepared)*10)
	argIdx := 1
	for i, pe := range prepared {
		valueStrings[i] = fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4,
			argIdx+5, argIdx+6, argIdx+7, argIdx+8, argIdx+9,
		)
		valueArgs = append(valueArgs,
			pe.store.Timestamp, pe.store.Symbol, pe.store.EventType,
			pe.store.Price, pe.store.Bid, pe.store.Ask, pe.store.Volume,
			pe.store.Exchange, pe.store.Provider, pe.raw,
		)
		argIdx += 10
	}

	query := fmt.Sprintf(
		`INSERT INTO market_events (timestamp, symbol, event_type, price, bid, ask, volume, exchange, provider, raw_data)
		 VALUES %s
		 RETURNING event_id`,
		strings.Join(valueStrings, ", "),
	)

	rows, err := tx.QueryContext(ctx, query, valueArgs...)
	if err != nil {
		return nil, fmt.Errorf("eventlog: bulk insert: %w", err)
	}
	defer rows.Close()

	ids := make([]int64, 0, len(prepared))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("eventlog: scan returned id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: rows iteration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("eventlog: commit batch: %w", err)
	}

	return ids, nil
}

// QueryBySymbol returns all events for a symbol within a time range.
// Creates a "db.query_by_symbol" span.
func (r *PostgresRepository) QueryBySymbol(ctx context.Context, symbol string, from, to time.Time) ([]*StoredEvent, error) {
	ctx, span := tracing.TracerForComponent("eventlog").Start(ctx, "db.query_by_symbol",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "query"),
			attribute.String("db.sql.table", "market_events"),
		),
	)
	defer span.End()

	rows, err := r.db.QueryContext(ctx,
		`SELECT event_id, timestamp, symbol, event_type, price, bid, ask, volume, exchange, provider, raw_data
		 FROM market_events
		 WHERE symbol = $1 AND timestamp BETWEEN $2 AND $3
		 ORDER BY event_id ASC`,
		symbol, from, to,
	)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("eventlog: query by symbol: %w", err)
	}
	defer rows.Close()

	events, err := r.scanEvents(rows)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.Int("db.row_count", len(events)))
	return events, nil
}

// QueryLatest returns the N most recent events for a symbol.
// Creates a "db.query_latest" span.
func (r *PostgresRepository) QueryLatest(ctx context.Context, symbol string, limit int) ([]*StoredEvent, error) {
	ctx, span := tracing.TracerForComponent("eventlog").Start(ctx, "db.query_latest",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "query"),
			attribute.String("db.sql.table", "market_events"),
			attribute.Int("db.query.limit", limit),
		),
	)
	defer span.End()

	rows, err := r.db.QueryContext(ctx,
		`SELECT event_id, timestamp, symbol, event_type, price, bid, ask, volume, exchange, provider, raw_data
		 FROM market_events
		 WHERE symbol = $1
		 ORDER BY event_id DESC
		 LIMIT $2`,
		symbol, limit,
	)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("eventlog: query latest: %w", err)
	}
	defer rows.Close()

	events, err := r.scanEvents(rows)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Reverse to chronological order.
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}

	span.SetAttributes(attribute.Int("db.row_count", len(events)))
	return events, nil
}

// QueryEvents returns a paginated page of events matching the given filters.
// Creates a "db.query_events" span to trace the replay query — the most
// valuable trace target for diagnosing slow replays.
func (r *PostgresRepository) QueryEvents(ctx context.Context, q ReplayQuery) (*ReplayResult, error) {
	ctx, span := tracing.TracerForComponent("eventlog").Start(ctx, "db.query_events",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "query"),
			attribute.String("db.sql.table", "market_events"),
			attribute.Int("db.query.limit", q.Limit),
		),
	)
	defer span.End()

	if q.Symbol != "" {
		span.SetAttributes(attribute.String("replay.symbol", q.Symbol))
	}
	if q.Limit <= 0 {
		q.Limit = 100
	}
	if q.Limit > 1000 {
		q.Limit = 1000
	}

	// Fetch one extra row to detect whether there are more results.
	queryLimit := q.Limit + 1

	// Build query dynamically based on filters.
	var conditions []string
	var args []interface{}
	argIdx := 1

	if q.Symbol != "" {
		conditions = append(conditions, fmt.Sprintf("symbol = $%d", argIdx))
		args = append(args, q.Symbol)
		argIdx++
	}

	if !q.From.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argIdx))
		args = append(args, q.From)
		argIdx++
	}

	if !q.To.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", argIdx))
		args = append(args, q.To)
		argIdx++
	}

	// Composite cursor: (timestamp, event_id) ensures strict chronological ordering.
	// Events with the same timestamp are broken by event_id.
	if !q.Cursor.Timestamp.IsZero() || q.Cursor.EventID > 0 {
		conditions = append(conditions, fmt.Sprintf("(timestamp, event_id) > ($%d, $%d)", argIdx, argIdx+1))
		args = append(args, q.Cursor.Timestamp, q.Cursor.EventID)
		argIdx += 2
	}

	where := ""
	if len(conditions) > 0 {
		where = "WHERE " + strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf(
		`SELECT event_id, timestamp, symbol, event_type, price, bid, ask, volume, exchange, provider, raw_data
		 FROM market_events
		 %s
		 ORDER BY timestamp ASC, event_id ASC
		 LIMIT $%d`,
		where, argIdx,
	)
	args = append(args, queryLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("eventlog: query events: %w", err)
	}
	defer rows.Close()

	events, err := r.scanEvents(rows)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := &ReplayResult{
		Events:  events,
		HasMore: len(events) > q.Limit,
	}

	// Trim to requested limit if we fetched an extra row.
	if len(events) > q.Limit {
		events = events[:q.Limit]
		result.Events = events
		last := events[len(events)-1]
		result.NextCursor = &Cursor{Timestamp: last.Timestamp, EventID: last.EventID}
	}

	span.SetAttributes(attribute.Int("db.row_count", len(result.Events)))
	return result, nil
}

// Count returns the total number of persisted events.
func (r *PostgresRepository) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM market_events`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("eventlog: count: %w", err)
	}
	return count, nil
}

// CountBySymbol returns the number of events for a specific symbol.
func (r *PostgresRepository) CountBySymbol(ctx context.Context, symbol string) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM market_events WHERE symbol = $1`, symbol,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("eventlog: count by symbol: %w", err)
	}
	return count, nil
}

func (r *PostgresRepository) partitionManager() {
	ticker := time.NewTicker(12 * time.Hour)
	defer ticker.Stop()

	// Retry on startup to wait for migrations
	for i := 0; i < 20; i++ {
		if err := r.ensurePartitions(); err == nil {
			break
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			_ = r.ensurePartitions()
		}
	}
}

func (r *PostgresRepository) ensurePartitions() error {
	now := time.Now().UTC()
	var lastErr error
	// create for today and next 3 days
	for i := 0; i < 4; i++ {
		date := now.AddDate(0, 0, i)
		tableName := fmt.Sprintf("market_events_%s", date.Format("2006_01_02"))
		start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
		end := start.AddDate(0, 0, 1)

		query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF market_events FOR VALUES FROM ('%s') TO ('%s')`,
			tableName, start.Format(time.RFC3339), end.Format(time.RFC3339))

		_, err := r.db.ExecContext(r.ctx, query)
		if err != nil && r.ctx.Err() == nil {
			r.log.Underlying().Error().Err(err).Str("partition", tableName).Str("event", "partition_create_failed").Msg("eventlog: failed to create partition")
			lastErr = err
		}
	}
	return lastErr
}

// Close closes the underlying database connection pool.
func (r *PostgresRepository) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return r.db.Close()
}

// DB returns the underlying *sql.DB for running migrations.
func (r *PostgresRepository) DB() *sql.DB {
	return r.db
}

// scanEvents iterates over rows and returns a slice of StoredEvents.
func (r *PostgresRepository) scanEvents(rows *sql.Rows) ([]*StoredEvent, error) {
	var events []*StoredEvent
	for rows.Next() {
		var e StoredEvent
		var rawJSON []byte

		err := rows.Scan(
			&e.EventID, &e.Timestamp, &e.Symbol, &e.EventType,
			&e.Price, &e.Bid, &e.Ask, &e.Volume,
			&e.Exchange, &e.Provider, &rawJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("eventlog: scan: %w", err)
		}

		if rawJSON != nil {
			e.RawData = rawJSON
		}

		events = append(events, &e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: rows: %w", err)
	}

	return events, nil
}

// RunMigrations executes the embedded SQL migration files.
func RunMigrations(ctx context.Context, db *sql.DB) error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS market_events (
			event_id    BIGSERIAL,
			timestamp   TIMESTAMPTZ     NOT NULL,
			symbol      VARCHAR(16)     NOT NULL,
			event_type  VARCHAR(32)     NOT NULL,
			price       DOUBLE PRECISION,
			bid         DOUBLE PRECISION,
			ask         DOUBLE PRECISION,
			volume      BIGINT,
			exchange    VARCHAR(32),
			provider    VARCHAR(64),
			raw_data    BYTEA,
			PRIMARY KEY (timestamp, event_id)
		) PARTITION BY RANGE (timestamp)`,
		// Index optimizes QueryBySymbol: covers (symbol, timestamp, event_id)
		// so PostgreSQL can satisfy WHERE and ORDER BY without a sort step.
		`CREATE INDEX IF NOT EXISTS idx_market_events_symbol_ts  ON market_events (symbol, timestamp, event_id)`,
		// Optimizes QueryLatest: filters by symbol, orders by event_id DESC.
		`CREATE INDEX IF NOT EXISTS idx_market_events_symbol_event_id  ON market_events (symbol, event_id)`,
		// BRIN index on timestamp for efficient time-range scans on append-only
		// time-series data. BRIN indexes are orders of magnitude smaller than
		// B-Tree indexes for sequential inserts and are ideal for partition pruning.
		`CREATE INDEX IF NOT EXISTS idx_market_events_timestamp_brin ON market_events USING brin (timestamp)`,
	}

	for i, m := range migrations {
		if _, err := db.ExecContext(ctx, m); err != nil {
			return fmt.Errorf("eventlog: migration %d: %w", i+1, err)
		}
	}

	return nil
}
