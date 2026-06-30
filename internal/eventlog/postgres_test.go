package eventlog

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/log"
)

// testPostgresConfig returns the PostgreSQL config for testing.
func testPostgresConfig() PostgresConfig {
	host := os.Getenv("POSTGRES_TEST_HOST")
	if host == "" {
		host = "localhost"
	}
	port := 5432
	if p := os.Getenv("POSTGRES_TEST_PORT"); p != "" {
		fmt.Sscanf(p, "%d", &port)
	}
	user := os.Getenv("POSTGRES_TEST_USER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("POSTGRES_TEST_PASSWORD")
	if password == "" {
		password = "postgres"
	}
	dbname := os.Getenv("POSTGRES_TEST_DBNAME")
	if dbname == "" {
		dbname = "rtmds_test"
	}

	return PostgresConfig{
		Host:         host,
		Port:         port,
		User:         user,
		Password:     password,
		DBName:       dbname,
		SSLMode:      "disable",
		MaxOpenConns: 5,
		MaxIdleConns: 2,
		MaxLifetime:  time.Minute,
	}
}

// skipIfNoPostgres skips the test if PostgreSQL is not available.
func skipIfNoPostgres(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cfg := testPostgresConfig()
	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
}

// cleanTable truncates the market_events table for test isolation.
func cleanTable(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = db.ExecContext(ctx, "TRUNCATE TABLE market_events RESTART IDENTITY")
}

func TestPostgresRepository_Append(t *testing.T) {
	skipIfNoPostgres(t)

	cfg := testPostgresConfig()
	log := log.New(nil, "test")

	repo, err := NewPostgresRepository(cfg, log)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Run migrations.
	if err := RunMigrations(ctx, repo.DB()); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	cleanTable(t, repo.DB())

	event := &StoredEvent{
		Timestamp: time.Now(),
		Symbol:    "AAPL",
		EventType: "trade",
		Price:     250.25,
		Volume:    100,
		Provider:  "test",
	}

	eventID, err := repo.Append(ctx, event)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if eventID <= 0 {
		t.Errorf("expected positive event_id, got %d", eventID)
	}

	t.Logf("appended event with ID: %d", eventID)
}

func TestPostgresRepository_AppendBatch(t *testing.T) {
	skipIfNoPostgres(t)

	cfg := testPostgresConfig()
	log := log.New(nil, "test")

	repo, err := NewPostgresRepository(cfg, log)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	if err := RunMigrations(ctx, repo.DB()); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	cleanTable(t, repo.DB())

	events := make([]*StoredEvent, 5)
	symbols := []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"}
	for i := range events {
		events[i] = &StoredEvent{
			Timestamp: time.Now(),
			Symbol:    symbols[i],
			EventType: "quote",
			Price:     float64(100 + i*10),
			Volume:    int64(1000 + i*100),
			Provider:  "test",
		}
	}

	ids, err := repo.AppendBatch(ctx, events)
	if err != nil {
		t.Fatalf("batch append failed: %v", err)
	}

	if len(ids) != 5 {
		t.Fatalf("expected 5 IDs, got %d", len(ids))
	}

	for i, id := range ids {
		if id <= 0 {
			t.Errorf("event %d: expected positive ID, got %d", i, id)
		}
	}

	t.Logf("batch appended events with IDs: %v", ids)
}

func TestPostgresRepository_Count(t *testing.T) {
	skipIfNoPostgres(t)

	cfg := testPostgresConfig()
	log := log.New(nil, "test")

	repo, err := NewPostgresRepository(cfg, log)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	if err := RunMigrations(ctx, repo.DB()); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	cleanTable(t, repo.DB())

	// Initial count should be 0.
	count, err := repo.Count(ctx)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	// Append 3 events.
	for i := 0; i < 3; i++ {
		_, err := repo.Append(ctx, &StoredEvent{
			Timestamp: time.Now(),
			Symbol:    "AAPL",
			EventType: "trade",
			Price:     250.0,
			Provider:  "test",
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	count, err = repo.Count(ctx)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestPostgresRepository_QueryBySymbol(t *testing.T) {
	skipIfNoPostgres(t)

	cfg := testPostgresConfig()
	log := log.New(nil, "test")

	repo, err := NewPostgresRepository(cfg, log)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	if err := RunMigrations(ctx, repo.DB()); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	cleanTable(t, repo.DB())

	now := time.Now()

	// Append AAPL events.
	for i := 0; i < 5; i++ {
		_, err := repo.Append(ctx, &StoredEvent{
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Symbol:    "AAPL",
			EventType: "trade",
			Price:     float64(200 + i),
			Provider:  "test",
		})
		if err != nil {
			t.Fatalf("append AAPL %d failed: %v", i, err)
		}
	}

	// Append MSFT events.
	for i := 0; i < 3; i++ {
		_, err := repo.Append(ctx, &StoredEvent{
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Symbol:    "MSFT",
			EventType: "trade",
			Price:     float64(300 + i),
			Provider:  "test",
		})
		if err != nil {
			t.Fatalf("append MSFT %d failed: %v", i, err)
		}
	}

	// Query AAPL events.
	events, err := repo.QueryBySymbol(ctx, "AAPL", now.Add(-time.Second), now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("query by symbol failed: %v", err)
	}

	if len(events) != 5 {
		t.Errorf("expected 5 AAPL events, got %d", len(events))
	}

	// Verify ordering.
	for i := 1; i < len(events); i++ {
		if events[i].Timestamp.Before(events[i-1].Timestamp) {
			t.Errorf("events not in chronological order at index %d", i)
		}
	}
}

func TestPostgresRepository_QueryLatest(t *testing.T) {
	skipIfNoPostgres(t)

	cfg := testPostgresConfig()
	log := log.New(nil, "test")

	repo, err := NewPostgresRepository(cfg, log)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	if err := RunMigrations(ctx, repo.DB()); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	cleanTable(t, repo.DB())

	now := time.Now()

	// Append 10 events.
	for i := 0; i < 10; i++ {
		_, err := repo.Append(ctx, &StoredEvent{
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Symbol:    "AAPL",
			EventType: "trade",
			Price:     float64(200 + i),
			Provider:  "test",
		})
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Query latest 3.
	events, err := repo.QueryLatest(ctx, "AAPL", 3)
	if err != nil {
		t.Fatalf("query latest failed: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	// Should be in chronological order (reversed from DESC).
	if events[0].Price != 207 || events[1].Price != 208 || events[2].Price != 209 {
		t.Errorf("unexpected prices: %v %v %v", events[0].Price, events[1].Price, events[2].Price)
	}
}

func TestPostgresRepository_CountBySymbol(t *testing.T) {
	skipIfNoPostgres(t)

	cfg := testPostgresConfig()
	log := log.New(nil, "test")

	repo, err := NewPostgresRepository(cfg, log)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	if err := RunMigrations(ctx, repo.DB()); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	cleanTable(t, repo.DB())

	// Append events.
	for i := 0; i < 3; i++ {
		_, _ = repo.Append(ctx, &StoredEvent{
			Timestamp: time.Now(), Symbol: "AAPL", EventType: "trade", Price: 250, Provider: "test",
		})
	}
	for i := 0; i < 2; i++ {
		_, _ = repo.Append(ctx, &StoredEvent{
			Timestamp: time.Now(), Symbol: "MSFT", EventType: "trade", Price: 300, Provider: "test",
		})
	}

	count, err := repo.CountBySymbol(ctx, "AAPL")
	if err != nil {
		t.Fatalf("count by symbol failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 AAPL events, got %d", count)
	}

	count, err = repo.CountBySymbol(ctx, "MSFT")
	if err != nil {
		t.Fatalf("count by symbol failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 MSFT events, got %d", count)
	}
}

func TestRunMigrations(t *testing.T) {
	skipIfNoPostgres(t)

	cfg := testPostgresConfig()
	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Run migrations (idempotent).
	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("first migration run failed: %v", err)
	}

	// Run again to verify idempotency.
	if err := RunMigrations(ctx, db); err != nil {
		t.Fatalf("second migration run failed: %v", err)
	}

	// Verify table exists.
	var exists bool
	err = db.QueryRowContext(ctx,
		`SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_name = 'market_events'
		)`,
	).Scan(&exists)
	if err != nil {
		t.Fatalf("check table existence: %v", err)
	}
	if !exists {
		t.Error("market_events table does not exist")
	}
}

func TestPostgresConfig_DSN(t *testing.T) {
	cfg := PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "secret",
		DBName:   "rtmds",
		SSLMode:  "disable",
	}

	dsn := cfg.DSN()
	expected := "host=localhost port=5432 user=postgres password=secret dbname=rtmds sslmode=disable"
	if dsn != expected {
		t.Errorf("DSN mismatch:\n got: %s\nwant: %s", dsn, expected)
	}
}
