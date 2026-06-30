package healthcheck

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCheck creates a health check that pings Redis.
// The optional onFailure callback is invoked when the check fails, allowing
// the caller to take corrective action (e.g., evict WebSocket clients).
func RedisCheck(client *redis.Client, onFailure ...func()) Check {
	var onFail func()
	if len(onFailure) > 0 {
		onFail = onFailure[0]
	}
	return Check{
		Name: "redis",
		CheckFn: func(ctx context.Context) error {
			if client == nil {
				if onFail != nil {
					onFail()
				}
				return fmt.Errorf("redis client is nil")
			}
			err := client.Ping(ctx).Err()
			if err != nil && onFail != nil {
				onFail()
			}
			return err
		},
		Timeout: 1 * time.Second,
	}
}

// PostgresCheck creates a health check that pings PostgreSQL.
func PostgresCheck(db *sql.DB) Check {
	return Check{
		Name: "postgres",
		CheckFn: func(ctx context.Context) error {
			return db.PingContext(ctx)
		},
		Timeout: 2 * time.Second,
	}
}

// PostgresCheckWithQuery creates a health check that runs a simple query against PostgreSQL.
// This verifies not just connectivity, but that the database is operational.
func PostgresCheckWithQuery(db *sql.DB) Check {
	return Check{
		Name: "postgres",
		CheckFn: func(ctx context.Context) error {
			var result int
			err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
			if err != nil {
				return fmt.Errorf("postgres query failed: %w", err)
			}
			if result != 1 {
				return fmt.Errorf("postgres query returned unexpected value: %d", result)
			}
			return nil
		},
		Timeout: 2 * time.Second,
	}
}

// SnapshotCheck creates a health check that verifies snapshot service readiness.
type SnapshotChecker interface {
	IsReady() bool
}

// SnapshotCheck creates a health check for the snapshot service.
func SnapshotCheck(snap SnapshotChecker) Check {
	return Check{
		Name: "snapshot",
		CheckFn: func(_ context.Context) error {
			if !snap.IsReady() {
				return fmt.Errorf("snapshot service not ready")
			}
			return nil
		},
		Timeout: 500 * time.Millisecond,
	}
}

// GatewayChecker is the interface required for gateway capability checks.
type GatewayChecker interface {
	// ClientCount returns the current number of connected clients.
	// Used for monitoring only — not for readiness decisions.
	ClientCount() int
}

// GatewayCheck creates a health check that verifies the gateway is operational.
// This checks *capability* (gateway is running), not *capacity* (client count).
// Capacity issues are handled via backpressure (HTTP 429) or scaling, never
// via readiness failures — to prevent cascading failures across the cluster.
func GatewayCheck(gw GatewayChecker) Check {
	return Check{
		Name: "websocket-gateway",
		CheckFn: func(_ context.Context) error {
			// Gateway is considered operational if we can query its state.
			// We intentionally do NOT check ClientCount >= MaxConnections
			// because that's a capacity check, not a capability check.
			// Capacity issues cause cascading failures when K8s removes
			// the pod from the load balancer, shifting traffic to remaining
			// gateways which then also become overloaded.
			_ = gw.ClientCount() // verify gateway is responsive
			return nil
		},
		Timeout: 500 * time.Millisecond,
	}
}

// Func creates a custom health check from a function.
func Func(name string, fn CheckFunc, timeout time.Duration) Check {
	return Check{
		Name:    name,
		CheckFn: fn,
		Timeout: timeout,
	}
}
