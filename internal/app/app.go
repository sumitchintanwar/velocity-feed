// Package app is the dependency-injection root. It constructs every
// component and starts / stops them in the correct order.
//
// Startup order (dependencies flow forward):
//
//	Config → Logger → Metrics → TopicManager → Gateway → Feed → Pipeline → Server
//
// Shutdown order (reverse of startup):
//
//	Server → Feed → Pipeline → Gateway → TopicManager → Metrics → Logger
//
// This ordering ensures no component receives work before its dependencies
// are ready, and no component is shut down while still being written to.
package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/sumit/rtmds/internal/clientqueue"
	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/internal/distribution/redisbus"
	"github.com/sumit/rtmds/internal/discovery"
	"github.com/sumit/rtmds/internal/eventlog"
	"github.com/sumit/rtmds/internal/feed"
	"github.com/sumit/rtmds/internal/healthcheck"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/pubsub"
	"github.com/sumit/rtmds/internal/sequencer"
	"github.com/sumit/rtmds/internal/snapshot"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/tracing"
	"github.com/sumit/rtmds/internal/transport"
	"github.com/sumit/rtmds/internal/websocket"
	"golang.org/x/sync/errgroup"
	"github.com/sumit/rtmds/internal/exchange"
)

// Build-time variables set via -ldflags.
var (
	Version  = "dev"
	Revision = "unknown"
)

// component records a registered component's metadata and lifecycle hooks.
type component struct {
	name    string
	start   func(ctx context.Context) error
	stop    func(ctx context.Context) error
	health  func(ctx context.Context) platform.HealthStatus
	order   int // startup order (lower = earlier)
}

// App holds every wired component and exposes Run.
type App struct {
	cfg      *config.Config
	log      *log.Logger // structured logger with context propagation
	server   *http.Server
	listener net.Listener
	pipeline *feed.Pipeline
	gateway  *websocket.Gateway
	tm       topicmanager.Manager
	metrics  *platform.Metrics
	gatherer prometheus.Gatherer
	tracer   *tracing.Tracer // OpenTelemetry tracer (nil when tracing disabled)

	// Redis distribution components (nil when Redis is disabled).
	redisClient   *redisbus.Publisher  // nil when not using Redis
	redisSub      *redisbus.Subscriber // nil when not using Redis
	registry      *discovery.Registry  // nil when discovery is disabled
	eventLog      eventlog.Repository  // nil when database is disabled

	// Health check registry for dependency checks (Redis, Postgres, etc.)
	healthRegistry *healthcheck.Registry

	// heartbeat maintains an atomic timestamp updated by main event loops.
	// The Liveness probe checks this to detect deadlocks (see health_check_review.md).
	heartbeat *healthcheck.Heartbeat

	// gatewayHealthy tracks whether the gateway is alive for the
	// service-discovery heartbeat health check. Set true after startup,
	// false before deregistration. Prevents zombie gateways from holding
	// registrations when the local process is unhealthy.
	gatewayHealthy atomic.Bool

	// component registry for ordered lifecycle management
	mu         sync.RWMutex
	components []component
	started    []string // names of started components, in order
}

// tcpKeepAliveListener wraps a *net.TCPListener to set TCP keepalive
// and disable Nagle's algorithm (TCP_NODELAY) on accepted connections.
// This reduces latency for small WebSocket frames and detects dead
// connections faster under high-concurrency workloads.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

// Accept implements net.Listener with TCP tuning.
func (ln tcpKeepAliveListener) Accept() (net.Conn, error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return nil, err
	}
	_ = tc.SetKeepAlive(true)
	_ = tc.SetKeepAlivePeriod(30 * time.Second)
	_ = tc.SetNoDelay(true)
	return tc, nil
}

// New creates a new App and wires all components. It does NOT start them.
// Call App.Run() to begin the startup sequence.
func New(cfg *config.Config) (*App, error) {
	rlog := log.NewFromConfig(log.Config{
		Level:       cfg.Log.Level,
		Format:      cfg.Log.Format,
		Service:     "rtmds",
		Environment: cfg.Log.Format, // reuse log format as environment hint
	})

	a := &App{
		cfg: cfg,
		log: rlog,
	}

	if err := a.wire(); err != nil {
		return nil, fmt.Errorf("app: wire: %w", err)
	}

	return a, nil
}

// Build is the legacy constructor that returns a ready-to-Run App.
// Deprecated: Use New() for new code.
func Build(cfg *config.Config) (*App, error) {
	return New(cfg)
}

// wire constructs all components in dependency order and registers them
// for lifecycle management. No component is started yet.
func (a *App) wire() error {
	a.log.Underlying().Info().Str("event", "wiring_started").Msg("wiring components")

	// ── 0. Tracing (order 5, before metrics) ────────────────────
	// Initialize OpenTelemetry SDK early so all subsequent components
	// can participate in distributed tracing. The tracer is nil when
	// tracing is disabled (noop provider installed).
	if err := a.initTracing(); err != nil {
		return fmt.Errorf("tracing init: %w", err)
	}

	// ── 0b. Health Check Registry & Heartbeat ────────────────────
	// Create these early because the pipeline needs the heartbeat reference.
	a.healthRegistry = healthcheck.NewRegistry()
	a.heartbeat = healthcheck.NewHeartbeat()

	// ── 1. Metrics ──────────────────────────────────────────────
	metrics, gatherer := platform.NewMetrics("rtmds")
	a.metrics = metrics
	a.gatherer = gatherer
	// The gatherer is a *prometheus.Registry which also implements Registerer.
	// Use it so topic/worker metrics appear in the same /metrics endpoint.
	reg := gatherer.(prometheus.Registerer)

	// Set build_info gauge for version tracking across rollouts.
	metrics.BuildInfo.WithLabelValues(Version, Revision).Set(1)

	a.registerComponent(component{
		name:  "metrics",
		order: 10,
		stop: func(ctx context.Context) error {
			a.log.Underlying().Info().Str("event", "metrics_flushed").Msg("metrics: flushed")
			return nil
		},
		health: func(ctx context.Context) platform.HealthStatus {
			return platform.OK()
		},
	})

	// ── 1b. Event Log (PostgreSQL) ─────────────────────────────
	// When database is enabled, create a PostgreSQL-backed event log
	// for persistent market event storage. Runs migrations on startup.
	if a.cfg.Database.Enabled {
		dbCfg := eventlog.PostgresConfig{
			Host:         a.cfg.Database.Host,
			Port:         a.cfg.Database.Port,
			User:         a.cfg.Database.User,
			Password:     a.cfg.Database.Password.Value(),
			DBName:       a.cfg.Database.DBName,
			SSLMode:      a.cfg.Database.SSLMode,
			MaxOpenConns: a.cfg.Database.MaxOpenConns,
			MaxIdleConns: a.cfg.Database.MaxIdleConns,
			MaxLifetime:  a.cfg.Database.MaxLifetime,
		}

		repo, err := eventlog.NewPostgresRepository(dbCfg, a.log)
		if err != nil {
			return fmt.Errorf("eventlog: %w", err)
		}
		a.eventLog = repo

		a.registerComponent(component{
			name:  "event-log",
			order: 11,
			start: func(ctx context.Context) error {
				if err := eventlog.RunMigrations(ctx, repo.DB()); err != nil {
					return fmt.Errorf("eventlog migrations: %w", err)
				}
				a.log.Underlying().Info().Str("event", "migrations_applied").Msg("event-log: migrations applied")
				return nil
			},
			stop: func(ctx context.Context) error {
				if err := repo.Close(); err != nil {
					a.log.Underlying().Warn().Err(err).Str("event", "eventlog_close_failed").Msg("event-log: close failed")
				}
				a.log.Underlying().Info().Str("event", "eventlog_stopped").Msg("event-log: stopped")
				return nil
			},
			health: func(ctx context.Context) platform.HealthStatus {
				if _, err := repo.Count(ctx); err != nil {
					return platform.Degraded("event log query failed: " + err.Error())
				}
				return platform.OK()
			},
		})
	}

	// ── 2. Topic Manager ────────────────────────────────────────
	// Use backpressure-aware queues with DropOldest policy and MaxAge
	// to prevent stale data accumulation. This fixes the catastrophic
	// latency observed in load tests where messages queue for minutes.
	queueCfg := clientqueue.DefaultConfig()
	queueCfg.QueueSize = 64
	queueCfg.MaxAge = 100 * time.Millisecond
	tm := topicmanager.NewWithQueue(0, &queueCfg, a.log, reg, metrics)
	a.tm = tm
	a.registerComponent(component{
		name:  "topic-manager",
		order: 20,
		stop: func(ctx context.Context) error {
			a.log.Underlying().Info().
				Int("topics", tm.TopicCount()).
				Str("event", "topic_manager_stopped").
				Msg("topic-manager: stopped")
			return nil
		},
		health: func(ctx context.Context) platform.HealthStatus {
			topics := tm.TopicCount()
			if topics == 0 {
				return platform.Degraded("no active topics")
			}
			return platform.OK()
		},
	})

	// ── 2b. Distributed Router (when Redis is enabled) ──────────
	// Wraps the local TopicManager with dynamic Redis subscription
	// management. Gateway independence: only subscribes to Redis
	// channels for symbols that local clients actually need.
	var router *topicmanager.DistributedRouter
	if a.cfg.Redis.Enabled {
		router = topicmanager.NewDistributedRouter(tm, "market:", a.log, a.cfg.Server.GetGatewayID(),
			topicmanager.WithSubscriptionChangeCallback(func(symbol string, change topicmanager.SubscriptionChange) {
				if a.redisSub == nil {
					return
				}
				switch change {
				case topicmanager.SubscribeRequested:
					a.redisSub.Subscribe(symbol)
					metrics.DistRedisSubscribeOps.Inc()
					a.log.Underlying().Info().Str("symbol", symbol).
						Str("event", "redis_channel_subscribed").
						Msg("distributed: subscribed to Redis channel")
				case topicmanager.UnsubscribeRequested:
					a.redisSub.Unsubscribe(symbol)
					metrics.DistRedisUnsubscribeOps.Inc()
					a.log.Underlying().Info().Str("symbol", symbol).
						Str("event", "redis_channel_unsubscribed").
						Msg("distributed: unsubscribed from Redis channel")
				}
			}),
		)
		a.log.Underlying().Info().Str("event", "distributed_router_initialized").Msg("distributed router: initialized")
	}

	// ── 2c. Snapshot Service ──────────────────────────────────
	// Maintains in-memory latest market state per symbol.
	// New subscribers receive current snapshots before live streaming.
	// TTL-based eviction removes inactive symbols to prevent memory leaks.
	// Checkpoint persistence enables fast restart recovery.
	snapLogger := log.SnapshotService(a.log)
	snapOpts := []snapshot.Option{
		snapshot.WithMaxAge(24 * time.Hour), // evict symbols inactive for 24h
		snapshot.WithLogger(snapLogger),
	}
	if a.cfg.Snapshot.Enabled {
		snapOpts = append(snapOpts, snapshot.WithCheckpoint(
			a.cfg.Snapshot.CheckpointPath,
			a.cfg.Snapshot.CheckpointInterval,
		))
	}
	snap := snapshot.New(snapOpts...)
	a.registerComponent(component{
		name:  "snapshot-service",
		order: 25,
		start: func(ctx context.Context) error {
			snap.Start(ctx)

			// Recovery: load checkpoint + replay missing events.
			var repo eventlog.Repository
			if a.eventLog != nil {
				repo = a.eventLog
			}
			if err := snap.Recover(ctx, repo); err != nil {
				snapLogger.Underlying().Warn().Err(err).Str("event", "recovery_failed").Msg("snapshot-service: recovery failed, starting fresh")
			}

			snap.MarkReady()
			snapLogger.Underlying().Info().
				Int("symbols", snap.Count()).
				Str("event", "snapshot_service_started").
				Msg("snapshot-service: started and ready")
			return nil
		},
		stop: func(ctx context.Context) error {
			snap.Stop()
			snapLogger.Underlying().Info().
				Int("symbols", snap.Count()).
				Str("event", "snapshot_service_stopped").
				Msg("snapshot-service: stopped")
			return nil
		},
		health: func(ctx context.Context) platform.HealthStatus {
			if !snap.IsReady() {
				return platform.Degraded("warming up")
			}
			return platform.OK()
		},
	})

	// ── 3. WebSocket Gateway ────────────────────────────────────
	// Rate limit: 500 new connections/sec per gateway to prevent thundering herd.
	// Gateway ID is used for sticky session routing verification.
	// When Redis is enabled, use the distributed router for gateway independence.
	tmForGateway := topicmanager.Manager(tm)
	if router != nil {
		tmForGateway = router
	}
	gwLogger := log.WebSocketGateway(a.log, a.cfg.Server.GetGatewayID())
	gw := websocket.NewGatewayWithSnapshot(tmForGateway, snap, gwLogger, metrics, 500, a.cfg.Server.GetGatewayID())
	a.gateway = gw
	a.registerComponent(component{
		name:  "websocket-gateway",
		order: 30,
		stop: func(ctx context.Context) error {
			gw.Shutdown(ctx)
			a.log.Underlying().Info().
				Int("connections_drained", gw.ClientCount()).
				Str("event", "gateway_stopped").
				Msg("websocket-gateway: stopped")
			return nil
		},
		health: func(ctx context.Context) platform.HealthStatus {
			count := gw.ClientCount()
			if count >= websocket.MaxConnections() {
				return platform.Degraded("at connection limit")
			}
			return platform.OK()
		},
	})

	// ── 4. Redis Client (shared) ───────────────────────────────
	// Create a single Redis client when Redis is enabled, shared by
	// publisher and subscriber.
	var redisClient *redis.Client
	if a.cfg.Redis.Enabled {
		redisClient = redisbus.NewClient(a.cfg.Redis.Addr, a.cfg.Redis.Password.Value(), a.cfg.Redis.DB)
		if err := redisbus.Ping(context.Background(), redisClient); err != nil {
			return fmt.Errorf("redis ping: %w", err)
		}
	}

	// ── 4b. Discovery Redis Client ─────────────────────────────
	// Subscriber-only gateways don't create a publisher, so we need
	// a separate client for service discovery when feed is disabled.
	var discoveryRedis *redis.Client

	// ── 5. Exchange Adapters ──────────────────────────────────────
	// Only create the exchange manager and pipeline when feed is enabled.
	// Subscriber-only gateways skip this and only run Redis subscriber + WS gateway.
	if a.cfg.Feed.Enabled {
		// Update simulator config if legacy Feed config options are used.
		adapters := a.cfg.Exchange.Adapters
		for i := range adapters {
			if adapters[i].Name == "simulator" {
				if adapters[i].Custom == nil {
					adapters[i].Custom = make(map[string]interface{})
				}
				if a.cfg.Feed.BenchmarkMode {
					adapters[i].Custom["tick_interval_ms"] = 0.1 // 100 microseconds
					a.log.Underlying().Info().Str("event", "benchmark_mode_enabled").Msg("benchmark mode: high-throughput feed (~50K msg/sec)")
				} else if a.cfg.Feed.TickInterval > 0 {
					adapters[i].Custom["tick_interval_ms"] = float64(a.cfg.Feed.TickInterval.Milliseconds())
					a.log.Underlying().Info().Dur("tick_interval", a.cfg.Feed.TickInterval).Str("event", "custom_tick_interval").Msg("custom feed tick interval")
				}
				// Optionally inherit global symbols if configured
				if len(a.cfg.Feed.Symbols) > 0 {
					// We only override if it's the exact default, but for backwards compatibility we'll just merge
					// Let's just rely on the adapter's symbols from config.go
				}
			}
		}

		f, err := exchange.NewManager(adapters, a.log)
		if err != nil {
			return fmt.Errorf("exchange manager: %w", err)
		}
		a.registerComponent(component{
			name:  "exchange-manager",
			order: 40,
			stop: func(ctx context.Context) error {
				a.log.Underlying().Info().
					Str("feed", f.Name()).
					Str("event", "exchange_manager_stopped").
					Msg("exchange-manager: stopped")
				return nil
			},
			health: func(ctx context.Context) platform.HealthStatus {
				return platform.OK()
			},
		})

		// ── 6. Pipeline (Feed → Publisher) ──────────────────────────
		// When Redis is enabled, the Pipeline publishes to Redis and a
		// RedisSubscriber forwards events to the local TopicManager.
		// When Redis is disabled, the Pipeline publishes directly to the
		// local TopicManager (single-instance mode).
		var publisher pubsub.Publisher

		if a.cfg.Redis.Enabled {
			a.log.Underlying().Info().Str("addr", a.cfg.Redis.Addr).Str("event", "redis_distribution_enabled").Msg("redis distribution enabled")

			// Create Redis publisher with async workers (non-blocking).
			// In benchmark mode, use more workers and larger queue for high throughput.
			pubWorkers := 4
			pubQueueSize := 8192
			if a.cfg.Feed.BenchmarkMode {
				pubWorkers = 32
				pubQueueSize = 262144
				a.log.Underlying().Info().
					Int("workers", pubWorkers).
					Int("queue_size", pubQueueSize).
					Str("event", "high_capacity_publisher").
					Msg("benchmark mode: high-capacity publisher")
			}
			redisPub := redisbus.NewPublisher(redisClient, a.log,
				redisbus.WithWorkers(pubWorkers),
				redisbus.WithQueueSize(pubQueueSize),
			)
			a.redisClient = redisPub

			publisher = redisPub

			// Register Redis components for lifecycle management.
			a.registerComponent(component{
				name:  "redis-publisher",
				order: 15,
				stop: func(ctx context.Context) error {
					redisPub.Close() // drains workers, waits for in-flight publishes
					a.log.Underlying().Info().Str("event", "redis_publisher_stopped").Msg("redis-publisher: stopped")
					return nil
				},
				health: func(ctx context.Context) platform.HealthStatus {
					if err := redisbus.Ping(ctx, redisClient); err != nil {
						return platform.Unhealthy("redis unreachable: " + err.Error())
					}
					return platform.OK()
				},
			})
		} else {
			// Single-instance mode: publish directly to local TopicManager.
			publisher = &topicPublisher{tm: tm}
		}

		// Wrap publisher with persistence layer when database is enabled.
		// Best-effort persist before publish: availability over strict durability.
		if a.eventLog != nil {
			publisher = eventlog.NewPersistingPublisher(a.eventLog, publisher, a.log)
			a.log.Underlying().Info().Str("event", "persisting_publisher_active").Msg("event-log: persisting publisher active")
		}

		// Wrap publisher with snapshot decorator to maintain in-memory market state.
		publisher = snapshot.NewSnapshotPublisher(publisher, snap)

		// Choose sequencer implementation: Redis-backed for distributed
		// consistency when scaled horizontally, in-memory for single instance.
		var seq sequencer.Generator
		if a.cfg.Redis.Enabled {
			seq = sequencer.NewRedisSequencer(redisClient, "seq:")
			a.log.Underlying().Info().Str("event", "distributed_sequencer").Msg("using distributed Redis sequencer")
		} else {
			seq = sequencer.New()
			a.log.Underlying().Info().Str("event", "in_memory_sequencer").Msg("using in-memory sequencer (single instance)")
		}

		feedLogger := log.FeedGenerator(a.log, f.Name())
		pipeline := feed.NewPipeline(f, publisher, seq, feedLogger, metrics, a.heartbeat)
		a.pipeline = pipeline
		a.registerComponent(component{
			name:  "pipeline",
			order: 50,
			stop: func(ctx context.Context) error {
				a.log.Underlying().Info().Str("event", "pipeline_stopped").Msg("pipeline: stopped")
				return nil
			},
			health: func(ctx context.Context) platform.HealthStatus {
				return platform.OK()
			},
		})
	} else {
		a.log.Underlying().Info().Str("event", "feed_disabled").Msg("feed generator disabled (subscriber-only mode)")
	}

	// ── 7. Redis Subscriber (always when Redis is enabled) ──────
	// Both primary and subscriber-only gateways run a Redis subscriber
	// to receive market data from the distributed bus. The subscriber
	// forwards events to the local TopicManager (or distributed router).
	// Dynamic subscriptions: only subscribes to Redis channels for
	// symbols that local clients actually need (gateway independence).
	if a.cfg.Redis.Enabled {
		mode := "subscriber-only mode"
		if a.cfg.Feed.Enabled {
			mode = "primary mode (publisher + subscriber)"
		}
		a.log.Underlying().Info().Str("addr", a.cfg.Redis.Addr).Str("mode", mode).Str("event", "redis_subscriber_configured").Msg("redis subscriber")

		// Stale data protection: when no message is received for 5s,
		// broadcast a degradation notice to all connected WebSocket clients
		// so their UI shows a disconnected/stale state instead of frozen data.
		staleCallback := func() {
			a.log.Underlying().Warn().Str("event", "stale_data_detected").Msg("redis subscriber: stale data detected, broadcasting degradation notice")
			if a.gateway != nil {
				a.gateway.Broadcast(websocket.ServerMessage{
					Type:    "system_degraded",
					Payload: "Market data stream interrupted. Data may be stale.",
				})
			}
		}

		// Create Redis subscriber that feeds the local TopicManager.
		// When distributed router is active, it feeds the router which
		// manages dynamic Redis subscriptions per local client demand.
		forwardTarget := topicmanager.Manager(tm)
		if router != nil {
			forwardTarget = router
		}
		redisSub := redisbus.NewSubscriber(redisClient, forwardTarget, a.log,
			redisbus.WithStaleCallback(5*time.Second, staleCallback),
		)
		a.redisSub = redisSub

		// No pre-subscribe-all: dynamic subscriptions are managed by the
		// distributed router. When a client subscribes to a symbol, the
		// router calls redisSub.Subscribe() via the onChange callback.

		a.registerComponent(component{
			name:  "redis-subscriber",
			order: 25,
			stop: func(ctx context.Context) error {
				redisSub.Stop()
				a.log.Underlying().Info().
					Uint64("received", redisSub.Received()).
					Str("event", "redis_subscriber_stopped").
					Msg("redis-subscriber: stopped")
				return nil
			},
			health: func(ctx context.Context) platform.HealthStatus {
				if redisSub.IsStale() {
					return platform.Unhealthy("market data stream is stale")
				}
				return platform.OK()
			},
		})
	}

	// ── 7b. Distributed Router Metrics ────────────────────────
	// Periodically updates Prometheus gauges for the distributed router.
	if router != nil {
		a.registerComponent(component{
			name:  "distributed-router-background",
			order: 26,
			start: func(ctx context.Context) error {
				router.StartReconciler(ctx, 5*time.Second)
				a.log.Underlying().Info().Str("event", "reconciler_started").Msg("distributed-router: reconciler started")
				return nil
			},
			health: func(ctx context.Context) platform.HealthStatus {
				return platform.OK()
			},
			stop: func(ctx context.Context) error {
				return nil
			},
		})
		// Update metrics immediately and let the health check refresh them.
		routerMetricsUpdate := func() {
			metrics.DistRedisSubscriptions.Set(float64(router.ActiveRedisSubscriptions()))
			metrics.DistSymbolsLocalSubs.Set(float64(router.SymbolsWithLocalSubscribers()))
			metrics.DistEventsRouted.Add(float64(router.EventsRouted()))
		}
		routerMetricsUpdate()
	}

	// ── 8. Service Discovery Registry ──────────────────────────
	// When discovery is enabled, register this gateway in the Redis
	// registry with a TTL heartbeat. Other gateways and load balancers
	// can query /gateways to discover active instances.
	if a.cfg.Discovery.Enabled {
		// Determine which Redis client to use for discovery.
		var regRedis *redis.Client
		if redisClient != nil {
			regRedis = redisClient // publisher already created
		} else if a.cfg.Redis.Enabled {
			// Subscriber-only gateway: create a dedicated client for discovery.
			discoveryRedis = redisbus.NewClient(a.cfg.Redis.Addr, a.cfg.Redis.Password.Value(), a.cfg.Redis.DB)
			regRedis = discoveryRedis
		}

		if regRedis != nil {
			opts := []discovery.RegistryOption{}
			if a.cfg.Discovery.TTL > 0 {
				opts = append(opts, discovery.WithTTL(a.cfg.Discovery.TTL))
			}
			if a.cfg.Discovery.HeartbeatInterval > 0 {
				opts = append(opts, discovery.WithHeartbeatInterval(a.cfg.Discovery.HeartbeatInterval))
			}
			// Deep health check: skip heartbeat if gateway is shutting down
			// or in a degraded state. This prevents zombie gateways from
			// holding registrations when the local process is unhealthy.
			opts = append(opts, discovery.WithHealthCheck(func() bool {
				return a.gatewayHealthy.Load()
			}))
			reg := discovery.NewRegistry(regRedis, a.log, opts...)
			a.registry = reg

			gatewayInfo := discovery.GatewayInfo{
				ID:            a.cfg.Server.GetGatewayID(),
				Addr:          a.cfg.Server.Host,
				Port:          a.cfg.Server.Port,
				Status:        "healthy",
				StartedAt:     time.Now(),
				LastHeartbeat: time.Now(),
			}

			a.registerComponent(component{
				name:  "service-discovery",
				order: 28, // after redis-subscriber, before http-server
			start: func(ctx context.Context) error {
				// Register with background context (startup ctx has 10s timeout).
				if err := reg.Register(context.Background(), gatewayInfo); err != nil {
					return fmt.Errorf("discovery register: %w", err)
				}
				a.log.Underlying().Info().
					Str("id", gatewayInfo.ID).
					Dur("ttl", reg.TTL()).
					Dur("heartbeat", reg.HeartbeatInterval()).
					Str("event", "service_discovery_registered").
					Msg("service discovery: registered")
				return nil
			},
			stop: func(ctx context.Context) error {
				// Stop heartbeat before deregistering.
				reg.StopHeartbeat()
				if err := reg.Deregister(ctx, a.cfg.Server.GetGatewayID()); err != nil {
					a.log.Underlying().Warn().Err(err).Str("event", "deregister_failed").Msg("service discovery: deregister failed")
				}
				a.log.Underlying().Info().Str("event", "service_discovery_deregistered").Msg("service discovery: deregistered")
				return nil
			},
				health: func(ctx context.Context) platform.HealthStatus {
					count, err := reg.Count(ctx)
					if err != nil {
						return platform.Degraded("discovery query failed: " + err.Error())
					}
					if count == 0 {
						return platform.Degraded("no gateways registered")
					}
					return platform.OK()
				},
			})
		} else {
			a.log.Underlying().Warn().Str("event", "discovery_disabled").Msg("service discovery requires redis — disabled")
		}
	}

	// ── 8b. Health Check Registration ───────────────────────────
	// Register health checks for all critical dependencies.
	// These are used by /readiness and /liveness endpoints.
	// Registry and heartbeat were created at the top of wire().

	// Register Redis check if Redis is enabled.
	// When Redis fails, evict all WebSocket clients so they reconnect
	// to a healthy gateway (prevents zombie connections — see health_check_review.md).
	if a.cfg.Redis.Enabled && redisClient != nil {
		a.healthRegistry.Register(healthcheck.RedisCheck(redisClient, func() {
			if a.gateway != nil {
				a.gateway.EvictAll(
					websocket.CloseInternalServerErr,
					"redis connection lost, reconnecting to healthy gateway",
				)
			}
		}))
	}

	// Register Postgres check if database is enabled
	if a.cfg.Database.Enabled && a.eventLog != nil {
		if pgRepo, ok := a.eventLog.(*eventlog.PostgresRepository); ok {
			a.healthRegistry.Register(healthcheck.PostgresCheck(pgRepo.DB()))
		}
	}

	// Register Snapshot check
	if snap != nil {
		a.healthRegistry.Register(healthcheck.SnapshotCheck(snap))
	}

	// Register Gateway check (capability only, not capacity — see health_check_review.md)
	a.healthRegistry.Register(healthcheck.GatewayCheck(gw))

	a.log.Underlying().Info().
		Int("checks", len(a.healthRegistry.Checks())).
		Str("event", "health_checks_registered").
		Msg("health check registry initialized")

	// ── 9. HTTP Server ──────────────────────────────────────────
	// Log level changer allows dynamic log level changes via HTTP.
	// Critical for incident response — enables DEBUG without restart.
	logChanger := &logLevelChanger{logger: a.log}
	httpRouter := transport.NewRouter(a.cfg, gw, a.log, metrics, gatherer, a, a.registry, a.eventLog, a.healthRegistry, a.heartbeat, logChanger)

	// Custom listener with TCP tuning for WebSocket workloads.
	// Enables TCP keepalive and sets larger read/write buffers to handle
	// thousands of concurrent WebSocket connections without resource exhaustion.
	ln, err := net.Listen("tcp", a.cfg.Server.Addr())
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	tunedLn := &tcpKeepAliveListener{ln.(*net.TCPListener)}

	srv := &http.Server{
		Handler:        httpRouter,
		ReadTimeout:    a.cfg.Server.ReadTimeout,
		WriteTimeout:   a.cfg.Server.WriteTimeout,
		MaxHeaderBytes: 1 << 20, // 1MB — sufficient for WS upgrade headers.
	}
	a.server = srv
	a.listener = tunedLn
	a.registerComponent(component{
		name:  "http-server",
		order: 60,
		stop: func(ctx context.Context) error {
			if err := srv.Shutdown(ctx); err != nil {
				return fmt.Errorf("http server shutdown: %w", err)
			}
			a.log.Underlying().Info().Str("event", "http_server_stopped").Msg("http-server: stopped")
			return nil
		},
		health: func(ctx context.Context) platform.HealthStatus {
			return platform.OK()
		},
	})

	a.log.Underlying().Info().Int("components", len(a.components)).Str("event", "wiring_complete").Msg("wiring complete")
	return nil
}

// initTracing initializes the OpenTelemetry SDK and registers a shutdown hook.
func (a *App) initTracing() error {
	cfg := tracing.Config{
		Enabled:     a.cfg.Tracing.Enabled,
		Endpoint:    a.cfg.Tracing.Endpoint,
		SampleRatio: a.cfg.Tracing.SampleRatio,
		ServiceName: "rtmds",
		Environment: a.cfg.Log.Format, // reuse log format as environment hint
		Version:     Version,
		Region:      "", // configurable via env in future
	}

	tracer, shutdown, err := tracing.InitTracer(cfg)
	if err != nil {
		return err
	}
	a.tracer = tracer

	// Register shutdown hook with order 5 (first to start, last to stop).
	a.registerComponent(component{
		name:  "tracing",
		order: 5,
		stop: func(ctx context.Context) error {
			if err := shutdown(ctx); err != nil {
				a.log.Underlying().Warn().Err(err).Str("event", "tracing_flush_failed").Msg("tracing: flush failed")
			}
			a.log.Underlying().Info().Str("event", "tracing_stopped").Msg("tracing: stopped")
			return nil
		},
		health: func(ctx context.Context) platform.HealthStatus {
			return platform.OK()
		},
	})

	return nil
}

// registerComponent adds a component to the lifecycle registry.
func (a *App) registerComponent(c component) {
	a.mu.Lock()
	a.components = append(a.components, c)
	a.mu.Unlock()
}

// HealthReport returns the health status of all registered components.
func (a *App) HealthReport(ctx context.Context) map[string]platform.HealthStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()

	report := make(map[string]platform.HealthStatus, len(a.components))
	for _, c := range a.components {
		if c.health != nil {
			report[c.name] = c.health(ctx)
		}
	}
	return report
}

// Run starts all application components in the correct order, blocks until
// ctx is cancelled, then shuts them down in reverse order.
//
// Startup sequence:
//  1. HTTP server starts listening (accepts connections but no data yet)
//  2. Feed + Pipeline start generating and routing data
//
// Shutdown sequence:
//  1. Feed stops generating data
//  2. Gateway drains active WebSocket connections
//  3. HTTP server stops accepting new requests
//  4. Metrics flushed
func (a *App) Run(ctx context.Context) error {
	// ── Startup ─────────────────────────────────────────────────
	startCtx, startCancel := context.WithTimeout(ctx, 10*time.Second)
	defer startCancel()

	if err := a.startup(startCtx); err != nil {
		return fmt.Errorf("startup failed: %w", err)
	}

	// Start Redis subscriber BEFORE accepting connections so the pubsub
	// handle is available when clients trigger dynamic subscriptions via
	// the distributed router's onChange callback.
	if a.redisSub != nil {
		a.redisSub.Start(ctx)
		a.log.Underlying().Info().
			Strs("symbols", a.cfg.Feed.Symbols).
			Str("event", "redis_subscriber_started").
			Msg("redis-subscriber: started with run context")
	}

	// Start service discovery heartbeat with the run context.
	// The startup context has a 10s timeout, but heartbeats must run
	// for the entire lifetime of the application.
	if a.registry != nil {
		info := discovery.GatewayInfo{
			ID:            a.cfg.Server.GetGatewayID(),
			Addr:          a.cfg.Server.Host,
			Port:          a.cfg.Server.Port,
			Status:        "healthy",
			StartedAt:     time.Now(),
			LastHeartbeat: time.Now(),
		}
		a.registry.StartHeartbeat(ctx, info)
		a.log.Underlying().Info().Str("event", "heartbeat_started").Msg("service discovery: heartbeat started")
	}

	a.log.Underlying().Info().
		Str("addr", a.listener.Addr().String()).
		Strs("symbols", a.cfg.Feed.Symbols).
		Str("event", "system_ready").
		Msg("system ready")

	// Mark gateway as healthy — heartbeat loop will now send refreshes.
	a.gatewayHealthy.Store(true)

	// ── Run ─────────────────────────────────────────────────────
	g, runCtx := errgroup.WithContext(ctx)

	// HTTP server — accepts connections after all components are initialized.
	// The redisSub.Start() call above ensures the pubsub handle is ready
	// for dynamic subscriptions triggered by incoming WebSocket clients.
	g.Go(func() error {
		if err := a.server.Serve(a.listener); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	// Graceful shutdown: when context is cancelled (SIGTERM/SIGINT),
	// call server.Shutdown to unblock Serve() so g.Wait() can return.
	go func() {
		<-runCtx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := a.server.Shutdown(shutCtx); err != nil {
			a.log.Underlying().Warn().Err(err).Str("event", "http_shutdown_error").Msg("http server shutdown error")
		}
	}()

	// Feed → Publisher pipeline (only when feed is enabled)
	if a.pipeline != nil {
		g.Go(func() error {
			if err := a.pipeline.Run(runCtx); err != nil {
				return fmt.Errorf("pipeline: %w", err)
			}
			return nil
		})
	}

	// Wait for shutdown signal
	if err := g.Wait(); err != nil {
		// HTTP server returns ErrServerClosed on shutdown — that's expected.
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	// Mark gateway unhealthy — heartbeat loop will skip refreshes,
	// allowing the TTL to expire and the gateway to de-register.
	a.gatewayHealthy.Store(false)

	// Shutdown all components in reverse order (deregister, stop heartbeat, etc.).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	a.shutdown(shutdownCtx)

	return nil
}

// startup starts components in dependency order.
func (a *App) startup(ctx context.Context) error {
	a.mu.RLock()
	components := make([]component, len(a.components))
	copy(components, a.components)
	a.mu.RUnlock()

	// Sort by order
	sortComponents(components)

	for _, c := range components {
		a.log.Underlying().Info().Str("component", c.name).Str("event", "component_starting").Msg("starting")
		if c.start != nil {
			if err := c.start(ctx); err != nil {
				return fmt.Errorf("start %s: %w", c.name, err)
			}
		}
		a.mu.Lock()
		a.started = append(a.started, c.name)
		a.mu.Unlock()
	}
	return nil
}

// shutdown stops started components in reverse order.
func (a *App) shutdown(ctx context.Context) {
	a.mu.RLock()
	started := make([]string, len(a.started))
	copy(started, a.started)
	a.mu.RUnlock()

	// Build a map for quick lookup
	componentMap := make(map[string]component)
	a.mu.RLock()
	for _, c := range a.components {
		componentMap[c.name] = c
	}
	a.mu.RUnlock()

	// Stop in reverse order
	for i := len(started) - 1; i >= 0; i-- {
		name := started[i]
		c, ok := componentMap[name]
		if !ok || c.stop == nil {
			continue
		}
		a.log.Underlying().Info().Str("component", name).Str("event", "component_stopping").Msg("stopping")
		if err := c.stop(ctx); err != nil {
			a.log.Underlying().Error().Err(err).Str("component", name).Str("event", "component_stop_failed").Msg("stop failed")
		}
	}
}

// sortComponents sorts components by startup order (ascending).
func sortComponents(components []component) {
	sort.Slice(components, func(i, j int) bool {
		return components[i].order < components[j].order
	})
}

// topicPublisher adapts topicmanager.Manager to the pubsub.Publisher
// interface expected by feed.Pipeline.
type topicPublisher struct {
	tm topicmanager.Manager
}

func (tp *topicPublisher) Publish(ctx context.Context, event marketdata.MarketEvent) {
	tp.tm.Publish(ctx, event)
}

// logLevelChanger implements transport.LogLevelChanger for dynamic log level changes.
type logLevelChanger struct {
	logger *log.Logger
}

// SetLogLevel changes the log level at runtime.
func (c *logLevelChanger) SetLogLevel(level string) error {
	return c.logger.SetLogLevel(level)
}
