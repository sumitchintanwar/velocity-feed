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
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/clientqueue"
	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/internal/distribution/redisbus"
	"github.com/sumit/rtmds/internal/discovery"
	"github.com/sumit/rtmds/internal/feed"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/marketdata/simulator"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/pubsub"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/transport"
	"github.com/sumit/rtmds/internal/websocket"
	"golang.org/x/sync/errgroup"
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
	log      zerolog.Logger
	server   *http.Server
	listener net.Listener
	pipeline *feed.Pipeline
	gateway  *websocket.Gateway
	tm       topicmanager.Manager
	metrics  *platform.Metrics
	gatherer prometheus.Gatherer

	// Redis distribution components (nil when Redis is disabled).
	redisClient   *redisbus.Publisher  // nil when not using Redis
	redisSub      *redisbus.Subscriber // nil when not using Redis
	registry      *discovery.Registry  // nil when discovery is disabled

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
	log := platform.NewLogger(cfg.Log.Level, cfg.Log.Format)

	a := &App{
		cfg: cfg,
		log: log,
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
	a.log.Info().Msg("wiring components")

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
			a.log.Info().Msg("metrics: flushed")
			return nil
		},
		health: func(ctx context.Context) platform.HealthStatus {
			return platform.OK()
		},
	})

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
			a.log.Info().
				Int("topics", tm.TopicCount()).
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
					a.log.Info().Str("symbol", symbol).
						Msg("distributed: subscribed to Redis channel")
				case topicmanager.UnsubscribeRequested:
					a.redisSub.Unsubscribe(symbol)
					metrics.DistRedisUnsubscribeOps.Inc()
					a.log.Info().Str("symbol", symbol).
						Msg("distributed: unsubscribed from Redis channel")
				}
			}),
		)
		a.log.Info().Msg("distributed router: initialized")
	}

	// ── 3. WebSocket Gateway ────────────────────────────────────
	// Rate limit: 500 new connections/sec per gateway to prevent thundering herd.
	// Gateway ID is used for sticky session routing verification.
	// When Redis is enabled, use the distributed router for gateway independence.
	tmForGateway := topicmanager.Manager(tm)
	if router != nil {
		tmForGateway = router
	}
	gw := websocket.NewGateway(tmForGateway, a.log, metrics, 500, a.cfg.Server.GetGatewayID())
	a.gateway = gw
	a.registerComponent(component{
		name:  "websocket-gateway",
		order: 30,
		stop: func(ctx context.Context) error {
			gw.Shutdown(ctx)
			a.log.Info().
				Int("connections_drained", gw.ClientCount()).
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
		redisClient = redisbus.NewClient(a.cfg.Redis.Addr, a.cfg.Redis.Password, a.cfg.Redis.DB)
		if err := redisbus.Ping(context.Background(), redisClient); err != nil {
			return fmt.Errorf("redis ping: %w", err)
		}
	}

	// ── 4b. Discovery Redis Client ─────────────────────────────
	// Subscriber-only gateways don't create a publisher, so we need
	// a separate client for service discovery when feed is disabled.
	var discoveryRedis *redis.Client

	// ── 5. Feed Generator ───────────────────────────────────────
	// Only create the feed generator and pipeline when feed is enabled.
	// Subscriber-only gateways skip this and only run Redis subscriber + WS gateway.
	if a.cfg.Feed.Enabled {
		// Select simulator config based on benchmark mode or tick interval override.
		simCfg := simulator.DefaultConfig()
		if a.cfg.Feed.BenchmarkMode {
			simCfg = simulator.BenchmarkConfig()
			a.log.Info().Msg("benchmark mode: high-throughput feed (~50K msg/sec)")
		} else if a.cfg.Feed.TickInterval > 0 {
			simCfg.TickInterval = a.cfg.Feed.TickInterval
			a.log.Info().Dur("tick_interval", a.cfg.Feed.TickInterval).Msg("custom feed tick interval")
		}

		f, err := simulator.New(
			simCfg,
			marketdata.WallClock{},
			a.cfg.Feed.Symbols...,
		)
		if err != nil {
			return fmt.Errorf("simulator: %w", err)
		}
		a.registerComponent(component{
			name:  "feed-generator",
			order: 40,
			stop: func(ctx context.Context) error {
				a.log.Info().
					Str("feed", f.Name()).
					Msg("feed-generator: stopped")
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
			a.log.Info().Str("addr", a.cfg.Redis.Addr).Msg("redis distribution enabled")

			// Create Redis publisher with async workers (non-blocking).
			// In benchmark mode, use more workers and larger queue for high throughput.
			pubWorkers := 4
			pubQueueSize := 8192
			if a.cfg.Feed.BenchmarkMode {
				pubWorkers = 32
				pubQueueSize = 262144
				a.log.Info().
					Int("workers", pubWorkers).
					Int("queue_size", pubQueueSize).
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
					a.log.Info().Msg("redis-publisher: stopped")
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

		pipeline := feed.NewPipeline(f, publisher, a.log, metrics)
		a.pipeline = pipeline
		a.registerComponent(component{
			name:  "pipeline",
			order: 50,
			stop: func(ctx context.Context) error {
				a.log.Info().Msg("pipeline: stopped")
				return nil
			},
			health: func(ctx context.Context) platform.HealthStatus {
				return platform.OK()
			},
		})
	} else {
		a.log.Info().Msg("feed generator disabled (subscriber-only mode)")
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
		a.log.Info().Str("addr", a.cfg.Redis.Addr).Str("mode", mode).Msg("redis subscriber")

		// Stale data protection: when no message is received for 5s,
		// broadcast a degradation notice to all connected WebSocket clients
		// so their UI shows a disconnected/stale state instead of frozen data.
		staleCallback := func() {
			a.log.Warn().Msg("redis subscriber: stale data detected, broadcasting degradation notice")
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
				a.log.Info().
					Uint64("received", redisSub.Received()).
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
				a.log.Info().Msg("distributed-router: reconciler started")
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
			discoveryRedis = redisbus.NewClient(a.cfg.Redis.Addr, a.cfg.Redis.Password, a.cfg.Redis.DB)
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
					a.log.Info().
						Str("id", gatewayInfo.ID).
						Dur("ttl", reg.TTL()).
						Dur("heartbeat", reg.HeartbeatInterval()).
						Msg("service discovery: registered")
					return nil
				},
				stop: func(ctx context.Context) error {
					// Stop heartbeat before deregistering.
					reg.StopHeartbeat()
					if err := reg.Deregister(ctx, a.cfg.Server.GetGatewayID()); err != nil {
						a.log.Warn().Err(err).Msg("service discovery: deregister failed")
					}
					a.log.Info().Msg("service discovery: deregistered")
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
			a.log.Warn().Msg("service discovery requires redis — disabled")
		}
	}

	// ── 9. HTTP Server ──────────────────────────────────────────
	httpRouter := transport.NewRouter(a.cfg, gw, a.log, metrics, gatherer, a, a.registry)

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
			a.log.Info().Msg("http-server: stopped")
			return nil
		},
		health: func(ctx context.Context) platform.HealthStatus {
			return platform.OK()
		},
	})

	a.log.Info().Int("components", len(a.components)).Msg("wiring complete")
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
		a.log.Info().
			Strs("symbols", a.cfg.Feed.Symbols).
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
		a.log.Info().Msg("service discovery: heartbeat started")
	}

	a.log.Info().
		Str("addr", a.listener.Addr().String()).
		Strs("symbols", a.cfg.Feed.Symbols).
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
			a.log.Warn().Err(err).Msg("http server shutdown error")
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
		a.log.Info().Str("component", c.name).Msg("starting")
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
		a.log.Info().Str("component", name).Msg("stopping")
		if err := c.stop(ctx); err != nil {
			a.log.Error().Err(err).Str("component", name).Msg("stop failed")
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
