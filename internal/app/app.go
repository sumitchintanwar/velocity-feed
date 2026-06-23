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
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/internal/feed"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/marketdata/simulator"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
	"github.com/sumit/rtmds/internal/transport"
	"github.com/sumit/rtmds/internal/websocket"
	"golang.org/x/sync/errgroup"
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
	pipeline *feed.Pipeline
	gateway  *websocket.Gateway
	tm       topicmanager.Manager
	metrics  *platform.Metrics
	gatherer prometheus.Gatherer

	// component registry for ordered lifecycle management
	mu         sync.RWMutex
	components []component
	started    []string // names of started components, in order
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
	tm := topicmanager.New(0)
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

	// ── 3. WebSocket Gateway ────────────────────────────────────
	gw := websocket.NewGateway(tm, a.log, metrics)
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

	// ── 4. Feed Generator ───────────────────────────────────────
	f, err := simulator.New(
		simulator.DefaultConfig(),
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

	// ── 5. Pipeline (Feed → Publisher) ──────────────────────────
	pipeline := feed.NewPipeline(f, &topicPublisher{tm: tm}, a.log)
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

	// ── 6. HTTP Server ──────────────────────────────────────────
	router := transport.NewRouter(a.cfg, gw, a.log, metrics, gatherer, a)
	srv := &http.Server{
		Addr:         a.cfg.Server.Addr(),
		Handler:      router,
		ReadTimeout:  a.cfg.Server.ReadTimeout,
		WriteTimeout: a.cfg.Server.WriteTimeout,
	}
	a.server = srv
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

	a.log.Info().
		Str("addr", a.server.Addr).
		Strs("symbols", a.cfg.Feed.Symbols).
		Msg("system ready")

	// ── Run ─────────────────────────────────────────────────────
	g, runCtx := errgroup.WithContext(ctx)

	// HTTP server
	g.Go(func() error {
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	// Feed → Publisher pipeline
	g.Go(func() error {
		if err := a.pipeline.Run(runCtx); err != nil {
			return fmt.Errorf("pipeline: %w", err)
		}
		return nil
	})

	// Wait for shutdown signal
	if err := g.Wait(); err != nil {
		// HTTP server returns ErrServerClosed on shutdown — that's expected.
		if err.Error() == "http server: Server closed" {
			return nil
		}
		return err
	}
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
