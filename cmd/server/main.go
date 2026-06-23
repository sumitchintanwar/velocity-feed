// Command server is the main entry point for the Real-Time Market Data System.
// It loads configuration, constructs the application, and runs until it
// receives SIGINT or SIGTERM.
//
// Boot sequence:
//  1. Parse flags and load configuration
//  2. Build application (wire dependencies, no components started yet)
//  3. Set up signal handling
//  4. Run application (starts components in order, blocks until shutdown)
//
// Shutdown sequence (on SIGINT/SIGTERM):
//  1. Feed stops generating data
//  2. Gateway drains active WebSocket connections
//  3. HTTP server stops accepting new requests
//  4. Metrics flushed
//  5. Process exits
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sumit/rtmds/internal/app"
	"github.com/sumit/rtmds/internal/config"
)

func main() {
	// ── 1. Parse flags ──────────────────────────────────────────
	cfgFile := flag.String("config", "", "path to YAML/TOML config file (optional)")
	flag.Parse()

	// ── 2. Load configuration ───────────────────────────────────
	cfg, err := config.Load(*cfgFile)
	if err != nil {
		// Use plain stderr here — logger isn't set up yet.
		_, _ = os.Stderr.WriteString(fmt.Sprintf("config error: %v\n", err))
		os.Exit(1)
	}

	// ── 3. Build application (wire dependencies) ────────────────
	application, err := app.New(cfg)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("app build error: %v\n", err))
		os.Exit(1)
	}

	// ── 4. Set up signal handling ───────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── 5. Run application ──────────────────────────────────────
	// This blocks until shutdown is triggered by signal or error.
	start := time.Now()
	if err := application.Run(ctx); err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("run error: %v\n", err))
		os.Exit(1)
	}

	// ── 6. Final shutdown log ───────────────────────────────────
	duration := time.Since(start)
	report := application.HealthReport(context.Background())
	reportJSON, _ := json.Marshal(report)
	fmt.Printf("shutdown complete in %v\nhealth: %s\n", duration.Round(time.Millisecond), reportJSON)
}
