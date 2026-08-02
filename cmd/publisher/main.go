package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/sumit/rtmds/internal/app"
	"github.com/sumit/rtmds/internal/config"
	"github.com/sumit/rtmds/pkg/marketdata"
	"github.com/sumit/rtmds/pkg/protocol"

	// Register exchange adapters
	_ "github.com/sumit/rtmds/internal/adapters/crypto"
	_ "github.com/sumit/rtmds/internal/adapters/nasdaq"
	_ "github.com/sumit/rtmds/internal/adapters/nyse"
	_ "github.com/sumit/rtmds/internal/adapters/simulator"
)

func main() {
	cfgFile := flag.String("config", "", "path to YAML/TOML config file (optional)")
	flag.Parse()

	cfg, err := config.Load(*cfgFile)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("config error: %v\n", err))
		os.Exit(1)
	}

	// Publisher-specific config overrides
	cfg.Feed.Enabled = true       // Publisher must run the feed
	cfg.Discovery.Enabled = false // Publisher doesn't need to register as a gateway
	cfg.Redis.Enabled = true      // Must publish to Redis

	if cfg.Profiling.Enabled {
		runtime.SetMutexProfileFraction(cfg.Profiling.MutexFraction)
		runtime.SetBlockProfileRate(cfg.Profiling.BlockRate)
	}

	// Inject serializers into marketdata package for zero-copy broadcasting
	marketdata.ProtobufEncoder = protocol.NewProtobufSerializer()
	marketdata.FlatBuffersEncoder = protocol.NewFlatBuffersSerializer()

	// Use specialized publisher app builder
	application, err := app.NewPublisherApp(cfg)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("app build error: %v\n", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	start := time.Now()
	if err := application.Run(ctx); err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("run error: %v\n", err))
		os.Exit(1)
	}

	duration := time.Since(start)
	report := application.HealthReport(context.Background())
	reportJSON, _ := json.Marshal(report)
	fmt.Printf("shutdown complete in %v\nhealth: %s\n", duration.Round(time.Millisecond), reportJSON)
}
