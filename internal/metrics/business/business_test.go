package business_test

import (
	"testing"

	"github.com/sumit/rtmds/internal/metrics/business/backpressure"
	"github.com/sumit/rtmds/internal/metrics/business/gateway"
	"github.com/sumit/rtmds/internal/metrics/business/publisher"
	"github.com/sumit/rtmds/internal/metrics/business/ratelimit"
	"github.com/sumit/rtmds/internal/metrics/business/reliability"
	"github.com/sumit/rtmds/internal/metrics/business/replay"
	"github.com/sumit/rtmds/internal/metrics/business/snapshot"
	"github.com/sumit/rtmds/internal/metrics/config"
	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/registry"
)

func TestBusinessMetrics_Instantiation(t *testing.T) {
	reg := registry.New()
	cfg := config.DefaultConfig()

	// 1. Publisher
	fPub := factory.New(reg, cfg, "") // root namespace for platform metrics
	pub, err := publisher.NewMetrics(fPub)
	if err != nil || pub == nil {
		t.Fatalf("failed to initialize publisher metrics: %v", err)
	}
	adapter := pub.NewAdapterMetrics("NASDAQ", "EQUITY")
	adapter.MessagesTotal.Inc()

	// 2. Gateway
	gate, err := gateway.NewMetrics(fPub)
	if err != nil || gate == nil {
		t.Fatalf("failed to initialize gateway metrics: %v", err)
	}
	gate.ActiveConnections.With("ws").Add(1)

	// 3. Replay
	rep, err := replay.NewMetrics(fPub)
	if err != nil || rep == nil {
		t.Fatalf("failed to initialize replay metrics: %v", err)
	}
	rep.RequestsTotal.With("success").Inc()

	// 4. Snapshot
	snap, err := snapshot.NewMetrics(fPub)
	if err != nil || snap == nil {
		t.Fatalf("failed to initialize snapshot metrics: %v", err)
	}
	snap.GenerationDuration.Observe(1.5)

	// 5. Reliability
	rel, err := reliability.NewMetrics(fPub)
	if err != nil || rel == nil {
		t.Fatalf("failed to initialize reliability metrics: %v", err)
	}
	rel.SequenceGapsTotal.With("NASDAQ", "L2").Inc()

	// 6. Rate Limit
	rate, err := ratelimit.NewMetrics(fPub)
	if err != nil || rate == nil {
		t.Fatalf("failed to initialize ratelimit metrics: %v", err)
	}
	rate.RequestsDeniedTotal.With("replay").Inc()

	// 7. Backpressure
	back, err := backpressure.NewMetrics(fPub)
	if err != nil || back == nil {
		t.Fatalf("failed to initialize backpressure metrics: %v", err)
	}
	back.EventsTotal.With("redis_publisher").Inc()
}
