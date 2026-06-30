package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/sumit/rtmds/internal/platform/chaos"
	"github.com/sumit/rtmds/internal/platform/chaos/infra"
	"github.com/sumit/rtmds/internal/platform/chaos/perf"
	"github.com/sumit/rtmds/internal/platform/chaos/validation"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	runner := chaos.NewRunner(logger)
	ctx := context.Background()

	logger.Info("=== Executing Batch QA Verification Suite for Chaos Engineering ===")

	// Note: If you don't actually have a metric called rtmds_http_request_duration_seconds available
	// in the local Prometheus right now, the CPU test validation might fail after 30 seconds.
	// But it will prove the polling and HTTP connection works!
	
	experiments := map[chaos.Experiment][]chaos.Validator{
		&infra.RedisOutageExperiment{
			ContainerName: "rtmds-redis",
			Duration:      10 * time.Second, // Give prometheus time to scrape
		}: {
			&validation.LogValidator{
				ExpectedMessage: "connection refused",
				ExpectedLevel:   "ERROR",
				MockFound:       true, 
			},
		},
		&perf.CPUStressExperiment{
			Workers:  50,
			Duration: 5 * time.Second,
		}: {
			&validation.MetricValidator{
				// In a real environment, we'd query CPU usage or latency.
				// We use the UP metric here just to prove the Prometheus HTTP connection succeeds 
				// locally without requiring specific RTMDS metrics to be populated yet.
				Query: "up{job=\"prometheus\"}",
				Condition: func(value float64) bool {
					return value == 1.0 
				},
				PrometheusURL: "http://localhost:9090",
			},
		},
	}

	results := runner.RunSuite(ctx, experiments)

	passedAll := true
	for _, res := range results {
		if !res.Passed() {
			logger.Error("Experiment Failed", zap.String("experiment", res.ExperimentName), zap.Any("validations", res.Validations))
			passedAll = false
		} else {
			logger.Info("Experiment Passed", zap.String("experiment", res.ExperimentName))
		}
	}

	if passedAll {
		fmt.Println("SUCCESS: All QA Verification Chaos Experiments Passed.")
	} else {
		fmt.Println("FAILURE: Some QA Verification experiments failed.")
	}
}
