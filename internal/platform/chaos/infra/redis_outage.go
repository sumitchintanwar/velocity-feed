package infra

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/sumit/rtmds/internal/platform/chaos"
)

// RedisOutageExperiment uses the local Docker daemon to pause the Redis container.
type RedisOutageExperiment struct {
	ContainerName string
	Duration      time.Duration
}

func (e *RedisOutageExperiment) Name() string {
	return "Infrastructure: Redis Outage"
}

func (e *RedisOutageExperiment) Setup(ctx context.Context) error {
	// Verify container is running before we try to break it
	cmd := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Status}}", e.ContainerName)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to inspect container %s: %w", e.ContainerName, err)
	}
	if string(out) != "running\n" {
		return fmt.Errorf("container %s is not running (state: %s)", e.ContainerName, string(out))
	}
	return nil
}

func (e *RedisOutageExperiment) InjectFault(ctx context.Context) error {
	// Pause the container to simulate an aggressive network partition / freeze
	cmd := exec.CommandContext(ctx, "docker", "pause", e.ContainerName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to pause container %s: %w", e.ContainerName, err)
	}
	return nil
}

func (e *RedisOutageExperiment) Validate(ctx context.Context, validators []chaos.Validator) []chaos.ValidationResult {
	// Let the fault soak so observers can record the metrics
	time.Sleep(e.Duration)

	var results []chaos.ValidationResult
	for _, v := range validators {
		res, err := v.Assert(ctx)
		if err != nil {
			results = append(results, chaos.ValidationResult{
				Success: false,
				Reason:  fmt.Sprintf("validator %s failed: %v", v.Name(), err),
			})
		} else {
			results = append(results, res)
		}
	}
	return results
}

func (e *RedisOutageExperiment) Teardown(ctx context.Context) error {
	// Unpause the container to restore service
	cmd := exec.CommandContext(ctx, "docker", "unpause", e.ContainerName)
	if err := cmd.Run(); err != nil {
		// If it wasn't paused, docker unpause returns an error. We can ignore it if the container is running.
		// For safety in this MVP, we just return the error.
		return fmt.Errorf("failed to unpause container %s: %w", e.ContainerName, err)
	}
	return nil
}
