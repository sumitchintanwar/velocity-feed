package commands

import (
	"context"
	"testing"
)

func TestPublisherCommands(t *testing.T) {
	bus := NewCommandBus()
	controller := &MockPublisherController{}

	// Initial state is not paused
	if controller.IsPaused() {
		t.Error("expected publisher to not be paused initially")
	}

	// Pause
	pauseCmd := &PausePublisherCommand{Controller: controller}
	if err := bus.Dispatch(context.Background(), pauseCmd); err != nil {
		t.Fatalf("unexpected error pausing publisher: %v", err)
	}

	if !controller.IsPaused() {
		t.Error("expected publisher to be paused")
	}

	// Idempotent pause (should fail)
	if err := bus.Dispatch(context.Background(), pauseCmd); err == nil {
		t.Error("expected error pausing already paused publisher")
	}

	// Resume
	resumeCmd := &ResumePublisherCommand{Controller: controller}
	if err := bus.Dispatch(context.Background(), resumeCmd); err != nil {
		t.Fatalf("unexpected error resuming publisher: %v", err)
	}

	if controller.IsPaused() {
		t.Error("expected publisher to not be paused")
	}

	// Idempotent resume (should fail)
	if err := bus.Dispatch(context.Background(), resumeCmd); err == nil {
		t.Error("expected error resuming already running publisher")
	}
}

func TestMaintenanceCommands(t *testing.T) {
	bus := NewCommandBus()
	controller := &MockMaintenanceController{}

	if controller.IsMaintenance() {
		t.Error("expected maintenance mode to be false initially")
	}

	// Enable
	enableCmd := &EnableMaintenanceCommand{Controller: controller}
	if err := bus.Dispatch(context.Background(), enableCmd); err != nil {
		t.Fatalf("unexpected error enabling maintenance: %v", err)
	}

	if !controller.IsMaintenance() {
		t.Error("expected maintenance mode to be true")
	}

	// Disable
	disableCmd := &DisableMaintenanceCommand{Controller: controller}
	if err := bus.Dispatch(context.Background(), disableCmd); err != nil {
		t.Fatalf("unexpected error disabling maintenance: %v", err)
	}

	if controller.IsMaintenance() {
		t.Error("expected maintenance mode to be false")
	}
}
