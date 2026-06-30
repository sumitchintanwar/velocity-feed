package commands

import (
	"context"
	"errors"
	"sync/atomic"
)

// MaintenanceController controls the global maintenance state of the service.
type MaintenanceController interface {
	EnableMaintenance() error
	DisableMaintenance() error
	IsMaintenance() bool
}

type MockMaintenanceController struct {
	maintenance atomic.Bool
}

func (m *MockMaintenanceController) EnableMaintenance() error {
	if m.maintenance.Swap(true) {
		return errors.New("maintenance mode is already enabled")
	}
	return nil
}

func (m *MockMaintenanceController) DisableMaintenance() error {
	if !m.maintenance.Swap(false) {
		return errors.New("maintenance mode is not enabled")
	}
	return nil
}

func (m *MockMaintenanceController) IsMaintenance() bool {
	return m.maintenance.Load()
}

// EnableMaintenanceCommand puts the service into maintenance mode (e.g. returning 503s for health checks).
type EnableMaintenanceCommand struct {
	Controller MaintenanceController
}

func (c *EnableMaintenanceCommand) Execute(ctx context.Context) error {
	if c.Controller == nil {
		return errors.New("maintenance controller is nil")
	}
	return c.Controller.EnableMaintenance()
}

func (c *EnableMaintenanceCommand) Name() string {
	return "EnableMaintenanceMode"
}

// DisableMaintenanceCommand removes the service from maintenance mode.
type DisableMaintenanceCommand struct {
	Controller MaintenanceController
}

func (c *DisableMaintenanceCommand) Execute(ctx context.Context) error {
	if c.Controller == nil {
		return errors.New("maintenance controller is nil")
	}
	return c.Controller.DisableMaintenance()
}

func (c *DisableMaintenanceCommand) Name() string {
	return "DisableMaintenanceMode"
}
