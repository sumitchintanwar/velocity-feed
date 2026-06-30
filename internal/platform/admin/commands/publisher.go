package commands

import (
	"context"
	"errors"
	"sync/atomic"
)

// PublisherController is an abstraction that represents the publisher's pause/resume state.
// In a real implementation, this would be backed by the actual Publisher service instance.
type PublisherController interface {
	Pause() error
	Resume() error
	IsPaused() bool
}

// MockPublisherController is provided for demonstration/testing until the real publisher is wired.
type MockPublisherController struct {
	paused atomic.Bool
}

func (m *MockPublisherController) Pause() error {
	if m.paused.Swap(true) {
		return errors.New("publisher is already paused")
	}
	return nil
}

func (m *MockPublisherController) Resume() error {
	if !m.paused.Swap(false) {
		return errors.New("publisher is not paused")
	}
	return nil
}

func (m *MockPublisherController) IsPaused() bool {
	return m.paused.Load()
}

// PausePublisherCommand pauses the distribution of live market data.
type PausePublisherCommand struct {
	Controller PublisherController
}

func (c *PausePublisherCommand) Execute(ctx context.Context) error {
	if c.Controller == nil {
		return errors.New("publisher controller is nil")
	}
	return c.Controller.Pause()
}

func (c *PausePublisherCommand) Name() string {
	return "PausePublisher"
}

// ResumePublisherCommand resumes the distribution of live market data.
type ResumePublisherCommand struct {
	Controller PublisherController
}

func (c *ResumePublisherCommand) Execute(ctx context.Context) error {
	if c.Controller == nil {
		return errors.New("publisher controller is nil")
	}
	return c.Controller.Resume()
}

func (c *ResumePublisherCommand) Name() string {
	return "ResumePublisher"
}
