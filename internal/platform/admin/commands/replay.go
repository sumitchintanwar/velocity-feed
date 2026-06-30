package commands

import (
	"context"
	"time"
)

// ReplayController defines the operations required to control the Replay Engine.
type ReplayController interface {
	Pause() error
	Resume() error
	Seek(timestamp time.Time) error
}

// PauseReplayCommand pauses the active replay session.
type PauseReplayCommand struct {
	Controller ReplayController
}

func (c *PauseReplayCommand) Execute(ctx context.Context) error {
	return c.Controller.Pause()
}

func (c *PauseReplayCommand) Name() string {
	return "PauseReplayCommand"
}

// ResumeReplayCommand resumes the paused replay session.
type ResumeReplayCommand struct {
	Controller ReplayController
}

func (c *ResumeReplayCommand) Execute(ctx context.Context) error {
	return c.Controller.Resume()
}

func (c *ResumeReplayCommand) Name() string {
	return "ResumeReplayCommand"
}

// SeekReplayCommand seeks the active replay session to a specific timestamp.
type SeekReplayCommand struct {
	Controller ReplayController
	Timestamp  time.Time
}

func (c *SeekReplayCommand) Execute(ctx context.Context) error {
	return c.Controller.Seek(c.Timestamp)
}

func (c *SeekReplayCommand) Name() string {
	return "SeekReplayCommand"
}
