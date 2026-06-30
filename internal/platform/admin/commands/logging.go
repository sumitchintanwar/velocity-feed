package commands

import (
	"context"
	"errors"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SetLogLevelCommand dynamically updates the global log level.
type SetLogLevelCommand struct {
	Level     zap.AtomicLevel
	Requested string
}

func (c *SetLogLevelCommand) Execute(ctx context.Context) error {
	req := strings.ToLower(strings.TrimSpace(c.Requested))
	
	var newLevel zapcore.Level
	switch req {
	case "debug":
		newLevel = zapcore.DebugLevel
	case "info":
		newLevel = zapcore.InfoLevel
	case "warn":
		newLevel = zapcore.WarnLevel
	case "error":
		newLevel = zapcore.ErrorLevel
	default:
		return errors.New("invalid log level requested")
	}

	c.Level.SetLevel(newLevel)
	return nil
}

func (c *SetLogLevelCommand) Name() string {
	return "SetLogLevel"
}
