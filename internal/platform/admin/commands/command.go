package commands

import (
	"context"
	"fmt"
	"sync"
)

// Command represents an operational action that can be executed.
type Command interface {
	Execute(ctx context.Context) error
	Name() string
}

// CommandFactory allows commands to be constructed dynamically per request (e.g. from a JSON payload)
type CommandFactory func(payload map[string]interface{}) (Command, error)

// CommandBus routes and executes commands.
type CommandBus struct {
	mu         sync.RWMutex
	factories  map[string]CommandFactory
}

func NewCommandBus() *CommandBus {
	return &CommandBus{
		factories: make(map[string]CommandFactory),
	}
}

// Register maps an operations path (e.g. "publisher/pause") to a factory that creates the Command.
func (b *CommandBus) Register(path string, factory CommandFactory) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.factories[path] = factory
}

// DispatchByPath dynamically creates and executes a command based on its registered path.
func (b *CommandBus) DispatchByPath(ctx context.Context, path string, payload map[string]interface{}) (string, error) {
	b.mu.RLock()
	factory, exists := b.factories[path]
	b.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("no command registered for path: %s", path)
	}

	cmd, err := factory(payload)
	if err != nil {
		return "", fmt.Errorf("failed to construct command: %w", err)
	}

	if err := cmd.Execute(ctx); err != nil {
		return cmd.Name(), fmt.Errorf("failed to execute command %s: %w", cmd.Name(), err)
	}

	return cmd.Name(), nil
}

// Dispatch executes a raw command directly.
func (b *CommandBus) Dispatch(ctx context.Context, cmd Command) error {
	if cmd == nil {
		return fmt.Errorf("command cannot be nil")
	}

	if err := cmd.Execute(ctx); err != nil {
		return fmt.Errorf("failed to execute command %s: %w", cmd.Name(), err)
	}
	return nil
}
