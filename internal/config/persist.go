package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// PersistedState represents configuration state that survives restarts.
// This captures runtime state that cannot be derived from the config file
// alone (e.g., last checkpoint cursor, active symbols).
type PersistedState struct {
	Version       int       `json:"version"`
	PersistedAt   time.Time `json:"persisted_at"`
	LastCursorTS  time.Time `json:"last_cursor_ts,omitempty"`
	LastCursorID  int64     `json:"last_cursor_id,omitempty"`
	ActiveSymbols []string  `json:"active_symbols,omitempty"`
}

// PersistState saves runtime configuration state to disk.
// Uses atomic write (write to temp, then rename) for crash safety.
func PersistState(path string, state PersistedState) error {
	state.Version = 1
	state.PersistedAt = time.Now()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("config: marshal state: %w", err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("config: mkdir: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("config: write: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("config: rename: %w", err)
	}

	return nil
}

// LoadPersistedState loads runtime configuration state from disk.
// Returns nil if the file does not exist (first start).
func LoadPersistedState(path string) (*PersistedState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("config: read state: %w", err)
	}

	var state PersistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("config: unmarshal state: %w", err)
	}

	if state.Version != 1 {
		return nil, fmt.Errorf("config: unsupported state version %d", state.Version)
	}

	return &state, nil
}
