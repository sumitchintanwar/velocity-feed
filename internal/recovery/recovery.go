// Package recovery implements the restart recovery orchestrator for the
// Real-Time Market Data System. It follows the design from
// RESTART_RECOVERY_DESIGN.md:
//
// Startup sequence:
//  1. Load configuration (validate)
//  2. Connect dependencies (Redis, Database)
//  3. Recover persistent state (snapshot checkpoint + event log replay)
//  4. Build in-memory state
//  5. Start data consumption
//  6. Accept traffic
//
// The RecoveryManager coordinates this sequence and ensures the system
// never accepts traffic before all dependencies are ready.
package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/log"
)

// State represents the current recovery state.
type State int

const (
	// StateInit is the initial state before recovery starts.
	StateInit State = iota
	// StateLoading means configuration is being loaded.
	StateLoading
	// StateConnecting means dependencies are being connected.
	StateConnecting
	// StateRecovering means persistent state is being recovered.
	StateRecovering
	// StateBuilding means in-memory state is being built.
	StateBuilding
	// StateReady means the system is ready to accept traffic.
	StateReady
	// StateFailed means recovery failed.
	StateFailed
)

// String returns a human-readable representation of the state.
func (s State) String() string {
	switch s {
	case StateInit:
		return "init"
	case StateLoading:
		return "loading"
	case StateConnecting:
		return "connecting"
	case StateRecovering:
		return "recovering"
	case StateBuilding:
		return "building"
	case StateReady:
		return "ready"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// RecoveryFile is the on-disk format for recovery state persistence.
type RecoveryFile struct {
	Version     int       `json:"version"`
	StartedAt   time.Time `json:"started_at"`
	LastState   string    `json:"last_state"`
	RecoveryCnt int       `json:"recovery_count"`
}

// RecoveryManager orchestrates the restart recovery sequence.
// It tracks the system state and ensures components are started
// in the correct dependency order.
type RecoveryManager struct {
	mu    sync.RWMutex
	state State
	log   *log.Logger

	// State persistence
	statePath string

	// Recovery metadata
	startedAt   time.Time
	recoveryCnt int

	// Dependency validation
	dependencies map[string]bool

	// Health callback
	healthCallback func(state State, reason string)
}

// Option configures the RecoveryManager.
type Option func(*RecoveryManager)

// WithLogger sets the logger.
func WithLogger(l *log.Logger) Option {
	return func(r *RecoveryManager) { r.log = l }
}

// WithStatePath enables state persistence to the given file path.
func WithStatePath(path string) Option {
	return func(r *RecoveryManager) { r.statePath = path }
}

// WithHealthCallback sets a callback for health status changes.
func WithHealthCallback(fn func(state State, reason string)) Option {
	return func(r *RecoveryManager) { r.healthCallback = fn }
}

// New creates a new RecoveryManager.
func New(opts ...Option) *RecoveryManager {
	r := &RecoveryManager{
		state:        StateInit,
		log:          log.New(io.Discard, "recovery"),
		dependencies: make(map[string]bool),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// State returns the current recovery state. Thread-safe.
func (r *RecoveryManager) State() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// IsReady returns true if the system is ready to accept traffic.
func (r *RecoveryManager) IsReady() bool {
	return r.State() == StateReady
}

// SetState transitions to a new recovery state. Thread-safe.
func (r *RecoveryManager) SetState(ctx context.Context, state State, reason string) {
	r.mu.Lock()
	oldState := r.state
	r.state = state
	r.mu.Unlock()

	log.Info(ctx, r.log).
		Str("from", oldState.String()).
		Str("to", state.String()).
		Str("reason", reason).
		Msg("recovery: state transition")

	if r.healthCallback != nil {
		r.healthCallback(state, reason)
	}

	// Persist state if configured (outside lock to avoid deadlock).
	if r.statePath != "" && state != StateInit {
		if err := r.persistState(); err != nil {
			log.Warn(ctx, r.log).Err(err).Msg("recovery: failed to persist state")
		}
	}
}

// RegisterDependency registers a dependency for validation.
func (r *RecoveryManager) RegisterDependency(name string) {
	r.mu.Lock()
	r.dependencies[name] = false
	r.mu.Unlock()
}

// DependencyReady marks a dependency as ready. Thread-safe.
func (r *RecoveryManager) DependencyReady(ctx context.Context, name string) {
	r.mu.Lock()
	r.dependencies[name] = true
	r.mu.Unlock()

	log.Info(ctx, r.log).Str("dependency", name).Msg("recovery: dependency ready")
}

// AllDependenciesReady returns true if all registered dependencies are ready.
func (r *RecoveryManager) AllDependenciesReady() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, ready := range r.dependencies {
		if !ready {
			return false
		}
	}
	return true
}

// WaitForDependencies blocks until all dependencies are ready or ctx is cancelled.
func (r *RecoveryManager) WaitForDependencies(ctx context.Context) error {
	r.SetState(ctx, StateConnecting, "waiting for dependencies")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if r.AllDependenciesReady() {
			log.Info(ctx, r.log).Msg("recovery: all dependencies ready")
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("recovery: dependency wait cancelled: %w", ctx.Err())
		case <-ticker.C:
			continue
		}
	}
}

// Recover executes the full recovery sequence:
//  1. Load configuration
//  2. Validate dependencies
//  3. Recover persistent state (snapshot)
//  4. Mark ready
//
// The recoverFn is called between dependency validation and ready.
// It should perform snapshot recovery and replay.
func (r *RecoveryManager) Recover(ctx context.Context, recoverFn func(ctx context.Context) error) error {
	r.startedAt = time.Now()

	// Step 1: Load configuration state (if persisted).
	r.SetState(ctx, StateLoading, "loading configuration")
	if err := r.loadState(ctx); err != nil {
		log.Warn(ctx, r.log).Err(err).Msg("recovery: failed to load previous state")
	}
	r.recoveryCnt++

	// Step 2: Wait for dependencies.
	if err := r.WaitForDependencies(ctx); err != nil {
		r.SetState(ctx, StateFailed, "dependency wait failed")
		return err
	}

	// Step 3: Recover persistent state.
	r.SetState(ctx, StateRecovering, "recovering persistent state")
	if recoverFn != nil {
		if err := recoverFn(ctx); err != nil {
			r.SetState(ctx, StateFailed, "recovery function failed")
			return fmt.Errorf("recovery: %w", err)
		}
	}

	// Step 4: Build in-memory state.
	r.SetState(ctx, StateBuilding, "building in-memory state")

	// Step 5: Mark ready.
	r.SetState(ctx, StateReady, "recovery complete")

	return nil
}

// persistState saves the recovery state to disk.
func (r *RecoveryManager) persistState() error {
	r.mu.RLock()
	cp := RecoveryFile{
		Version:     1,
		StartedAt:   r.startedAt,
		LastState:   r.state.String(),
		RecoveryCnt: r.recoveryCnt,
	}
	r.mu.RUnlock()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	dir := filepath.Dir(r.statePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	tmpPath := r.statePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	if err := os.Rename(tmpPath, r.statePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

// loadState loads the recovery state from disk.
func (r *RecoveryManager) loadState(ctx context.Context) error {
	if r.statePath == "" {
		return nil
	}

	data, err := os.ReadFile(r.statePath)
	if err != nil {
		return err
	}

	var cp RecoveryFile
	if err := json.Unmarshal(data, &cp); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	r.mu.Lock()
	r.recoveryCnt = cp.RecoveryCnt
	r.startedAt = cp.StartedAt
	r.mu.Unlock()

	log.Info(ctx, r.log).
		Str("last_state", cp.LastState).
		Int("recovery_count", cp.RecoveryCnt).
		Time("started_at", cp.StartedAt).
		Msg("recovery: loaded previous state")

	return nil
}

// RecoveryReport returns a summary of the recovery process.
type RecoveryReport struct {
	State        string        `json:"state"`
	RecoveryCnt  int           `json:"recovery_count"`
	StartedAt    time.Time     `json:"started_at"`
	Duration     time.Duration `json:"duration"`
	Dependencies map[string]bool `json:"dependencies"`
}

// Report generates a recovery report. Thread-safe.
func (r *RecoveryManager) Report() RecoveryReport {
	r.mu.RLock()
	defer r.mu.RUnlock()

	deps := make(map[string]bool, len(r.dependencies))
	for k, v := range r.dependencies {
		deps[k] = v
	}

	return RecoveryReport{
		State:        r.state.String(),
		RecoveryCnt:  r.recoveryCnt,
		StartedAt:    r.startedAt,
		Duration:     time.Since(r.startedAt),
		Dependencies: deps,
	}
}
