package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPersistAndLoadState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	state := PersistedState{
		LastCursorTS:  time.Now(),
		LastCursorID:  12345,
		ActiveSymbols: []string{"AAPL", "MSFT", "GOOG"},
	}

	if err := PersistState(path, state); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	loaded, err := LoadPersistedState(path)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	if loaded == nil {
		t.Fatal("expected non-nil state")
	}
	if loaded.Version != 1 {
		t.Errorf("expected version 1, got %d", loaded.Version)
	}
	if loaded.LastCursorID != 12345 {
		t.Errorf("expected cursor ID 12345, got %d", loaded.LastCursorID)
	}
	if len(loaded.ActiveSymbols) != 3 {
		t.Errorf("expected 3 symbols, got %d", len(loaded.ActiveSymbols))
	}
}

func TestLoadPersistedState_NotExist(t *testing.T) {
	loaded, err := LoadPersistedState("/nonexistent/path/state.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded != nil {
		t.Fatal("expected nil for non-existent file")
	}
}

func TestPersistState_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	// Write initial state.
	state1 := PersistedState{LastCursorID: 100}
	if err := PersistState(path, state1); err != nil {
		t.Fatalf("first persist failed: %v", err)
	}

	// Overwrite with new state.
	state2 := PersistedState{LastCursorID: 200}
	if err := PersistState(path, state2); err != nil {
		t.Fatalf("second persist failed: %v", err)
	}

	// Verify no temp files remain.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("temp file left behind: %s", e.Name())
		}
	}

	// Verify final state.
	loaded, err := LoadPersistedState(path)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.LastCursorID != 200 {
		t.Errorf("expected cursor ID 200, got %d", loaded.LastCursorID)
	}
}

func TestPersistState_CreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deep", "state.json")

	state := PersistedState{LastCursorID: 42}
	if err := PersistState(path, state); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	loaded, err := LoadPersistedState(path)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.LastCursorID != 42 {
		t.Errorf("expected cursor ID 42, got %d", loaded.LastCursorID)
	}
}
