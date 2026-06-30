package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// SecretRotator watches secret files for changes and triggers a callback.
// This is used for Kubernetes Secret rotation — when a Secret is rotated,
// the mounted volume is updated, and this watcher detects the change.
//
// Usage:
//
//	watcher := config.NewSecretRotator([]config.SecretFile{
//	    {Path: "/etc/secrets/redis-password", OnChange: func(v string) { ... }},
//	}, logger)
//	go watcher.Watch(ctx)
type SecretRotator struct {
	secrets []SecretFile
	watcher *fsnotify.Watcher
	mu      sync.Mutex
	logger  interface{ Printf(string, ...interface{}) }
}

// SecretFile defines a secret file to watch and a callback when it changes.
type SecretFile struct {
	// Path is the filesystem path to the secret file.
	Path string

	// OnChange is called with the new file content when the file changes.
	OnChange func(newValue string)

	// Interval is how often to re-read the file (backup for missed events).
	// Default: 30 seconds.
	Interval time.Duration
}

// NewSecretRotator creates a new file watcher for secret rotation.
func NewSecretRotator(secrets []SecretFile, logger interface{ Printf(string, ...interface{}) }) *SecretRotator {
	if logger == nil {
		logger = log.Default()
	}

	// Set default intervals
	for i := range secrets {
		if secrets[i].Interval <= 0 {
			secrets[i].Interval = 30 * time.Second
		}
	}

	return &SecretRotator{
		secrets: secrets,
		logger:  logger,
	}
}

// Watch starts watching secret files for changes. It blocks until ctx is cancelled.
// It uses both fsnotify for filesystem events and a periodic poller as backup.
func (w *SecretRotator) Watch(ctx context.Context) error {
	var err error
	w.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("secret rotator: create watcher: %w", err)
	}
	defer w.watcher.Close()

	// Add all secret directories to the watcher
	for _, secret := range w.secrets {
		dir := filepath.Dir(secret.Path)
		if err := w.watcher.Add(dir); err != nil {
			w.logger.Printf("secret rotator: failed to watch directory %s: %v", dir, err)
			continue
		}
		w.logger.Printf("secret rotator: watching %s", secret.Path)
	}

	// Start periodic pollers as backup
	for _, secret := range w.secrets {
		go w.poller(ctx, secret)
	}

	// Process fsnotify events
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-w.watcher.Events:
			if !ok {
				return nil
			}
			w.handleEvent(event)
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return nil
			}
			w.logger.Printf("secret rotator: watcher error: %v", err)
		}
	}
}

// handleEvent processes a filesystem event and triggers callbacks if needed.
func (w *SecretRotator) handleEvent(event fsnotify.Event) {
	// Only care about write events
	if event.Op&fsnotify.Write == 0 && event.Op&fsnotify.Create == 0 {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, secret := range w.secrets {
		if event.Name == secret.Path {
			w.logger.Printf("secret rotator: change detected in %s", secret.Path)
			w.reloadSecret(secret)
		}
	}
}

// reloadSecret reads the file and calls the OnChange callback.
func (w *SecretRotator) reloadSecret(secret SecretFile) {
	data, err := os.ReadFile(secret.Path)
	if err != nil {
		w.logger.Printf("secret rotator: failed to read %s: %v", secret.Path, err)
		return
	}

	value := strings.TrimSpace(string(data))
	if secret.OnChange != nil {
		w.logger.Printf("secret rotator: reloading %s", secret.Path)
		secret.OnChange(value)
	}
}

// poller periodically re-reads secret files as a backup for missed fsnotify events.
func (w *SecretRotator) poller(ctx context.Context, secret SecretFile) {
	ticker := time.NewTicker(secret.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			w.reloadSecret(secret)
			w.mu.Unlock()
		}
	}
}
