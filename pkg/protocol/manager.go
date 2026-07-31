package protocol

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/sumit/rtmds/pkg/marketdata"
)

// Manager orchestrates the execution of Codecs based on active client subscriptions.
type Manager struct {
	mu           sync.RWMutex
	codecs       map[Format]Serializer
	activeCounts map[Format]int
}

// NewManager creates a new serialization pipeline manager.
func NewManager() *Manager {
	return &Manager{
		codecs:       make(map[Format]Serializer),
		activeCounts: make(map[Format]int),
	}
}

// RegisterCodec adds a codec to the manager.
func (m *Manager) RegisterCodec(c Serializer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.codecs[c.Format()] = c
}

// TrackClient increments the active subscriber count for a given format.
func (m *Manager) TrackClient(f Format) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeCounts[f]++
}

// UntrackClient decrements the active subscriber count for a given format.
func (m *Manager) UntrackClient(f Format) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeCounts[f] > 0 {
		m.activeCounts[f]--
	}
}

// Serialize lazily encodes the event only into formats that have active subscribers.
// It returns a PreSerializedMessage that handles pooling lifecycle.
func (m *Manager) Serialize(event marketdata.MarketEvent) (PreSerializedMessage, error) {
	m.mu.RLock()
	// Copy the active formats so we don't hold the lock during serialization
	activeFormats := make([]Format, 0, len(m.activeCounts))
	for f, count := range m.activeCounts {
		if count > 0 {
			if _, ok := m.codecs[f]; ok {
				activeFormats = append(activeFormats, f)
			}
		}
	}
	m.mu.RUnlock()

	if len(activeFormats) == 0 {
		return nil, fmt.Errorf("no active clients or registered codecs")
	}

	payloads := make(map[Format]*bytes.Buffer, len(activeFormats))

	// Track buffers to return to the pool in case of partial errors or final release
	buffersToReturn := make([]*bytes.Buffer, 0, len(activeFormats))

	for _, f := range activeFormats {
		// Codec existence is guaranteed by the lock above since we only unregister on teardown,
		// but we still do a safe read.
		m.mu.RLock()
		codec := m.codecs[f]
		m.mu.RUnlock()

		buf := GetBuffer()
		buffersToReturn = append(buffersToReturn, buf)

		b, err := codec.Serialize(event)
		if err != nil {
			// On error, return all buffers to the pool
			for _, b := range buffersToReturn {
				PutBuffer(b)
			}
			return nil, fmt.Errorf("failed to encode format %s: %w", f.String(), err)
		}

		// Copy bytes to pooled buffer to manage lifecycle uniformly
		buf.Write(b)
		payloads[f] = buf
	}

	// Create a pooled message that will return all allocated buffers
	// exactly once when its reference count hits 0.
	msg := NewPooledMessage(payloads, func() {
		for _, b := range buffersToReturn {
			PutBuffer(b)
		}
	})

	return msg, nil
}
