package websocket

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/platform"
)

func TestHeartbeatManager_Timeouts(t *testing.T) {
	l := log.New(os.Stdout, "info")
	m, _ := platform.NewMetrics("test_hb")

	// Fast timeouts for testing
	hm := NewHeartbeatManager(l, m, 10*time.Millisecond, 2) // 20ms timeout
	hm.cleanupInterval = 5 * time.Millisecond               // fast cleanup

	go hm.Run()
	defer hm.Stop()

	timedOut := make(chan struct{})
	var once sync.Once
	hm.Register("client-1", func() {
		once.Do(func() { close(timedOut) })
	}, nil)

	// Simulate sending a ping
	hm.RecordPing("client-1")

	// Should time out after 20ms (wait 50ms just to be sure)
	select {
	case <-timedOut:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected client to time out")
	}
}

func TestHeartbeatManager_PongPreventsTimeout(t *testing.T) {
	l := log.New(os.Stdout, "info")
	m, _ := platform.NewMetrics("test_hb")

	// Fast timeouts for testing
	hm := NewHeartbeatManager(l, m, 10*time.Millisecond, 2)
	hm.cleanupInterval = 5 * time.Millisecond

	go hm.Run()
	defer hm.Stop()

	timedOut := make(chan struct{})
	var once sync.Once
	hm.Register("client-2", func() {
		once.Do(func() { close(timedOut) })
	}, nil)

	// Keep sending pongs to prevent timeout
	go func() {
		for i := 0; i < 10; i++ {
			hm.RecordPong("client-2")
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Should NOT time out
	select {
	case <-timedOut:
		t.Fatal("client timed out despite receiving pong")
	case <-time.After(50 * time.Millisecond):
		// success
	}
}
