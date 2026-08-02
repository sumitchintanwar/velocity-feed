package chaos

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrChaosDrop is returned when a message is artificially dropped.
	ErrChaosDrop = errors.New("chaos: message dropped")
	// ErrChaosDisconnect is returned when a client is abruptly disconnected.
	ErrChaosDisconnect = errors.New("chaos: connection disconnected")
)

// Config defines the probability thresholds for chaos injection.
type Config struct {
	// DropProbability is the chance (0.0 - 1.0) a message is dropped.
	DropProbability float64
	// DelayProbability is the chance (0.0 - 1.0) a message is delayed.
	DelayProbability float64
	// MaxDelay is the maximum duration for a network delay.
	MaxDelay time.Duration
	// DisconnectProbability is the chance (0.0 - 1.0) a connection is dropped.
	DisconnectProbability float64
}

// SnapshotStats holds the point-in-time metrics for injected faults.
type SnapshotStats struct {
	Drops       int64
	Delays      int64
	Disconnects int64
}

// Engine orchestrates chaos injection on the network level.
// It wraps net.Listeners to simulate network faults without modifying production code.
type Engine struct {
	cfg   Config
	mu    sync.RWMutex
	conns map[*Conn]struct{}

	dropsInjected       atomic.Int64
	delaysInjected      atomic.Int64
	disconnectsInjected atomic.Int64

	killFunc func()
}

// Stats returns the current fault injection counts.
func (e *Engine) Stats() SnapshotStats {
	return SnapshotStats{
		Drops:       e.dropsInjected.Load(),
		Delays:      e.delaysInjected.Load(),
		Disconnects: e.disconnectsInjected.Load(),
	}
}

// NewEngine creates a new Chaos Engineering engine.
func NewEngine() *Engine {
	return &Engine{
		conns: make(map[*Conn]struct{}),
	}
}

// SetConfig updates the network fault injection configuration.
func (e *Engine) SetConfig(cfg Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg
}

// SetKillFunc registers a callback to gracefully (or forcefully) shut down
// the gateway during testing.
func (e *Engine) SetKillFunc(f func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.killFunc = f
}

// WrapListener returns a net.Listener that injects faults into accepted connections.
func (e *Engine) WrapListener(l net.Listener) net.Listener {
	return &Listener{
		Listener: l,
		engine:   e,
	}
}

// KillGateway executes the registered kill function, or forcefully terminates
// the process using SIGKILL if no function is registered.
func (e *Engine) KillGateway() {
	e.mu.RLock()
	kf := e.killFunc
	e.mu.RUnlock()

	if kf != nil {
		kf()
	} else {
		p, err := os.FindProcess(os.Getpid())
		if err == nil {
			_ = p.Signal(syscall.SIGKILL)
		}
	}
}

// Sleep simulates a global GC pause or thread starvation.
func (e *Engine) Sleep(d time.Duration) {
	time.Sleep(d)
}

// DisconnectClients forcefully drops all active TCP connections.
func (e *Engine) DisconnectClients() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for c := range e.conns {
		_ = c.Conn.Close()
	}
	// Purge the list
	e.conns = make(map[*Conn]struct{})
}

func (e *Engine) addConn(c *Conn) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.conns[c] = struct{}{}
}

func (e *Engine) removeConn(c *Conn) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.conns, c)
}

// RandomFailures starts a background loop that injects randomized chaos events
// at the specified interval.
func (e *Engine) RandomFailures(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r := rand.Float64()
			switch {
			case r < 0.1:
				e.DisconnectClients()
			case r < 0.2:
				e.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			case r < 0.5:
				e.mu.Lock()
				e.cfg = Config{
					DropProbability:       0.0, // Silent application-layer drops break TCP abstraction
					DelayProbability:      rand.Float64() * 0.2,
					MaxDelay:              time.Duration(rand.Intn(20)) * time.Millisecond,
					DisconnectProbability: rand.Float64() * 0.02,
				}
				e.mu.Unlock()
			case r < 0.6:
				e.mu.Lock()
				e.cfg = Config{} // Clear chaos
				e.mu.Unlock()
			}
		}
	}
}

// Listener wraps a net.Listener to produce chaos-enabled connections.
type Listener struct {
	net.Listener
	engine *Engine
}

// Accept intercepts the connection and wraps it.
func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		Conn:   c,
		engine: l.engine,
	}
	l.engine.addConn(conn)

	return conn, nil
}

// Conn wraps a net.Conn with configurable failure injection.
type Conn struct {
	net.Conn
	engine *Engine
}

// Read applies chaos before delegating to the underlying socket.
func (c *Conn) Read(b []byte) (n int, err error) {
	if err := c.applyChaos(); err != nil {
		if errors.Is(err, ErrChaosDisconnect) {
			_ = c.Close()
			return 0, io.EOF
		}
		if errors.Is(err, ErrChaosDrop) {
			// Simulate read failure.
			return 0, ErrChaosDrop
		}
	}

	n, err = c.Conn.Read(b)
	if err != nil {
		c.engine.removeConn(c)
	}
	return n, err
}

// Write applies chaos before delegating to the underlying socket.
func (c *Conn) Write(b []byte) (n int, err error) {
	if err := c.applyChaos(); err != nil {
		if errors.Is(err, ErrChaosDisconnect) {
			_ = c.Close()
			return 0, io.ErrClosedPipe
		}
		if errors.Is(err, ErrChaosDrop) {
			// Pretend the packet was written but blackholed by the network.
			return len(b), nil
		}
	}

	n, err = c.Conn.Write(b)
	if err != nil {
		c.engine.removeConn(c)
	}
	return n, err
}

// Close removes the connection from the engine's tracking map and closes it.
func (c *Conn) Close() error {
	c.engine.removeConn(c)
	return c.Conn.Close()
}

// applyChaos evaluates the current probabilities and injects latency or errors.
func (c *Conn) applyChaos() error {
	c.engine.mu.RLock()
	cfg := c.engine.cfg
	c.engine.mu.RUnlock()

	// Fast path for clean network
	if cfg.DisconnectProbability == 0 && cfg.DropProbability == 0 && cfg.DelayProbability == 0 {
		return nil
	}

	if cfg.DisconnectProbability > 0 && rand.Float64() < cfg.DisconnectProbability {
		c.engine.disconnectsInjected.Add(1)
		return ErrChaosDisconnect
	}

	if cfg.DropProbability > 0 && rand.Float64() < cfg.DropProbability {
		c.engine.dropsInjected.Add(1)
		return ErrChaosDrop
	}

	if cfg.DelayProbability > 0 && rand.Float64() < cfg.DelayProbability {
		if cfg.MaxDelay > 0 {
			c.engine.delaysInjected.Add(1)
			delay := time.Duration(rand.Int63n(int64(cfg.MaxDelay)))
			time.Sleep(delay)
		}
	}

	return nil
}
