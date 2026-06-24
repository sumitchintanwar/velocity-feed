package backpressure

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
)

// Channel is a configurable backpressure buffer that wraps a Go channel
// with pluggable policy behavior. It supports three modes:
//
//   - DropOldest: uses a ring buffer internally. On overflow, the oldest
//     event is evicted. The consumer channel always has the latest events.
//   - DropNewest: uses a buffered channel. On overflow, the incoming event
//     is silently dropped.
//   - Disconnect: uses drop-oldest as the underlying buffer and tracks drop
//     counts. After MaxConsecutiveDrops or sustained drop rate, calls the
//     disconnect callback.
//
// Channel is safe for concurrent use by multiple producers and a single consumer.
type Channel struct {
	cfg     Config
	log     zerolog.Logger
	metrics *Metrics

	// For DropOldest and Disconnect: ring buffer + forwarding goroutine.
	ring     *Ring
	forwardC chan marketdata.MarketEvent

	// For DropNewest: plain buffered channel.
	ch chan marketdata.MarketEvent

	// Disconnect tracking.
	consecutiveDrops atomic.Int64
	dropBuckets      *dropBucketWindow
	dropWindowMu     sync.Mutex
	disconnectOnce   sync.Once
	disconnectFn     DisconnectFunc
	closed           atomic.Bool
	totalDropped     atomic.Uint64

	// Lifecycle for forwardLoop.
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// dropBucketWindow is a circular array of drop-count buckets, one per second.
// Total memory: 10 buckets × 16 bytes = 160 bytes (vs unbounded []time.Time).
type dropBucketWindow struct {
	buckets    [10]int64
	slot       int   // current bucket index
	prevSec    int64 // second of the last recorded drop
	numBuckets int
}

func newDropBucketWindow() *dropBucketWindow {
	return &dropBucketWindow{numBuckets: 10}
}

// record adds one drop and advances the window. Returns the total drops in the window.
func (w *dropBucketWindow) record(now time.Time) int64 {
	sec := now.Unix()
	if sec > w.prevSec {
		adv := sec - w.prevSec
		if adv >= int64(w.numBuckets) {
			for i := range w.buckets {
				w.buckets[i] = 0
			}
		} else {
			for i := int64(1); i <= adv; i++ {
				w.buckets[(int64(w.slot)+i)%int64(w.numBuckets)] = 0
			}
		}
		w.slot = int(sec % int64(w.numBuckets))
		w.prevSec = sec
	}
	w.buckets[w.slot]++

	var total int64
	for _, v := range w.buckets {
		total += v
	}
	return total
}

// reset zeros all buckets.
func (w *dropBucketWindow) reset() {
	for i := range w.buckets {
		w.buckets[i] = 0
	}
	w.slot = 0
	w.prevSec = 0
}

// NewChannel creates a new backpressure channel with the given config.
func NewChannel(cfg Config, log zerolog.Logger, metrics *Metrics, onDisconnect DisconnectFunc) *Channel {
	if err := cfg.Validate(); err != nil {
		panic("backpressure: invalid config: " + err.Error())
	}

	c := &Channel{
		cfg:          cfg,
		log:          log,
		metrics:      metrics,
		disconnectFn: onDisconnect,
		stopCh:       make(chan struct{}),
		dropBuckets:  newDropBucketWindow(),
	}

	switch cfg.Policy {
	case PolicyDropOldest, PolicyDisconnect:
		c.ring = NewRingWithMaxAge(cfg.BufferSize, cfg.MaxAge)
		c.forwardC = make(chan marketdata.MarketEvent, min(cfg.BufferSize, 64))
		c.wg.Add(1)
		go c.forwardLoop()

	case PolicyDropNewest:
		c.ch = make(chan marketdata.MarketEvent, cfg.BufferSize)
	}

	return c
}

// Send pushes an event into the buffer. Returns true if accepted.
func (c *Channel) Send(ev marketdata.MarketEvent) bool {
	if c.closed.Load() {
		return false
	}

	switch c.cfg.Policy {
	case PolicyDropOldest:
		return c.sendDropOldest(ev)
	case PolicyDropNewest:
		return c.sendDropNewest(ev)
	case PolicyDisconnect:
		return c.sendDisconnect(ev)
	default:
		return false
	}
}

func (c *Channel) sendDropOldest(ev marketdata.MarketEvent) bool {
	prevDropped := c.ring.TotalDropped()
	written := c.ring.Push(ev)
	newDropped := c.ring.TotalDropped()

	if written {
		c.metrics.BufferOccupancy.Observe(float64(c.ring.Len()) / float64(c.ring.Cap()))
	}

	if newDropped > prevDropped {
		dropped := newDropped - prevDropped
		c.totalDropped.Add(dropped)
		c.metrics.EventsDroppedTotal.Add(float64(dropped))
	} else {
		c.resetConsecutiveOnSuccess()
	}

	return written
}

func (c *Channel) sendDropNewest(ev marketdata.MarketEvent) bool {
	select {
	case c.ch <- ev:
		c.metrics.BufferOccupancy.Observe(float64(len(c.ch)) / float64(c.cfg.BufferSize))
		c.resetConsecutiveOnSuccess()
		return true
	case <-c.stopCh:
		return false
	default:
		c.totalDropped.Add(1)
		c.metrics.EventsDroppedTotal.Inc()
		c.recordDrop()
		return false
	}
}

func (c *Channel) sendDisconnect(ev marketdata.MarketEvent) bool {
	prevDropped := c.ring.TotalDropped()
	written := c.ring.Push(ev)
	newDropped := c.ring.TotalDropped()

	if written {
		c.metrics.BufferOccupancy.Observe(float64(c.ring.Len()) / float64(c.ring.Cap()))
	}

	if newDropped > prevDropped {
		c.totalDropped.Add(newDropped - prevDropped)
		c.metrics.EventsDroppedTotal.Add(float64(newDropped - prevDropped))
		c.recordDrop()
	} else {
		c.resetConsecutiveOnSuccess()
	}

	return written
}

// resetConsecutiveOnSuccess resets the consecutive drop counter when an event
// is delivered without dropping. This prevents MaxConsecutiveDrops from acting
// as an absolute lifetime limit.
func (c *Channel) resetConsecutiveOnSuccess() {
	if c.consecutiveDrops.Load() > 0 {
		c.consecutiveDrops.Store(0)
		c.metrics.ConsecutiveDrops.Set(0)
	}
}

// recordDrop tracks a drop event and checks disconnect thresholds.
func (c *Channel) recordDrop() {
	now := time.Now()
	c.dropWindowMu.Lock()
	windowDrops := c.dropBuckets.record(now)
	c.dropWindowMu.Unlock()

	c.consecutiveDrops.Add(1)
	c.metrics.ConsecutiveDrops.Set(float64(c.consecutiveDrops.Load()))

	if c.cfg.MaxConsecutiveDrops > 0 && c.consecutiveDrops.Load() >= int64(c.cfg.MaxConsecutiveDrops) {
		c.triggerDisconnect("consecutive drops exceeded")
		return
	}
	if c.cfg.DropWindow > 0 && c.cfg.DropThreshold > 0 && windowDrops > 0 {
		dropRate := float64(windowDrops) / float64(c.cfg.BufferSize)
		if dropRate >= c.cfg.DropThreshold {
			c.triggerDisconnect("sustained drop rate exceeded")
		}
	}
}

// triggerDisconnect calls the disconnect function exactly once.
func (c *Channel) triggerDisconnect(reason string) {
	c.disconnectOnce.Do(func() {
		c.metrics.ConsumerDisconnectsTotal.Inc()
		c.log.Warn().Str("reason", reason).Msg("consumer disconnect triggered")
		if c.disconnectFn != nil {
			go c.disconnectFn(reason)
		}
	})
}

// forwardLoop drains the ring buffer and sends events to the consumer channel.
// Uses Ring.WaitForData() which parks on the ring's sync.Cond — no spin-wait.
// Blocks on forwardC send so the ring buffer fills up when the consumer is slow,
// allowing correct DropOldest overwrite behavior.
func (c *Channel) forwardLoop() {
	defer c.wg.Done()
	for {
		// Park until the ring has data or shutdown.
		if !c.ring.WaitForData(c.stopCh) {
			return
		}

		ev, ok := c.ring.Pop()
		if !ok {
			continue
		}

		// Block until the consumer reads or shutdown.
		select {
		case c.forwardC <- ev:
		case <-c.stopCh:
			return
		}
	}
}

// C returns the receive-only channel for the consumer.
func (c *Channel) C() <-chan marketdata.MarketEvent {
	if c.forwardC != nil {
		return c.forwardC
	}
	return c.ch
}

// Len returns the number of events currently buffered.
func (c *Channel) Len() int {
	if c.ring != nil {
		return c.ring.Len()
	}
	return len(c.ch)
}

// Cap returns the buffer capacity.
func (c *Channel) Cap() int {
	return c.cfg.BufferSize
}

// ConsecutiveDrops returns the current consecutive drop count.
func (c *Channel) ConsecutiveDrops() int64 {
	return c.consecutiveDrops.Load()
}

// TotalDropped returns the total number of events dropped.
func (c *Channel) TotalDropped() uint64 {
	return c.totalDropped.Load()
}

// ResetDrops resets the consecutive drop counter and clears the drop window.
func (c *Channel) ResetDrops() {
	c.consecutiveDrops.Store(0)
	c.metrics.ConsecutiveDrops.Set(0)
	c.dropWindowMu.Lock()
	c.dropBuckets.reset()
	c.dropWindowMu.Unlock()
}

// Close shuts down the channel and waits for the forward loop to exit.
func (c *Channel) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.stopCh)
		// Wake forwardLoop if it's parked in WaitForData.
		// Must hold r.mu to close the race between Broadcast and Wait.
		if c.ring != nil {
			c.ring.mu.Lock()
			c.ring.cond.Broadcast()
			c.ring.mu.Unlock()
		}
		c.wg.Wait()
		// Drain any remaining events from the ring buffer.
		if c.ring != nil {
			for {
				ev, ok := c.ring.Pop()
				if !ok {
					break
				}
				select {
				case c.forwardC <- ev:
				default:
					c.totalDropped.Add(1)
					c.metrics.EventsDroppedTotal.Inc()
				}
			}
		}
		if c.forwardC != nil {
			close(c.forwardC)
		}
		if c.ch != nil {
			close(c.ch)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------- CachedChannel (pre-encoded JSON support) ----------

// CachedChannel is a backpressure buffer for *CachedEvent.
// It mirrors Channel but carries pre-encoded JSON bytes,
// eliminating redundant serialization in the fan-out path.
type CachedChannel struct {
	cfg     Config
	log     zerolog.Logger
	metrics *Metrics

	ring     *CachedRing
	forwardC chan *marketdata.CachedEvent

	consecutiveDrops atomic.Int64
	dropBuckets      *dropBucketWindow
	dropWindowMu     sync.Mutex
	disconnectOnce   sync.Once
	disconnectFn     DisconnectFunc
	closed           atomic.Bool
	totalDropped     atomic.Uint64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewCachedChannel creates a CachedChannel with the given config.
func NewCachedChannel(cfg Config, log zerolog.Logger, metrics *Metrics, onDisconnect DisconnectFunc) *CachedChannel {
	if err := cfg.Validate(); err != nil {
		panic("backpressure: invalid cached channel config: " + err.Error())
	}
	c := &CachedChannel{
		cfg:          cfg,
		log:          log,
		metrics:      metrics,
		disconnectFn: onDisconnect,
		stopCh:       make(chan struct{}),
		dropBuckets:  newDropBucketWindow(),
	}
	switch cfg.Policy {
	case PolicyDropOldest, PolicyDisconnect:
		c.ring = NewCachedRing(cfg.BufferSize)
		c.forwardC = make(chan *marketdata.CachedEvent, min(cfg.BufferSize, 64))
		c.wg.Add(1)
		go c.forwardLoop()
	case PolicyDropNewest:
		c.forwardC = make(chan *marketdata.CachedEvent, cfg.BufferSize)
	}
	return c
}

// Send pushes a CachedEvent into the buffer. Returns true if accepted.
func (c *CachedChannel) Send(ev *marketdata.CachedEvent) bool {
	if c.closed.Load() {
		return false
	}
	switch c.cfg.Policy {
	case PolicyDropOldest:
		return c.sendDropOldest(ev)
	case PolicyDropNewest:
		return c.sendDropNewest(ev)
	case PolicyDisconnect:
		return c.sendDisconnect(ev)
	default:
		return false
	}
}

func (c *CachedChannel) sendDropOldest(ev *marketdata.CachedEvent) bool {
	prevDropped := c.ring.TotalDropped()
	written := c.ring.Push(ev)
	newDropped := c.ring.TotalDropped()
	if written {
		c.metrics.BufferOccupancy.Observe(float64(c.ring.Len()) / float64(c.ring.Cap()))
	}
	if newDropped > prevDropped {
		dropped := newDropped - prevDropped
		c.totalDropped.Add(dropped)
		c.metrics.EventsDroppedTotal.Add(float64(dropped))
	} else {
		c.resetConsecutiveOnSuccess()
	}
	return written
}

func (c *CachedChannel) sendDropNewest(ev *marketdata.CachedEvent) bool {
	select {
	case c.forwardC <- ev:
		c.metrics.BufferOccupancy.Observe(float64(len(c.forwardC)) / float64(c.cfg.BufferSize))
		c.resetConsecutiveOnSuccess()
		return true
	case <-c.stopCh:
		return false
	default:
		c.totalDropped.Add(1)
		c.metrics.EventsDroppedTotal.Inc()
		c.recordDrop()
		return false
	}
}

func (c *CachedChannel) sendDisconnect(ev *marketdata.CachedEvent) bool {
	prevDropped := c.ring.TotalDropped()
	written := c.ring.Push(ev)
	newDropped := c.ring.TotalDropped()
	if written {
		c.metrics.BufferOccupancy.Observe(float64(c.ring.Len()) / float64(c.ring.Cap()))
	}
	if newDropped > prevDropped {
		c.totalDropped.Add(newDropped - prevDropped)
		c.metrics.EventsDroppedTotal.Add(float64(newDropped - prevDropped))
		c.recordDrop()
	} else {
		c.resetConsecutiveOnSuccess()
	}
	return written
}

func (c *CachedChannel) resetConsecutiveOnSuccess() {
	if c.consecutiveDrops.Load() > 0 {
		c.consecutiveDrops.Store(0)
		c.metrics.ConsecutiveDrops.Set(0)
	}
}

func (c *CachedChannel) recordDrop() {
	now := time.Now()
	c.dropWindowMu.Lock()
	windowDrops := c.dropBuckets.record(now)
	c.dropWindowMu.Unlock()
	c.consecutiveDrops.Add(1)
	c.metrics.ConsecutiveDrops.Set(float64(c.consecutiveDrops.Load()))
	if c.cfg.MaxConsecutiveDrops > 0 && c.consecutiveDrops.Load() >= int64(c.cfg.MaxConsecutiveDrops) {
		c.triggerDisconnect("consecutive drops exceeded")
		return
	}
	if c.cfg.DropWindow > 0 && c.cfg.DropThreshold > 0 && windowDrops > 0 {
		dropRate := float64(windowDrops) / float64(c.cfg.BufferSize)
		if dropRate >= c.cfg.DropThreshold {
			c.triggerDisconnect("sustained drop rate exceeded")
		}
	}
}

func (c *CachedChannel) triggerDisconnect(reason string) {
	c.disconnectOnce.Do(func() {
		c.metrics.ConsumerDisconnectsTotal.Inc()
		c.log.Warn().Str("reason", reason).Msg("consumer disconnect triggered")
		if c.disconnectFn != nil {
			go c.disconnectFn(reason)
		}
	})
}

func (c *CachedChannel) forwardLoop() {
	defer c.wg.Done()
	for {
		if !c.ring.WaitForData(c.stopCh) {
			return
		}
		ev, ok := c.ring.Pop()
		if !ok {
			continue
		}
		// Non-blocking send: if the consumer channel is full, drop the event
		// rather than stalling the ring drain. This prevents the publisher from
		// seeing a full ring and blocking on Push — critical for maintaining
		// throughput when consumers are slow.
		select {
		case c.forwardC <- ev:
		case <-c.stopCh:
			return
		default:
			// Consumer channel full — drop to keep pipeline flowing.
			c.totalDropped.Add(1)
			c.metrics.EventsDroppedTotal.Inc()
		}
	}
}

// C returns the receive-only channel.
func (c *CachedChannel) C() <-chan *marketdata.CachedEvent {
	return c.forwardC
}

// Len returns buffered event count.
func (c *CachedChannel) Len() int {
	if c.ring != nil {
		return c.ring.Len()
	}
	return len(c.forwardC)
}

// Cap returns buffer capacity.
func (c *CachedChannel) Cap() int {
	return c.cfg.BufferSize
}

// ConsecutiveDrops returns current consecutive drop count.
func (c *CachedChannel) ConsecutiveDrops() int64 {
	return c.consecutiveDrops.Load()
}

// TotalDropped returns total dropped events.
func (c *CachedChannel) TotalDropped() uint64 {
	return c.totalDropped.Load()
}

// ResetDrops resets the consecutive drop counter.
func (c *CachedChannel) ResetDrops() {
	c.consecutiveDrops.Store(0)
	c.metrics.ConsecutiveDrops.Set(0)
	c.dropWindowMu.Lock()
	c.dropBuckets.reset()
	c.dropWindowMu.Unlock()
}

// Close shuts down the channel.
func (c *CachedChannel) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.stopCh)
		if c.ring != nil {
			c.ring.mu.Lock()
			c.ring.cond.Broadcast()
			c.ring.mu.Unlock()
		}
		c.wg.Wait()
		if c.ring != nil {
			for {
				ev, ok := c.ring.Pop()
				if !ok {
					break
				}
				select {
				case c.forwardC <- ev:
				default:
					c.totalDropped.Add(1)
					c.metrics.EventsDroppedTotal.Inc()
				}
			}
		}
		if c.forwardC != nil {
			close(c.forwardC)
		}
	}
}
