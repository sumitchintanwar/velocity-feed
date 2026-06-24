package backpressure

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sumit/rtmds/internal/marketdata"
)

// Ring is a fixed-capacity ring buffer for MarketEvent.
// It implements the drop-oldest strategy: when full, the next write
// overwrites the oldest unread event.
//
// Protected by a mutex. The critical section is tiny (single slot
// read/write) so contention is minimal even under high publish rates.
type Ring struct {
	mu   sync.Mutex
	buf  []marketdata.MarketEvent
	cap  int
	mask int64

	head int64 // next write position
	tail int64 // next read position

	totalPushed  atomic.Uint64
	totalDropped atomic.Uint64

	// maxAge, if > 0, drops events older than this duration.
	// Enforced during Push: events with Timestamp + maxAge < now are dropped.
	maxAge time.Duration

	// Condition variable for signaling data availability.
	// Uses r.mu as its lock so that Push (which holds r.mu) and
	// WaitForData (which calls Wait under r.mu) are synchronized
	// on the same lock — preventing missed wakeups.
	cond *sync.Cond
}

// NewRing creates a ring buffer with the given capacity.
// Capacity is rounded up to the next power of 2.
func NewRing(capacity int) *Ring {
	return NewRingWithMaxAge(capacity, 0)
}

// NewRingWithMaxAge creates a ring buffer with max age enforcement.
// If maxAge > 0, events older than maxAge are dropped during Push.
func NewRingWithMaxAge(capacity int, maxAge time.Duration) *Ring {
	c := nextPow2(capacity)
	r := &Ring{
		buf:    make([]marketdata.MarketEvent, c),
		cap:    c,
		mask:   int64(c - 1),
		maxAge: maxAge,
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Push adds an event to the ring. If full, the oldest unread event
// is overwritten. Returns true if the event was written.
//
// If maxAge > 0, events with Timestamp + maxAge < now are dropped
// (returns false) to prevent stale data delivery.
//
// After writing, signals any goroutine blocked in WaitForData.
func (r *Ring) Push(ev marketdata.MarketEvent) bool {
	if r == nil || r.buf == nil {
		return false
	}

	// Check max age: drop events that are too old.
	if r.maxAge > 0 {
		if te, ok := ev.(interface{ GetTimestamp() time.Time }); ok {
			if time.Since(te.GetTimestamp()) > r.maxAge {
				r.totalDropped.Add(1)
				return false
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.buf[r.head&r.mask] = ev
	unread := r.head - r.tail
	r.head++
	if unread >= int64(r.cap) {
		r.totalDropped.Add(1)
		r.tail++
	}
	r.totalPushed.Add(1)

	// Signal the forwardLoop that data is available.
	// This is safe because we hold r.mu, which is the same lock
	// the consumer uses in WaitForData — no missed wakeup.
	r.cond.Signal()

	return true
}

// WaitForData blocks until the ring has at least one event or until
// the provided stop channel is closed. Returns true if data is available,
// false if shutdown was signaled.
//
// Uses sync.Cond with r.mu so that Signal() in Push() and Wait() here
// are coordinated on the same lock — eliminating the missed-wakeup race.
func (r *Ring) WaitForData(stopCh <-chan struct{}) bool {
	if r == nil || r.buf == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.tail >= r.head {
		// Check for shutdown under lock.
		select {
		case <-stopCh:
			return false
		default:
		}
		r.cond.Wait()
	}
	return true
}

// Pop removes and returns the oldest event. Returns the event and true
// if available, or nil and false if empty.
// Zeroes the slot after read to release the MarketEvent reference for GC.
func (r *Ring) Pop() (marketdata.MarketEvent, bool) {
	if r == nil || r.buf == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tail >= r.head {
		return nil, false
	}
	idx := r.tail & r.mask
	ev := r.buf[idx]
	r.buf[idx] = nil // release reference for GC
	r.tail++
	return ev, true
}

// Len returns the number of unread events.
func (r *Ring) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	n := r.head - r.tail
	r.mu.Unlock()
	if n < 0 {
		return 0
	}
	return int(n)
}

// Cap returns the ring capacity.
func (r *Ring) Cap() int {
	if r == nil {
		return 0
	}
	return r.cap
}

// TotalPushed returns the total number of events pushed.
func (r *Ring) TotalPushed() uint64 {
	if r == nil {
		return 0
	}
	return r.totalPushed.Load()
}

// TotalDropped returns the total number of events dropped (overwritten).
func (r *Ring) TotalDropped() uint64 {
	if r == nil {
		return 0
	}
	return r.totalDropped.Load()
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

// ---------- CachedRing (pre-encoded JSON support) ----------

// CachedRing is a fixed-capacity ring buffer for *CachedEvent.
// Same logic as Ring but carries pre-encoded JSON bytes.
type CachedRing struct {
	mu   sync.Mutex
	buf  []*marketdata.CachedEvent
	cap  int
	mask int64

	head int64
	tail int64

	totalPushed  atomic.Uint64
	totalDropped atomic.Uint64

	cond *sync.Cond
}

// NewCachedRing creates a ring buffer with the given capacity.
func NewCachedRing(capacity int) *CachedRing {
	c := nextPow2(capacity)
	r := &CachedRing{
		buf:  make([]*marketdata.CachedEvent, c),
		cap:  c,
		mask: int64(c - 1),
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Push adds an event to the ring. If full, the oldest is overwritten.
func (r *CachedRing) Push(ev *marketdata.CachedEvent) bool {
	if r == nil || r.buf == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.buf[r.head&r.mask] = ev
	unread := r.head - r.tail
	r.head++
	if unread >= int64(r.cap) {
		r.totalDropped.Add(1)
		r.tail++
	}
	r.totalPushed.Add(1)
	r.cond.Signal()
	return true
}

// WaitForData blocks until data is available or shutdown is signaled.
func (r *CachedRing) WaitForData(stopCh <-chan struct{}) bool {
	if r == nil || r.buf == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.tail >= r.head {
		select {
		case <-stopCh:
			return false
		default:
		}
		r.cond.Wait()
	}
	return true
}

// Pop removes and returns the oldest event.
func (r *CachedRing) Pop() (*marketdata.CachedEvent, bool) {
	if r == nil || r.buf == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tail >= r.head {
		return nil, false
	}
	idx := r.tail & r.mask
	ev := r.buf[idx]
	r.buf[idx] = nil
	r.tail++
	return ev, true
}

// Len returns the number of unread events.
func (r *CachedRing) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	n := r.head - r.tail
	r.mu.Unlock()
	if n < 0 {
		return 0
	}
	return int(n)
}

// Cap returns the ring capacity.
func (r *CachedRing) Cap() int {
	if r == nil {
		return 0
	}
	return r.cap
}

// TotalPushed returns the total number of events pushed.
func (r *CachedRing) TotalPushed() uint64 {
	if r == nil {
		return 0
	}
	return r.totalPushed.Load()
}

// TotalDropped returns the total number of events dropped.
func (r *CachedRing) TotalDropped() uint64 {
	if r == nil {
		return 0
	}
	return r.totalDropped.Load()
}
