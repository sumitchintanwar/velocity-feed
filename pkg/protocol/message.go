package protocol

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

// PooledMessage implements PreSerializedMessage using atomic reference counting.
// It wraps multiple pre-serialized format buffers and a cleanup function.
type PooledMessage struct {
	payloads   map[Format]*bytes.Buffer
	refCount   atomic.Int32
	returnFunc func()
}

// NewPooledMessage creates a new PooledMessage wrapping the provided buffers.
// The returnFunc is called exactly once when the reference count reaches 0.
func NewPooledMessage(payloads map[Format]*bytes.Buffer, returnFunc func()) *PooledMessage {
	p := &PooledMessage{
		payloads:   payloads,
		returnFunc: returnFunc,
	}
	p.refCount.Store(1)
	return p
}

// Payload returns the raw byte slice for the requested format.
func (p *PooledMessage) Payload(format Format) ([]byte, error) {
	buf, ok := p.payloads[format]
	if !ok || buf == nil {
		return nil, fmt.Errorf("payload not available in format %s", format.String())
	}
	return buf.Bytes(), nil
}

// Retain increments the reference count.
func (p *PooledMessage) Retain() {
	p.refCount.Add(1)
}

// Release decrements the reference count.
// If the count drops to 0, it invokes the returnFunc to recycle the buffers.
// Panics if called when the refCount is already 0.
func (p *PooledMessage) Release() {
	val := p.refCount.Add(-1)
	if val < 0 {
		panic("PooledMessage: Release called more times than Retain")
	}
	if val == 0 {
		if p.returnFunc != nil {
			p.returnFunc()
		}
	}
}
