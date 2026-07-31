package protocol

import (
	"bytes"
	"sync"
)

// bufferPool is a highly-optimized global pool for bytes.Buffer instances.
// This allows the serialization pipeline to reuse buffers, drastically reducing
// memory allocations and garbage collection overhead during high-throughput
// market data broadcasting.
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate a reasonable capacity for typical market data events
		// to avoid resizing on the first write.
		buf := new(bytes.Buffer)
		buf.Grow(1024)
		return buf
	},
}

// GetBuffer retrieves a bytes.Buffer from the pool.
// The buffer is guaranteed to be empty (Reset has been called).
func GetBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// PutBuffer returns a bytes.Buffer to the pool.
// It resets the buffer and imposes a maximum capacity limit to prevent
// memory leaks from exceptionally large payloads pinning too much memory.
func PutBuffer(buf *bytes.Buffer) {
	if buf.Cap() > 65536 { // 64KB max capacity limit
		// Let it be garbage collected if it grew too large
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}
