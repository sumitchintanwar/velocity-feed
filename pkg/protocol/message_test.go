package protocol

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPooledMessage_Lifecycle(t *testing.T) {
	var recycled atomic.Bool

	buf1 := GetBuffer()
	buf1.WriteString("hello json")

	payloads := map[Format]*bytes.Buffer{
		FormatJSON: buf1,
	}

	msg := NewPooledMessage(payloads, func() {
		recycled.Store(true)
		PutBuffer(buf1)
	})

	// Test Payload Retrieval
	b, err := msg.Payload(FormatJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != "hello json" {
		t.Errorf("expected 'hello json', got %q", string(b))
	}

	_, err = msg.Payload(FormatProtobuf)
	if err == nil {
		t.Error("expected error for missing payload format")
	}

	// Test Retain and Release
	msg.Retain() // ref = 2

	msg.Release() // ref = 1
	if recycled.Load() {
		t.Error("buffers were recycled prematurely")
	}

	msg.Release() // ref = 0
	if !recycled.Load() {
		t.Error("buffers were not recycled when ref count hit 0")
	}
}

func TestPooledMessage_ReleaseUnderflow(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on release underflow, but got none")
		}
	}()

	msg := NewPooledMessage(nil, nil)
	// Ref count is initially 1
	msg.Release() // ref = 0
	msg.Release() // panics
}

func TestPooledMessage_Concurrency(t *testing.T) {
	var returnCount atomic.Int32
	msg := NewPooledMessage(nil, func() {
		returnCount.Add(1)
	})

	// Set initial ref count to exactly match the number of workers
	workers := 100
	for i := 0; i < workers; i++ {
		msg.Retain()
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg.Release()
		}()
	}
	wg.Wait()

	// Final release to drop from 1 to 0
	msg.Release()

	if returnCount.Load() != 1 {
		t.Errorf("expected returnFunc to be called exactly 1 time, got %d", returnCount.Load())
	}
}

func BenchmarkRetainRelease(b *testing.B) {
	msg := NewPooledMessage(nil, nil)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg.Retain()
			msg.Release()
		}
	})
}
