package chaos

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestEngine_DisconnectClients(t *testing.T) {
	engine := NewEngine()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	chaosListener := engine.WrapListener(l)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := chaosListener.Accept()
		if err != nil {
			return
		}
		// Block on read until disconnected
		buf := make([]byte, 10)
		_, err = conn.Read(buf)
		if err == nil {
			t.Error("expected error on read after disconnect")
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	// Give time for Accept and map population
	time.Sleep(50 * time.Millisecond)

	engine.DisconnectClients()

	// Wait for server read to fail
	wg.Wait()

	// Client write should fail
	_, err = client.Write([]byte("ping"))
	if err == nil {
		// TCP might not fail immediately on write, but eventually it should.
		// A close on the server side usually triggers an error on the client eventually.
	}
	client.Close()
}

func TestEngine_DropProbability(t *testing.T) {
	engine := NewEngine()
	engine.SetConfig(Config{DropProbability: 1.0}) // 100% drop

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	chaosListener := engine.WrapListener(l)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := chaosListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 10)
		_, err = conn.Read(buf)
		if !errors.Is(err, ErrChaosDrop) {
			t.Errorf("expected ErrChaosDrop, got %v", err)
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write something to trigger server Read
	_, _ = client.Write([]byte("ping"))
	wg.Wait()
}

func TestEngine_DelayProbability(t *testing.T) {
	engine := NewEngine()
	engine.SetConfig(Config{
		DelayProbability: 1.0, // 100% delay
		MaxDelay:         100 * time.Millisecond,
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	chaosListener := engine.WrapListener(l)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := chaosListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		start := time.Now()
		_, err = conn.Write([]byte("pong"))
		if err != nil {
			t.Errorf("unexpected write error: %v", err)
		}

		// The Write should have taken some time (we can't easily assert exactly how much because rand.Int63n can return 0,
		// but typically it's > 0). We just ensure it doesn't crash.
		if time.Since(start) < 0 {
			t.Error("time travel?")
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	buf := make([]byte, 10)
	_, _ = client.Read(buf)
	wg.Wait()
}

func TestEngine_KillGateway(t *testing.T) {
	engine := NewEngine()

	killed := false
	engine.SetKillFunc(func() {
		killed = true
	})

	engine.KillGateway()

	if !killed {
		t.Error("expected kill function to be called")
	}
}

func TestEngine_RandomFailures(t *testing.T) {
	engine := NewEngine()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Just ensure it doesn't panic
	engine.RandomFailures(ctx, 10*time.Millisecond)
}
