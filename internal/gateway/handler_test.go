package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/wal"
)

type mockConnection struct {
	mu        sync.Mutex
	responses []ConnectResponse
	messages  []*wal.Message
}

func (m *mockConnection) SendResponse(resp ConnectResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockConnection) SendStream(msg *wal.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	return nil
}

func (m *mockConnection) Close() error {
	return nil
}

func (m *mockConnection) getResponses() []ConnectResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]ConnectResponse, len(m.responses))
	copy(res, m.responses)
	return res
}

func (m *mockConnection) getMessages() []*wal.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]*wal.Message, len(m.messages))
	copy(res, m.messages)
	return res
}

func TestServerHandleReconnect_Success(t *testing.T) {
	replayer := &mockReplayer{
		msgs: []*wal.Message{
			{Sequence: 10, Payload: []byte("TEST")},
		},
		delay: 10 * time.Millisecond,
	}

	server := NewServer(replayer)
	conn := &mockConnection{}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Wait for HandleReconnect to exit (which it shouldn't until context is canceled)
	go func() {
		server.HandleReconnect(ctx, ConnectRequest{ResumeFrom: 10}, conn)
	}()

	time.Sleep(50 * time.Millisecond)

	responses := conn.getResponses()
	if len(responses) != 1 {
		t.Fatalf("Expected 1 response, got %d", len(responses))
	}
	if !responses[0].Success {
		t.Fatalf("Expected success response, got error: %s", responses[0].Error)
	}

	messages := conn.getMessages()
	if len(messages) != 1 {
		t.Fatalf("Expected 1 message streamed, got %d", len(messages))
	}
	if messages[0].Sequence != 10 {
		t.Errorf("Expected sequence 10, got %d", messages[0].Sequence)
	}
}

func TestServerHandleReconnect_Failure(t *testing.T) {
	expectedErr := errors.New("missing sequence")
	replayer := &mockReplayer{
		err:   expectedErr,
		delay: 10 * time.Millisecond,
	}

	server := NewServer(replayer)
	conn := &mockConnection{}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := server.HandleReconnect(ctx, ConnectRequest{ResumeFrom: 999}, conn)

	if err == nil {
		t.Fatalf("Expected error from HandleReconnect, got nil")
	}

	responses := conn.getResponses()
	if len(responses) != 1 {
		t.Fatalf("Expected 1 response sent to client, got %d", len(responses))
	}
	if responses[0].Success {
		t.Fatalf("Expected failure response, got success")
	}
	if responses[0].Error != expectedErr.Error() {
		t.Errorf("Expected error string %q, got %q", expectedErr.Error(), responses[0].Error)
	}
}
