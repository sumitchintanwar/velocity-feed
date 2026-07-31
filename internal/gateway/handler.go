package gateway

import (
	"context"
	"fmt"

	"github.com/sumit/rtmds/internal/wal"
)

type ConnectRequest struct {
	ResumeFrom uint64
}

// ClientConnection acts as a mockable interface for a WebSocket or TCP socket.
type ClientConnection interface {
	SendResponse(resp ConnectResponse) error
	SendStream(msg *wal.Message) error
	Close() error
}

// Server handles incoming reconnect requests and wires them to sessions.
type Server struct {
	replayer Replayer
	// A real server would contain a fan-out mechanism (like a sync.Map of active sessions)
	// and a broadcaster goroutine. We omit that here to focus purely on the handler logic.
}

func NewServer(replayer Replayer) *Server {
	return &Server{
		replayer: replayer,
	}
}

// HandleReconnect is the main protocol endpoint.
// It receives a reconnect request, validates it via the replayer, and streams data.
func (s *Server) HandleReconnect(ctx context.Context, req ConnectRequest, conn ClientConnection) error {
	// 1. Create a session for this specific client
	session := NewSession(s.replayer, req.ResumeFrom)

	// Note: In a complete application, we would register `session.LiveCh` with a global broadcaster here.
	// s.broadcaster.Register(session.LiveCh)
	// defer s.broadcaster.Unregister(session.LiveCh)

	// 2. Start the session state machine in the background
	// This immediately begins buffering live messages (if any arrive) so nothing is dropped.
	go session.Run(ctx)

	// 3. Wait for the server to validate the sequence (via the Replayer inside Session)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resp := <-session.RespCh:
		// Send the validation result back to the client
		if err := conn.SendResponse(resp); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}

		// If validation failed (sequence missing, etc), we terminate the handler
		if !resp.Success {
			return fmt.Errorf("reconnect rejected: %s", resp.Error)
		}
	}

	// 4. Success! Loop forever streaming messages to the client socket.
	// This channel emits both historical replay messages AND live messages seamlessly.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-session.SendCh:
			if err := conn.SendStream(msg); err != nil {
				return fmt.Errorf("failed to send stream message: %w", err)
			}
		}
	}
}
