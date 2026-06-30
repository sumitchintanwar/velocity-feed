package adapters

import (
	"context"
	"time"

	"github.com/sumit/rtmds/internal/platform/lifecycle"
)

// MockService simulates a heavy business component with artificial startup/shutdown delays.
type MockService struct {
	ServiceName string
	StartDelay  time.Duration
	StopDelay   time.Duration
	StartErr    error
	StopErr     error
	
	WasStarted bool
	WasStopped bool
}

func (m *MockService) Name() string {
	return m.ServiceName
}

func (m *MockService) Start(ctx context.Context) error {
	select {
	case <-time.After(m.StartDelay):
		if m.StartErr == nil {
			m.WasStarted = true
		}
		return m.StartErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MockService) Stop(ctx context.Context) error {
	select {
	case <-time.After(m.StopDelay):
		m.WasStopped = true
		return m.StopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Factories for the requested services in the framework spec

func NewMockRedis() lifecycle.Component {
	return &MockService{ServiceName: "Redis", StartDelay: 10 * time.Millisecond}
}

func NewMockPostgres() lifecycle.Component {
	return &MockService{ServiceName: "PostgreSQL", StartDelay: 15 * time.Millisecond}
}

func NewMockPublisher() lifecycle.Component {
	return &MockService{ServiceName: "Publisher", StartDelay: 5 * time.Millisecond}
}

func NewMockTopicManager() lifecycle.Component {
	return &MockService{ServiceName: "TopicManager", StartDelay: 5 * time.Millisecond}
}

func NewMockFeedGenerator() lifecycle.Component {
	return &MockService{ServiceName: "FeedGenerator", StartDelay: 20 * time.Millisecond}
}

func NewMockGateway() lifecycle.Component {
	return &MockService{ServiceName: "Gateway", StartDelay: 10 * time.Millisecond}
}

func NewMockReplayAPI() lifecycle.Component {
	return &MockService{ServiceName: "ReplayAPI", StartDelay: 5 * time.Millisecond}
}

func NewMockSnapshotService() lifecycle.Component {
	return &MockService{ServiceName: "SnapshotService", StartDelay: 5 * time.Millisecond}
}

func NewMockRecoveryManager() lifecycle.Component {
	return &MockService{ServiceName: "RecoveryManager", StartDelay: 25 * time.Millisecond}
}
