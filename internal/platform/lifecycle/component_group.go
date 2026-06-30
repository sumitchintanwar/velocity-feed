package lifecycle

import (
	"context"
	"fmt"
	"sync"
)

// ComponentGroup implements the Component interface but internally
// wraps a slice of Components. When the group is started or stopped,
// it executes all of its internal components concurrently.
type ComponentGroup struct {
	GroupName  string
	components []Component
}

// NewComponentGroup creates a new group for concurrent lifecycle execution.
func NewComponentGroup(name string, comps ...Component) *ComponentGroup {
	return &ComponentGroup{
		GroupName:  name,
		components: comps,
	}
}

// Name returns the group identifier.
func (g *ComponentGroup) Name() string {
	return g.GroupName
}

// Start executes Start() on all internal components concurrently.
// If any component fails, it returns the error immediately and does NOT automatically
// rollback the group, because the Lifecycle Manager handles the fail-fast rollback
// globally across all successfully started components.
func (g *ComponentGroup) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(g.components))

	for _, comp := range g.components {
		wg.Add(1)
		go func(c Component) {
			defer wg.Done()
			if err := c.Start(ctx); err != nil {
				errCh <- fmt.Errorf("group %s: failed to start %s: %w", g.GroupName, c.Name(), err)
			}
		}(comp)
	}

	wg.Wait()
	close(errCh)

	// Return the first error if any occurred
	for err := range errCh {
		return err
	}
	return nil
}

// Stop executes Stop() on all internal components concurrently.
func (g *ComponentGroup) Stop(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(g.components))

	for _, comp := range g.components {
		wg.Add(1)
		go func(c Component) {
			defer wg.Done()
			if err := c.Stop(ctx); err != nil {
				errCh <- fmt.Errorf("group %s: failed to stop %s: %w", g.GroupName, c.Name(), err)
			}
		}(comp)
	}

	wg.Wait()
	close(errCh)

	// Aggregate all errors
	var finalErr error
	for err := range errCh {
		if finalErr == nil {
			finalErr = err
		} else {
			finalErr = fmt.Errorf("%v; %v", finalErr, err)
		}
	}
	
	return finalErr
}
