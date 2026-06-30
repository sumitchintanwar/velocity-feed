package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SetupSignalHandler creates a context that cancels when the application
// receives a termination signal (SIGINT or SIGTERM) from the OS or Docker/Kubernetes.
func SetupSignalHandler() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		// Stop intercepting so a second signal forces a hard exit
		signal.Stop(sigCh) 
		cancel()
	}()

	return ctx, cancel
}
