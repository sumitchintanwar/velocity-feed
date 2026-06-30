package main

import (
	"fmt"
	"time"
	"os"

	"github.com/sumit/rtmds/pkg/client"
	"context"
	"github.com/sumit/rtmds/internal/log"
)

func main() {
	logger := log.NewFromConfig(log.Config{
		Service: "testclient",
		Format:  "text",
	})

	opts := client.DefaultOptions()
	opts.InitialBackoff = 1 * time.Second
	c, err := client.New("ws://localhost:8080/ws", opts)
	if err != nil {
		log.Error(context.Background(), logger).Err(err).Msg("Failed to connect")
		os.Exit(1)
	}
	defer c.Close()

	log.Info(context.Background(), logger).Msg("Connected. Subscribing to AAPL, MSFT...")
	if err := c.Subscribe("AAPL", "MSFT"); err != nil {
		log.Error(context.Background(), logger).Err(err).Msg("Failed to subscribe")
		os.Exit(1)
	}

	go func() {
		for msg := range c.Messages() {
			fmt.Printf("Received: %s\n", msg.Type)
		}
		log.Info(context.Background(), logger).Msg("Message channel closed")
	}()

	<-c.Done()
	log.Info(context.Background(), logger).Msg("Client shut down")
}
