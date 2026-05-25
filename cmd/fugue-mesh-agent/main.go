package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"fugue/internal/meshrecovery"
)

func main() {
	logger := log.Default()
	cfg := meshrecovery.MeshAgentFromEnv()
	agent, err := meshrecovery.NewMeshAgent(cfg, logger)
	if err != nil {
		logger.Fatalf("mesh agent config invalid: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("mesh agent exited: %v", err)
	}
}
