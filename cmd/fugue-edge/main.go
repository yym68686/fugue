package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"fugue/internal/config"
	"fugue/internal/edge"
)

func main() {
	cfg := config.EdgeFromEnv()
	logger := log.Default()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	service := edge.NewService(cfg, logger)
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("edge exited: %v", err)
	}
}
