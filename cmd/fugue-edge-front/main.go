package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"fugue/internal/config"
	"fugue/internal/edgefront"
)

func main() {
	cfg := config.EdgeFrontFromEnv()
	logger := log.Default()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	service := edgefront.NewService(cfg, logger)
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("edge front exited: %v", err)
	}
}
