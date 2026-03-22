package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"fugue/internal/config"
	"fugue/internal/controller"
	"fugue/internal/store"
)

func main() {
	cfg := config.ControllerFromEnv()
	logger := log.Default()
	store := store.New(cfg.StorePath)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	service := controller.New(store, cfg, logger)
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("controller exited: %v", err)
	}
}
