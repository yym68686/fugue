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
	cfg := meshrecovery.RecoveryFromEnv()
	authority, err := meshrecovery.NewRecoveryAuthority(cfg, logger)
	if err != nil {
		logger.Fatalf("mesh recovery config invalid: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := authority.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("mesh recovery exited: %v", err)
	}
}
