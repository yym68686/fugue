package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"fugue/internal/edgegroupfront"
)

func main() {
	logger := log.Default()
	cfg := edgegroupfront.ConfigFromEnv()
	if raw := os.Getenv("FUGUE_EDGE_FRONT_REQUIRE_ACTIVATION_STATE"); raw != "" {
		required, err := strconv.ParseBool(raw)
		if err != nil {
			logger.Printf("invalid boolean in FUGUE_EDGE_FRONT_REQUIRE_ACTIVATION_STATE=%q, using fallback false", raw)
		} else {
			cfg.RequireActivationState = required
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	service := edgegroupfront.NewService(cfg, logger)
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("edge front exited: %v", err)
	}
}
