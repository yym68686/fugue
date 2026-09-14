package main

import (
	"context"
	"errors"
	"log"
	"os"
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

	service := edge.NewServiceWithEdgeSources(cfg, edge.RouteBundleSourceFromEnv(), edge.InventoryProducerConfigFromEnv(), logger)
	service.PlatformTokenFile = os.Getenv("FUGUE_EDGE_PLATFORM_TOKEN_FILE")
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("edge exited: %v", err)
	}
}
