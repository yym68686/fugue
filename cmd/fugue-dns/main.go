package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"fugue/internal/config"
	"fugue/internal/dnsserver"
)

func main() {
	cfg := config.DNSFromEnv()
	logger := log.Default()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	service := dnsserver.NewService(cfg, logger)
	service.PlatformTokenFile = os.Getenv("FUGUE_DNS_PLATFORM_TOKEN_FILE")
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("dns exited: %v", err)
	}
}
