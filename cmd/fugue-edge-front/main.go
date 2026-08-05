package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"fugue/internal/config"
	"fugue/internal/edgegroupfront"
)

func main() {
	logger := log.Default()
	legacy := config.EdgeFrontFromEnv()
	cfg := edgegroupfront.Config{
		HTTPListenAddr: legacy.HTTPListenAddr, HTTPSListenAddr: legacy.HTTPSListenAddr, HealthAddr: legacy.HealthAddr,
		EdgeID: legacy.EdgeID, EdgeGroupID: legacy.EdgeGroupID, NodeHost: legacy.NodeHost, HTTPMode: legacy.HTTPMode,
		ActiveSlotFile: legacy.ActiveSlotFile, DefaultSlot: legacy.DefaultSlot, DialTimeout: legacy.DialTimeout,
		ShutdownTimeout: legacy.ShutdownTimeout, ProxyProtocol: legacy.ProxyProtocol,
		ProcNetSNMPPath: legacy.ProcNetSNMPPath, ProcNetNetstatPath: legacy.ProcNetNetstatPath,
		Slots: make(map[string]edgegroupfront.SlotTargets, len(legacy.Slots)),
	}
	for slot, target := range legacy.Slots {
		cfg.Slots[slot] = edgegroupfront.SlotTargets{HTTPAddress: target.HTTPAddress, HTTPSAddress: target.HTTPSAddress}
	}
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
