package main

import (
	"context"
	"encoding/json"
	"errors"
	"fugue/internal/dataprewarm"
	"log"
	"os"
	"os/signal"
	"syscall"

	"fugue/internal/config"
	"fugue/internal/controller"
	"fugue/internal/livediagnostics"
	"fugue/internal/store"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--data-prewarm-worker" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		file, err := os.Open("/plan/plan.json")
		if err != nil {
			log.Fatal("prewarm plan unavailable")
		}
		defer file.Close()
		var plan dataprewarm.Plan
		if err = json.NewDecoder(file).Decode(&plan); err != nil {
			log.Fatal("invalid prewarm plan")
		}
		encoder := json.NewEncoder(os.Stdout)
		if err = dataprewarm.Run(ctx, plan, "/cache", func(p dataprewarm.Progress) { _ = encoder.Encode(p) }); err != nil {
			log.Fatal(err)
		}
		return
	}

	cfg := config.ControllerFromEnv()
	logger := log.Default()
	store := store.New(cfg.StorePath, cfg.DatabaseURL)
	if err := store.Init(); err != nil {
		logger.Fatalf("init store: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := livediagnostics.StartRuntimeEndpoint(ctx, "controller"); err != nil {
		logger.Printf("live diagnostics runtime endpoint unavailable: %v", err)
	}

	service := controller.New(store, cfg, logger)
	if err := service.StartMetricsServer(ctx, cfg.MetricsBindAddr); err != nil {
		logger.Fatalf("start controller metrics server: %v", err)
	}
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("controller exited: %v", err)
	}
}
