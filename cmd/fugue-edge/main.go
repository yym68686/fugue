package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fugue/internal/config"
	"fugue/internal/edge"
	"fugue/internal/livediagnostics/runtimeprofile"
	"fugue/internal/prewarmconfig"
	"fugue/internal/runtimeobservation"
)

func main() {
	cfg := config.EdgeFromEnv()
	logger := log.Default()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(os.Args) > 1 {
		if len(os.Args) != 2 || os.Args[1] != "check-private-prewarm" {
			logger.Fatal("unknown Edge command")
		}
		ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		s := edge.NewServiceWithEdgeSources(cfg, edge.RouteBundleSourceFromEnv(), edge.InventoryProducerConfig{}, logger)
		hosts, err := prewarmconfig.Parse(os.Getenv("FUGUE_EDGE_PRIVATE_PREWARM_HOSTS"))
		if err != nil {
			logger.Fatal(err)
		}
		result, err := s.CheckPrivatePrewarm(ctx, hosts)
		if err != nil {
			logger.Fatal(err)
		}
		if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
			logger.Fatal(err)
		}
		return
	}
	if err := runtimeprofile.StartRuntimeEndpoint(ctx, "edge"); err != nil {
		logger.Printf("live diagnostics runtime endpoint unavailable: %v", err)
	}

	service := edge.NewServiceWithEdgeSources(cfg, edge.RouteBundleSourceFromEnv(), edge.InventoryProducerConfigFromEnv(), logger)
	service.PlatformTokenFile = os.Getenv("FUGUE_EDGE_PLATFORM_TOKEN_FILE")
	if err := runtimeobservation.Start(ctx, "edge", map[string]runtimeobservation.Provider{"traffic-serving": service.ServingObservationSnapshot}); err != nil {
		logger.Printf("local serving observation endpoint unavailable: %v", err)
	}
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("edge exited: %v", err)
	}
}
