package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"fugue/internal/api"
	"fugue/internal/auth"
	"fugue/internal/config"
	"fugue/internal/store"
)

func main() {
	cfg := config.APIFromEnv()
	logger := log.Default()
	store := store.New(cfg.StorePath, cfg.DatabaseURL)
	if err := store.Init(); err != nil {
		logger.Fatalf("init store: %v", err)
	}

	server := api.NewServer(store, auth.New(store, cfg.BootstrapAdminKey), logger, api.ServerConfig{
		AppBaseDomain:                cfg.AppBaseDomain,
		APIPublicDomain:              cfg.APIPublicDomain,
		EdgeTLSAskToken:              cfg.EdgeTLSAskToken,
		RegistryPushBase:             cfg.RegistryPushBase,
		RegistryPullBase:             cfg.RegistryPullBase,
		ClusterJoinRegistryEndpoint:  cfg.ClusterJoinRegistryEndpoint,
		ClusterJoinServer:            cfg.ClusterJoinServer,
		ClusterJoinCAHash:            cfg.ClusterJoinCAHash,
		ClusterJoinBootstrapTokenTTL: cfg.ClusterJoinBootstrapTokenTTL,
		ClusterJoinMeshProvider:      cfg.ClusterJoinMeshProvider,
		ClusterJoinMeshLoginServer:   cfg.ClusterJoinMeshLoginServer,
		ClusterJoinMeshAuthKey:       cfg.ClusterJoinMeshAuthKey,
		ImportWorkDir:                cfg.ImportWorkDir,
	})
	httpServer := &http.Server{
		Addr:              cfg.BindAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		server.SetReady(false)
		if cfg.ShutdownDrainDelay > 0 {
			time.Sleep(cfg.ShutdownDrainDelay)
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Printf("shutdown error: %v", err)
		}
	}()

	logger.Printf("fugue-api listening on %s", cfg.BindAddr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("listen and serve: %v", err)
	}
}
