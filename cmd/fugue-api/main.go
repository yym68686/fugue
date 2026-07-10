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

	authenticator := auth.New(store, cfg.BootstrapAdminKey)
	authenticator.WorkloadIdentitySigningKey = cfg.WorkloadIdentitySigningKey
	authenticator.PlatformComponentIdentityKeyring = platformComponentIdentityKeyringFromEnv()

	server := api.NewServer(store, authenticator, logger, api.ServerConfig{
		DatabaseURL:                      cfg.DatabaseURL,
		ControlPlaneNamespace:            cfg.ControlPlaneNamespace,
		ControlPlaneReleaseInstance:      cfg.ControlPlaneReleaseInstance,
		ControlPlaneCNPGBackupEnabled:    cfg.ControlPlaneCNPGBackupEnabled,
		ControlPlaneCNPGBackupName:       cfg.ControlPlaneCNPGBackupName,
		RegistryGCLeaseName:              cfg.RegistryGCLeaseName,
		ControlPlaneGitHubRepository:     cfg.ControlPlaneGitHubRepository,
		ControlPlaneGitHubWorkflow:       cfg.ControlPlaneGitHubWorkflow,
		ControlPlaneGitHubAPIURL:         cfg.ControlPlaneGitHubAPIURL,
		ControlPlaneGitHubToken:          cfg.ControlPlaneGitHubToken,
		AppBaseDomain:                    cfg.AppBaseDomain,
		APIPublicDomain:                  cfg.APIPublicDomain,
		SSHPublicHost:                    cfg.SSHPublicHost,
		SSHPublicPortStart:               cfg.SSHPublicPortStart,
		SSHPublicPortEnd:                 cfg.SSHPublicPortEnd,
		DNSStaticRecordsJSON:             cfg.DNSStaticRecordsJSON,
		DNSNameservers:                   cfg.DNSNameservers,
		DNSRouteAAnswerIPs:               cfg.DNSRouteAAnswerIPs,
		DNSBundleTTL:                     cfg.DNSBundleTTL,
		PlatformRoutesJSON:               cfg.PlatformRoutesJSON,
		EdgeQualityRankingMode:           cfg.EdgeQualityRankingMode,
		AppSafeZeroDowntimePublicEnabled: cfg.AppSafeZeroDowntimePublicEnabled,
		EdgeTLSAskToken:                  cfg.EdgeTLSAskToken,
		AllowLegacyEdgeToken:             cfg.AllowLegacyEdgeToken,
		ImageStoreMode:                   cfg.ImageStoreMode,
		RegistryPushBase:                 cfg.RegistryPushBase,
		RegistryPullBase:                 cfg.RegistryPullBase,
		ClusterJoinRegistryEndpoint:      cfg.ClusterJoinRegistryEndpoint,
		MovableRWOStorageClass:           cfg.MovableRWOStorageClass,
		ManagedPostgresStorageClass:      cfg.ManagedPostgresStorageClass,
		ClusterJoinServer:                cfg.ClusterJoinServer,
		ClusterJoinServerFallbacks:       cfg.ClusterJoinServerFallbacks,
		ClusterJoinCAHash:                cfg.ClusterJoinCAHash,
		ClusterJoinBootstrapTokenTTL:     cfg.ClusterJoinBootstrapTokenTTL,
		ClusterJoinK3SVersion:            cfg.ClusterJoinK3SVersion,
		ClusterJoinMeshProvider:          cfg.ClusterJoinMeshProvider,
		ClusterJoinMeshLoginServer:       cfg.ClusterJoinMeshLoginServer,
		ClusterJoinMeshAuthKey:           cfg.ClusterJoinMeshAuthKey,
		BundleSigningKey:                 cfg.BundleSigningKey,
		BundleSigningKeyID:               cfg.BundleSigningKeyID,
		BundleSigningPreviousKey:         cfg.BundleSigningPreviousKey,
		BundleSigningPreviousKeyID:       cfg.BundleSigningPreviousKeyID,
		BundleRevokedKeyIDs:              cfg.BundleRevokedKeyIDs,
		BundleValidFor:                   cfg.BundleValidFor,
		ImportWorkDir:                    cfg.ImportWorkDir,
		Observability:                    cfg.Observability,
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	server.StartBackgroundWarmers(ctx)
	go server.StartBackgroundEdgeQualityRollups(ctx)
	go server.StartBackgroundEdgeDNSArtifacts(ctx)
	go server.StartBackgroundAppDatabaseImports(ctx)
	go server.StartBackgroundBackups(ctx)

	var metricsServer *http.Server
	if cfg.MetricsBindAddr != "" {
		metricsServer = &http.Server{
			Addr:              cfg.MetricsBindAddr,
			Handler:           server.MetricsHandler(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			logger.Printf("fugue-api metrics listening on %s", cfg.MetricsBindAddr)
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Fatalf("metrics listen and serve: %v", err)
			}
		}()
	}

	httpServer := &http.Server{
		Addr:              cfg.BindAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

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
		if metricsServer != nil {
			if err := metricsServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Printf("metrics shutdown error: %v", err)
			}
		}
	}()

	logger.Printf("fugue-api listening on %s", cfg.BindAddr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("listen and serve: %v", err)
	}
}
