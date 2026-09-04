package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fugue/internal/api"
	"fugue/internal/auth"
	"fugue/internal/config"
	"fugue/internal/edgeauthkey"
	"fugue/internal/livediagnostics"
	"fugue/internal/model"
	"fugue/internal/store"
)

func main() {
	cfg := config.APIFromEnv()
	if len(os.Args) == 2 && (os.Args[1] == "sign-edge-activation" || os.Args[1] == "sign-edge-remediation") {
		if err := runEdgeActivationSigner(os.Args[1], cfg.EdgeActivationPlanSigningProjectionDir); err != nil {
			fmt.Fprintln(os.Stderr, "edge activation signing failed")
			os.Exit(1)
		}
		return
	}
	keySnapshot, err := edgeauthkey.Load(cfg.EdgeActivationPlanSigningProjectionDir)
	if err == nil {
		cfg.EdgeActivationPlanSigningKey = keySnapshot.Key
		cfg.EdgeActivationPlanSigningKeyID = keySnapshot.KeyID
		cfg.EdgeActivationPlanSigningKeyGeneration = keySnapshot.Generation
	}
	logger := log.Default()
	store := store.New(cfg.StorePath, cfg.DatabaseURL)
	if err := store.Init(); err != nil {
		logger.Fatalf("init store: %v", err)
	}

	authenticator := auth.New(store, cfg.BootstrapAdminKey)
	authenticator.WorkloadIdentitySigningKey = cfg.WorkloadIdentitySigningKey
	authenticator.PlatformComponentIdentityKeyring = platformComponentIdentityKeyringFromEnv()
	authenticator.EdgeRouteIntentIdentityKeyring = edgeRouteIntentIdentityKeyringFromEnv()

	server := api.NewServer(store, authenticator, logger, api.ServerConfig{
		DatabaseURL:                 cfg.DatabaseURL,
		ControlPlaneNamespace:       cfg.ControlPlaneNamespace,
		ControlPlaneReleaseInstance: cfg.ControlPlaneReleaseInstance,
		BackupCoordination: api.BackupCoordinationConfig{
			LeaseName:      cfg.BackupCoordination.LeaseName,
			LeaseNamespace: cfg.BackupCoordination.LeaseNamespace,
			LeaseDuration:  cfg.BackupCoordination.LeaseDuration,
			RenewPeriod:    cfg.BackupCoordination.RenewPeriod,
		},
		ControlPlaneCNPGBackupEnabled:          cfg.ControlPlaneCNPGBackupEnabled,
		ControlPlaneCNPGBackupName:             cfg.ControlPlaneCNPGBackupName,
		RegistryGCLeaseName:                    cfg.RegistryGCLeaseName,
		ControlPlaneGitHubRepository:           cfg.ControlPlaneGitHubRepository,
		ControlPlaneGitHubWorkflow:             cfg.ControlPlaneGitHubWorkflow,
		ControlPlaneGitHubAPIURL:               cfg.ControlPlaneGitHubAPIURL,
		ControlPlaneGitHubToken:                cfg.ControlPlaneGitHubToken,
		AppBaseDomain:                          cfg.AppBaseDomain,
		APIPublicDomain:                        cfg.APIPublicDomain,
		SSHPublicHost:                          cfg.SSHPublicHost,
		SSHPublicPortStart:                     cfg.SSHPublicPortStart,
		SSHPublicPortEnd:                       cfg.SSHPublicPortEnd,
		DNSStaticRecordsJSON:                   cfg.DNSStaticRecordsJSON,
		DNSNameservers:                         cfg.DNSNameservers,
		DNSRouteAAnswerIPs:                     cfg.DNSRouteAAnswerIPs,
		DNSBundleTTL:                           cfg.DNSBundleTTL,
		EdgeAuthorityServicesJSON:              cfg.EdgeAuthorityServicesJSON,
		PlatformRoutesJSON:                     cfg.PlatformRoutesJSON,
		EdgeQualityRankingMode:                 cfg.EdgeQualityRankingMode,
		AppSafeZeroDowntimePublicEnabled:       cfg.AppSafeZeroDowntimePublicEnabled,
		ImageStoreMode:                         cfg.ImageStoreMode,
		RegistryPushBase:                       cfg.RegistryPushBase,
		RegistryPullBase:                       cfg.RegistryPullBase,
		ClusterJoinRegistryEndpoint:            cfg.ClusterJoinRegistryEndpoint,
		MovableRWOStorageClass:                 cfg.MovableRWOStorageClass,
		ManagedPostgresStorageClass:            cfg.ManagedPostgresStorageClass,
		ClusterJoinServer:                      cfg.ClusterJoinServer,
		ClusterJoinServerFallbacks:             cfg.ClusterJoinServerFallbacks,
		ClusterJoinCAHash:                      cfg.ClusterJoinCAHash,
		ClusterJoinBootstrapTokenTTL:           cfg.ClusterJoinBootstrapTokenTTL,
		ClusterJoinK3SVersion:                  cfg.ClusterJoinK3SVersion,
		ClusterJoinMeshProvider:                cfg.ClusterJoinMeshProvider,
		ClusterJoinMeshLoginServer:             cfg.ClusterJoinMeshLoginServer,
		ClusterJoinMeshAuthKey:                 cfg.ClusterJoinMeshAuthKey,
		BundleSigningKey:                       cfg.BundleSigningKey,
		BundleSigningKeyID:                     cfg.BundleSigningKeyID,
		BundleSigningPreviousKey:               cfg.BundleSigningPreviousKey,
		BundleSigningPreviousKeyID:             cfg.BundleSigningPreviousKeyID,
		BundleRevokedKeyIDs:                    cfg.BundleRevokedKeyIDs,
		HeartbeatAuditKeyring:                  platformConsumerHeartbeatAuditKeyringFromEnv(),
		BundleValidFor:                         cfg.BundleValidFor,
		EdgeActivationPlanSigningKey:           cfg.EdgeActivationPlanSigningKey,
		EdgeActivationPlanSigningKeyID:         cfg.EdgeActivationPlanSigningKeyID,
		EdgeActivationPlanSigningKeyGeneration: cfg.EdgeActivationPlanSigningKeyGeneration,
		EdgeActivationPlanSigningProjectionDir: cfg.EdgeActivationPlanSigningProjectionDir,
		ImportWorkDir:                          cfg.ImportWorkDir,
		AutomationShadowLoop: api.AutomationShadowLoopConfig{
			Enabled:  cfg.AutomationShadowLoopEnabled,
			Interval: cfg.AutomationShadowLoopInterval,
		},
		Observability: cfg.Observability,
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := livediagnostics.StartRuntimeEndpoint(ctx, "api"); err != nil {
		logger.Printf("live diagnostics runtime endpoint unavailable: %v", err)
	}
	server.StartBackgroundWarmers(ctx)
	go server.StartBackgroundEdgeQualityRollups(ctx)
	go server.StartBackgroundEdgeDNSArtifacts(ctx)
	go server.StartBackgroundAppDatabaseImports(ctx)
	go server.StartBackgroundBackups(ctx)
	go server.StartBackgroundAutomationShadowLoop(ctx)

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
	edgeControlTLSConfig, err := edgeControlRouteIntentTLSConfigFromEnv(os.Getenv)
	if err != nil {
		logger.Fatalf("configure edge-control RouteIntent TLS: %v", err)
	}
	edgeControlTLSServer, edgeControlTLSListener, err := newEdgeControlRouteIntentTLSServer(edgeControlTLSConfig, server.Handler())
	if err != nil {
		logger.Fatalf("configure edge-control RouteIntent TLS listener: %v", err)
	}
	if edgeControlTLSServer != nil {
		go func() {
			logger.Printf("fugue-api edge-control RouteIntent TLS listening on %s", edgeControlTLSConfig.BindAddr)
			if err := edgeControlTLSServer.Serve(edgeControlTLSListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Fatalf("edge-control RouteIntent TLS serve: %v", err)
			}
		}()
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
		if edgeControlTLSServer != nil {
			if err := edgeControlTLSServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Printf("edge-control RouteIntent TLS shutdown error: %v", err)
			}
		}
	}()

	logger.Printf("fugue-api listening on %s", cfg.BindAddr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("listen and serve: %v", err)
	}
}

func runEdgeActivationSigner(mode, projectionDirectory string) error {
	snapshot, err := edgeauthkey.Load(projectionDirectory)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 1<<20))
	decoder.DisallowUnknownFields()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	switch mode {
	case "sign-edge-activation":
		var request model.EdgeActivationAdvance
		if err := decoder.Decode(&request); err != nil {
			return err
		}
		if err := ensureSignerEOF(decoder); err != nil {
			return err
		}
		if err := api.SignEdgeActivationAdvance(&request, snapshot.Key, snapshot.KeyID, snapshot.Generation, request.Authorization.RunnerObservedSecretUID, request.Authorization.RunnerObservedSecretVersion); err != nil {
			return err
		}
		return encoder.Encode(request)
	case "sign-edge-remediation":
		var request model.EdgeRemediationAdvance
		if err := decoder.Decode(&request); err != nil {
			return err
		}
		if err := ensureSignerEOF(decoder); err != nil {
			return err
		}
		if err := api.SignEdgeRemediationAdvance(&request, snapshot.Key, snapshot.KeyID, snapshot.Generation, request.Authorization.RunnerObservedSecretUID, request.Authorization.RunnerObservedSecretVersion); err != nil {
			return err
		}
		return encoder.Encode(request)
	default:
		return errors.New("unsupported edge activation signing command")
	}
}

func ensureSignerEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("edge activation signing input has multiple documents")
		}
		return err
	}
	return nil
}
