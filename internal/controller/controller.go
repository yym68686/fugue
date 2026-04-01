package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type Service struct {
	Store              *store.Store
	Config             config.ControllerConfig
	Renderer           runtime.Renderer
	Logger             *log.Logger
	importer           sourceImporter
	registryPushBase   string
	registryPullBase   string
	latestGitHubCommit func(ctx context.Context, repoURL, repoAuthToken, branch string) (string, string, error)
	newKubeClient      func(namespace string) (*kubeClient, error)
	now                func() time.Time
}

type sourceImporter interface {
	ImportDockerImageSource(context.Context, sourceimport.DockerImageSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error)
	ImportGitHubSource(context.Context, sourceimport.GitHubSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error)
	ImportUploadedArchiveSource(context.Context, sourceimport.UploadSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error)
	SuggestGitHubComposeServiceEnv(context.Context, sourceimport.GitHubComposeServiceEnvRequest) (map[string]string, error)
	SuggestUploadedComposeServiceEnv(context.Context, sourceimport.UploadComposeServiceEnvRequest) (map[string]string, error)
}

type operationLane int

const (
	operationLaneForeground operationLane = iota
	operationLaneGitHubSyncImport
)

func New(store *store.Store, cfg config.ControllerConfig, logger *log.Logger) *Service {
	return &Service{
		Store:              store,
		Config:             cfg,
		Renderer:           runtime.Renderer{BaseDir: cfg.RenderDir},
		Logger:             logger,
		importer:           sourceimport.NewImporter(cfg.ImportWorkDir, logger, builderPodPolicyFromConfig(cfg.BuilderSchedulingJSON, logger)),
		registryPushBase:   strings.TrimSpace(cfg.RegistryPushBase),
		registryPullBase:   strings.TrimSpace(cfg.RegistryPullBase),
		latestGitHubCommit: sourceimport.LatestGitHubCommit,
		newKubeClient:      newKubeClient,
		now:                time.Now,
	}
}

func builderPodPolicyFromConfig(raw string, logger *log.Logger) sourceimport.BuilderPodPolicy {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return sourceimport.BuilderPodPolicy{}
	}
	var policy sourceimport.BuilderPodPolicy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		if logger == nil {
			logger = log.Default()
		}
		logger.Printf("invalid controller builder scheduling config: %v", err)
		return sourceimport.BuilderPodPolicy{}
	}
	return policy
}

func (s *Service) Run(ctx context.Context) error {
	if s.Logger == nil {
		s.Logger = log.Default()
	}
	if err := s.Store.Init(); err != nil {
		return err
	}
	if s.Config.FallbackPollInterval <= 0 {
		s.Config.FallbackPollInterval = 30 * time.Second
	}
	if s.Config.LeaderElectionRetryPeriod <= 0 {
		s.Config.LeaderElectionRetryPeriod = 2 * time.Second
	}
	if s.Config.GitHubSyncTimeout <= 0 {
		s.Config.GitHubSyncTimeout = 20 * time.Second
	}
	if s.Config.GitHubSyncRetryBaseDelay <= 0 {
		s.Config.GitHubSyncRetryBaseDelay = 5 * time.Minute
	}
	if s.Config.GitHubSyncRetryMaxDelay <= 0 {
		s.Config.GitHubSyncRetryMaxDelay = time.Hour
	}
	if s.Config.GitHubSyncRetryMaxDelay < s.Config.GitHubSyncRetryBaseDelay {
		s.Config.GitHubSyncRetryMaxDelay = s.Config.GitHubSyncRetryBaseDelay
	}
	if s.Config.ManagedAppRolloutTimeout <= 0 {
		s.Config.ManagedAppRolloutTimeout = 10 * time.Minute
	}
	if s.Config.LeaderElectionLeaseDuration <= 0 {
		s.Config.LeaderElectionLeaseDuration = 15 * time.Second
	}
	if s.Config.LeaderElectionRenewDeadline <= 0 {
		s.Config.LeaderElectionRenewDeadline = 10 * time.Second
	}
	if s.Config.LegacyControllerCheckInterval <= 0 {
		s.Config.LegacyControllerCheckInterval = 2 * time.Second
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.Config.LeaderElectionEnabled {
		return s.runWithLeaderElection(ctx)
	}
	return s.runActiveLoop(ctx)
}

func (s *Service) runActiveLoop(ctx context.Context) error {
	eventDriven := strings.TrimSpace(s.Config.DatabaseURL) != ""
	s.Logger.Printf(
		"controller active loop started; event_driven=%v poll_interval=%s fallback_poll_interval=%s github_sync_interval=%s render_dir=%s kubectl_apply=%v",
		eventDriven,
		s.Config.PollInterval,
		s.Config.FallbackPollInterval,
		s.Config.GitHubSyncInterval,
		s.Config.RenderDir,
		s.Config.KubectlApply,
	)
	requeued, err := s.Store.RequeueInFlightManagedOperations("operation requeued after controller restart")
	if err != nil {
		return fmt.Errorf("requeue in-flight managed operations: %w", err)
	}
	if requeued > 0 {
		s.Logger.Printf("requeued %d in-flight managed operations after controller start", requeued)
	}
	if err := s.cleanupZombieBuildJobs(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.Logger.Printf("zombie build job cleanup error: %v", err)
	}

	foregroundOps := s.startPendingOperationWorker(ctx, operationLaneForeground)
	backgroundImports := s.startPendingOperationWorker(ctx, operationLaneGitHubSyncImport)
	triggerPendingOperationWorkers(foregroundOps, backgroundImports)

	if !eventDriven {
		ticker := time.NewTicker(s.Config.PollInterval)
		defer ticker.Stop()
		var githubTicker *time.Ticker
		if s.Config.GitHubSyncInterval > 0 {
			githubTicker = time.NewTicker(s.Config.GitHubSyncInterval)
			defer githubTicker.Stop()
		}

		for {
			if err := s.reconcileOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("reconcile error: %v", err)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-githubTickerChan(githubTicker):
				if err := s.syncGitHubApps(ctx); err != nil && !errors.Is(err, context.Canceled) {
					s.Logger.Printf("github sync error: %v", err)
				} else {
					triggerPendingOperationWorkers(backgroundImports)
				}
			case <-ticker.C:
			}
		}
	}

	if err := s.reconcileOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.Logger.Printf("reconcile error: %v", err)
	}
	if s.Config.GitHubSyncInterval > 0 {
		if err := s.syncGitHubApps(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.Logger.Printf("initial github sync error: %v", err)
		} else {
			triggerPendingOperationWorkers(backgroundImports)
		}
	}

	staleTicker := time.NewTicker(s.Config.PollInterval)
	defer staleTicker.Stop()
	fallbackTicker := time.NewTicker(s.Config.FallbackPollInterval)
	defer fallbackTicker.Stop()
	managedAppTicker := time.NewTicker(s.Config.PollInterval)
	defer managedAppTicker.Stop()
	var githubTicker *time.Ticker
	if s.Config.GitHubSyncInterval > 0 {
		githubTicker = time.NewTicker(s.Config.GitHubSyncInterval)
		defer githubTicker.Stop()
	}
	operationEvents := listenForOperationEvents(ctx, s.Logger, s.Config.DatabaseURL)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-operationEvents:
			if !ok {
				operationEvents = nil
				continue
			}
			triggerPendingOperationWorkers(foregroundOps, backgroundImports)
			if s.Config.KubectlApply {
				if err := s.reconcileManagedApps(ctx); err != nil && !errors.Is(err, context.Canceled) {
					s.Logger.Printf("managed app reconcile error: %v", err)
				}
			}
		case <-fallbackTicker.C:
			triggerPendingOperationWorkers(foregroundOps, backgroundImports)
			if s.Config.KubectlApply {
				if err := s.reconcileManagedApps(ctx); err != nil && !errors.Is(err, context.Canceled) {
					s.Logger.Printf("fallback managed app reconcile error: %v", err)
				}
			}
		case <-managedAppTicker.C:
			if s.Config.KubectlApply {
				if err := s.reconcileManagedApps(ctx); err != nil && !errors.Is(err, context.Canceled) {
					s.Logger.Printf("periodic managed app reconcile error: %v", err)
				}
			}
		case <-githubTickerChan(githubTicker):
			if err := s.syncGitHubApps(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("github sync error: %v", err)
			} else {
				triggerPendingOperationWorkers(backgroundImports)
			}
		case <-staleTicker.C:
			if err := s.markRuntimeOfflineStale(); err != nil {
				s.Logger.Printf("runtime stale sweep error: %v", err)
			}
			if err := s.cleanupZombieBuildJobs(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("zombie build job cleanup error: %v", err)
			}
		}
	}
}

func (s *Service) reconcileOnce(ctx context.Context) error {
	if err := s.markRuntimeOfflineStale(); err != nil {
		return err
	}
	if err := s.cleanupZombieBuildJobs(ctx); err != nil {
		return err
	}
	if s.Config.KubectlApply {
		return s.reconcileManagedApps(ctx)
	}
	return nil
}

func (s *Service) markRuntimeOfflineStale() error {
	if _, err := s.Store.MarkRuntimeOfflineStale(s.Config.RuntimeOfflineAfter); err != nil {
		return fmt.Errorf("mark runtime offline: %w", err)
	}
	return nil
}

func triggerPendingOperationWorkers(workers ...chan struct{}) {
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		select {
		case worker <- struct{}{}:
		default:
		}
	}
}

func (lane operationLane) String() string {
	switch lane {
	case operationLaneForeground:
		return "foreground"
	case operationLaneGitHubSyncImport:
		return "github-sync-import"
	default:
		return "unknown"
	}
}

func (s *Service) startPendingOperationWorker(ctx context.Context, lane operationLane) chan struct{} {
	trigger := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(s.Config.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-trigger:
			case <-ticker.C:
			}
			if err := s.drainPendingOperationsInLane(ctx, lane); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("drain pending operations (%s) error: %v", lane, err)
			}
		}
	}()
	return trigger
}

func (s *Service) claimNextPendingOperationInLane(lane operationLane) (model.Operation, bool, error) {
	switch lane {
	case operationLaneForeground:
		return s.Store.ClaimNextPendingForegroundOperation()
	case operationLaneGitHubSyncImport:
		return s.Store.ClaimNextPendingGitHubSyncImportOperation()
	default:
		return model.Operation{}, false, fmt.Errorf("unsupported operation lane %d", lane)
	}
}

func (s *Service) drainPendingOperationsInLane(ctx context.Context, lane operationLane) error {
	for {
		op, found, err := s.claimNextPendingOperationInLane(lane)
		if err != nil {
			return fmt.Errorf("claim pending operation: %w", err)
		}
		if !found {
			return nil
		}
		if err := s.handleClaimedOperation(ctx, op); err != nil {
			return err
		}
	}
}

func (s *Service) handleClaimedOperation(ctx context.Context, op model.Operation) error {
	switch op.ExecutionMode {
	case model.ExecutionModeManaged:
		if err := s.executeManagedOperation(ctx, op); err != nil {
			if errors.Is(err, context.Canceled) {
				if _, requeueErr := s.Store.RequeueManagedOperation(op.ID, "operation requeued after controller interruption"); requeueErr != nil && !errors.Is(requeueErr, store.ErrConflict) {
					s.Logger.Printf("operation %s requeue after interruption failed: %v", op.ID, requeueErr)
				}
				return err
			}
			s.Logger.Printf("operation %s failed: %v", op.ID, err)
			if _, failErr := s.Store.FailOperation(op.ID, err.Error()); failErr != nil {
				s.Logger.Printf("operation %s fail update error: %v", op.ID, failErr)
			}
		}
	case model.ExecutionModeAgent:
		s.Logger.Printf("operation %s dispatched to runtime %s", op.ID, op.AssignedRuntimeID)
	default:
		s.Logger.Printf("operation %s has unknown execution mode %s", op.ID, op.ExecutionMode)
	}
	return nil
}

func (s *Service) executeManagedOperation(ctx context.Context, op model.Operation) error {
	app, err := s.Store.GetApp(op.AppID)
	if err != nil {
		return fmt.Errorf("load app %s: %w", op.AppID, err)
	}

	switch op.Type {
	case model.OperationTypeImport:
		return s.executeManagedImportOperation(ctx, op, app)
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return fmt.Errorf("deploy operation %s missing desired spec", op.ID)
		}
		app.Spec = *op.DesiredSpec
	case model.OperationTypeScale:
		if op.DesiredReplicas == nil {
			return fmt.Errorf("scale operation %s missing desired replicas", op.ID)
		}
		app.Spec.Replicas = *op.DesiredReplicas
	case model.OperationTypeDelete:
	case model.OperationTypeMigrate:
		if op.TargetRuntimeID == "" {
			return fmt.Errorf("migrate operation %s missing target runtime", op.ID)
		}
		app.Spec.RuntimeID = op.TargetRuntimeID
	default:
		return fmt.Errorf("unsupported operation type %s", op.Type)
	}

	legacyPostgresCleanupNeeded := false
	if op.Type == model.OperationTypeDelete || op.Type == model.OperationTypeMigrate {
		legacyPostgresCleanupNeeded = len(store.LegacyPostgresStoragePaths(app)) > 0
	}

	scheduling, err := s.managedSchedulingConstraints(app.Spec.RuntimeID)
	if err != nil {
		return err
	}

	bundle, err := s.Renderer.RenderAppBundle(app, scheduling)
	if err != nil {
		return fmt.Errorf("render manifest for app %s: %w", app.ID, err)
	}

	if s.Config.KubectlApply {
		switch op.Type {
		case model.OperationTypeDelete:
			if err := s.deleteManagedAppDesiredState(ctx, app); err != nil {
				return fmt.Errorf("delete managed app desired state %s: %w", app.ID, err)
			}
			if legacyPostgresCleanupNeeded {
				client, err := s.kubeClient()
				if err != nil {
					return fmt.Errorf("initialize kubernetes cleanup client: %w", err)
				}
				if err := s.triggerLegacyPostgresCleanup(ctx, client, app, op.ID); err != nil {
					return fmt.Errorf("trigger legacy postgres cleanup for deleted app %s: %w", app.ID, err)
				}
			}
		default:
			bundle, err = s.Renderer.RenderManagedAppBundle(app, scheduling)
			if err != nil {
				return fmt.Errorf("render managed app manifest for app %s: %w", app.ID, err)
			}
			if err := s.applyManagedAppDesiredState(ctx, app, scheduling); err != nil {
				return fmt.Errorf("apply managed app desired state %s: %w", app.ID, err)
			}
			if err := s.waitForManagedAppRollout(ctx, app); err != nil {
				return fmt.Errorf("wait for managed app rollout %s: %w", app.ID, err)
			}
			if op.Type == model.OperationTypeMigrate && legacyPostgresCleanupNeeded {
				client, err := s.kubeClient()
				if err != nil {
					return fmt.Errorf("initialize kubernetes cleanup client: %w", err)
				}
				if err := s.triggerLegacyPostgresCleanup(ctx, client, app, op.ID); err != nil {
					return fmt.Errorf("trigger legacy postgres cleanup for migrated app %s: %w", app.ID, err)
				}
				if err := s.clearLegacyPostgresMetadataAndSyncManagedApp(ctx, client, app.ID, scheduling); err != nil {
					return fmt.Errorf("clear legacy postgres metadata for migrated app %s: %w", app.ID, err)
				}
			}
		}
	}

	message := fmt.Sprintf("managed app reconciled in namespace %s", bundle.TenantNamespace)
	if op.Type == model.OperationTypeDelete {
		message = fmt.Sprintf("managed app deleted from namespace %s", bundle.TenantNamespace)
	}
	_, err = s.Store.CompleteManagedOperation(op.ID, bundle.ManifestPath, message)
	if err != nil {
		return fmt.Errorf("complete operation %s: %w", op.ID, err)
	}
	s.Logger.Printf("operation %s completed on managed runtime; manifest=%s", op.ID, bundle.ManifestPath)
	return nil
}

func (s *Service) managedSchedulingConstraints(runtimeID string) (runtime.SchedulingConstraints, error) {
	if strings.TrimSpace(runtimeID) == "" {
		return runtime.SchedulingConstraints{}, nil
	}
	runtimeObj, err := s.Store.GetRuntime(runtimeID)
	if err != nil {
		return runtime.SchedulingConstraints{}, fmt.Errorf("load runtime %s: %w", runtimeID, err)
	}
	return runtime.SchedulingForRuntime(runtimeObj), nil
}

func (s *Service) kubeClient() (*kubeClient, error) {
	factory := s.newKubeClient
	if factory == nil {
		factory = newKubeClient
	}
	return factory(s.Config.KubectlNamespace)
}

func githubTickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}
