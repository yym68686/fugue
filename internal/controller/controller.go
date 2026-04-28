package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type Service struct {
	Store                         *store.Store
	Config                        config.ControllerConfig
	Renderer                      runtime.Renderer
	Logger                        *log.Logger
	importer                      sourceImporter
	registryPushBase              string
	registryPullBase              string
	inspectManagedImage           appimages.InspectFunc
	inspectManagedImageConfig     imageConfigInspector
	deleteManagedImage            func(context.Context, string) (appimages.DeleteResult, error)
	resolveManagedImageDigestRef  func(context.Context, string) (string, error)
	syncBillingImageStorage       bool
	latestGitHubCommit            func(ctx context.Context, repoURL, repoAuthToken, branch string) (string, string, error)
	newKubeClient                 func(namespace string) (*kubeClient, error)
	kubeClientMu                  sync.Mutex
	kubeClients                   map[string]*kubeClient
	importImageInspectRetryDelay  time.Duration
	importImageInspectMaxAttempts int
	now                           func() time.Time
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
	operationLaneForegroundImport operationLane = iota
	operationLaneForegroundActivate
	operationLaneGitHubSyncImport
	operationLaneGitHubSyncActivate
)

func New(store *store.Store, cfg config.ControllerConfig, logger *log.Logger) *Service {
	return &Service{
		Store:  store,
		Config: cfg,
		Renderer: runtime.Renderer{
			BaseDir: cfg.RenderDir,
			WorkloadIdentity: runtime.WorkloadIdentityConfig{
				APIBaseURL: runtimeAPIBaseURL(cfg.APIPublicDomain),
				SigningKey: strings.TrimSpace(cfg.WorkloadIdentitySigningKey),
			},
		},
		Logger:                       logger,
		importer:                     sourceimport.NewImporter(cfg.ImportWorkDir, logger, sourceimport.BuilderPodPolicy{}),
		registryPushBase:             strings.TrimSpace(cfg.RegistryPushBase),
		registryPullBase:             strings.TrimSpace(cfg.RegistryPullBase),
		resolveManagedImageDigestRef: sourceimport.ResolveRemoteImageDigestRef,
		latestGitHubCommit:           sourceimport.LatestGitHubCommit,
		newKubeClient:                newKubeClient,
		now:                          time.Now,
	}
}

func runtimeAPIBaseURL(publicDomain string) string {
	return runtime.NormalizeWorkloadIdentityAPIBaseURL(publicDomain)
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
	if s.inspectManagedImage == nil {
		s.inspectManagedImage = appimages.NewRemoteInspector().InspectImage
	}
	if s.inspectManagedImageConfig == nil {
		s.inspectManagedImageConfig = sourceimport.InspectRemoteImageConfig
	}
	if s.deleteManagedImage == nil {
		s.deleteManagedImage = appimages.DeleteRemoteImage
	}
	s.syncBillingImageStorage = true
	if s.Config.LeaderElectionEnabled {
		return s.runWithLeaderElection(ctx)
	}
	return s.runActiveLoop(ctx)
}

func (s *Service) runActiveLoop(ctx context.Context) error {
	eventDriven := strings.TrimSpace(s.Config.DatabaseURL) != ""
	if s.Config.ForegroundImportWorkers < 0 {
		s.Config.ForegroundImportWorkers = 0
	}
	if s.Config.GitHubSyncImportWorkers < 0 {
		s.Config.GitHubSyncImportWorkers = 0
	}
	s.Logger.Printf(
		"controller active loop started; event_driven=%v poll_interval=%s fallback_poll_interval=%s github_sync_interval=%s render_dir=%s kubectl_apply=%v foreground_import_workers=%d github_sync_import_workers=%d import_worker_limit_note=%q",
		eventDriven,
		s.Config.PollInterval,
		s.Config.FallbackPollInterval,
		s.Config.GitHubSyncInterval,
		s.Config.RenderDir,
		s.Config.KubectlApply,
		s.Config.ForegroundImportWorkers,
		s.Config.GitHubSyncImportWorkers,
		"0=unbounded",
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

	foregroundImports := s.startPendingOperationWorkers(ctx, operationLaneForegroundImport, s.Config.ForegroundImportWorkers)
	foregroundActivations := s.startPendingOperationWorkers(ctx, operationLaneForegroundActivate, 1)
	backgroundImports := s.startPendingOperationWorkers(ctx, operationLaneGitHubSyncImport, s.Config.GitHubSyncImportWorkers)
	backgroundActivations := s.startPendingOperationWorkers(ctx, operationLaneGitHubSyncActivate, 1)
	triggerBackgroundOps := func() {
		triggerPendingOperationWorkers(backgroundImports...)
		triggerPendingOperationWorkers(backgroundActivations...)
	}
	triggerPendingOperationWorkers(foregroundImports...)
	triggerPendingOperationWorkers(foregroundActivations...)
	triggerBackgroundOps()

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
					triggerBackgroundOps()
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
			triggerBackgroundOps()
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
			triggerPendingOperationWorkers(foregroundImports...)
			triggerPendingOperationWorkers(foregroundActivations...)
			triggerBackgroundOps()
			if s.Config.KubectlApply {
				if err := s.reconcileManagedApps(ctx); err != nil && !errors.Is(err, context.Canceled) {
					s.Logger.Printf("managed app reconcile error: %v", err)
				}
			}
		case <-fallbackTicker.C:
			triggerPendingOperationWorkers(foregroundImports...)
			triggerPendingOperationWorkers(foregroundActivations...)
			triggerBackgroundOps()
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
				triggerBackgroundOps()
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
	if err := s.queueAutomaticFailovers(); err != nil {
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
	if err := s.syncManagedOwnedClusterRuntimeStatuses(); err != nil && s.Logger != nil {
		s.Logger.Printf("managed-owned cluster runtime status sync error: %v", err)
	}
	return nil
}

func (s *Service) syncManagedOwnedClusterRuntimeStatuses() error {
	if !s.Config.KubectlApply {
		return nil
	}

	newClient := s.newKubeClient
	if newClient == nil {
		newClient = newKubeClient
	}

	client, err := newClient("")
	if err != nil {
		return fmt.Errorf("new kube client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodeReadyByName, err := client.listNodeReadyStates(ctx)
	if err != nil {
		return fmt.Errorf("list node readiness: %w", err)
	}
	if _, err := s.Store.SyncManagedOwnedClusterRuntimeStatuses(nodeReadyByName); err != nil {
		return fmt.Errorf("sync managed-owned cluster runtime statuses: %w", err)
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
	case operationLaneForegroundImport:
		return "foreground-import"
	case operationLaneForegroundActivate:
		return "foreground-activate"
	case operationLaneGitHubSyncImport:
		return "github-sync-import"
	case operationLaneGitHubSyncActivate:
		return "github-sync-activate"
	default:
		return "unknown"
	}
}

func (s *Service) startPendingOperationWorkers(ctx context.Context, lane operationLane, count int) []chan struct{} {
	if count < 0 {
		count = 0
	}
	if count == 0 {
		trigger := make(chan struct{}, 1)
		go func(trigger chan struct{}) {
			ticker := time.NewTicker(s.Config.PollInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-trigger:
				case <-ticker.C:
				}
				if err := s.dispatchPendingOperationsInLane(ctx, lane); err != nil && !errors.Is(err, context.Canceled) {
					s.Logger.Printf("dispatch pending operations (%s) error: %v", lane, err)
				}
			}
		}(trigger)
		return []chan struct{}{trigger}
	}
	triggers := make([]chan struct{}, 0, count)
	for range count {
		trigger := make(chan struct{}, 1)
		triggers = append(triggers, trigger)
		go func(trigger chan struct{}) {
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
		}(trigger)
	}
	return triggers
}

func (s *Service) dispatchPendingOperationsInLane(ctx context.Context, lane operationLane) error {
	for {
		op, found, err := s.claimNextPendingOperationInLane(lane)
		if err != nil {
			return fmt.Errorf("claim pending operation: %w", err)
		}
		if !found {
			return nil
		}

		go func(op model.Operation) {
			if err := s.handleClaimedOperation(ctx, op); err != nil && !errors.Is(err, context.Canceled) && s.Logger != nil {
				s.Logger.Printf("operation %s async lane %s error: %v", op.ID, lane, err)
			}
		}(op)
	}
}

func (s *Service) claimNextPendingOperationInLane(lane operationLane) (model.Operation, bool, error) {
	for {
		activeOps, err := s.Store.ListActiveOperations()
		if err != nil {
			return model.Operation{}, false, fmt.Errorf("list active operations: %w", err)
		}
		apps, err := s.loadAppsForPendingOperationClaim(activeOps, lane)
		if err != nil {
			return model.Operation{}, false, fmt.Errorf("list apps: %w", err)
		}
		appsByID, composeAppsByProject := indexAppsForPendingOperations(apps)
		activeOpsByApp := indexActiveOperationsByApp(activeOps)

		restartScan := false
		for _, op := range activeOps {
			if op.Status != model.OperationStatusPending {
				continue
			}
			if !pendingOperationMatchesLane(op, lane) {
				continue
			}
			app, ok := appsByID[op.AppID]
			ready, terminalReason := pendingOperationReadyForClaim(op, app, ok, activeOpsByApp, composeAppsByProject)
			if terminalReason != "" {
				if s.Logger != nil {
					s.Logger.Printf("operation %s failed before claim: %s", op.ID, terminalReason)
				}
				if _, failErr := s.Store.FailOperation(op.ID, terminalReason); failErr != nil {
					if errors.Is(failErr, store.ErrConflict) || errors.Is(failErr, store.ErrNotFound) {
						restartScan = true
						break
					}
					return model.Operation{}, false, fmt.Errorf("fail blocked pending operation %s: %w", op.ID, failErr)
				}
				restartScan = true
				break
			}
			if !ready {
				continue
			}
			claimed, claimedOK, claimErr := s.Store.TryClaimPendingOperation(op.ID)
			if claimErr != nil {
				return model.Operation{}, false, fmt.Errorf("try claim operation %s: %w", op.ID, claimErr)
			}
			if !claimedOK {
				restartScan = true
				break
			}
			return claimed, true, nil
		}
		if restartScan {
			continue
		}
		return model.Operation{}, false, nil
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
			if errors.Is(err, errOperationNoLongerActive) {
				s.Logger.Printf("operation %s stopped before completion: %v", op.ID, err)
				return nil
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

func (s *Service) executeManagedOperation(ctx context.Context, op model.Operation) (err error) {
	timer := newControllerOperationTimer(s.now)
	defer func() {
		timer.Log(s.Logger, "managed_operation", op, err)
	}()

	app, err := s.Store.GetApp(op.AppID)
	if err != nil {
		return fmt.Errorf("load app %s: %w", op.AppID, err)
	}
	timer.Mark("load_app")
	var completionDesiredSpec *model.AppSpec

	switch op.Type {
	case model.OperationTypeImport:
		return s.executeManagedImportOperation(ctx, op, app)
	case model.OperationTypeFailover:
		return s.executeManagedFailoverOperation(ctx, op, app)
	case model.OperationTypeDatabaseSwitchover:
		return s.executeManagedDatabaseSwitchoverOperation(ctx, op, app)
	case model.OperationTypeDatabaseLocalize:
		return s.executeManagedDatabaseLocalizeOperation(ctx, op, app)
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return fmt.Errorf("deploy operation %s missing desired spec", op.ID)
		}
		app.Spec = *op.DesiredSpec
		buildSource := model.AppBuildSource(app)
		if op.DesiredSource != nil {
			buildSource = model.CloneAppSource(op.DesiredSource)
		}
		originSource := model.AppOriginSource(app)
		if op.DesiredOriginSource != nil {
			originSource = model.CloneAppSource(op.DesiredOriginSource)
		}
		model.SetAppSourceState(&app, originSource, buildSource)
		if alignedSpec, changed, err := s.alignManagedPostgresRuntimeToObservedPrimary(ctx, app); err != nil {
			if s.Logger != nil {
				s.Logger.Printf("skip managed postgres runtime alignment for app %s: %v", app.ID, err)
			}
		} else if changed {
			app.Spec = alignedSpec
			completionDesiredSpec = cloneControllerAppSpec(&alignedSpec)
		}
	case model.OperationTypeScale:
		if op.DesiredReplicas == nil {
			return fmt.Errorf("scale operation %s missing desired replicas", op.ID)
		}
		app.Spec.Replicas = *op.DesiredReplicas
	case model.OperationTypeDelete:
	case model.OperationTypeMigrate:
		if op.DesiredSpec != nil {
			app.Spec = *op.DesiredSpec
		} else {
			if op.TargetRuntimeID == "" {
				return fmt.Errorf("migrate operation %s missing target runtime", op.ID)
			}
			app.Spec.RuntimeID = op.TargetRuntimeID
		}
		buildSource := model.AppBuildSource(app)
		if op.DesiredSource != nil {
			buildSource = model.CloneAppSource(op.DesiredSource)
		}
		originSource := model.AppOriginSource(app)
		if op.DesiredOriginSource != nil {
			originSource = model.CloneAppSource(op.DesiredOriginSource)
		}
		model.SetAppSourceState(&app, originSource, buildSource)
	default:
		return fmt.Errorf("unsupported operation type %s", op.Type)
	}
	timer.Mark("prepare_operation")

	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	timer.Mark("operation_active_check")

	app, err = store.OverlayDesiredManagedPostgres(app)
	if err != nil {
		return fmt.Errorf("overlay desired managed postgres state for app %s: %w", app.ID, err)
	}
	timer.Mark("overlay_postgres")
	if op.Type == model.OperationTypeDeploy {
		if err := s.ensureManagedDeployImageReady(ctx, app); err != nil {
			return err
		}
		timer.Mark("image_ready")
	}
	postgresPlacements, err := s.managedPostgresPlacements(ctx, app)
	if err != nil {
		return fmt.Errorf("resolve managed postgres placements for app %s: %w", app.ID, err)
	}
	timer.Mark("postgres_placement")

	scheduling, err := s.managedSchedulingConstraintsForApp(ctx, app)
	if err != nil {
		return err
	}
	app = s.appWithResolvedLaunchOverride(ctx, app)
	timer.Mark("scheduling")

	bundle, err := s.Renderer.RenderAppBundleWithPlacements(app, scheduling, postgresPlacements)
	if err != nil {
		return fmt.Errorf("render manifest for app %s: %w", app.ID, err)
	}
	timer.Mark("render_bundle")

	if s.Config.KubectlApply {
		switch op.Type {
		case model.OperationTypeDelete:
			if err := s.deleteManagedAppDesiredState(ctx, app); err != nil {
				return fmt.Errorf("delete managed app desired state %s: %w", app.ID, err)
			}
			timer.Mark("delete_desired_state")
		default:
			bundle, err = s.Renderer.RenderManagedAppBundle(app, scheduling)
			if err != nil {
				return fmt.Errorf("render managed app manifest for app %s: %w", app.ID, err)
			}
			if err := s.applyManagedAppDesiredState(ctx, app, scheduling); err != nil {
				return fmt.Errorf("apply managed app desired state %s: %w", app.ID, err)
			}
			timer.Mark("apply_desired_state")
			if err := s.waitForManagedAppRollout(ctx, app, op.ID); err != nil {
				return fmt.Errorf("wait for managed app rollout %s: %w", app.ID, err)
			}
			timer.Mark("rollout_wait")
		}
	}

	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	timer.Mark("final_active_check")

	message := fmt.Sprintf("managed app reconciled in namespace %s", bundle.TenantNamespace)
	if op.Type == model.OperationTypeDelete {
		message = fmt.Sprintf("managed app deleted from namespace %s", bundle.TenantNamespace)
	}
	if completionDesiredSpec != nil {
		_, err = s.Store.CompleteManagedOperationWithResult(op.ID, bundle.ManifestPath, message, completionDesiredSpec, op.DesiredSource)
	} else {
		_, err = s.Store.CompleteManagedOperation(op.ID, bundle.ManifestPath, message)
	}
	if err != nil {
		return fmt.Errorf("complete operation %s: %w", op.ID, err)
	}
	timer.Mark("complete_operation")
	if op.Type == model.OperationTypeDeploy {
		deployedApp, appErr := s.Store.GetApp(app.ID)
		if appErr != nil {
			if s.Logger != nil {
				s.Logger.Printf("reload deployed app %s after completion failed: %v", app.ID, appErr)
			}
		} else {
			if err := s.pruneExcessManagedAppImages(ctx, deployedApp); err != nil && s.Logger != nil {
				s.Logger.Printf("prune excess managed app images for app=%s failed: %v", deployedApp.ID, err)
			}
			if err := s.syncTenantBillingImageStorage(ctx, deployedApp.TenantID); err != nil && s.Logger != nil {
				s.Logger.Printf("sync billing image storage after deploy for tenant=%s failed: %v", deployedApp.TenantID, err)
			}
		}
		timer.Mark("post_deploy_cleanup")
	}
	if op.Type == model.OperationTypeDelete {
		if err := s.cleanupDeletedAppImages(ctx, app); err != nil && s.Logger != nil {
			s.Logger.Printf("cleanup deleted app images for app=%s failed: %v", app.ID, err)
		}
		if err := s.syncTenantBillingImageStorage(ctx, app.TenantID); err != nil && s.Logger != nil {
			s.Logger.Printf("sync billing image storage after delete for tenant=%s failed: %v", app.TenantID, err)
		}
		timer.Mark("post_delete_cleanup")
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
	namespace := strings.TrimSpace(s.Config.KubectlNamespace)
	s.kubeClientMu.Lock()
	if s.kubeClients != nil {
		if cached := s.kubeClients[namespace]; cached != nil {
			s.kubeClientMu.Unlock()
			return cached, nil
		}
	}
	s.kubeClientMu.Unlock()

	client, err := factory(namespace)
	if err != nil {
		return nil, err
	}

	s.kubeClientMu.Lock()
	defer s.kubeClientMu.Unlock()
	if s.kubeClients == nil {
		s.kubeClients = make(map[string]*kubeClient)
	}
	if cached := s.kubeClients[namespace]; cached != nil {
		return cached, nil
	}
	s.kubeClients[namespace] = client
	return client, nil
}

func githubTickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}
