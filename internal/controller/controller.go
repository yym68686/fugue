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
	Store            *store.Store
	Config           config.ControllerConfig
	Renderer         runtime.Renderer
	Logger           *log.Logger
	importer         *sourceimport.Importer
	registryPushBase string
	registryPullBase string
}

func New(store *store.Store, cfg config.ControllerConfig, logger *log.Logger) *Service {
	return &Service{
		Store:            store,
		Config:           cfg,
		Renderer:         runtime.Renderer{BaseDir: cfg.RenderDir},
		Logger:           logger,
		importer:         sourceimport.NewImporter(cfg.ImportWorkDir, logger, builderPodPolicyFromConfig(cfg.BuilderSchedulingJSON, logger)),
		registryPushBase: strings.TrimSpace(cfg.RegistryPushBase),
		registryPullBase: strings.TrimSpace(cfg.RegistryPullBase),
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
	if s.Config.LeaderElectionLeaseDuration <= 0 {
		s.Config.LeaderElectionLeaseDuration = 15 * time.Second
	}
	if s.Config.LeaderElectionRenewDeadline <= 0 {
		s.Config.LeaderElectionRenewDeadline = 10 * time.Second
	}
	if s.Config.LegacyControllerCheckInterval <= 0 {
		s.Config.LegacyControllerCheckInterval = 2 * time.Second
	}
	if s.Config.LeaderElectionEnabled {
		return s.runWithLeaderElection(ctx)
	}
	return s.runActiveLoop(ctx)
}

func (s *Service) runActiveLoop(ctx context.Context) error {
	eventDriven := strings.TrimSpace(s.Config.DatabaseURL) != ""
	s.Logger.Printf(
		"controller active loop started; event_driven=%v poll_interval=%s fallback_poll_interval=%s render_dir=%s kubectl_apply=%v",
		eventDriven,
		s.Config.PollInterval,
		s.Config.FallbackPollInterval,
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

	if !eventDriven {
		ticker := time.NewTicker(s.Config.PollInterval)
		defer ticker.Stop()

		for {
			if err := s.reconcileOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("reconcile error: %v", err)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	}

	if err := s.reconcileOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.Logger.Printf("reconcile error: %v", err)
	}

	staleTicker := time.NewTicker(s.Config.PollInterval)
	defer staleTicker.Stop()
	fallbackTicker := time.NewTicker(s.Config.FallbackPollInterval)
	defer fallbackTicker.Stop()
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
			if err := s.drainPendingOperations(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("drain pending operations error: %v", err)
			}
		case <-fallbackTicker.C:
			if err := s.drainPendingOperations(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("fallback drain pending operations error: %v", err)
			}
		case <-staleTicker.C:
			if err := s.markRuntimeOfflineStale(); err != nil {
				s.Logger.Printf("runtime stale sweep error: %v", err)
			}
		}
	}
}

func (s *Service) reconcileOnce(ctx context.Context) error {
	if err := s.markRuntimeOfflineStale(); err != nil {
		return err
	}
	return s.drainPendingOperations(ctx)
}

func (s *Service) markRuntimeOfflineStale() error {
	if _, err := s.Store.MarkRuntimeOfflineStale(s.Config.RuntimeOfflineAfter); err != nil {
		return fmt.Errorf("mark runtime offline: %w", err)
	}
	return nil
}

func (s *Service) drainPendingOperations(ctx context.Context) error {
	for {
		op, found, err := s.Store.ClaimNextPendingOperation()
		if err != nil {
			return fmt.Errorf("claim pending operation: %w", err)
		}
		if !found {
			return nil
		}

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
	}
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
			if err := runtime.DeleteManagedApp(app, scheduling); err != nil {
				return fmt.Errorf("delete managed app %s: %w", app.ID, err)
			}
		default:
			if err := runtime.ApplyManagedApp(app, scheduling); err != nil {
				return fmt.Errorf("apply managed app %s: %w", app.ID, err)
			}
		}
	}

	message := fmt.Sprintf("managed runtime applied in namespace %s", bundle.TenantNamespace)
	if op.Type == model.OperationTypeDelete {
		message = fmt.Sprintf("managed runtime deleted app resources in namespace %s", bundle.TenantNamespace)
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
