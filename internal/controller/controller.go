package controller

import (
	"context"
	"fmt"
	"log"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

type Service struct {
	Store    *store.Store
	Config   config.ControllerConfig
	Renderer runtime.Renderer
	Logger   *log.Logger
}

func New(store *store.Store, cfg config.ControllerConfig, logger *log.Logger) *Service {
	return &Service{
		Store:    store,
		Config:   cfg,
		Renderer: runtime.Renderer{BaseDir: cfg.RenderDir},
		Logger:   logger,
	}
}

func (s *Service) Run(ctx context.Context) error {
	if s.Logger == nil {
		s.Logger = log.Default()
	}
	if err := s.Store.Init(); err != nil {
		return err
	}

	s.Logger.Printf("controller started; poll_interval=%s render_dir=%s kubectl_apply=%v", s.Config.PollInterval, s.Config.RenderDir, s.Config.KubectlApply)
	ticker := time.NewTicker(s.Config.PollInterval)
	defer ticker.Stop()

	for {
		if err := s.reconcileOnce(); err != nil {
			s.Logger.Printf("reconcile error: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) reconcileOnce() error {
	if _, err := s.Store.MarkRuntimeOfflineStale(s.Config.RuntimeOfflineAfter); err != nil {
		return fmt.Errorf("mark runtime offline: %w", err)
	}

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
			if err := s.executeManagedOperation(op); err != nil {
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

func (s *Service) executeManagedOperation(op model.Operation) error {
	app, err := s.Store.GetApp(op.AppID)
	if err != nil {
		return fmt.Errorf("load app %s: %w", op.AppID, err)
	}

	switch op.Type {
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
	case model.OperationTypeMigrate:
		if op.TargetRuntimeID == "" {
			return fmt.Errorf("migrate operation %s missing target runtime", op.ID)
		}
		app.Spec.RuntimeID = op.TargetRuntimeID
	default:
		return fmt.Errorf("unsupported operation type %s", op.Type)
	}

	bundle, err := s.Renderer.RenderAppBundle(app)
	if err != nil {
		return fmt.Errorf("render manifest for app %s: %w", app.ID, err)
	}

	if s.Config.KubectlApply {
		if err := runtime.ApplyManagedApp(app); err != nil {
			return fmt.Errorf("apply managed app %s: %w", app.ID, err)
		}
	}

	_, err = s.Store.CompleteManagedOperation(op.ID, bundle.ManifestPath, fmt.Sprintf("managed runtime applied in namespace %s", bundle.TenantNamespace))
	if err != nil {
		return fmt.Errorf("complete operation %s: %w", op.ID, err)
	}
	s.Logger.Printf("operation %s completed on managed runtime; manifest=%s", op.ID, bundle.ManifestPath)
	return nil
}
