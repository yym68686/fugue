package controller

import (
	"context"
	"errors"

	"fugue/internal/model"
	"fugue/internal/store"
)

// Replacement Pods inherit the immutable release template; restore only the
// verified request, never patch a Deployment template to force convergence.
func (s *Service) reconcileAppMemoryResizeTarget(ctx context.Context, client *kubeClient, app model.App) (bool, error) {
	if app.Spec.Replicas <= 0 || app.Spec.Resources == nil {
		return false, nil
	}
	_, fingerprint, needed, err := s.planAppRequestResize(ctx, client, app, appRequestResizeTarget{"memory", app.Spec.Resources.MemoryMebibytes})
	if err != nil {
		var waiting *appRequestResizeWaiting
		if errors.As(err, &waiting) {
			if recordErr := s.recordAppCPUResizeWait(ctx, app, fingerprint, waiting.Error(), "memory"); recordErr != nil {
				return false, recordErr
			}
		}
		return false, err
	}
	if !needed {
		return false, nil
	}
	admitted, err := s.admitAppCPUResizeReconcile(ctx, app, fingerprint, "memory")
	if err != nil {
		return false, err
	}
	if !admitted {
		return false, &appRequestResizeWaiting{reason: "memory request restore is waiting for its retry budget or a relevant state change"}
	}
	_, err = s.Store.CreateAppRequestResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppMemoryResize,
		RequestedByType: model.ActorTypeSystem, RequestedByID: "app-memory-resize-reconcile", DesiredSpec: &app.Spec}, app.Spec)
	if errors.Is(err, store.ErrConflict) {
		return true, nil
	}
	return err == nil, err
}

func appMemoryResizeWaitingCondition(err error) (string, bool) {
	var waiting *appRequestResizeWaiting
	if errors.As(err, &waiting) {
		return waiting.Error(), true
	}
	return "", false
}
