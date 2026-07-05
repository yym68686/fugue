package controller

import (
	"fmt"
	"reflect"

	"fugue/internal/model"
)

func (s *Service) completeStaleDeployOperationIfNeeded(op model.Operation, currentApp model.App) (bool, error) {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false, nil
	}
	if deployOperationDesiredStateMatchesApp(op, currentApp) {
		message := "deploy skipped because desired state is already current"
		currentSpec := cloneControllerAppSpec(&currentApp.Spec)
		currentBuildSource := model.AppBuildSource(currentApp)
		currentOriginSource := model.AppOriginSource(currentApp)
		completed, err := s.Store.CompleteManagedOperationWithSourceState(op.ID, "", message, currentSpec, currentBuildSource, currentOriginSource)
		if err != nil {
			return false, fmt.Errorf("complete no-op deploy operation %s: %w", op.ID, err)
		}
		if s.Logger != nil {
			s.Logger.Printf("operation %s skipped no-op deploy state", op.ID)
		}
		s.logOperationAppEvent("completed", "info", completed, currentApp, message, map[string]any{
			"noop_deploy_skipped": true,
		})
		return true, nil
	}

	newer, found, err := s.completedDeployAfterOperationMatchingCurrentApp(op, currentApp)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	message := fmt.Sprintf("deploy skipped because app desired state was already updated by newer operation %s", newer.ID)
	currentSpec := cloneControllerAppSpec(&currentApp.Spec)
	currentBuildSource := model.AppBuildSource(currentApp)
	currentOriginSource := model.AppOriginSource(currentApp)
	completed, err := s.Store.CompleteManagedOperationWithSourceState(op.ID, "", message, currentSpec, currentBuildSource, currentOriginSource)
	if err != nil {
		return false, fmt.Errorf("complete stale deploy operation %s: %w", op.ID, err)
	}
	if s.Logger != nil {
		s.Logger.Printf("operation %s skipped stale deploy state; newer_operation=%s", op.ID, newer.ID)
	}
	s.logOperationAppEvent("completed", "info", completed, currentApp, message, map[string]any{
		"stale_deploy_skipped": true,
		"newer_operation_id":   newer.ID,
	})
	return true, nil
}

func (s *Service) completedDeployAfterOperationMatchingCurrentApp(op model.Operation, currentApp model.App) (model.Operation, bool, error) {
	ops, err := s.Store.ListOperationsByApp(op.TenantID, true, op.AppID)
	if err != nil {
		return model.Operation{}, false, fmt.Errorf("list operations for stale deploy guard: %w", err)
	}

	var newest model.Operation
	for _, candidate := range ops {
		if candidate.ID == op.ID ||
			candidate.Type != model.OperationTypeDeploy ||
			candidate.Status != model.OperationStatusCompleted ||
			candidate.CompletedAt == nil ||
			!candidate.CompletedAt.After(op.CreatedAt) {
			continue
		}
		if !deployOperationDesiredStateMatchesApp(candidate, currentApp) {
			continue
		}
		if deployOperationDesiredStatesEqual(candidate, op) {
			continue
		}
		if newest.ID == "" || candidate.CompletedAt.After(*newest.CompletedAt) {
			newest = candidate
		}
	}
	if newest.ID == "" {
		return model.Operation{}, false, nil
	}
	return newest, true, nil
}

func deployOperationDesiredStateMatchesApp(op model.Operation, app model.App) bool {
	if op.DesiredSpec == nil || !reflect.DeepEqual(*op.DesiredSpec, app.Spec) {
		return false
	}
	if op.DesiredSource != nil && !appSourcesEqual(op.DesiredSource, model.AppBuildSource(app)) {
		return false
	}
	if op.DesiredOriginSource != nil && !appSourcesEqual(op.DesiredOriginSource, model.AppOriginSource(app)) {
		return false
	}
	return true
}

func deployOperationDesiredStatesEqual(left, right model.Operation) bool {
	if !reflect.DeepEqual(left.DesiredSpec, right.DesiredSpec) {
		return false
	}
	if !appSourcesEqual(left.DesiredSource, right.DesiredSource) {
		return false
	}
	return appSourcesEqual(left.DesiredOriginSource, right.DesiredOriginSource)
}

func appSourcesEqual(left, right *model.AppSource) bool {
	return reflect.DeepEqual(model.CloneAppSource(left), model.CloneAppSource(right))
}
