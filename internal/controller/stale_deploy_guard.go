package controller

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"time"

	"fugue/internal/store"
	"reflect"

	"fugue/internal/model"
)

func (s *Service) completeStaleDeployOperationIfNeeded(ctx context.Context, op model.Operation, currentApp model.App) (bool, error) {
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

	newer, found, err := s.completedDeployAfterOperationMatchingCurrentApp(ctx, op, currentApp)
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

func (s *Service) completedDeployAfterOperationMatchingCurrentApp(ctx context.Context, op model.Operation, currentApp model.App) (model.Operation, bool, error) {
	var cursor *store.CompletedDeployCursor
	for {
		ops, err := s.completedDeployCandidatePage(ctx, op, cursor)
		if err != nil {
			return model.Operation{}, false, fmt.Errorf("read candidates for stale deploy guard: %w", err)
		}
		for _, candidate := range ops {
			if candidate.ID != op.ID && deployOperationDesiredStateMatchesApp(candidate, currentApp) && !deployOperationDesiredStatesEqual(candidate, op) {
				return candidate, true, nil
			}
		}
		if len(ops) < store.CompletedDeployCandidatePageSize {
			return model.Operation{}, false, nil
		}
		last := ops[len(ops)-1]
		cursor = &store.CompletedDeployCursor{CompletedAt: *last.CompletedAt, ID: last.ID}
	}
}

// Retry one transient read failure, but never skip the guard on failure or
// continue after the owning operation/leader has been canceled.
func (s *Service) completedDeployCandidatePage(ctx context.Context, op model.Operation, cursor *store.CompletedDeployCursor) ([]model.Operation, error) {
	return retryCompletedDeployRead(ctx, func() ([]model.Operation, error) {
		return s.Store.ListCompletedDeployCandidates(ctx, op.TenantID, op.AppID, op.CreatedAt, cursor)
	})
}

func retryCompletedDeployRead(ctx context.Context, read func() ([]model.Operation, error)) ([]model.Operation, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ops, err := read()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil || attempt >= 1 {
			return ops, err
		}
		var networkError net.Error
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, driver.ErrBadConn) && !(errors.As(err, &networkError) && networkError.Timeout()) {
			return nil, err
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
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
