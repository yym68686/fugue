package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
)

type ManagedBackingServiceRuntimeStatus struct {
	ServiceID               string
	CurrentRuntimeStartedAt *time.Time
	CurrentRuntimeReadyAt   *time.Time
}

func (s *Store) SyncManagedAppRuntimeStatus(appID string, currentReleaseStartedAt, currentReleaseReadyAt *time.Time, services []ManagedBackingServiceRuntimeStatus) error {
	if strings.TrimSpace(appID) == "" {
		return ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSyncManagedAppRuntimeStatus(appID, currentReleaseStartedAt, currentReleaseReadyAt, services)
	}
	return s.withLockedState(true, func(state *model.State) error {
		appIndex := findApp(state, appID)
		if appIndex < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		syncAppReleaseRuntimeStatus(&state.Apps[appIndex], currentReleaseStartedAt, currentReleaseReadyAt, now)
		for _, serviceStatus := range services {
			if strings.TrimSpace(serviceStatus.ServiceID) == "" {
				continue
			}
			serviceIndex := findBackingService(state, serviceStatus.ServiceID)
			if serviceIndex < 0 {
				continue
			}
			syncBackingServiceRuntimeStatus(&state.BackingServices[serviceIndex], serviceStatus.CurrentRuntimeStartedAt, serviceStatus.CurrentRuntimeReadyAt, now)
		}
		return nil
	})
}

// SyncManagedAppObservedStatus publishes the Kubernetes-observed phase and
// replica count together with the serving timestamps.  Runtime timestamps
// alone are insufficient: a ManagedApp can remain serving while its CR status
// carries a recoverable controller warning.  Leaving the durable App status
// at "unknown" makes compose dependents treat a ready workload as unavailable.
func (s *Store) SyncManagedAppObservedStatus(appID, phase string, replicas int, message string, currentReleaseStartedAt, currentReleaseReadyAt *time.Time, services []ManagedBackingServiceRuntimeStatus) error {
	if err := s.SyncManagedAppRuntimeStatus(appID, currentReleaseStartedAt, currentReleaseReadyAt, services); err != nil {
		return err
	}
	if strings.TrimSpace(appID) == "" {
		return ErrInvalidInput
	}
	if replicas < 0 {
		replicas = 0
	}
	normalizedPhase := strings.TrimSpace(strings.ToLower(phase))
	switch normalizedPhase {
	case "ready":
		normalizedPhase = "deployed"
	case "error":
		// A zero-downtime block preserves a proven serving replica while
		// recording the controller warning. Keep dependents routable.
		if replicas > 0 && currentReleaseReadyAt != nil {
			normalizedPhase = "deployed"
		} else {
			normalizedPhase = "failed"
		}
	case "pending":
		normalizedPhase = "deploying"
	case "progressing":
		normalizedPhase = "deploying"
	case "disabled", "deleting", "deployed", "deploying", "failed":
	default:
		normalizedPhase = "unknown"
	}
	return s.updateManagedAppObservedStatus(appID, normalizedPhase, replicas, message)
}

func (s *Store) updateManagedAppObservedStatus(appID, phase string, replicas int, message string) error {
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		app, err := s.pgGetApp(appID)
		if err != nil {
			return err
		}
		changed := app.Status.Phase != phase || app.Status.CurrentReplicas != replicas || app.Status.LastMessage != strings.TrimSpace(message)
		if !changed {
			return nil
		}
		app.Status.Phase = phase
		app.Status.CurrentReplicas = replicas
		app.Status.LastMessage = strings.TrimSpace(message)
		app.Status.UpdatedAt = time.Now().UTC()
		statusJSON, err := marshalJSON(app.Status)
		if err != nil {
			return err
		}
		_, err = s.db.ExecContext(ctx, `UPDATE fugue_apps SET status_json = $2, updated_at = $3 WHERE id = $1`, app.ID, statusJSON, app.UpdatedAt)
		return err
	}
	return s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, appID)
		if index < 0 {
			return ErrNotFound
		}
		app := &state.Apps[index]
		if app.Status.Phase == phase && app.Status.CurrentReplicas == replicas && app.Status.LastMessage == strings.TrimSpace(message) {
			return nil
		}
		app.Status.Phase = phase
		app.Status.CurrentReplicas = replicas
		app.Status.LastMessage = strings.TrimSpace(message)
		app.Status.UpdatedAt = time.Now().UTC()
		return nil
	})
}

func (s *Store) pgSyncManagedAppRuntimeStatus(appID string, currentReleaseStartedAt, currentReleaseReadyAt *time.Time, services []ManagedBackingServiceRuntimeStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, appID, true)
	if err != nil {
		return mapDBErr(err)
	}
	now := time.Now().UTC()
	if syncAppReleaseRuntimeStatus(&app, currentReleaseStartedAt, currentReleaseReadyAt, now) {
		if err := s.pgUpdateAppRuntimeStatusTx(ctx, tx, app); err != nil {
			return err
		}
	}

	for _, serviceStatus := range services {
		if strings.TrimSpace(serviceStatus.ServiceID) == "" {
			continue
		}
		service, err := s.pgGetBackingServiceTx(ctx, tx, serviceStatus.ServiceID, true)
		if err != nil {
			if errors.Is(mapDBErr(err), ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return mapDBErr(err)
		}
		if syncBackingServiceRuntimeStatus(&service, serviceStatus.CurrentRuntimeStartedAt, serviceStatus.CurrentRuntimeReadyAt, now) {
			if err := s.pgUpdateBackingServiceRuntimeStatusTx(ctx, tx, service); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *Store) pgUpdateAppRuntimeStatusTx(ctx context.Context, tx *sql.Tx, app model.App) error {
	statusJSON, err := marshalJSON(app.Status)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_apps
SET status_json = $2,
	updated_at = $3
WHERE id = $1
`, app.ID, statusJSON, app.UpdatedAt); err != nil {
		return err
	}
	return nil
}

func (s *Store) pgUpdateBackingServiceRuntimeStatusTx(ctx context.Context, tx *sql.Tx, service model.BackingService) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_backing_services
SET current_runtime_started_at = $2,
	current_runtime_ready_at = $3,
	updated_at = $4
WHERE id = $1
`, service.ID, service.CurrentRuntimeStartedAt, service.CurrentRuntimeReadyAt, service.UpdatedAt); err != nil {
		return err
	}
	return nil
}

func syncAppReleaseRuntimeStatus(app *model.App, currentReleaseStartedAt, currentReleaseReadyAt *time.Time, now time.Time) bool {
	if app == nil {
		return false
	}
	changed := false
	if !runtimeTimestampEqual(app.Status.CurrentReleaseStartedAt, currentReleaseStartedAt) {
		app.Status.CurrentReleaseStartedAt = cloneRuntimeTimestamp(currentReleaseStartedAt)
		changed = true
	}
	if !runtimeTimestampEqual(app.Status.CurrentReleaseReadyAt, currentReleaseReadyAt) {
		app.Status.CurrentReleaseReadyAt = cloneRuntimeTimestamp(currentReleaseReadyAt)
		changed = true
	}
	if !changed {
		return false
	}
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
	return true
}

func syncBackingServiceRuntimeStatus(service *model.BackingService, currentRuntimeStartedAt, currentRuntimeReadyAt *time.Time, now time.Time) bool {
	if service == nil {
		return false
	}
	changed := false
	if !runtimeTimestampEqual(service.CurrentRuntimeStartedAt, currentRuntimeStartedAt) {
		service.CurrentRuntimeStartedAt = cloneRuntimeTimestamp(currentRuntimeStartedAt)
		changed = true
	}
	if !runtimeTimestampEqual(service.CurrentRuntimeReadyAt, currentRuntimeReadyAt) {
		service.CurrentRuntimeReadyAt = cloneRuntimeTimestamp(currentRuntimeReadyAt)
		changed = true
	}
	if !changed {
		return false
	}
	service.UpdatedAt = now
	return true
}

func cloneRuntimeTimestamp(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func runtimeTimestampEqual(left, right *time.Time) bool {
	switch {
	case left == nil && right == nil:
		return true
	case left == nil || right == nil:
		return false
	default:
		return left.UTC().Equal(right.UTC())
	}
}
