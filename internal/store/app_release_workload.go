package store

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
)

func marshalReleaseWorkload(workload *model.AppReleaseWorkload) ([]byte, error) {
	if workload == nil {
		return nil, nil
	}
	return json.Marshal(workload)
}

func preserveReleaseWorkload(current model.AppRelease, desired *model.AppRelease) error {
	if desired.RevisionWorkload != nil && !reflect.DeepEqual(current.RevisionWorkload, desired.RevisionWorkload) {
		return ErrConflict
	}
	if current.RevisionWorkload != nil && (desired.AppID != current.AppID || desired.TenantID != current.TenantID ||
		desired.SourceRef != current.SourceRef || desired.ResolvedImageRef != current.ResolvedImageRef ||
		desired.RuntimeID != current.RuntimeID || !reflect.DeepEqual(desired.SpecSnapshot, current.SpecSnapshot)) {
		return ErrConflict
	}
	desired.RevisionWorkload = current.RevisionWorkload
	return nil
}

// BindAppReleaseWorkload compares the observed release under the same lock as
// the write. General status/target writers cannot bind or erase this record.
func (s *Store) BindAppReleaseWorkload(ctx context.Context, expected model.AppRelease, workload model.AppReleaseWorkload) (model.AppRelease, error) {
	return s.bindAppReleaseWorkload(ctx, expected, workload, nil)
}

// MigrateAppReleaseWorkload binds a withdrawn historical revision only when
// its persisted rollout audit identifies the same terminal source operation.
// The controller must first verify the live executable resources read-only.
func (s *Store) MigrateAppReleaseWorkload(ctx context.Context, expected model.AppRelease, source model.Operation, workload model.AppReleaseWorkload) (model.AppRelease, error) {
	return s.bindAppReleaseWorkload(ctx, expected, workload, &source)
}

func (s *Store) bindAppReleaseWorkload(ctx context.Context, expected model.AppRelease, workload model.AppReleaseWorkload, source *model.Operation) (model.AppRelease, error) {
	if expected.ID == "" || workload.DeploymentGeneration < 1 || !workload.BoundAt.IsZero() {
		return model.AppRelease{}, ErrInvalidInput
	}
	for _, value := range []string{workload.OperationID, workload.Namespace, workload.DeploymentName, workload.DeploymentUID,
		workload.ServiceName, workload.ServiceUID, workload.ReleaseKey, workload.RuntimeID, workload.ImageRef} {
		if strings.TrimSpace(value) != value || value == "" {
			return model.AppRelease{}, ErrInvalidInput
		}
	}
	if err := ctx.Err(); err != nil {
		return model.AppRelease{}, err
	}
	if s.usingDatabase() {
		return s.pgBindAppReleaseWorkload(ctx, expected, workload, source)
	}
	var out model.AppRelease
	err := s.withLockedState(true, func(state *model.State) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		index := findAppReleaseByID(state.AppReleases, expected.ID)
		if index < 0 {
			return ErrNotFound
		}
		var op model.Operation
		for _, item := range state.Operations {
			if item.ID == workload.OperationID {
				op = item
				break
			}
		}
		bound, err := bindReleaseWorkload(state.AppReleases[index], expected, op, workload)
		if source != nil {
			bound, err = migrateReleaseWorkload(state.AppReleases[index], expected, op, *source, workload, state.AuditEvents)
		}
		if err != nil {
			return err
		}
		state.AppReleases[index], out = bound, bound
		return nil
	})
	return out, err
}

func bindReleaseWorkload(current, expected model.AppRelease, op model.Operation, workload model.AppReleaseWorkload) (model.AppRelease, error) {
	if current.AppID != expected.AppID || current.TenantID != expected.TenantID || op.AppID != current.AppID ||
		op.TenantID != current.TenantID || op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return model.AppRelease{}, ErrConflict
	}
	if current.RevisionWorkload != nil {
		prior := *current.RevisionWorkload
		prior.BoundAt = time.Time{}
		if prior != workload {
			return model.AppRelease{}, ErrConflict
		}
		return current, nil
	}
	if !current.UpdatedAt.Equal(expected.UpdatedAt) || current.Role != model.AppReleaseRoleCandidate ||
		current.Status != model.AppReleaseStatusCreating || op.Status != model.OperationStatusRunning ||
		current.DeploymentName != workload.DeploymentName || current.ServiceName != workload.ServiceName ||
		current.RuntimeID != workload.RuntimeID || current.ResolvedImageRef != workload.ImageRef {
		return model.AppRelease{}, ErrConflict
	}
	workload.BoundAt = time.Now().UTC().Truncate(time.Microsecond)
	current.RevisionWorkload = &workload
	current.UpdatedAt = workload.BoundAt
	return current, nil
}

func (s *Store) pgBindAppReleaseWorkload(ctx context.Context, expected model.AppRelease, workload model.AppReleaseWorkload, source *model.Operation) (model.AppRelease, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppRelease{}, err
	}
	defer tx.Rollback()
	// Operation completion already locks operation before release.
	op, err := s.pgGetOperationTx(ctx, tx, workload.OperationID, true)
	if err != nil {
		return model.AppRelease{}, mapDBErr(err)
	}
	current, err := s.pgGetAppReleaseForUpdateTx(ctx, tx, expected.ID)
	if err != nil {
		return model.AppRelease{}, mapDBErr(err)
	}
	bound, err := bindReleaseWorkload(current, expected, op, workload)
	if source != nil {
		events, readErr := pgReleaseWorkloadSourceEvents(ctx, tx, expected)
		if readErr != nil {
			return model.AppRelease{}, readErr
		}
		bound, err = migrateReleaseWorkload(current, expected, op, *source, workload, events)
	}
	if err != nil {
		return model.AppRelease{}, err
	}
	if current.RevisionWorkload == nil {
		data, err := json.Marshal(bound.RevisionWorkload)
		if err != nil {
			return model.AppRelease{}, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE fugue_app_releases SET revision_workload_json=$2,updated_at=$3 WHERE id=$1`, bound.ID, data, bound.UpdatedAt); err != nil {
			return model.AppRelease{}, mapDBErr(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return model.AppRelease{}, mapDBErr(err)
	}
	return bound, nil
}
