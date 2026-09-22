package store

import (
	"context"
	"database/sql"
	"reflect"
	"time"

	"fugue/internal/model"
)

// FindAppReleaseWorkloadSource resolves persisted provenance, never a nearest
// operation by timestamp or an application-level last successful operation.
func (s *Store) FindAppReleaseWorkloadSource(ctx context.Context, release model.AppRelease) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		events, err := pgReleaseWorkloadSourceEvents(ctx, s.db, release)
		if err != nil {
			return "", err
		}
		return releaseWorkloadSource(release, events)
	}
	var id string
	err := s.withLockedState(false, func(state *model.State) error {
		var err error
		id, err = releaseWorkloadSource(release, state.AuditEvents)
		return err
	})
	return id, err
}

func releaseWorkloadSource(release model.AppRelease, events []model.AuditEvent) (string, error) {
	if !AppReleaseAwaitingDrain(release) || release.ID == "" || release.AppID == "" || release.TenantID == "" {
		return "", ErrConflict
	}
	action := "app.release.promote"
	if release.Status == model.AppReleaseStatusFailed {
		action = "app.release.abort.auto"
	} else if release.PromotedAt == nil || release.PromotedAt.Before(release.CreatedAt) {
		return "", ErrConflict
	}
	var source string
	for _, event := range events {
		if event.TenantID != release.TenantID || event.ActorType != model.ActorTypeSystem || event.ActorID != "safe-rollout-controller" ||
			event.TargetType != "app_release" || event.TargetID != release.ID || event.Action != action || event.Metadata["app_id"] != release.AppID ||
			event.Metadata["app_release_id"] != release.ID || event.CreatedAt.Before(release.CreatedAt) {
			continue
		}
		if action == "app.release.promote" && (event.Metadata["mode"] != "safe_zero_downtime" || event.CreatedAt.Before(*release.PromotedAt)) {
			continue
		}
		id := event.Metadata["operation_id"]
		if id == "" || source != "" && source != id {
			return "", ErrConflict
		}
		source = id
	}
	if source == "" {
		return "", ErrNotFound
	}
	return source, nil
}

func pgReleaseWorkloadSourceEvents(ctx context.Context, db interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, release model.AppRelease) ([]model.AuditEvent, error) {
	rows, err := db.QueryContext(ctx, `SELECT id,tenant_id,actor_type,actor_id,action,target_type,target_id,
 metadata_json,chain_id,chain_sequence,previous_hash,event_hash,provenance_json,created_at
 FROM fugue_audit_events WHERE tenant_id=$1 AND created_at >= $2 AND target_id=$3
 AND actor_type=$4 AND actor_id='safe-rollout-controller' AND target_type='app_release'
 AND action IN ('app.release.promote','app.release.abort.auto') ORDER BY created_at LIMIT 65`,
		release.TenantID, release.CreatedAt, release.ID, model.ActorTypeSystem)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []model.AuditEvent
	for rows.Next() {
		event, err := scanAuditEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if len(events) > 64 {
		return nil, ErrConflict // Never silently truncate provenance conflicts.
	}
	return events, rows.Err()
}

func migrateReleaseWorkload(current, expected model.AppRelease, op, source model.Operation, workload model.AppReleaseWorkload, events []model.AuditEvent) (model.AppRelease, error) {
	// GetOperation attaches independently stored timing observations; the row
	// lock read intentionally excludes them. They are not executable provenance.
	op.ControllerTimingSegments, source.ControllerTimingSegments = nil, nil
	if !reflect.DeepEqual(current, expected) || !reflect.DeepEqual(op, source) || current.RevisionWorkload != nil ||
		current.SpecSnapshot == nil || op.DesiredSpec == nil || op.Type != model.OperationTypeDeploy || op.ID != workload.OperationID ||
		op.CreatedAt.IsZero() || op.CompletedAt == nil || op.CompletedAt.Before(op.CreatedAt) ||
		op.AppID != current.AppID || op.TenantID != current.TenantID || current.RuntimeID != workload.RuntimeID || current.ResolvedImageRef != workload.ImageRef ||
		current.Status == model.AppReleaseStatusDraining && op.Status != model.OperationStatusCompleted ||
		current.Status == model.AppReleaseStatusFailed && op.Status != model.OperationStatusFailed {
		return model.AppRelease{}, ErrConflict
	}
	id, err := releaseWorkloadSource(current, events)
	if err != nil || id != op.ID {
		return model.AppRelease{}, ErrConflict
	}
	if current.Status == model.AppReleaseStatusDraining && (current.CreatedAt.Before(op.CreatedAt) || current.PromotedAt == nil || current.PromotedAt.After(*op.CompletedAt)) {
		return model.AppRelease{}, ErrConflict
	}
	workload.BoundAt = time.Now().UTC().Truncate(time.Microsecond)
	current.RevisionWorkload = &workload
	current.UpdatedAt = workload.BoundAt
	return current, nil
}
