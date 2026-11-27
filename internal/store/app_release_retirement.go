package store

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
	retirement "fugue/internal/releaseretirement/contract"
)

type AppReleaseRetirementTarget struct{ TenantID, AppID string }

// Scheduling discovery never needs executable snapshots or every historical
// release row. Return each app once; the retirement transaction still reloads
// its exact release, operation and traffic policy before making any mutation.
func (s *Store) ListAppReleaseRetirementTargets(ctx context.Context) ([]AppReleaseRetirementTarget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.ensureDatabaseReady(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT tenant_id, app_id FROM fugue_app_releases
			WHERE (role='candidate' AND status='failed') OR (role='previous' AND status='draining')
			OR (status='retired' AND revision_workload_json IS NOT NULL)
			OR (role='stable' AND status='serving' AND deployment_name<>'' AND service_name<>''
				AND COALESCE(revision_workload_json->>'deployment_name','')<>''
				AND COALESCE(revision_workload_json->>'service_name','')<>''
				AND deployment_name<>(revision_workload_json->>'deployment_name')
				AND service_name<>(revision_workload_json->>'service_name')) ORDER BY tenant_id, app_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []AppReleaseRetirementTarget
	for rows.Next() {
		var target AppReleaseRetirementTarget
		if err := rows.Scan(&target.TenantID, &target.AppID); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

// retireStoppedHistoricalAppRelease records observed resource identity only in
// the same transaction that makes an unbound historical release a tombstone.
// A damaged intent snapshot never gains a bound, reactivatable intermediate state.
func (s *Store) retireStoppedHistoricalAppRelease(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy, op model.Operation, workload model.AppReleaseWorkload) (model.AppRelease, error) {
	if previous.RevisionWorkload != nil || previous.Role != model.AppReleaseRolePrevious || previous.Status != model.AppReleaseStatusDraining || workload.DeploymentGeneration < 1 || !workload.BoundAt.IsZero() {
		return model.AppRelease{}, ErrInvalidInput
	}
	for _, v := range []string{workload.OperationID, workload.Namespace, workload.DeploymentName, workload.DeploymentUID, workload.ServiceName, workload.ServiceUID, workload.ReleaseKey, workload.RuntimeID, workload.ImageRef} {
		if v == "" {
			return model.AppRelease{}, ErrInvalidInput
		}
	}
	return s.retireDrainedAppRelease(ctx, previous, stable, policy, &op, &workload)
}

// Commit only against the exact release and policy versions that passed the
// gates. Do not overwrite a newer rollback, retention or canonical target.
func (s *Store) retireDrainedAppRelease(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy, source *model.Operation, workload *model.AppReleaseWorkload) (model.AppRelease, error) {
	if previous.ID == "" || stable.ID == "" || previous.ID == stable.ID || policy.AppID == "" {
		return model.AppRelease{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return model.AppRelease{}, err
	}
	return s.pgRetireDrainedAppRelease(ctx, previous, stable, policy, source, workload)
}

func retireDrainedRelease(currentPrevious, currentStable model.AppRelease, currentPolicy model.AppTrafficPolicy, previous, stable model.AppRelease, policy model.AppTrafficPolicy, now time.Time) (model.AppRelease, error) {
	if !reflect.DeepEqual(currentPrevious, previous) || !reflect.DeepEqual(currentStable, stable) || !reflect.DeepEqual(currentPolicy, policy) ||
		previous.AppID != policy.AppID || previous.TenantID != policy.TenantID || stable.AppID != policy.AppID || stable.TenantID != policy.TenantID ||
		!retirement.AwaitingDrain(previous) ||
		stable.Role != model.AppReleaseRoleStable || stable.Status != model.AppReleaseStatusServing ||
		policy.Mode != model.AppTrafficModeSingle || policy.StableReleaseID != stable.ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 ||
		previous.RetentionUntil != nil && now.Before(*previous.RetentionUntil) {
		return model.AppRelease{}, ErrConflict
	}
	now = now.Truncate(time.Microsecond)
	currentPrevious.Role = model.AppReleaseRoleRetired
	currentPrevious.Status = model.AppReleaseStatusRetired
	currentPrevious.RetiredAt = &now
	currentPrevious.UpdatedAt = now
	currentPrevious.StatusReason = "safe rollout previous stable drained"
	if previous.Status == model.AppReleaseStatusFailed {
		currentPrevious.StatusReason = "failed candidate drained after rollback"
	}
	currentPrevious.ReleaseMessage = "retired after verified traffic withdrawal and Pod drain"
	return currentPrevious, nil
}

func (s *Store) pgRetireDrainedAppRelease(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy, source *model.Operation, workload *model.AppReleaseWorkload) (model.AppRelease, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppRelease{}, err
	}
	defer tx.Rollback()
	var op model.Operation
	if source != nil {
		op, err = s.pgGetOperationTx(ctx, tx, source.ID, true)
		if err != nil {
			return model.AppRelease{}, mapDBErr(err)
		}
	}
	// Match completed-deploy sync: traffic row, then release rows. A stale
	// legacy writer using reverse order is rejected by PostgreSQL deadlock
	// detection and retried with fresh evidence, never treated as success.
	currentPolicy, err := scanAppTrafficPolicy(tx.QueryRowContext(ctx, `SELECT `+appTrafficPolicySelectColumns+` FROM fugue_app_traffic_policies WHERE app_id=$1 FOR UPDATE`, policy.AppID))
	if err != nil {
		return model.AppRelease{}, mapDBErr(err)
	}
	ids := []string{previous.ID, stable.ID}
	sort.Strings(ids)
	current := map[string]model.AppRelease{}
	for _, id := range ids {
		r, err := s.pgGetAppReleaseForUpdateTx(ctx, tx, id)
		if err != nil {
			return model.AppRelease{}, mapDBErr(err)
		}
		current[id] = r
	}
	result, err := retireDrainedRelease(current[previous.ID], current[stable.ID], currentPolicy, previous, stable, policy, time.Now().UTC())
	if err != nil {
		return model.AppRelease{}, err
	}
	if source != nil {
		events, err := pgReleaseWorkloadSourceEvents(ctx, tx, previous)
		if err != nil {
			return model.AppRelease{}, err
		}
		bound, err := migrateReleaseWorkload(current[previous.ID], previous, op, *source, *workload, events)
		if err != nil {
			return model.AppRelease{}, err
		}
		result.RevisionWorkload = bound.RevisionWorkload
		result.StatusReason = "historical runtime verified stopped; original intent retained for audit"
		raw, err := json.Marshal(bound.RevisionWorkload)
		if err != nil {
			return model.AppRelease{}, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE fugue_app_releases SET revision_workload_json=$2 WHERE id=$1`, previous.ID, raw); err != nil {
			return model.AppRelease{}, mapDBErr(err)
		}
	}
	result, err = s.pgUpdateAppReleaseTx(ctx, tx, result)
	if err != nil {
		return model.AppRelease{}, err
	}
	if err = tx.Commit(); err != nil {
		return model.AppRelease{}, mapDBErr(err)
	}
	return result, nil
}
