package store

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
)

// AppReleaseAwaitingDrain identifies withdrawn lifecycle states. It does not
// authorize retirement: traffic, retention and runtime proof must still pass.
func AppReleaseAwaitingDrain(release model.AppRelease) bool {
	return release.Role == model.AppReleaseRolePrevious && release.Status == model.AppReleaseStatusDraining ||
		release.Role == model.AppReleaseRoleCandidate && release.Status == model.AppReleaseStatusFailed
}

// RetireDrainedAppRelease commits against the exact release and policy versions
// observed by the drain/traffic gates. It never overwrites a newer rollback,
// retention decision or canonical target with the caller's stale snapshot.
func (s *Store) RetireDrainedAppRelease(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy) (model.AppRelease, error) {
	return s.retireDrainedAppRelease(ctx, previous, stable, policy, nil, nil)
}

// RetireStoppedHistoricalAppRelease records observed resource identity only in
// the same transaction that makes an unbound historical release a tombstone.
// A damaged intent snapshot never gains a bound, reactivatable intermediate state.
func (s *Store) RetireStoppedHistoricalAppRelease(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy, op model.Operation, workload model.AppReleaseWorkload) (model.AppRelease, error) {
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

func (s *Store) retireDrainedAppRelease(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy, source *model.Operation, workload *model.AppReleaseWorkload) (model.AppRelease, error) {
	if previous.ID == "" || stable.ID == "" || previous.ID == stable.ID || policy.AppID == "" {
		return model.AppRelease{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return model.AppRelease{}, err
	}
	if s.usingDatabase() {
		return s.pgRetireDrainedAppRelease(ctx, previous, stable, policy, source, workload)
	}
	var result model.AppRelease
	err := s.withLockedState(true, func(state *model.State) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		pi, si, ti := findAppReleaseByID(state.AppReleases, previous.ID), findAppReleaseByID(state.AppReleases, stable.ID), findAppTrafficPolicyByApp(state.AppTrafficPolicies, policy.AppID)
		if pi < 0 || si < 0 || ti < 0 {
			return ErrNotFound
		}
		var err error
		result, err = retireDrainedRelease(state.AppReleases[pi], state.AppReleases[si], state.AppTrafficPolicies[ti], previous, stable, policy, time.Now().UTC())
		if err != nil {
			return err
		}
		if source != nil {
			var op model.Operation
			for _, o := range state.Operations {
				if o.ID == source.ID {
					op = o
					break
				}
			}
			bound, err := migrateReleaseWorkload(state.AppReleases[pi], previous, op, *source, *workload, state.AuditEvents)
			if err != nil {
				return err
			}
			result.RevisionWorkload = bound.RevisionWorkload
			result.StatusReason = "historical runtime verified stopped; original intent retained for audit"
		}
		if err = guardReleaseRetirement(state, state.AppReleases[pi], result); err != nil {
			return err
		}
		state.AppReleases[pi] = result
		return nil
	})
	return result, err
}

func retireDrainedRelease(currentPrevious, currentStable model.AppRelease, currentPolicy model.AppTrafficPolicy, previous, stable model.AppRelease, policy model.AppTrafficPolicy, now time.Time) (model.AppRelease, error) {
	if !reflect.DeepEqual(currentPrevious, previous) || !reflect.DeepEqual(currentStable, stable) || !reflect.DeepEqual(currentPolicy, policy) ||
		previous.AppID != policy.AppID || previous.TenantID != policy.TenantID || stable.AppID != policy.AppID || stable.TenantID != policy.TenantID ||
		!AppReleaseAwaitingDrain(previous) ||
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
