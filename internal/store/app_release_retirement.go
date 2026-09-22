package store

import (
	"context"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
)

// RetireDrainedAppRelease commits against the exact release and policy versions
// observed by the drain/traffic gates. It never overwrites a newer rollback,
// retention decision or canonical target with the caller's stale snapshot.
func (s *Store) RetireDrainedAppRelease(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy) (model.AppRelease, error) {
	if previous.ID == "" || stable.ID == "" || previous.ID == stable.ID || policy.AppID == "" {
		return model.AppRelease{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return model.AppRelease{}, err
	}
	if s.usingDatabase() {
		return s.pgRetireDrainedAppRelease(ctx, previous, stable, policy)
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
		previous.Role != model.AppReleaseRolePrevious || previous.Status != model.AppReleaseStatusDraining ||
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
	currentPrevious.ReleaseMessage = "retired after verified traffic withdrawal and Pod drain"
	return currentPrevious, nil
}

func (s *Store) pgRetireDrainedAppRelease(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy) (model.AppRelease, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppRelease{}, err
	}
	defer tx.Rollback()
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
	result, err = s.pgUpdateAppReleaseTx(ctx, tx, result)
	if err != nil {
		return model.AppRelease{}, err
	}
	if err = tx.Commit(); err != nil {
		return model.AppRelease{}, mapDBErr(err)
	}
	return result, nil
}
