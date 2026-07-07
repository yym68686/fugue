package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgSyncStableReleaseForCompletedDeployTx(ctx context.Context, tx *sql.Tx, app model.App, op model.Operation, now time.Time) error {
	if !shouldSyncStableReleaseForCompletedDeploy(app, op) {
		return nil
	}
	policy, err := scanAppTrafficPolicy(tx.QueryRowContext(ctx, `
SELECT `+appTrafficPolicySelectColumns+`
FROM fugue_app_traffic_policies
WHERE app_id = $1
FOR UPDATE
`, app.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return mapDBErr(err)
	}
	if !trafficPolicyAllowsStableReleaseAutoSync(app, policy) {
		return nil
	}

	desired, ok := currentStableReleaseForApp(app, now)
	if !ok {
		return nil
	}
	reusable, found, err := s.pgFindReusableCurrentStableReleaseTx(ctx, tx, desired)
	if err != nil {
		return err
	}
	if found {
		reusable.Role = model.AppReleaseRoleStable
		reusable.Status = model.AppReleaseStatusServing
		reusable.StatusReason = ""
		reusable.ReadyAt = firstNonNilTime(reusable.ReadyAt, &now)
		reusable.PromotedAt = &now
		reusable.UpdatedAt = now
		if _, err := s.pgUpdateAppReleaseTx(ctx, tx, reusable); err != nil {
			return err
		}
		desired = reusable
	} else {
		desired, err = normalizeAppReleaseForStore(desired)
		if err != nil {
			return err
		}
		if _, err := s.pgCreateAppReleaseTx(ctx, tx, desired); err != nil {
			return err
		}
	}

	if policy.StableReleaseID != "" && policy.StableReleaseID != desired.ID {
		if oldStable, err := s.pgGetAppReleaseForUpdateTx(ctx, tx, policy.StableReleaseID); err == nil && oldStable.AppID == app.ID {
			oldStable.Role = model.AppReleaseRolePrevious
			oldStable.UpdatedAt = now
			if _, err := s.pgUpdateAppReleaseTx(ctx, tx, oldStable); err != nil {
				return err
			}
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}

	policy = stableReleaseSyncedTrafficPolicy(policy, desired.ID, now)
	if _, err := s.pgUpsertAppTrafficPolicyTx(ctx, tx, policy); err != nil {
		return err
	}
	return nil
}

func (s *Store) pgFindReusableCurrentStableReleaseTx(ctx context.Context, tx *sql.Tx, desired model.AppRelease) (model.AppRelease, bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT `+appReleaseSelectColumns+`
FROM fugue_app_releases
WHERE tenant_id = $1 AND app_id = $2
FOR UPDATE
`, desired.TenantID, desired.AppID)
	if err != nil {
		return model.AppRelease{}, false, mapDBErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		release, err := scanAppRelease(rows)
		if err != nil {
			return model.AppRelease{}, false, err
		}
		if appReleaseMatchesCurrentStable(release, desired) {
			return release, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return model.AppRelease{}, false, err
	}
	return model.AppRelease{}, false, nil
}

func (s *Store) pgGetAppReleaseForUpdateTx(ctx context.Context, tx *sql.Tx, releaseID string) (model.AppRelease, error) {
	release, err := scanAppRelease(tx.QueryRowContext(ctx, `
SELECT `+appReleaseSelectColumns+`
FROM fugue_app_releases
WHERE id = $1
FOR UPDATE
`, releaseID))
	return release, err
}

func (s *Store) pgCreateAppReleaseTx(ctx context.Context, tx *sql.Tx, release model.AppRelease) (model.AppRelease, error) {
	specJSON, err := marshalNullableAppSpec(release.SpecSnapshot)
	if err != nil {
		return model.AppRelease{}, err
	}
	out, err := scanAppRelease(tx.QueryRowContext(ctx, `
INSERT INTO fugue_app_releases (
	id, tenant_id, app_id, role, source_ref, resolved_image_ref, upstream_url, runtime_id,
	deployment_name, service_name, status, status_reason, rollback_target_release_id, release_message, spec_snapshot_json,
	ready_at, promoted_at, retired_at, retention_until, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8,
	$9, $10, $11, $12, $13, $14, $15,
	$16, $17, $18, $19, $20, $21
)
RETURNING `+appReleaseSelectColumns,
		release.ID, release.TenantID, release.AppID, release.Role, release.SourceRef, release.ResolvedImageRef, release.UpstreamURL, release.RuntimeID,
		release.DeploymentName, release.ServiceName, release.Status, release.StatusReason, release.RollbackTargetID, release.ReleaseMessage, specJSON,
		release.ReadyAt, release.PromotedAt, release.RetiredAt, release.RetentionUntil, release.CreatedAt, release.UpdatedAt))
	return out, mapDBErr(err)
}

func (s *Store) pgUpdateAppReleaseTx(ctx context.Context, tx *sql.Tx, release model.AppRelease) (model.AppRelease, error) {
	specJSON, err := marshalNullableAppSpec(release.SpecSnapshot)
	if err != nil {
		return model.AppRelease{}, err
	}
	out, err := scanAppRelease(tx.QueryRowContext(ctx, `
UPDATE fugue_app_releases
SET tenant_id = $2,
	app_id = $3,
	role = $4,
	source_ref = $5,
	resolved_image_ref = $6,
	upstream_url = $7,
	runtime_id = $8,
	deployment_name = $9,
	service_name = $10,
	status = $11,
	status_reason = $12,
	rollback_target_release_id = $13,
	release_message = $14,
	spec_snapshot_json = $15,
	ready_at = $16,
	promoted_at = $17,
	retired_at = $18,
	retention_until = $19,
	updated_at = $20
WHERE id = $1
RETURNING `+appReleaseSelectColumns,
		release.ID, release.TenantID, release.AppID, release.Role, release.SourceRef, release.ResolvedImageRef, release.UpstreamURL, release.RuntimeID,
		release.DeploymentName, release.ServiceName, release.Status, release.StatusReason, release.RollbackTargetID, release.ReleaseMessage, specJSON,
		release.ReadyAt, release.PromotedAt, release.RetiredAt, release.RetentionUntil, release.UpdatedAt))
	return out, mapDBErr(err)
}

func (s *Store) pgUpsertAppTrafficPolicyTx(ctx context.Context, tx *sql.Tx, policy model.AppTrafficPolicy) (model.AppTrafficPolicy, error) {
	policy, err := normalizeAppTrafficPolicyForStore(policy)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	out, err := scanAppTrafficPolicy(tx.QueryRowContext(ctx, `
INSERT INTO fugue_app_traffic_policies (
	id, tenant_id, app_id, mode, stable_release_id, candidate_release_id,
	stable_weight, candidate_weight, sticky_header, sticky_cookie,
	updated_by_type, updated_by_id, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10,
	$11, $12, $13, $14
)
ON CONFLICT (app_id) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id,
	mode = EXCLUDED.mode,
	stable_release_id = EXCLUDED.stable_release_id,
	candidate_release_id = EXCLUDED.candidate_release_id,
	stable_weight = EXCLUDED.stable_weight,
	candidate_weight = EXCLUDED.candidate_weight,
	sticky_header = EXCLUDED.sticky_header,
	sticky_cookie = EXCLUDED.sticky_cookie,
	updated_by_type = EXCLUDED.updated_by_type,
	updated_by_id = EXCLUDED.updated_by_id,
	updated_at = EXCLUDED.updated_at
RETURNING `+appTrafficPolicySelectColumns,
		policy.ID, policy.TenantID, policy.AppID, policy.Mode, policy.StableReleaseID, policy.CandidateReleaseID,
		policy.StableWeight, policy.CandidateWeight, policy.StickyHeader, policy.StickyCookie,
		policy.UpdatedByType, policy.UpdatedByID, policy.CreatedAt, policy.UpdatedAt))
	if err != nil {
		return model.AppTrafficPolicy{}, fmt.Errorf("upsert app traffic policy during stable release sync: %w", mapDBErr(err))
	}
	return out, nil
}
