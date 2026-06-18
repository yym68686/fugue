package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const appReleaseSelectColumns = `id, tenant_id, app_id, role, source_ref, resolved_image_ref, upstream_url, runtime_id, deployment_name, service_name, status, status_reason, spec_snapshot_json, ready_at, promoted_at, retired_at, created_at, updated_at`
const appTrafficPolicySelectColumns = `id, tenant_id, app_id, mode, stable_release_id, candidate_release_id, stable_weight, candidate_weight, sticky_header, sticky_cookie, updated_by_type, updated_by_id, created_at, updated_at`

func (s *Store) pgCreateAppRelease(release model.AppRelease) (model.AppRelease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	specJSON, err := marshalNullableAppSpec(release.SpecSnapshot)
	if err != nil {
		return model.AppRelease{}, err
	}
	out, err := scanAppRelease(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_app_releases (
	id, tenant_id, app_id, role, source_ref, resolved_image_ref, upstream_url, runtime_id,
	deployment_name, service_name, status, status_reason, spec_snapshot_json,
	ready_at, promoted_at, retired_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8,
	$9, $10, $11, $12, $13,
	$14, $15, $16, $17, $18
)
RETURNING `+appReleaseSelectColumns,
		release.ID, release.TenantID, release.AppID, release.Role, release.SourceRef, release.ResolvedImageRef, release.UpstreamURL, release.RuntimeID,
		release.DeploymentName, release.ServiceName, release.Status, release.StatusReason, specJSON,
		release.ReadyAt, release.PromotedAt, release.RetiredAt, release.CreatedAt, release.UpdatedAt))
	return out, mapDBErr(err)
}

func (s *Store) pgUpdateAppRelease(release model.AppRelease) (model.AppRelease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	specJSON, err := marshalNullableAppSpec(release.SpecSnapshot)
	if err != nil {
		return model.AppRelease{}, err
	}
	out, err := scanAppRelease(s.db.QueryRowContext(ctx, `
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
	spec_snapshot_json = $13,
	ready_at = $14,
	promoted_at = $15,
	retired_at = $16,
	updated_at = $17
WHERE id = $1
RETURNING `+appReleaseSelectColumns,
		release.ID, release.TenantID, release.AppID, release.Role, release.SourceRef, release.ResolvedImageRef, release.UpstreamURL, release.RuntimeID,
		release.DeploymentName, release.ServiceName, release.Status, release.StatusReason, specJSON,
		release.ReadyAt, release.PromotedAt, release.RetiredAt, release.UpdatedAt))
	return out, mapDBErr(err)
}

func (s *Store) pgGetAppRelease(tenantID string, platformAdmin bool, releaseID string) (model.AppRelease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := []any{strings.TrimSpace(releaseID)}
	query := `SELECT ` + appReleaseSelectColumns + ` FROM fugue_app_releases WHERE id = $1`
	if !platformAdmin {
		args = append(args, strings.TrimSpace(tenantID))
		query += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	out, err := scanAppRelease(s.db.QueryRowContext(ctx, query, args...))
	return out, mapDBErr(err)
}

func (s *Store) pgListAppReleases(filter model.AppReleaseFilter) ([]model.AppRelease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	} else if filter.TenantID != "" {
		args = append(args, filter.TenantID)
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf("app_id = $%d", len(args)))
	}
	if filter.Role != "" {
		args = append(args, filter.Role)
		clauses = append(clauses, fmt.Sprintf("role = $%d", len(args)))
	}
	if !filter.IncludeRetired {
		args = append(args, model.AppReleaseRoleRetired)
		clauses = append(clauses, fmt.Sprintf("role <> $%d", len(args)))
	}

	query := `SELECT ` + appReleaseSelectColumns + ` FROM fugue_app_releases`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY app_id ASC, role ASC, created_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	return scanAppReleaseRows(rows)
}

func (s *Store) pgGetAppTrafficPolicy(tenantID string, platformAdmin bool, appID string) (model.AppTrafficPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := []any{strings.TrimSpace(appID)}
	query := `SELECT ` + appTrafficPolicySelectColumns + ` FROM fugue_app_traffic_policies WHERE app_id = $1`
	if !platformAdmin {
		args = append(args, strings.TrimSpace(tenantID))
		query += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	out, err := scanAppTrafficPolicy(s.db.QueryRowContext(ctx, query, args...))
	return out, mapDBErr(err)
}

func (s *Store) pgListAppTrafficPolicies(tenantID string, platformAdmin bool) ([]model.AppTrafficPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := []any{}
	query := `SELECT ` + appTrafficPolicySelectColumns + ` FROM fugue_app_traffic_policies`
	if !platformAdmin {
		args = append(args, strings.TrimSpace(tenantID))
		query += " WHERE tenant_id = $1"
	} else if strings.TrimSpace(tenantID) != "" {
		args = append(args, strings.TrimSpace(tenantID))
		query += " WHERE tenant_id = $1"
	}
	query += " ORDER BY app_id ASC, updated_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	return scanAppTrafficPolicyRows(rows)
}

func (s *Store) pgUpsertAppTrafficPolicy(policy model.AppTrafficPolicy) (model.AppTrafficPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := scanAppTrafficPolicy(s.db.QueryRowContext(ctx, `
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
	return out, mapDBErr(err)
}

func scanAppReleaseRows(rows *sql.Rows) ([]model.AppRelease, error) {
	out := []model.AppRelease{}
	for rows.Next() {
		release, err := scanAppRelease(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, release)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func scanAppRelease(scanner sqlScanner) (model.AppRelease, error) {
	var release model.AppRelease
	var specJSON []byte
	var readyAt sql.NullTime
	var promotedAt sql.NullTime
	var retiredAt sql.NullTime
	if err := scanner.Scan(
		&release.ID,
		&release.TenantID,
		&release.AppID,
		&release.Role,
		&release.SourceRef,
		&release.ResolvedImageRef,
		&release.UpstreamURL,
		&release.RuntimeID,
		&release.DeploymentName,
		&release.ServiceName,
		&release.Status,
		&release.StatusReason,
		&specJSON,
		&readyAt,
		&promotedAt,
		&retiredAt,
		&release.CreatedAt,
		&release.UpdatedAt,
	); err != nil {
		return model.AppRelease{}, err
	}
	if len(specJSON) > 0 && string(specJSON) != "null" {
		var spec model.AppSpec
		if err := json.Unmarshal(specJSON, &spec); err != nil {
			return model.AppRelease{}, err
		}
		release.SpecSnapshot = &spec
	}
	if readyAt.Valid {
		release.ReadyAt = &readyAt.Time
	}
	if promotedAt.Valid {
		release.PromotedAt = &promotedAt.Time
	}
	if retiredAt.Valid {
		release.RetiredAt = &retiredAt.Time
	}
	return release, nil
}

func scanAppTrafficPolicyRows(rows *sql.Rows) ([]model.AppTrafficPolicy, error) {
	out := []model.AppTrafficPolicy{}
	for rows.Next() {
		policy, err := scanAppTrafficPolicy(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func scanAppTrafficPolicy(scanner sqlScanner) (model.AppTrafficPolicy, error) {
	var policy model.AppTrafficPolicy
	if err := scanner.Scan(
		&policy.ID,
		&policy.TenantID,
		&policy.AppID,
		&policy.Mode,
		&policy.StableReleaseID,
		&policy.CandidateReleaseID,
		&policy.StableWeight,
		&policy.CandidateWeight,
		&policy.StickyHeader,
		&policy.StickyCookie,
		&policy.UpdatedByType,
		&policy.UpdatedByID,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	); err != nil {
		return model.AppTrafficPolicy{}, err
	}
	return policy, nil
}

func marshalNullableAppSpec(spec *model.AppSpec) (any, error) {
	if spec == nil {
		return nil, nil
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
