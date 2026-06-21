package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT id, hostname, tenant_id, app_id, edge_group_id, excluded_edge_ids, excluded_edge_group_ids, exclusion_reason, exclusion_expires_at, route_policy, enabled, created_at, updated_at
FROM fugue_edge_route_policies
ORDER BY hostname ASC, created_at ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list edge route policies: %w", err)
	}
	defer rows.Close()

	policies := make([]model.EdgeRoutePolicy, 0)
	for rows.Next() {
		policy, err := scanEdgeRoutePolicy(rows)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge route policies: %w", err)
	}
	return policies, nil
}

func (s *Store) pgGetEdgeRoutePolicy(hostname string) (model.EdgeRoutePolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	policy, err := scanEdgeRoutePolicy(s.db.QueryRowContext(ctx, `
SELECT id, hostname, tenant_id, app_id, edge_group_id, excluded_edge_ids, excluded_edge_group_ids, exclusion_reason, exclusion_expires_at, route_policy, enabled, created_at, updated_at
FROM fugue_edge_route_policies
WHERE lower(hostname) = lower($1)
`, hostname))
	if err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	return policy, nil
}

func (s *Store) pgPutEdgeRoutePolicy(policy model.EdgeRoutePolicy) (model.EdgeRoutePolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	if policy.ID == "" {
		policy.ID = model.NewID("edge_route_policy")
	}
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = now
	}
	policy.UpdatedAt = now
	excludedEdgeIDsJSON, err := marshalJSON(policy.ExcludedEdgeIDs)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	excludedEdgeGroupIDsJSON, err := marshalJSON(policy.ExcludedEdgeGroupIDs)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}

	row := s.db.QueryRowContext(ctx, `
INSERT INTO fugue_edge_route_policies (
	id, hostname, tenant_id, app_id, edge_group_id, excluded_edge_ids, excluded_edge_group_ids,
	exclusion_reason, exclusion_expires_at, route_policy, enabled, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12, $13)
ON CONFLICT (hostname) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id,
	app_id = EXCLUDED.app_id,
	edge_group_id = EXCLUDED.edge_group_id,
	excluded_edge_ids = EXCLUDED.excluded_edge_ids,
	excluded_edge_group_ids = EXCLUDED.excluded_edge_group_ids,
	exclusion_reason = EXCLUDED.exclusion_reason,
	exclusion_expires_at = EXCLUDED.exclusion_expires_at,
	route_policy = EXCLUDED.route_policy,
	enabled = EXCLUDED.enabled,
	updated_at = EXCLUDED.updated_at
RETURNING id, hostname, tenant_id, app_id, edge_group_id, excluded_edge_ids, excluded_edge_group_ids, exclusion_reason, exclusion_expires_at, route_policy, enabled, created_at, updated_at
`, policy.ID, policy.Hostname, policy.TenantID, policy.AppID, policy.EdgeGroupID, excludedEdgeIDsJSON, excludedEdgeGroupIDsJSON, policy.ExclusionReason, policy.ExclusionExpiresAt, policy.RoutePolicy, policy.Enabled, policy.CreatedAt, policy.UpdatedAt)
	stored, err := scanEdgeRoutePolicy(row)
	if err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	return stored, nil
}

func (s *Store) pgDeleteEdgeRoutePolicy(hostname string) (model.EdgeRoutePolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	policy, err := scanEdgeRoutePolicy(s.db.QueryRowContext(ctx, `
DELETE FROM fugue_edge_route_policies
WHERE lower(hostname) = lower($1)
RETURNING id, hostname, tenant_id, app_id, edge_group_id, excluded_edge_ids, excluded_edge_group_ids, exclusion_reason, exclusion_expires_at, route_policy, enabled, created_at, updated_at
`, hostname))
	if err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	return policy, nil
}

func scanEdgeRoutePolicy(scanner sqlScanner) (model.EdgeRoutePolicy, error) {
	var policy model.EdgeRoutePolicy
	var edgeGroupID sql.NullString
	var excludedEdgeIDsRaw []byte
	var excludedEdgeGroupIDsRaw []byte
	var exclusionExpiresAt sql.NullTime
	if err := scanner.Scan(
		&policy.ID,
		&policy.Hostname,
		&policy.TenantID,
		&policy.AppID,
		&edgeGroupID,
		&excludedEdgeIDsRaw,
		&excludedEdgeGroupIDsRaw,
		&policy.ExclusionReason,
		&exclusionExpiresAt,
		&policy.RoutePolicy,
		&policy.Enabled,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	); err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	excludedEdgeIDs, err := decodeJSONValue[[]string](excludedEdgeIDsRaw)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	excludedEdgeGroupIDs, err := decodeJSONValue[[]string](excludedEdgeGroupIDsRaw)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	policy.Hostname = normalizeEdgeRoutePolicyHostname(policy.Hostname)
	policy.EdgeGroupID = normalizeEdgeGroupID(edgeGroupID.String)
	policy.ExcludedEdgeIDs = normalizeEdgeRoutePolicyIDList(excludedEdgeIDs, false)
	policy.ExcludedEdgeGroupIDs = normalizeEdgeRoutePolicyIDList(excludedEdgeGroupIDs, true)
	policy.ExclusionReason = strings.TrimSpace(policy.ExclusionReason)
	if exclusionExpiresAt.Valid {
		expiresAt := exclusionExpiresAt.Time.UTC()
		policy.ExclusionExpiresAt = &expiresAt
	}
	policy.RoutePolicy = model.NormalizeEdgeRoutePolicy(policy.RoutePolicy)
	if policy.RoutePolicy == "" {
		policy.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
	}
	policy.Enabled = model.EdgeRoutePolicyAllowsTraffic(policy.RoutePolicy)
	if !policy.Enabled {
		policy.EdgeGroupID = ""
		policy.ExcludedEdgeIDs = nil
		policy.ExcludedEdgeGroupIDs = nil
		policy.ExclusionReason = ""
		policy.ExclusionExpiresAt = nil
	}
	return policy, nil
}
