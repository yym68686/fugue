package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const edgeRoutePolicySelectColumns = `id, hostname, tenant_id, app_id, edge_group_id, excluded_edge_ids, excluded_edge_group_ids, exclusion_reason, exclusion_expires_at, exclusion_scope, exclusion_owner_digest, exclusion_created_at, exclusion_generation, exclusion_fence, min_healthy_edge_nodes, route_policy, enabled, created_at, updated_at`

func (s *Store) pgListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT `+edgeRoutePolicySelectColumns+` FROM fugue_edge_route_policies ORDER BY hostname ASC, created_at ASC`)
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
	policy, err := scanEdgeRoutePolicy(s.db.QueryRowContext(ctx, `SELECT `+edgeRoutePolicySelectColumns+` FROM fugue_edge_route_policies WHERE lower(hostname) = lower($1)`, hostname))
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
	return pgUpsertEdgeRoutePolicy(ctx, s.db, policy)
}

func (s *Store) pgPutEdgeRoutePolicyCAS(policy model.EdgeRoutePolicy, expectedGeneration uint64, expectedFence string) (model.EdgeRoutePolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return model.EdgeRoutePolicy{}, fmt.Errorf("begin edge route exclusion CAS: %w", err)
	}
	defer tx.Rollback()

	now, err := pgEdgeServerTime(ctx, tx)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	current, getErr := scanEdgeRoutePolicy(tx.QueryRowContext(ctx, `SELECT `+edgeRoutePolicySelectColumns+` FROM fugue_edge_route_policies WHERE lower(hostname) = lower($1) FOR UPDATE`, policy.Hostname))
	switch {
	case errors.Is(getErr, sql.ErrNoRows):
		if expectedGeneration != 0 || expectedFence != "" {
			return model.EdgeRoutePolicy{}, ErrConflict
		}
		policy.ID = firstNonEmptyStore(policy.ID, model.NewID("edge_route_policy"))
		policy.CreatedAt = now
	case getErr != nil:
		return model.EdgeRoutePolicy{}, mapDBErr(getErr)
	default:
		if current.ExclusionGeneration != expectedGeneration || strings.TrimSpace(current.ExclusionFence) != expectedFence {
			return model.EdgeRoutePolicy{}, ErrConflict
		}
		removedEdges := removedEdgeExclusionIDs(current.ExcludedEdgeIDs, policy.ExcludedEdgeIDs, false)
		removedGroups := removedEdgeExclusionIDs(current.ExcludedEdgeGroupIDs, policy.ExcludedEdgeGroupIDs, true)
		if len(removedEdges) > 0 || len(removedGroups) > 0 {
			if err := s.pgValidateEdgeExclusionClearTx(ctx, tx, removedEdges, removedGroups, true); err != nil {
				return model.EdgeRoutePolicy{}, err
			}
		}
		policy.ID = current.ID
		policy.CreatedAt = current.CreatedAt
	}
	policy.ExclusionGeneration = expectedGeneration + 1
	policy.ExclusionFence = model.NewID("edge_exclusion_fence")
	policy.UpdatedAt = now
	if model.EdgeRoutePolicyHasExclusions(policy) {
		if policy.ExclusionCreatedAt == nil {
			created := now
			policy.ExclusionCreatedAt = &created
		}
		policy.ExclusionScope = model.EdgeRoutePolicyExclusionScope(policy)
	} else {
		clearEdgeRoutePolicyExclusionMetadata(&policy)
		policy.ExclusionGeneration = expectedGeneration + 1
		policy.ExclusionFence = model.NewID("edge_exclusion_fence")
	}
	stored, err := pgUpsertEdgeRoutePolicy(ctx, tx, policy)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeRoutePolicy{}, fmt.Errorf("commit edge route exclusion CAS: %w", err)
	}
	return stored, nil
}

type edgeRoutePolicyQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func pgUpsertEdgeRoutePolicy(ctx context.Context, queryer edgeRoutePolicyQueryer, policy model.EdgeRoutePolicy) (model.EdgeRoutePolicy, error) {
	if policy.ExcludedEdgeIDs == nil {
		policy.ExcludedEdgeIDs = []string{}
	}
	if policy.ExcludedEdgeGroupIDs == nil {
		policy.ExcludedEdgeGroupIDs = []string{}
	}
	excludedEdgeIDsJSON, err := marshalJSON(policy.ExcludedEdgeIDs)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	excludedEdgeGroupIDsJSON, err := marshalJSON(policy.ExcludedEdgeGroupIDs)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	row := queryer.QueryRowContext(ctx, `
INSERT INTO fugue_edge_route_policies (
	id, hostname, tenant_id, app_id, edge_group_id, excluded_edge_ids, excluded_edge_group_ids,
	exclusion_reason, exclusion_expires_at, exclusion_scope, exclusion_owner_digest, exclusion_created_at,
	exclusion_generation, exclusion_fence, min_healthy_edge_nodes, route_policy, enabled, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
ON CONFLICT (hostname) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id, app_id = EXCLUDED.app_id, edge_group_id = EXCLUDED.edge_group_id,
	excluded_edge_ids = EXCLUDED.excluded_edge_ids, excluded_edge_group_ids = EXCLUDED.excluded_edge_group_ids,
	exclusion_reason = EXCLUDED.exclusion_reason, exclusion_expires_at = EXCLUDED.exclusion_expires_at,
	exclusion_scope = EXCLUDED.exclusion_scope, exclusion_owner_digest = EXCLUDED.exclusion_owner_digest,
	exclusion_created_at = EXCLUDED.exclusion_created_at, exclusion_generation = EXCLUDED.exclusion_generation,
	exclusion_fence = EXCLUDED.exclusion_fence, min_healthy_edge_nodes = EXCLUDED.min_healthy_edge_nodes,
	route_policy = EXCLUDED.route_policy, enabled = EXCLUDED.enabled, updated_at = EXCLUDED.updated_at
RETURNING `+edgeRoutePolicySelectColumns,
		policy.ID, policy.Hostname, policy.TenantID, policy.AppID, policy.EdgeGroupID,
		excludedEdgeIDsJSON, excludedEdgeGroupIDsJSON, policy.ExclusionReason, policy.ExclusionExpiresAt,
		policy.ExclusionScope, policy.ExclusionOwnerDigest, policy.ExclusionCreatedAt, policy.ExclusionGeneration,
		policy.ExclusionFence, policy.MinHealthyEdgeNodes, policy.RoutePolicy, policy.Enabled, policy.CreatedAt, policy.UpdatedAt)
	stored, err := scanEdgeRoutePolicy(row)
	if err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	return stored, nil
}

func (s *Store) pgDeleteEdgeRoutePolicy(hostname string) (model.EdgeRoutePolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return model.EdgeRoutePolicy{}, fmt.Errorf("begin edge route policy delete: %w", err)
	}
	defer tx.Rollback()
	current, err := scanEdgeRoutePolicy(tx.QueryRowContext(ctx, `SELECT `+edgeRoutePolicySelectColumns+` FROM fugue_edge_route_policies WHERE lower(hostname)=lower($1) FOR UPDATE`, hostname))
	if err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	if model.EdgeRoutePolicyHasExclusions(current) {
		return model.EdgeRoutePolicy{}, ErrConflict
	}
	policy, err := scanEdgeRoutePolicy(tx.QueryRowContext(ctx, `DELETE FROM fugue_edge_route_policies WHERE lower(hostname) = lower($1) RETURNING `+edgeRoutePolicySelectColumns, hostname))
	if err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	return policy, nil
}

func (s *Store) pgDeleteEdgeRoutePolicyCAS(hostname string, expectedGeneration uint64, expectedFence string) (model.EdgeRoutePolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return model.EdgeRoutePolicy{}, fmt.Errorf("begin edge exclusion delete CAS: %w", err)
	}
	defer tx.Rollback()
	current, err := scanEdgeRoutePolicy(tx.QueryRowContext(ctx, `SELECT `+edgeRoutePolicySelectColumns+` FROM fugue_edge_route_policies WHERE lower(hostname)=lower($1) FOR UPDATE`, hostname))
	if err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	if current.ExclusionGeneration != expectedGeneration || current.ExclusionFence != expectedFence {
		return model.EdgeRoutePolicy{}, ErrConflict
	}
	if model.EdgeRoutePolicyHasExclusions(current) {
		if err := s.pgValidateEdgeExclusionClearTx(ctx, tx, current.ExcludedEdgeIDs, current.ExcludedEdgeGroupIDs, true); err != nil {
			return model.EdgeRoutePolicy{}, err
		}
	}
	policy, err := scanEdgeRoutePolicy(tx.QueryRowContext(ctx, `DELETE FROM fugue_edge_route_policies WHERE lower(hostname) = lower($1) AND exclusion_generation = $2 AND exclusion_fence = $3 RETURNING `+edgeRoutePolicySelectColumns, hostname, expectedGeneration, expectedFence))
	if errors.Is(err, sql.ErrNoRows) {
		return model.EdgeRoutePolicy{}, ErrConflict
	}
	if err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeRoutePolicy{}, mapDBErr(err)
	}
	return policy, nil
}

func scanEdgeRoutePolicy(scanner sqlScanner) (model.EdgeRoutePolicy, error) {
	var policy model.EdgeRoutePolicy
	var edgeGroupID sql.NullString
	var excludedEdgeIDsRaw, excludedEdgeGroupIDsRaw []byte
	var exclusionExpiresAt, exclusionCreatedAt sql.NullTime
	var exclusionGeneration int64
	if err := scanner.Scan(
		&policy.ID, &policy.Hostname, &policy.TenantID, &policy.AppID, &edgeGroupID,
		&excludedEdgeIDsRaw, &excludedEdgeGroupIDsRaw, &policy.ExclusionReason, &exclusionExpiresAt,
		&policy.ExclusionScope, &policy.ExclusionOwnerDigest, &exclusionCreatedAt, &exclusionGeneration,
		&policy.ExclusionFence, &policy.MinHealthyEdgeNodes, &policy.RoutePolicy, &policy.Enabled,
		&policy.CreatedAt, &policy.UpdatedAt,
	); err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	if exclusionGeneration < 0 {
		return model.EdgeRoutePolicy{}, fmt.Errorf("invalid negative edge exclusion generation")
	}
	policy.ExclusionGeneration = uint64(exclusionGeneration)
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
	policy.ExclusionScope = strings.TrimSpace(strings.ToLower(policy.ExclusionScope))
	policy.ExclusionOwnerDigest = strings.TrimSpace(strings.ToLower(policy.ExclusionOwnerDigest))
	policy.ExclusionFence = strings.TrimSpace(policy.ExclusionFence)
	if exclusionExpiresAt.Valid {
		expiresAt := exclusionExpiresAt.Time.UTC()
		policy.ExclusionExpiresAt = &expiresAt
	}
	if exclusionCreatedAt.Valid {
		createdAt := exclusionCreatedAt.Time.UTC()
		policy.ExclusionCreatedAt = &createdAt
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
		clearEdgeRoutePolicyExclusionMetadata(&policy)
	}
	return policy, nil
}
