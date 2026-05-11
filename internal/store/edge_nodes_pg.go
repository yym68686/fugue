package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgListEdgeNodes(edgeGroupID string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, edge_group_id, region, country, public_hostname, public_ipv4, public_ipv6, mesh_ip,
	status, healthy, draining, route_bundle_version, dns_bundle_version, caddy_route_count,
	caddy_applied_version, caddy_last_error, cache_status, last_error, token_prefix, token_hash,
	last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_edge_nodes`
	args := []any{}
	if edgeGroupID != "" {
		query += ` WHERE edge_group_id = $1`
		args = append(args, edgeGroupID)
	}
	query += ` ORDER BY edge_group_id ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list edge nodes: %w", err)
	}
	defer rows.Close()

	nodes := []model.EdgeNode{}
	for rows.Next() {
		node, err := scanEdgeNode(rows)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, redactEdgeNode(node))
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate edge nodes: %w", err)
	}
	groups, err := s.pgListEdgeGroups(ctx, edgeGroupID)
	if err != nil {
		return nil, nil, err
	}
	return nodes, groups, nil
}

func (s *Store) pgGetEdgeNode(edgeID string) (model.EdgeNode, model.EdgeGroup, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	node, err := scanEdgeNode(s.db.QueryRowContext(ctx, `
SELECT id, edge_group_id, region, country, public_hostname, public_ipv4, public_ipv6, mesh_ip,
	status, healthy, draining, route_bundle_version, dns_bundle_version, caddy_route_count,
	caddy_applied_version, caddy_last_error, cache_status, last_error, token_prefix, token_hash,
	last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_edge_nodes
WHERE id = $1
`, edgeID))
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, mapDBErr(err)
	}
	groups, err := s.pgListEdgeGroups(ctx, node.EdgeGroupID)
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, err
	}
	group := model.EdgeGroup{ID: node.EdgeGroupID}
	if len(groups) > 0 {
		group = groups[0]
	}
	return redactEdgeNode(node), group, nil
}

func (s *Store) pgCreateEdgeNodeToken(node model.EdgeNode) (model.EdgeNode, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	secret := model.NewSecret("fugue_edge")
	node.TokenPrefix = model.SecretPrefix(secret)
	node.TokenHash = model.HashSecret(secret)
	now := time.Now().UTC()
	if node.CreatedAt.IsZero() {
		node.CreatedAt = now
	}
	node.UpdatedAt = now

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EdgeNode{}, "", fmt.Errorf("begin edge token transaction: %w", err)
	}
	defer tx.Rollback()

	if err := pgUpsertEdgeGroup(ctx, tx, node, now); err != nil {
		return model.EdgeNode{}, "", err
	}
	stored, err := pgUpsertEdgeNode(ctx, tx, node, true)
	if err != nil {
		return model.EdgeNode{}, "", err
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeNode{}, "", fmt.Errorf("commit edge token transaction: %w", err)
	}
	return redactEdgeNode(stored), secret, nil
}

func (s *Store) pgAuthenticateEdgeNode(secret string) (model.EdgeNode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EdgeNode{}, fmt.Errorf("begin authenticate edge node transaction: %w", err)
	}
	defer tx.Rollback()

	node, err := scanEdgeNode(tx.QueryRowContext(ctx, `
SELECT id, edge_group_id, region, country, public_hostname, public_ipv4, public_ipv6, mesh_ip,
	status, healthy, draining, route_bundle_version, dns_bundle_version, caddy_route_count,
	caddy_applied_version, caddy_last_error, cache_status, last_error, token_prefix, token_hash,
	last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_edge_nodes
WHERE token_hash = $1 AND token_hash <> ''
`, model.HashSecret(secret)))
	if err != nil {
		return model.EdgeNode{}, mapDBErr(err)
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_edge_nodes SET last_seen_at = $2, updated_at = $2 WHERE id = $1
`, node.ID, now); err != nil {
		return model.EdgeNode{}, mapDBErr(err)
	}
	node.LastSeenAt = &now
	node.UpdatedAt = now
	if err := tx.Commit(); err != nil {
		return model.EdgeNode{}, fmt.Errorf("commit authenticate edge node transaction: %w", err)
	}
	return redactEdgeNode(node), nil
}

func (s *Store) pgUpdateEdgeHeartbeat(node model.EdgeNode) (model.EdgeNode, model.EdgeGroup, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	node.LastSeenAt = &now
	node.LastHeartbeatAt = &now
	node.UpdatedAt = now
	if node.CreatedAt.IsZero() {
		node.CreatedAt = now
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, fmt.Errorf("begin edge heartbeat transaction: %w", err)
	}
	defer tx.Rollback()

	if err := pgUpsertEdgeGroup(ctx, tx, node, now); err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, err
	}
	stored, err := pgUpsertEdgeNode(ctx, tx, node, false)
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, fmt.Errorf("commit edge heartbeat transaction: %w", err)
	}
	groups, err := s.pgListEdgeGroups(ctx, stored.EdgeGroupID)
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, err
	}
	group := model.EdgeGroup{ID: stored.EdgeGroupID}
	if len(groups) > 0 {
		group = groups[0]
	}
	return redactEdgeNode(stored), group, nil
}

func pgUpsertEdgeGroup(ctx context.Context, tx *sql.Tx, node model.EdgeNode, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_edge_groups (id, region, country, status, created_at, updated_at)
VALUES ($1, $2, $3, '', $4, $4)
ON CONFLICT (id) DO UPDATE SET
	region = CASE WHEN EXCLUDED.region <> '' THEN EXCLUDED.region ELSE fugue_edge_groups.region END,
	country = CASE WHEN EXCLUDED.country <> '' THEN EXCLUDED.country ELSE fugue_edge_groups.country END,
	updated_at = EXCLUDED.updated_at
`, node.EdgeGroupID, node.Region, node.Country, now); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func pgUpsertEdgeNode(ctx context.Context, tx *sql.Tx, node model.EdgeNode, replaceToken bool) (model.EdgeNode, error) {
	row := tx.QueryRowContext(ctx, `
INSERT INTO fugue_edge_nodes (
	id, edge_group_id, region, country, public_hostname, public_ipv4, public_ipv6, mesh_ip,
	status, healthy, draining, route_bundle_version, dns_bundle_version, caddy_route_count,
	caddy_applied_version, caddy_last_error, cache_status, last_error, token_prefix, token_hash,
	last_seen_at, last_heartbeat_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8,
	$9, $10, $11, $12, $13, $14,
	$15, $16, $17, $18, $19, $20,
	$21, $22, $23, $24
)
ON CONFLICT (id) DO UPDATE SET
	edge_group_id = EXCLUDED.edge_group_id,
	region = EXCLUDED.region,
	country = EXCLUDED.country,
	public_hostname = EXCLUDED.public_hostname,
	public_ipv4 = EXCLUDED.public_ipv4,
	public_ipv6 = EXCLUDED.public_ipv6,
	mesh_ip = EXCLUDED.mesh_ip,
	status = EXCLUDED.status,
	healthy = EXCLUDED.healthy,
	draining = EXCLUDED.draining,
	route_bundle_version = EXCLUDED.route_bundle_version,
	dns_bundle_version = EXCLUDED.dns_bundle_version,
	caddy_route_count = EXCLUDED.caddy_route_count,
	caddy_applied_version = EXCLUDED.caddy_applied_version,
	caddy_last_error = EXCLUDED.caddy_last_error,
	cache_status = EXCLUDED.cache_status,
	last_error = EXCLUDED.last_error,
	token_prefix = CASE WHEN $25 THEN EXCLUDED.token_prefix ELSE fugue_edge_nodes.token_prefix END,
	token_hash = CASE WHEN $25 THEN EXCLUDED.token_hash ELSE fugue_edge_nodes.token_hash END,
	last_seen_at = EXCLUDED.last_seen_at,
	last_heartbeat_at = EXCLUDED.last_heartbeat_at,
	updated_at = EXCLUDED.updated_at
RETURNING id, edge_group_id, region, country, public_hostname, public_ipv4, public_ipv6, mesh_ip,
	status, healthy, draining, route_bundle_version, dns_bundle_version, caddy_route_count,
	caddy_applied_version, caddy_last_error, cache_status, last_error, token_prefix, token_hash,
	last_seen_at, last_heartbeat_at, created_at, updated_at
`, node.ID, node.EdgeGroupID, node.Region, node.Country, node.PublicHostname, node.PublicIPv4, node.PublicIPv6, node.MeshIP,
		node.Status, node.Healthy, node.Draining, node.RouteBundleVersion, node.DNSBundleVersion, node.CaddyRouteCount,
		node.CaddyAppliedVersion, node.CaddyLastError, node.CacheStatus, node.LastError, node.TokenPrefix, node.TokenHash,
		node.LastSeenAt, node.LastHeartbeatAt, node.CreatedAt, node.UpdatedAt, replaceToken)
	stored, err := scanEdgeNode(row)
	if err != nil {
		return model.EdgeNode{}, mapDBErr(err)
	}
	return stored, nil
}

func (s *Store) pgListEdgeGroups(ctx context.Context, edgeGroupID string) ([]model.EdgeGroup, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	query := `
SELECT
	g.id,
	g.region,
	g.country,
	CASE
		WHEN COUNT(n.id) FILTER (WHERE n.healthy AND NOT n.draining AND n.status = $1) > 0 THEN $1
		WHEN COUNT(n.id) > 0 THEN $2
		ELSE COALESCE(NULLIF(g.status, ''), $3)
	END AS status,
	COUNT(n.id)::int AS node_count,
	COUNT(n.id) FILTER (WHERE n.healthy AND NOT n.draining AND n.status = $1)::int AS healthy_node_count,
	(COUNT(n.id) FILTER (WHERE n.healthy AND NOT n.draining AND n.status = $1) > 0) AS has_healthy_nodes,
	MAX(n.last_seen_at) AS last_seen_at,
	g.created_at,
	g.updated_at
FROM fugue_edge_groups AS g
LEFT JOIN fugue_edge_nodes AS n ON n.edge_group_id = g.id`
	args := []any{model.EdgeHealthHealthy, model.EdgeHealthUnhealthy, model.EdgeHealthUnknown}
	if edgeGroupID != "" {
		query += ` WHERE g.id = $4`
		args = append(args, edgeGroupID)
	}
	query += `
GROUP BY g.id, g.region, g.country, g.status, g.created_at, g.updated_at
ORDER BY g.id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list edge groups: %w", err)
	}
	defer rows.Close()
	groups := []model.EdgeGroup{}
	for rows.Next() {
		group, err := scanEdgeGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge groups: %w", err)
	}
	return groups, nil
}

func scanEdgeNode(scanner sqlScanner) (model.EdgeNode, error) {
	var node model.EdgeNode
	var lastSeenAt sql.NullTime
	var lastHeartbeatAt sql.NullTime
	if err := scanner.Scan(
		&node.ID,
		&node.EdgeGroupID,
		&node.Region,
		&node.Country,
		&node.PublicHostname,
		&node.PublicIPv4,
		&node.PublicIPv6,
		&node.MeshIP,
		&node.Status,
		&node.Healthy,
		&node.Draining,
		&node.RouteBundleVersion,
		&node.DNSBundleVersion,
		&node.CaddyRouteCount,
		&node.CaddyAppliedVersion,
		&node.CaddyLastError,
		&node.CacheStatus,
		&node.LastError,
		&node.TokenPrefix,
		&node.TokenHash,
		&lastSeenAt,
		&lastHeartbeatAt,
		&node.CreatedAt,
		&node.UpdatedAt,
	); err != nil {
		return model.EdgeNode{}, err
	}
	if lastSeenAt.Valid {
		node.LastSeenAt = &lastSeenAt.Time
	}
	if lastHeartbeatAt.Valid {
		node.LastHeartbeatAt = &lastHeartbeatAt.Time
	}
	normalizeEdgeNodeForRead(&node)
	return node, nil
}

func scanEdgeGroup(scanner sqlScanner) (model.EdgeGroup, error) {
	var group model.EdgeGroup
	var lastSeenAt sql.NullTime
	if err := scanner.Scan(
		&group.ID,
		&group.Region,
		&group.Country,
		&group.Status,
		&group.NodeCount,
		&group.HealthyNodeCount,
		&group.HasHealthyNodes,
		&lastSeenAt,
		&group.CreatedAt,
		&group.UpdatedAt,
	); err != nil {
		return model.EdgeGroup{}, err
	}
	group.ID = normalizeEdgeGroupID(group.ID)
	group.Region = normalizeEdgeMetadataValue(group.Region)
	group.Country = normalizeEdgeMetadataValue(group.Country)
	group.Status = model.NormalizeEdgeHealthStatus(strings.TrimSpace(group.Status))
	if group.Status == "" {
		group.Status = model.EdgeHealthUnknown
	}
	if lastSeenAt.Valid {
		group.LastSeenAt = &lastSeenAt.Time
	}
	return group, nil
}
