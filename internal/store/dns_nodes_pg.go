package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgListDNSNodes(edgeGroupID string) ([]model.DNSNode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, edge_group_id, public_hostname, public_ipv4, public_ipv6, mesh_ip, zone,
	status, healthy, dns_bundle_version, record_count, cache_status,
	serving_generation, lkg_generation, cache_write_errors, cache_load_errors, bundle_sync_errors,
	query_count, query_error_count, query_rcode_counts_json, query_qtype_counts_json, listen_addr,
	udp_addr, tcp_addr, udp_listen, tcp_listen, last_error, last_seen_at, last_heartbeat_at,
	created_at, updated_at
FROM fugue_dns_nodes`
	args := []any{}
	if edgeGroupID != "" {
		query += ` WHERE edge_group_id = $1`
		args = append(args, edgeGroupID)
	}
	query += ` ORDER BY edge_group_id ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list dns nodes: %w", err)
	}
	defer rows.Close()

	nodes := []model.DNSNode{}
	for rows.Next() {
		node, err := scanDNSNode(rows)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dns nodes: %w", err)
	}
	return nodes, nil
}

func (s *Store) pgGetDNSNode(dnsNodeID string) (model.DNSNode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	node, err := scanDNSNode(s.db.QueryRowContext(ctx, `
SELECT id, edge_group_id, public_hostname, public_ipv4, public_ipv6, mesh_ip, zone,
	status, healthy, dns_bundle_version, record_count, cache_status,
	serving_generation, lkg_generation, cache_write_errors, cache_load_errors, bundle_sync_errors,
	query_count, query_error_count, query_rcode_counts_json, query_qtype_counts_json, listen_addr,
	udp_addr, tcp_addr, udp_listen, tcp_listen, last_error, last_seen_at, last_heartbeat_at,
	created_at, updated_at
FROM fugue_dns_nodes
WHERE id = $1
`, dnsNodeID))
	if err != nil {
		return model.DNSNode{}, mapDBErr(err)
	}
	return node, nil
}

func (s *Store) pgUpdateDNSHeartbeat(node model.DNSNode) (model.DNSNode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	node.LastSeenAt = &now
	node.LastHeartbeatAt = &now
	node.UpdatedAt = now
	if node.CreatedAt.IsZero() {
		node.CreatedAt = now
	}
	return pgUpsertDNSNode(ctx, s.db, node)
}

type dnsNodeDB interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func pgUpsertDNSNode(ctx context.Context, db dnsNodeDB, node model.DNSNode) (model.DNSNode, error) {
	rcodeCountsJSON, err := marshalJSON(node.QueryRCodeCounts)
	if err != nil {
		return model.DNSNode{}, err
	}
	qtypeCountsJSON, err := marshalJSON(node.QueryQTypeCounts)
	if err != nil {
		return model.DNSNode{}, err
	}
	row := db.QueryRowContext(ctx, `
INSERT INTO fugue_dns_nodes (
	id, edge_group_id, public_hostname, public_ipv4, public_ipv6, mesh_ip, zone,
	status, healthy, dns_bundle_version, record_count, cache_status,
	serving_generation, lkg_generation, cache_write_errors, cache_load_errors, bundle_sync_errors,
	query_count, query_error_count, query_rcode_counts_json, query_qtype_counts_json, listen_addr,
	udp_addr, tcp_addr, udp_listen, tcp_listen, last_error, last_seen_at, last_heartbeat_at,
	created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12,
	$13, $14, $15, $16, $17,
	$18, $19, $20, $21, $22,
	$23, $24, $25, $26, $27,
	$28, $29, $30, $31
)
ON CONFLICT (id) DO UPDATE SET
	edge_group_id = EXCLUDED.edge_group_id,
	public_hostname = EXCLUDED.public_hostname,
	public_ipv4 = EXCLUDED.public_ipv4,
	public_ipv6 = EXCLUDED.public_ipv6,
	mesh_ip = EXCLUDED.mesh_ip,
	zone = EXCLUDED.zone,
	status = EXCLUDED.status,
	healthy = EXCLUDED.healthy,
	dns_bundle_version = EXCLUDED.dns_bundle_version,
	serving_generation = EXCLUDED.serving_generation,
	lkg_generation = EXCLUDED.lkg_generation,
	record_count = EXCLUDED.record_count,
	cache_status = EXCLUDED.cache_status,
	cache_write_errors = EXCLUDED.cache_write_errors,
	cache_load_errors = EXCLUDED.cache_load_errors,
	bundle_sync_errors = EXCLUDED.bundle_sync_errors,
	query_count = EXCLUDED.query_count,
	query_error_count = EXCLUDED.query_error_count,
	query_rcode_counts_json = EXCLUDED.query_rcode_counts_json,
	query_qtype_counts_json = EXCLUDED.query_qtype_counts_json,
	listen_addr = EXCLUDED.listen_addr,
	udp_addr = EXCLUDED.udp_addr,
	tcp_addr = EXCLUDED.tcp_addr,
	udp_listen = EXCLUDED.udp_listen,
	tcp_listen = EXCLUDED.tcp_listen,
	last_error = EXCLUDED.last_error,
	last_seen_at = EXCLUDED.last_seen_at,
	last_heartbeat_at = EXCLUDED.last_heartbeat_at,
	updated_at = EXCLUDED.updated_at
RETURNING id, edge_group_id, public_hostname, public_ipv4, public_ipv6, mesh_ip, zone,
	status, healthy, dns_bundle_version, record_count, cache_status,
	serving_generation, lkg_generation, cache_write_errors, cache_load_errors, bundle_sync_errors,
	query_count, query_error_count, query_rcode_counts_json, query_qtype_counts_json, listen_addr,
	udp_addr, tcp_addr, udp_listen, tcp_listen, last_error, last_seen_at, last_heartbeat_at,
	created_at, updated_at
`, node.ID, node.EdgeGroupID, node.PublicHostname, node.PublicIPv4, node.PublicIPv6, node.MeshIP, node.Zone,
		node.Status, node.Healthy, node.DNSBundleVersion, node.RecordCount, node.CacheStatus,
		node.ServingGeneration, node.LKGGeneration,
		int64(node.CacheWriteErrors), int64(node.CacheLoadErrors), int64(node.BundleSyncErrors), int64(node.QueryCount), int64(node.QueryErrorCount),
		rcodeCountsJSON, qtypeCountsJSON, node.ListenAddr, node.UDPAddr, node.TCPAddr,
		node.UDPListen, node.TCPListen, node.LastError, node.LastSeenAt, node.LastHeartbeatAt, node.CreatedAt, node.UpdatedAt)
	stored, err := scanDNSNode(row)
	if err != nil {
		return model.DNSNode{}, mapDBErr(err)
	}
	return stored, nil
}

func scanDNSNode(scanner sqlScanner) (model.DNSNode, error) {
	var node model.DNSNode
	var cacheWriteErrors int64
	var cacheLoadErrors int64
	var bundleSyncErrors int64
	var queryCount int64
	var queryErrorCount int64
	var rcodeCountsRaw []byte
	var qtypeCountsRaw []byte
	var lastSeenAt sql.NullTime
	var lastHeartbeatAt sql.NullTime
	if err := scanner.Scan(
		&node.ID,
		&node.EdgeGroupID,
		&node.PublicHostname,
		&node.PublicIPv4,
		&node.PublicIPv6,
		&node.MeshIP,
		&node.Zone,
		&node.Status,
		&node.Healthy,
		&node.DNSBundleVersion,
		&node.RecordCount,
		&node.CacheStatus,
		&node.ServingGeneration,
		&node.LKGGeneration,
		&cacheWriteErrors,
		&cacheLoadErrors,
		&bundleSyncErrors,
		&queryCount,
		&queryErrorCount,
		&rcodeCountsRaw,
		&qtypeCountsRaw,
		&node.ListenAddr,
		&node.UDPAddr,
		&node.TCPAddr,
		&node.UDPListen,
		&node.TCPListen,
		&node.LastError,
		&lastSeenAt,
		&lastHeartbeatAt,
		&node.CreatedAt,
		&node.UpdatedAt,
	); err != nil {
		return model.DNSNode{}, err
	}
	node.CacheWriteErrors = uint64FromDB(cacheWriteErrors)
	node.CacheLoadErrors = uint64FromDB(cacheLoadErrors)
	node.BundleSyncErrors = uint64FromDB(bundleSyncErrors)
	node.QueryCount = uint64FromDB(queryCount)
	node.QueryErrorCount = uint64FromDB(queryErrorCount)
	rcodeCounts, err := decodeJSONValue[map[string]uint64](rcodeCountsRaw)
	if err != nil {
		return model.DNSNode{}, err
	}
	qtypeCounts, err := decodeJSONValue[map[string]uint64](qtypeCountsRaw)
	if err != nil {
		return model.DNSNode{}, err
	}
	node.QueryRCodeCounts = rcodeCounts
	node.QueryQTypeCounts = qtypeCounts
	if lastSeenAt.Valid {
		node.LastSeenAt = &lastSeenAt.Time
	}
	if lastHeartbeatAt.Valid {
		node.LastHeartbeatAt = &lastHeartbeatAt.Time
	}
	normalizeDNSNodeForRead(&node)
	return node, nil
}

func uint64FromDB(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
