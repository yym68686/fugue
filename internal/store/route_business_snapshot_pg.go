package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *Store) pgCaptureRouteBusinessSnapshot(parent context.Context) (RouteBusinessSnapshot, error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return RouteBusinessSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := readRouteBusinessSnapshotTx(ctx, tx)
	if err != nil {
		return RouteBusinessSnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		return RouteBusinessSnapshot{}, err
	}
	snapshot.normalize()
	return snapshot, nil
}

func readRouteBusinessSnapshotTx(ctx context.Context, tx *sql.Tx) (RouteBusinessSnapshot, error) {
	var out RouteBusinessSnapshot
	// This first read fixes the MVCC snapshot and policy evaluation time.
	if err := tx.QueryRowContext(ctx, `SELECT transaction_timestamp(), pg_current_snapshot()::text`).Scan(&out.CapturedAt, &out.Revision); err != nil {
		return out, err
	}
	out.Revision = "postgres:" + out.Revision
	var err error
	out.Apps, err = readRouteBusinessRows(ctx, tx, `SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at FROM fugue_apps WHERE `+appVisiblePhasePredicate, scanApp)
	if err != nil {
		return out, fmt.Errorf("capture route apps: %w", err)
	}
	out.Domains, err = readRouteBusinessRows(ctx, tx, `SELECT hostname, tenant_id, app_id, status, dns_mode, dns_zone_id, dns_record_id, dns_record_source, dns_status, dns_record_kind, tls_status, verification_txt_name, verification_txt_value, route_target, last_message, dns_last_message, tls_last_message, last_checked_at, dns_last_checked_at, verified_at, tls_last_checked_at, tls_ready_at, created_at, updated_at FROM fugue_app_domains WHERE status='verified'`, scanAppDomain)
	if err != nil {
		return out, fmt.Errorf("capture route domains: %w", err)
	}
	out.RouteTables, err = readRouteBusinessRows(ctx, tx, `SELECT project_id, tenant_id, domains_json, entrypoints_json, created_at, updated_at FROM fugue_project_route_tables`, scanProjectRouteTable)
	if err != nil {
		return out, fmt.Errorf("capture project route tables: %w", err)
	}
	out.Runtimes, err = readRouteBusinessRows(ctx, tx, `SELECT id, tenant_id, name, machine_name, type, access_mode, public_offer_json, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at FROM fugue_runtimes`, scanRuntime)
	if err != nil {
		return out, fmt.Errorf("capture route runtimes: %w", err)
	}
	out.RoutePolicies, err = readRouteBusinessRows(ctx, tx, `SELECT `+edgeRoutePolicySelectColumns+` FROM fugue_edge_route_policies`, scanEdgeRoutePolicy)
	if err != nil {
		return out, fmt.Errorf("capture route policies: %w", err)
	}
	out.Releases, err = readRouteBusinessRows(ctx, tx, `SELECT `+appReleaseSelectColumns+` FROM fugue_app_releases`, scanAppRelease)
	if err != nil {
		return out, fmt.Errorf("capture route releases: %w", err)
	}
	out.TrafficPolicies, err = readRouteBusinessRows(ctx, tx, `SELECT `+appTrafficPolicySelectColumns+` FROM fugue_app_traffic_policies`, scanAppTrafficPolicy)
	if err != nil {
		return out, fmt.Errorf("capture traffic policies: %w", err)
	}
	out.HostedZones, err = readRouteBusinessRows(ctx, tx, hostedZoneSelect(), scanHostedZone)
	if err != nil {
		return out, fmt.Errorf("capture hosted dns zones: %w", err)
	}
	out.DNSRecords, err = readRouteBusinessRows(ctx, tx, dnsRecordSelect()+` WHERE status NOT IN ('disabled', 'conflict')`, scanDNSRecord)
	if err != nil {
		return out, fmt.Errorf("capture hosted dns records: %w", err)
	}
	out.ACMEChallenges, err = readRouteBusinessRows(ctx, tx, `SELECT id, zone, name, value, ttl, owner, created_by, expires_at, created_at, updated_at FROM fugue_dns_acme_challenges`, scanDNSACMEChallenge)
	if err != nil {
		return out, fmt.Errorf("capture ACME challenges: %w", err)
	}
	return out, nil
}

func readRouteBusinessRows[T any](ctx context.Context, tx *sql.Tx, query string, scan func(sqlScanner) (T, error)) ([]T, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]T, 0)
	for rows.Next() {
		value, err := scan(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}
