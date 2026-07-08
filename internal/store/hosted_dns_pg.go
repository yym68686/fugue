package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgListHostedZones(tenantID string, platformAdmin bool) ([]model.HostedZone, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := hostedZoneSelect() + ` WHERE status <> $1`
	args := []any{model.HostedZoneStatusDeleted}
	if !platformAdmin && strings.TrimSpace(tenantID) != "" {
		args = append(args, strings.TrimSpace(tenantID))
		query += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	query += ` ORDER BY zone_name ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list hosted dns zones: %w", err)
	}
	defer rows.Close()
	zones := []model.HostedZone{}
	for rows.Next() {
		zone, err := scanHostedZone(rows)
		if err != nil {
			return nil, err
		}
		zones = append(zones, zone)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hosted dns zones: %w", err)
	}
	return zones, nil
}

func (s *Store) pgGetHostedZoneByName(zoneName string) (model.HostedZone, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	zone, err := scanHostedZone(s.db.QueryRowContext(ctx, hostedZoneSelect()+`
WHERE lower(zone_name) = lower($1)
  AND status <> $2
`, zoneName, model.HostedZoneStatusDeleted))
	if err != nil {
		return model.HostedZone{}, mapDBErr(err)
	}
	return zone, nil
}

func (s *Store) pgPutHostedZone(zone model.HostedZone) (model.HostedZone, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	parentNSJSON, err := marshalJSON(zone.ParentNameservers)
	if err != nil {
		return model.HostedZone{}, err
	}
	expectedNSJSON, err := marshalJSON(zone.ExpectedNameservers)
	if err != nil {
		return model.HostedZone{}, err
	}
	now := time.Now().UTC()
	if zone.CreatedAt.IsZero() {
		zone.CreatedAt = now
	}
	zone.UpdatedAt = now
	row := s.db.QueryRowContext(ctx, `
INSERT INTO fugue_dns_zones (
	id, tenant_id, project_id, zone_name, status, delegation_status,
	parent_nameservers_json, expected_nameservers_json, created_by,
	last_checked_at, last_message, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9,
	$10, $11, $12, $13
)
ON CONFLICT (id) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id,
	project_id = EXCLUDED.project_id,
	zone_name = EXCLUDED.zone_name,
	status = EXCLUDED.status,
	delegation_status = EXCLUDED.delegation_status,
	parent_nameservers_json = EXCLUDED.parent_nameservers_json,
	expected_nameservers_json = EXCLUDED.expected_nameservers_json,
	last_checked_at = EXCLUDED.last_checked_at,
	last_message = EXCLUDED.last_message,
	updated_at = EXCLUDED.updated_at
RETURNING id, tenant_id, project_id, zone_name, status, delegation_status,
	parent_nameservers_json, expected_nameservers_json, created_by,
	last_checked_at, last_message, created_at, updated_at
`, zone.ID, zone.TenantID, zone.ProjectID, zone.ZoneName, zone.Status, zone.DelegationStatus,
		parentNSJSON, expectedNSJSON, zone.CreatedBy, zone.LastCheckedAt, zone.LastMessage, zone.CreatedAt, zone.UpdatedAt)
	stored, err := scanHostedZone(row)
	if err != nil {
		return model.HostedZone{}, mapDBErr(err)
	}
	return stored, nil
}

func (s *Store) pgDeleteHostedZone(zoneName string) (model.HostedZone, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.HostedZone{}, fmt.Errorf("begin delete hosted dns zone transaction: %w", err)
	}
	defer tx.Rollback()
	zone, err := scanHostedZone(tx.QueryRowContext(ctx, hostedZoneSelect()+`
WHERE lower(zone_name) = lower($1)
  AND status <> $2
FOR UPDATE
`, zoneName, model.HostedZoneStatusDeleted))
	if err != nil {
		return model.HostedZone{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE fugue_dns_zones SET status = $2, updated_at = $3 WHERE id = $1`, zone.ID, model.HostedZoneStatusDeleted, time.Now().UTC()); err != nil {
		return model.HostedZone{}, fmt.Errorf("mark hosted dns zone deleted: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.HostedZone{}, fmt.Errorf("commit delete hosted dns zone transaction: %w", err)
	}
	return zone, nil
}

func (s *Store) pgListDNSRecords(zoneID string) ([]model.DNSRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, dnsRecordSelect()+`
WHERE zone_id = $1
  AND status <> $2
ORDER BY fqdn ASC, type ASC, id ASC
`, zoneID, model.DNSRecordStatusDisabled)
	if err != nil {
		return nil, fmt.Errorf("list hosted dns records: %w", err)
	}
	defer rows.Close()
	records := []model.DNSRecord{}
	for rows.Next() {
		record, err := scanDNSRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hosted dns records: %w", err)
	}
	return records, nil
}

func (s *Store) pgGetDNSRecord(zoneID, recordID string) (model.DNSRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	record, err := scanDNSRecord(s.db.QueryRowContext(ctx, dnsRecordSelect()+`
WHERE zone_id = $1
  AND id = $2
`, zoneID, recordID))
	if err != nil {
		return model.DNSRecord{}, mapDBErr(err)
	}
	return record, nil
}

func (s *Store) pgPutDNSRecord(zone model.HostedZone, record model.DNSRecord, overwrite bool) (model.DNSRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.DNSRecord{}, fmt.Errorf("begin put hosted dns record transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := scanHostedZone(tx.QueryRowContext(ctx, hostedZoneSelect()+`
WHERE id = $1
  AND status <> $2
FOR UPDATE
`, zone.ID, model.HostedZoneStatusDeleted)); err != nil {
		return model.DNSRecord{}, mapDBErr(err)
	}
	existingRecords, err := pgListDNSRecordsTx(ctx, tx, zone.ID)
	if err != nil {
		return model.DNSRecord{}, err
	}
	if err := validateDNSRecordConflicts(record, existingRecords, overwrite); err != nil {
		return model.DNSRecord{}, err
	}
	stored, err := pgUpsertDNSRecordTx(ctx, tx, record)
	if err != nil {
		return model.DNSRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.DNSRecord{}, fmt.Errorf("commit put hosted dns record transaction: %w", err)
	}
	return stored, nil
}

func (s *Store) pgDeleteDNSRecord(zoneID, recordID string) (model.DNSRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	record, err := scanDNSRecord(s.db.QueryRowContext(ctx, dnsRecordSelect()+`
WHERE zone_id = $1
  AND id = $2
`, zoneID, recordID))
	if err != nil {
		return model.DNSRecord{}, mapDBErr(err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM fugue_dns_records WHERE zone_id = $1 AND id = $2`, zoneID, recordID); err != nil {
		return model.DNSRecord{}, fmt.Errorf("delete hosted dns record: %w", err)
	}
	return record, nil
}

func (s *Store) pgDeleteDNSRecordsBySourceRef(zoneID, source, sourceRefType, sourceRefID string) ([]model.DNSRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, dnsRecordSelect()+`
WHERE zone_id = $1
  AND source = $2
  AND source_ref_type = $3
  AND source_ref_id = $4
`, zoneID, source, sourceRefType, sourceRefID)
	if err != nil {
		return nil, fmt.Errorf("list hosted dns records by source ref: %w", err)
	}
	records := []model.DNSRecord{}
	for rows.Next() {
		record, err := scanDNSRecord(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate hosted dns records by source ref: %w", err)
	}
	rows.Close()
	if _, err := s.db.ExecContext(ctx, `
DELETE FROM fugue_dns_records
WHERE zone_id = $1
  AND source = $2
  AND source_ref_type = $3
  AND source_ref_id = $4
`, zoneID, source, sourceRefType, sourceRefID); err != nil {
		return nil, fmt.Errorf("delete hosted dns records by source ref: %w", err)
	}
	return records, nil
}

func pgListDNSRecordsTx(ctx context.Context, tx *sql.Tx, zoneID string) ([]model.DNSRecord, error) {
	rows, err := tx.QueryContext(ctx, dnsRecordSelect()+`
WHERE zone_id = $1
  AND status <> $2
FOR UPDATE
`, zoneID, model.DNSRecordStatusDisabled)
	if err != nil {
		return nil, fmt.Errorf("list hosted dns records for update: %w", err)
	}
	defer rows.Close()
	records := []model.DNSRecord{}
	for rows.Next() {
		record, err := scanDNSRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hosted dns records for update: %w", err)
	}
	return records, nil
}

func pgUpsertDNSRecordTx(ctx context.Context, tx *sql.Tx, record model.DNSRecord) (model.DNSRecord, error) {
	valuesJSON, err := marshalJSON(record.Values)
	if err != nil {
		return model.DNSRecord{}, err
	}
	flattenedAJSON, err := marshalJSON(record.FlattenedA)
	if err != nil {
		return model.DNSRecord{}, err
	}
	flattenedAAAAJSON, err := marshalJSON(record.FlattenedAAAA)
	if err != nil {
		return model.DNSRecord{}, err
	}
	now := time.Now().UTC()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	row := tx.QueryRowContext(ctx, `
INSERT INTO fugue_dns_records (
	id, zone_id, tenant_id, name, fqdn, type, values_json, ttl,
	flatten_mode, flatten_target, flatten_ipv4_policy, flatten_ipv6_policy,
	flatten_ttl_policy, flatten_fallback_policy, flatten_status,
	flattened_a_json, flattened_aaaa_json, last_resolved_at, resolve_error,
	source, source_ref_type, source_ref_id, status, created_by,
	last_published_at, last_message, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8,
	$9, $10, $11, $12,
	$13, $14, $15,
	$16, $17, $18, $19,
	$20, $21, $22, $23, $24,
	$25, $26, $27, $28
)
ON CONFLICT (id) DO UPDATE SET
	name = EXCLUDED.name,
	fqdn = EXCLUDED.fqdn,
	type = EXCLUDED.type,
	values_json = EXCLUDED.values_json,
	ttl = EXCLUDED.ttl,
	flatten_mode = EXCLUDED.flatten_mode,
	flatten_target = EXCLUDED.flatten_target,
	flatten_ipv4_policy = EXCLUDED.flatten_ipv4_policy,
	flatten_ipv6_policy = EXCLUDED.flatten_ipv6_policy,
	flatten_ttl_policy = EXCLUDED.flatten_ttl_policy,
	flatten_fallback_policy = EXCLUDED.flatten_fallback_policy,
	flatten_status = EXCLUDED.flatten_status,
	flattened_a_json = EXCLUDED.flattened_a_json,
	flattened_aaaa_json = EXCLUDED.flattened_aaaa_json,
	last_resolved_at = EXCLUDED.last_resolved_at,
	resolve_error = EXCLUDED.resolve_error,
	source = EXCLUDED.source,
	source_ref_type = EXCLUDED.source_ref_type,
	source_ref_id = EXCLUDED.source_ref_id,
	status = EXCLUDED.status,
	last_published_at = EXCLUDED.last_published_at,
	last_message = EXCLUDED.last_message,
	updated_at = EXCLUDED.updated_at
RETURNING id, zone_id, tenant_id, name, fqdn, type, values_json, ttl,
	flatten_mode, flatten_target, flatten_ipv4_policy, flatten_ipv6_policy,
	flatten_ttl_policy, flatten_fallback_policy, flatten_status,
	flattened_a_json, flattened_aaaa_json, last_resolved_at, resolve_error,
	source, source_ref_type, source_ref_id, status, created_by,
	last_published_at, last_message, created_at, updated_at
`, record.ID, record.ZoneID, record.TenantID, record.Name, record.FQDN, record.Type, valuesJSON, record.TTL,
		record.FlattenMode, record.FlattenTarget, record.FlattenIPv4Policy, record.FlattenIPv6Policy,
		record.FlattenTTLPolicy, record.FlattenFallbackPolicy, record.FlattenStatus,
		flattenedAJSON, flattenedAAAAJSON, record.LastResolvedAt, record.ResolveError,
		record.Source, record.SourceRefType, record.SourceRefID, record.Status, record.CreatedBy,
		record.LastPublishedAt, record.LastMessage, record.CreatedAt, record.UpdatedAt)
	stored, err := scanDNSRecord(row)
	if err != nil {
		return model.DNSRecord{}, mapDBErr(err)
	}
	return stored, nil
}

func hostedZoneSelect() string {
	return `SELECT id, tenant_id, project_id, zone_name, status, delegation_status,
	parent_nameservers_json, expected_nameservers_json, created_by,
	last_checked_at, last_message, created_at, updated_at
FROM fugue_dns_zones `
}

func dnsRecordSelect() string {
	return `SELECT id, zone_id, tenant_id, name, fqdn, type, values_json, ttl,
	flatten_mode, flatten_target, flatten_ipv4_policy, flatten_ipv6_policy,
	flatten_ttl_policy, flatten_fallback_policy, flatten_status,
	flattened_a_json, flattened_aaaa_json, last_resolved_at, resolve_error,
	source, source_ref_type, source_ref_id, status, created_by,
	last_published_at, last_message, created_at, updated_at
FROM fugue_dns_records `
}

func scanHostedZone(scanner sqlScanner) (model.HostedZone, error) {
	var zone model.HostedZone
	var parentNSJSON []byte
	var expectedNSJSON []byte
	var lastCheckedAt sql.NullTime
	if err := scanner.Scan(
		&zone.ID,
		&zone.TenantID,
		&zone.ProjectID,
		&zone.ZoneName,
		&zone.Status,
		&zone.DelegationStatus,
		&parentNSJSON,
		&expectedNSJSON,
		&zone.CreatedBy,
		&lastCheckedAt,
		&zone.LastMessage,
		&zone.CreatedAt,
		&zone.UpdatedAt,
	); err != nil {
		return model.HostedZone{}, err
	}
	parentNS, err := decodeJSONValue[[]string](parentNSJSON)
	if err != nil {
		return model.HostedZone{}, err
	}
	expectedNS, err := decodeJSONValue[[]string](expectedNSJSON)
	if err != nil {
		return model.HostedZone{}, err
	}
	zone.ParentNameservers = parentNS
	zone.ExpectedNameservers = expectedNS
	if lastCheckedAt.Valid {
		value := lastCheckedAt.Time.UTC()
		zone.LastCheckedAt = &value
	}
	return normalizeHostedZoneForRead(zone), nil
}

func scanDNSRecord(scanner sqlScanner) (model.DNSRecord, error) {
	var record model.DNSRecord
	var valuesJSON []byte
	var flattenedAJSON []byte
	var flattenedAAAAJSON []byte
	var lastResolvedAt sql.NullTime
	var lastPublishedAt sql.NullTime
	if err := scanner.Scan(
		&record.ID,
		&record.ZoneID,
		&record.TenantID,
		&record.Name,
		&record.FQDN,
		&record.Type,
		&valuesJSON,
		&record.TTL,
		&record.FlattenMode,
		&record.FlattenTarget,
		&record.FlattenIPv4Policy,
		&record.FlattenIPv6Policy,
		&record.FlattenTTLPolicy,
		&record.FlattenFallbackPolicy,
		&record.FlattenStatus,
		&flattenedAJSON,
		&flattenedAAAAJSON,
		&lastResolvedAt,
		&record.ResolveError,
		&record.Source,
		&record.SourceRefType,
		&record.SourceRefID,
		&record.Status,
		&record.CreatedBy,
		&lastPublishedAt,
		&record.LastMessage,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return model.DNSRecord{}, err
	}
	values, err := decodeJSONValue[[]string](valuesJSON)
	if err != nil {
		return model.DNSRecord{}, err
	}
	flattenedA, err := decodeJSONValue[[]string](flattenedAJSON)
	if err != nil {
		return model.DNSRecord{}, err
	}
	flattenedAAAA, err := decodeJSONValue[[]string](flattenedAAAAJSON)
	if err != nil {
		return model.DNSRecord{}, err
	}
	record.Values = values
	record.FlattenedA = flattenedA
	record.FlattenedAAAA = flattenedAAAA
	if lastResolvedAt.Valid {
		value := lastResolvedAt.Time.UTC()
		record.LastResolvedAt = &value
	}
	if lastPublishedAt.Valid {
		value := lastPublishedAt.Time.UTC()
		record.LastPublishedAt = &value
	}
	return normalizeDNSRecordForRead(record), nil
}
