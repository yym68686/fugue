package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgListAppDomains(appID string) ([]model.AppDomain, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT hostname, tenant_id, app_id, status, dns_mode, dns_zone_id, dns_record_id, dns_record_source, dns_status, dns_record_kind, tls_status, verification_txt_name, verification_txt_value, route_target, last_message, dns_last_message, tls_last_message, last_checked_at, dns_last_checked_at, verified_at, tls_last_checked_at, tls_ready_at, created_at, updated_at
FROM fugue_app_domains
WHERE app_id = $1
ORDER BY created_at ASC, hostname ASC
`, appID)
	if err != nil {
		return nil, fmt.Errorf("list app domains: %w", err)
	}
	defer rows.Close()

	domains := make([]model.AppDomain, 0)
	for rows.Next() {
		domain, err := scanAppDomain(rows)
		if err != nil {
			return nil, err
		}
		domains = append(domains, domain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate app domains: %w", err)
	}
	return domains, nil
}

func (s *Store) pgListVerifiedAppDomains() ([]model.AppDomain, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT hostname, tenant_id, app_id, status, dns_mode, dns_zone_id, dns_record_id, dns_record_source, dns_status, dns_record_kind, tls_status, verification_txt_name, verification_txt_value, route_target, last_message, dns_last_message, tls_last_message, last_checked_at, dns_last_checked_at, verified_at, tls_last_checked_at, tls_ready_at, created_at, updated_at
FROM fugue_app_domains
WHERE status = $1
ORDER BY hostname ASC
`, model.AppDomainStatusVerified)
	if err != nil {
		return nil, fmt.Errorf("list verified app domains: %w", err)
	}
	defer rows.Close()

	domains := make([]model.AppDomain, 0)
	for rows.Next() {
		domain, err := scanAppDomain(rows)
		if err != nil {
			return nil, err
		}
		domains = append(domains, domain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate verified app domains: %w", err)
	}
	return domains, nil
}

func (s *Store) pgGetAppDomain(hostname string) (model.AppDomain, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	domain, err := scanAppDomain(s.db.QueryRowContext(ctx, `
SELECT hostname, tenant_id, app_id, status, dns_mode, dns_zone_id, dns_record_id, dns_record_source, dns_status, dns_record_kind, tls_status, verification_txt_name, verification_txt_value, route_target, last_message, dns_last_message, tls_last_message, last_checked_at, dns_last_checked_at, verified_at, tls_last_checked_at, tls_ready_at, created_at, updated_at
FROM fugue_app_domains
WHERE lower(hostname) = lower($1)
`, hostname))
	if err != nil {
		return model.AppDomain{}, mapDBErr(err)
	}
	return domain, nil
}

func (s *Store) pgPutAppDomain(domain model.AppDomain) (model.AppDomain, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppDomain{}, fmt.Errorf("begin put app domain transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, domain.AppID, true)
	if err != nil {
		return model.AppDomain{}, mapDBErr(err)
	}
	if isDeletedApp(app) {
		return model.AppDomain{}, ErrNotFound
	}
	if domain.TenantID == "" {
		domain.TenantID = app.TenantID
	}
	if domain.TenantID != app.TenantID {
		return model.AppDomain{}, ErrInvalidInput
	}

	var routeOwnerID string
	err = tx.QueryRowContext(ctx, `
SELECT id
FROM fugue_apps
WHERE lower(route_json->>'hostname') = lower($1)
  AND lower(COALESCE(NULLIF(route_json->>'path_prefix', ''), '/')) = '/'
  AND id <> $2
LIMIT 1
	`, domain.Hostname, domain.AppID).Scan(&routeOwnerID)
	switch {
	case err == nil:
		return model.AppDomain{}, ErrConflict
	case err != nil && err != sql.ErrNoRows:
		return model.AppDomain{}, fmt.Errorf("check app route hostname conflict: %w", err)
	}

	existing, err := scanAppDomain(tx.QueryRowContext(ctx, `
SELECT hostname, tenant_id, app_id, status, dns_mode, dns_zone_id, dns_record_id, dns_record_source, dns_status, dns_record_kind, tls_status, verification_txt_name, verification_txt_value, route_target, last_message, dns_last_message, tls_last_message, last_checked_at, dns_last_checked_at, verified_at, tls_last_checked_at, tls_ready_at, created_at, updated_at
FROM fugue_app_domains
WHERE lower(hostname) = lower($1)
FOR UPDATE
`, domain.Hostname))
	existingFound := err == nil
	if err != nil && err != sql.ErrNoRows {
		return model.AppDomain{}, mapDBErr(err)
	}
	if existingFound && existing.AppID != domain.AppID {
		return model.AppDomain{}, ErrConflict
	}

	now := time.Now().UTC()
	if existingFound {
		if domain.CreatedAt.IsZero() {
			domain.CreatedAt = existing.CreatedAt
		}
	} else if domain.CreatedAt.IsZero() {
		domain.CreatedAt = now
	}
	domain.UpdatedAt = now
	if domain.Status == model.AppDomainStatusVerified && domain.VerifiedAt == nil {
		verifiedAt := now
		domain.VerifiedAt = &verifiedAt
	}
	if domain.Status == model.AppDomainStatusVerified {
		if domain.DNSStatus == "" {
			domain.DNSStatus = model.AppDomainDNSStatusReady
		}
		if domain.DNSRecordKind == "" {
			domain.DNSRecordKind = model.AppDomainDNSRecordKindCNAME
		}
		if domain.TLSStatus == "" {
			domain.TLSStatus = model.AppDomainTLSStatusPending
		}
		if domain.TLSStatus == model.AppDomainTLSStatusReady {
			if domain.TLSReadyAt == nil {
				readyAt := now
				domain.TLSReadyAt = &readyAt
			}
		} else {
			domain.TLSReadyAt = nil
		}
	} else {
		if domain.DNSStatus == "" {
			domain.DNSStatus = model.AppDomainDNSStatusPending
		}
		if domain.DNSRecordKind == "" {
			domain.DNSRecordKind = model.AppDomainDNSRecordKindNone
		}
		domain.TLSStatus = ""
		domain.TLSLastMessage = ""
		domain.TLSLastCheckedAt = nil
		domain.TLSReadyAt = nil
	}

	if existingFound {
		if _, err := tx.ExecContext(ctx, `
	UPDATE fugue_app_domains
	SET tenant_id = $2,
		app_id = $3,
		status = $4,
		dns_mode = $5,
		dns_zone_id = $6,
		dns_record_id = $7,
		dns_record_source = $8,
		dns_status = $9,
		dns_record_kind = $10,
		tls_status = $11,
		verification_txt_name = $12,
		verification_txt_value = $13,
		route_target = $14,
		last_message = $15,
		dns_last_message = $16,
		tls_last_message = $17,
		last_checked_at = $18,
		dns_last_checked_at = $19,
		verified_at = $20,
		tls_last_checked_at = $21,
		tls_ready_at = $22,
		created_at = $23,
		updated_at = $24
	WHERE lower(hostname) = lower($1)
	`, domain.Hostname, domain.TenantID, domain.AppID, domain.Status, domain.DNSMode, domain.DNSZoneID, domain.DNSRecordID, domain.DNSRecordSource, domain.DNSStatus, domain.DNSRecordKind, domain.TLSStatus, domain.VerificationTXTName, domain.VerificationTXTValue, domain.RouteTarget, domain.LastMessage, domain.DNSLastMessage, domain.TLSLastMessage, domain.LastCheckedAt, domain.DNSLastCheckedAt, domain.VerifiedAt, domain.TLSLastCheckedAt, domain.TLSReadyAt, domain.CreatedAt, domain.UpdatedAt); err != nil {
			return model.AppDomain{}, mapDBErr(err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
	INSERT INTO fugue_app_domains (hostname, tenant_id, app_id, status, dns_mode, dns_zone_id, dns_record_id, dns_record_source, dns_status, dns_record_kind, tls_status, verification_txt_name, verification_txt_value, route_target, last_message, dns_last_message, tls_last_message, last_checked_at, dns_last_checked_at, verified_at, tls_last_checked_at, tls_ready_at, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
	`, domain.Hostname, domain.TenantID, domain.AppID, domain.Status, domain.DNSMode, domain.DNSZoneID, domain.DNSRecordID, domain.DNSRecordSource, domain.DNSStatus, domain.DNSRecordKind, domain.TLSStatus, domain.VerificationTXTName, domain.VerificationTXTValue, domain.RouteTarget, domain.LastMessage, domain.DNSLastMessage, domain.TLSLastMessage, domain.LastCheckedAt, domain.DNSLastCheckedAt, domain.VerifiedAt, domain.TLSLastCheckedAt, domain.TLSReadyAt, domain.CreatedAt, domain.UpdatedAt); err != nil {
			return model.AppDomain{}, mapDBErr(err)
		}
	}

	if err := tx.Commit(); err != nil {
		return model.AppDomain{}, fmt.Errorf("commit put app domain transaction: %w", err)
	}
	return cloneAppDomain(domain), nil
}

func (s *Store) pgDeleteAppDomain(appID, hostname string) (model.AppDomain, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppDomain{}, fmt.Errorf("begin delete app domain transaction: %w", err)
	}
	defer tx.Rollback()

	domain, err := scanAppDomain(tx.QueryRowContext(ctx, `
SELECT hostname, tenant_id, app_id, status, dns_mode, dns_zone_id, dns_record_id, dns_record_source, dns_status, dns_record_kind, tls_status, verification_txt_name, verification_txt_value, route_target, last_message, dns_last_message, tls_last_message, last_checked_at, dns_last_checked_at, verified_at, tls_last_checked_at, tls_ready_at, created_at, updated_at
FROM fugue_app_domains
WHERE lower(hostname) = lower($1)
FOR UPDATE
`, hostname))
	if err != nil {
		return model.AppDomain{}, mapDBErr(err)
	}
	if domain.AppID != appID {
		return model.AppDomain{}, ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_edge_tls_certificates WHERE lower(hostname) = lower($1)`, hostname); err != nil {
		return model.AppDomain{}, fmt.Errorf("delete edge TLS certificate for app domain %s: %w", hostname, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_app_domains WHERE lower(hostname) = lower($1)`, hostname); err != nil {
		return model.AppDomain{}, fmt.Errorf("delete app domain %s: %w", hostname, err)
	}
	if err := tx.Commit(); err != nil {
		return model.AppDomain{}, fmt.Errorf("commit delete app domain transaction: %w", err)
	}
	return domain, nil
}

func (s *Store) pgDeleteAppDomainsByAppTx(ctx context.Context, tx *sql.Tx, appID string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_edge_tls_certificates WHERE app_id = $1 OR hostname IN (SELECT hostname FROM fugue_app_domains WHERE app_id = $1)`, appID); err != nil {
		return fmt.Errorf("delete edge TLS certificates for app %s: %w", appID, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_app_domains WHERE app_id = $1`, appID); err != nil {
		return fmt.Errorf("delete app domains for %s: %w", appID, err)
	}
	return nil
}

func (s *Store) pgGetVerifiedAppByCustomDomainHostname(ctx context.Context, hostname string) (model.App, error) {
	app, err := scanApp(s.db.QueryRowContext(ctx, `
SELECT a.id, a.tenant_id, a.project_id, a.name, a.description, a.source_json, a.route_json, a.spec_json, a.status_json, a.created_at, a.updated_at
FROM fugue_apps AS a
JOIN fugue_app_domains AS d ON d.app_id = a.id
WHERE lower(d.hostname) = lower($1)
  AND d.status = $2
LIMIT 1
`, hostname, model.AppDomainStatusVerified))
	if err != nil {
		return model.App{}, mapDBErr(err)
	}
	return app, nil
}

func scanAppDomain(scanner sqlScanner) (model.AppDomain, error) {
	var domain model.AppDomain
	var dnsMode sql.NullString
	var dnsZoneID sql.NullString
	var dnsRecordID sql.NullString
	var dnsRecordSource sql.NullString
	var dnsStatus sql.NullString
	var dnsRecordKind sql.NullString
	var tlsStatus sql.NullString
	var dnsLastMessage sql.NullString
	var lastCheckedAt sql.NullTime
	var dnsLastCheckedAt sql.NullTime
	var verifiedAt sql.NullTime
	var tlsLastCheckedAt sql.NullTime
	var tlsReadyAt sql.NullTime
	if err := scanner.Scan(
		&domain.Hostname,
		&domain.TenantID,
		&domain.AppID,
		&domain.Status,
		&dnsMode,
		&dnsZoneID,
		&dnsRecordID,
		&dnsRecordSource,
		&dnsStatus,
		&dnsRecordKind,
		&tlsStatus,
		&domain.VerificationTXTName,
		&domain.VerificationTXTValue,
		&domain.RouteTarget,
		&domain.LastMessage,
		&dnsLastMessage,
		&domain.TLSLastMessage,
		&lastCheckedAt,
		&dnsLastCheckedAt,
		&verifiedAt,
		&tlsLastCheckedAt,
		&tlsReadyAt,
		&domain.CreatedAt,
		&domain.UpdatedAt,
	); err != nil {
		return model.AppDomain{}, err
	}
	domain.Hostname = normalizeAppDomainHostname(domain.Hostname)
	domain.RouteTarget = normalizeAppDomainHostname(domain.RouteTarget)
	domain.VerificationTXTName = normalizeTXTRecordName(domain.VerificationTXTName)
	domain.VerificationTXTValue = strings.TrimSpace(domain.VerificationTXTValue)
	domain.Status = normalizeAppDomainStatus(domain.Status)
	domain.DNSMode = model.NormalizeAppDomainDNSMode(dnsMode.String)
	domain.DNSZoneID = strings.TrimSpace(dnsZoneID.String)
	domain.DNSRecordID = strings.TrimSpace(dnsRecordID.String)
	domain.DNSRecordSource = strings.TrimSpace(dnsRecordSource.String)
	domain.DNSStatus = model.NormalizeAppDomainDNSStatus(dnsStatus.String)
	domain.DNSRecordKind = model.NormalizeAppDomainDNSRecordKind(dnsRecordKind.String)
	domain.TLSStatus = model.NormalizeAppDomainTLSStatus(tlsStatus.String)
	domain.LastMessage = strings.TrimSpace(domain.LastMessage)
	domain.DNSLastMessage = strings.TrimSpace(dnsLastMessage.String)
	domain.TLSLastMessage = strings.TrimSpace(domain.TLSLastMessage)
	if domain.DNSStatus == "" {
		if domain.Status == model.AppDomainStatusVerified {
			domain.DNSStatus = model.AppDomainDNSStatusReady
		} else {
			domain.DNSStatus = model.AppDomainDNSStatusPending
		}
	}
	if domain.DNSMode == "" {
		domain.DNSMode = model.AppDomainDNSModeExternal
	}
	if domain.DNSRecordKind == "" {
		if domain.Status == model.AppDomainStatusVerified {
			domain.DNSRecordKind = model.AppDomainDNSRecordKindCNAME
		} else {
			domain.DNSRecordKind = model.AppDomainDNSRecordKindNone
		}
	}
	if domain.Status == model.AppDomainStatusVerified && domain.TLSStatus == "" {
		domain.TLSStatus = model.AppDomainTLSStatusPending
	}
	if lastCheckedAt.Valid {
		value := lastCheckedAt.Time.UTC()
		domain.LastCheckedAt = &value
	}
	if dnsLastCheckedAt.Valid {
		value := dnsLastCheckedAt.Time.UTC()
		domain.DNSLastCheckedAt = &value
	}
	if verifiedAt.Valid {
		value := verifiedAt.Time.UTC()
		domain.VerifiedAt = &value
	}
	if tlsLastCheckedAt.Valid {
		value := tlsLastCheckedAt.Time.UTC()
		domain.TLSLastCheckedAt = &value
	}
	if tlsReadyAt.Valid {
		value := tlsReadyAt.Time.UTC()
		domain.TLSReadyAt = &value
	}
	return domain, nil
}
