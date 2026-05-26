package store

import (
	"context"
	"database/sql"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgGetEdgeTLSCertificate(hostname string) (model.EdgeTLSCertificate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cert, err := scanEdgeTLSCertificate(s.db.QueryRowContext(ctx, `
SELECT hostname, tenant_id, app_id, certificate_pem, private_key_pem, metadata_json, issuer_storage, certificate_sha256, not_after, uploaded_by_edge_id, uploaded_by_edge_group_id, created_at, updated_at
FROM fugue_edge_tls_certificates
WHERE lower(hostname) = lower($1)
`, hostname))
	if err != nil {
		return model.EdgeTLSCertificate{}, mapDBErr(err)
	}
	return cert, nil
}

func (s *Store) pgPutEdgeTLSCertificate(cert model.EdgeTLSCertificate) (model.EdgeTLSCertificate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	if cert.CreatedAt.IsZero() {
		cert.CreatedAt = now
	}
	cert.UpdatedAt = now

	row := s.db.QueryRowContext(ctx, `
INSERT INTO fugue_edge_tls_certificates (hostname, tenant_id, app_id, certificate_pem, private_key_pem, metadata_json, issuer_storage, certificate_sha256, not_after, uploaded_by_edge_id, uploaded_by_edge_group_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (hostname) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id,
	app_id = EXCLUDED.app_id,
	certificate_pem = EXCLUDED.certificate_pem,
	private_key_pem = EXCLUDED.private_key_pem,
	metadata_json = EXCLUDED.metadata_json,
	issuer_storage = EXCLUDED.issuer_storage,
	certificate_sha256 = EXCLUDED.certificate_sha256,
	not_after = EXCLUDED.not_after,
	uploaded_by_edge_id = EXCLUDED.uploaded_by_edge_id,
	uploaded_by_edge_group_id = EXCLUDED.uploaded_by_edge_group_id,
	updated_at = EXCLUDED.updated_at
RETURNING hostname, tenant_id, app_id, certificate_pem, private_key_pem, metadata_json, issuer_storage, certificate_sha256, not_after, uploaded_by_edge_id, uploaded_by_edge_group_id, created_at, updated_at
`, cert.Hostname, cert.TenantID, cert.AppID, cert.CertificatePEM, cert.PrivateKeyPEM, cert.MetadataJSON, cert.IssuerStorage, cert.CertificateSHA256, cert.NotAfter, cert.UploadedByEdgeID, cert.UploadedByEdgeGroupID, cert.CreatedAt, cert.UpdatedAt)
	stored, err := scanEdgeTLSCertificate(row)
	if err != nil {
		return model.EdgeTLSCertificate{}, mapDBErr(err)
	}
	return stored, nil
}

func (s *Store) pgDeleteEdgeTLSCertificate(hostname string) (model.EdgeTLSCertificate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cert, err := scanEdgeTLSCertificate(s.db.QueryRowContext(ctx, `
DELETE FROM fugue_edge_tls_certificates
WHERE lower(hostname) = lower($1)
RETURNING hostname, tenant_id, app_id, certificate_pem, private_key_pem, metadata_json, issuer_storage, certificate_sha256, not_after, uploaded_by_edge_id, uploaded_by_edge_group_id, created_at, updated_at
`, hostname))
	if err != nil {
		return model.EdgeTLSCertificate{}, mapDBErr(err)
	}
	return cert, nil
}

func scanEdgeTLSCertificate(scanner sqlScanner) (model.EdgeTLSCertificate, error) {
	var cert model.EdgeTLSCertificate
	var tenantID sql.NullString
	var appID sql.NullString
	var metadataJSON sql.NullString
	var issuerStorage sql.NullString
	var certificateSHA256 sql.NullString
	var notAfter sql.NullTime
	var uploadedByEdgeID sql.NullString
	var uploadedByEdgeGroupID sql.NullString
	if err := scanner.Scan(
		&cert.Hostname,
		&tenantID,
		&appID,
		&cert.CertificatePEM,
		&cert.PrivateKeyPEM,
		&metadataJSON,
		&issuerStorage,
		&certificateSHA256,
		&notAfter,
		&uploadedByEdgeID,
		&uploadedByEdgeGroupID,
		&cert.CreatedAt,
		&cert.UpdatedAt,
	); err != nil {
		return model.EdgeTLSCertificate{}, err
	}
	cert.TenantID = tenantID.String
	cert.AppID = appID.String
	cert.MetadataJSON = metadataJSON.String
	cert.IssuerStorage = issuerStorage.String
	cert.CertificateSHA256 = certificateSHA256.String
	cert.UploadedByEdgeID = uploadedByEdgeID.String
	cert.UploadedByEdgeGroupID = uploadedByEdgeGroupID.String
	if notAfter.Valid {
		value := notAfter.Time.UTC()
		cert.NotAfter = &value
	}
	return cloneEdgeTLSCertificate(cert), nil
}
