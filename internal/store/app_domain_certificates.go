package store

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"fugue/internal/model"
)

// ImportAppDomainCertificate rechecks domain ownership under the same write
// boundary as the fingerprint CAS. It does not mutate DNS or readiness facts.
func (s *Store) ImportAppDomainCertificate(cert model.EdgeTLSCertificate, projectID, expected string, names []string) (model.EdgeTLSCertificate, error) {
	cert, err := normalizeEdgeTLSCertificateForStore(cert)
	if err != nil {
		return model.EdgeTLSCertificate{}, err
	}
	if cert.CertificateSHA256 == "" || cert.NotAfter == nil || projectID == "" || len(names) == 0 || len(names) > 16 {
		return model.EdgeTLSCertificate{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgImportAppDomainCertificate(cert, projectID, expected, names)
	}
	var out model.EdgeTLSCertificate
	err = s.withLockedState(true, func(state *model.State) error {
		ai := findApp(state, cert.AppID)
		if ai < 0 || state.Apps[ai].TenantID != cert.TenantID || state.Apps[ai].ProjectID != projectID {
			return ErrConflict
		}
		for _, host := range names {
			di := findAppDomain(state, host)
			if di < 0 {
				return ErrConflict
			}
			d := state.AppDomains[di]
			if d.AppID != cert.AppID || d.TenantID != cert.TenantID || d.Status != model.AppDomainStatusVerified {
				return ErrConflict
			}
		}
		index := findEdgeTLSCertificate(state, cert.Hostname)
		now := time.Now().UTC()
		if index >= 0 {
			current := state.EdgeTLSCertificates[index]
			if current.AppID != cert.AppID || current.TenantID != cert.TenantID {
				return ErrConflict
			}
			if current.CertificateSHA256 == cert.CertificateSHA256 {
				out = cloneEdgeTLSCertificate(current)
				return nil
			}
			if expected == "" || current.CertificateSHA256 != expected ||
				(current.NotAfter != nil && current.NotAfter.After(now) && !cert.NotAfter.After(*current.NotAfter)) {
				return ErrConflict
			}
			cert.CreatedAt = current.CreatedAt
		} else {
			if expected != "" {
				return ErrConflict
			}
			cert.CreatedAt = now
		}
		cert.UpdatedAt = now
		if index >= 0 {
			state.EdgeTLSCertificates[index] = cloneEdgeTLSCertificate(cert)
		} else {
			state.EdgeTLSCertificates = append(state.EdgeTLSCertificates, cloneEdgeTLSCertificate(cert))
		}
		out = cloneEdgeTLSCertificate(cert)
		return nil
	})
	return out, err
}

func (s *Store) pgImportAppDomainCertificate(cert model.EdgeTLSCertificate, projectID, expected string, names []string) (model.EdgeTLSCertificate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EdgeTLSCertificate{}, err
	}
	defer tx.Rollback()
	var owner string
	err = tx.QueryRowContext(ctx, `SELECT id FROM fugue_apps WHERE id=$1 AND tenant_id=$2 AND project_id=$3 FOR SHARE`, cert.AppID, cert.TenantID, projectID).Scan(&owner)
	if err == sql.ErrNoRows {
		return model.EdgeTLSCertificate{}, ErrConflict
	}
	if err != nil {
		return model.EdgeTLSCertificate{}, err
	}
	for _, host := range names {
		err = tx.QueryRowContext(ctx, `SELECT hostname FROM fugue_app_domains WHERE lower(hostname)=lower($1) AND app_id=$2 AND tenant_id=$3 AND status=$4 FOR SHARE`, host, cert.AppID, cert.TenantID, model.AppDomainStatusVerified).Scan(&owner)
		if err == sql.ErrNoRows {
			return model.EdgeTLSCertificate{}, ErrConflict
		}
		if err != nil {
			return model.EdgeTLSCertificate{}, err
		}
	}
	// All certificate writers use this row's atomic ON CONFLICT boundary. An
	// expired/unrelated predecessor is still subject to the caller's exact CAS.
	now := time.Now().UTC()
	columns := `hostname, tenant_id, app_id, certificate_pem, private_key_pem, metadata_json, issuer_storage, certificate_sha256, not_after, uploaded_by_edge_id, uploaded_by_edge_group_id, created_at, updated_at`
	query := `INSERT INTO fugue_edge_tls_certificates (` + columns + `) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12
WHERE $13='' OR EXISTS (SELECT 1 FROM fugue_edge_tls_certificates WHERE hostname=$1)
ON CONFLICT (hostname) DO UPDATE SET
 certificate_pem=EXCLUDED.certificate_pem, private_key_pem=EXCLUDED.private_key_pem,
 metadata_json=EXCLUDED.metadata_json, issuer_storage=EXCLUDED.issuer_storage,
 certificate_sha256=EXCLUDED.certificate_sha256, not_after=EXCLUDED.not_after,
 uploaded_by_edge_id=EXCLUDED.uploaded_by_edge_id, uploaded_by_edge_group_id=EXCLUDED.uploaded_by_edge_group_id,
 updated_at=CASE WHEN fugue_edge_tls_certificates.certificate_sha256=EXCLUDED.certificate_sha256 THEN fugue_edge_tls_certificates.updated_at ELSE EXCLUDED.updated_at END
WHERE fugue_edge_tls_certificates.tenant_id=EXCLUDED.tenant_id AND fugue_edge_tls_certificates.app_id=EXCLUDED.app_id
 AND (fugue_edge_tls_certificates.certificate_sha256=EXCLUDED.certificate_sha256 OR
 ($13<>'' AND fugue_edge_tls_certificates.certificate_sha256=$13 AND
 (fugue_edge_tls_certificates.not_after IS NULL OR fugue_edge_tls_certificates.not_after <= $12 OR EXCLUDED.not_after > fugue_edge_tls_certificates.not_after)))
RETURNING ` + columns
	out, err := scanEdgeTLSCertificate(tx.QueryRowContext(ctx, query, cert.Hostname, cert.TenantID, cert.AppID, cert.CertificatePEM, cert.PrivateKeyPEM, cert.MetadataJSON, cert.IssuerStorage, cert.CertificateSHA256, cert.NotAfter, cert.UploadedByEdgeID, cert.UploadedByEdgeGroupID, now, strings.TrimSpace(expected)))
	if err == sql.ErrNoRows {
		return model.EdgeTLSCertificate{}, ErrConflict
	}
	if err != nil {
		return model.EdgeTLSCertificate{}, mapDBErr(err)
	}
	if err = tx.Commit(); err != nil {
		return model.EdgeTLSCertificate{}, err
	}
	return out, nil
}
