package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"fugue/internal/backupadapter"
	"fugue/internal/model"
)

// BackupBackendObservation is the complete backend view needed to materialize
// a backup-control spec. Physical backend configuration, inline credentials,
// and encrypted secret metadata never leave the store layer.
type BackupBackendObservation struct {
	BackendID  string `json:"backendId"`
	TenantID   string `json:"tenantId,omitempty"`
	Generation string `json:"generation"`
}

// GetBackupBackendObservation returns one statement/lock-consistent,
// read-only backend snapshot. It intentionally differs from
// GetBackupBackendForUse: callers cannot obtain an access-key id, secret,
// token, ciphertext, encryption key id, or secret record identifier.
func (s *Store) GetBackupBackendObservation(idOrName, tenantID string, platformAdmin bool) (BackupBackendObservation, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return BackupBackendObservation{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetBackupBackendObservation(idOrName, tenantID, platformAdmin)
	}
	var observation BackupBackendObservation
	err := s.withLockedState(false, func(state *model.State) error {
		index := findBackupBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		backend := model.NormalizeBackupBackend(state.BackupBackends[index])
		var secret *model.BackupBackendSecret
		if secretIndex := findBackupBackendSecret(state, backend.CredentialSecretID, backend.ID); secretIndex >= 0 {
			copy := state.BackupBackendSecrets[secretIndex]
			secret = &copy
		}
		var observationErr error
		observation, observationErr = newBackupBackendObservation(backend, secret)
		if observationErr != nil {
			return observationErr
		}
		return nil
	})
	return observation, err
}

func (s *Store) pgGetBackupBackendObservation(idOrName, tenantID string, platformAdmin bool) (BackupBackendObservation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	slug := model.Slugify(idOrName)
	query := backupBackendObservationSelectSQL() + ` WHERE (b.id = $1 OR b.name = $1 OR b.slug = $2)`
	args := []any{idOrName, slug}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND (b.tenant_id IS NULL OR b.tenant_id = $3)`
	}
	backend, secret, err := scanBackupBackendObservation(s.db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return BackupBackendObservation{}, err
	}
	return newBackupBackendObservation(backend, secret)
}

func scanBackupBackendObservation(scanner sqlRowScanner) (model.BackupBackend, *model.BackupBackendSecret, error) {
	var backend model.BackupBackend
	var capabilitiesRaw, credentialsRaw []byte
	var tenantID sql.NullString
	var lastTestedAt sql.NullTime
	var secretID, secretTenantID, secretBackendID, ciphertext, keyID sql.NullString
	var secretCreatedAt, secretUpdatedAt, secretLastRotated sql.NullTime
	if err := scanner.Scan(
		&backend.ID, &tenantID, &backend.Name, &backend.Slug, &backend.Provider, &backend.Bucket,
		&backend.Region, &backend.Endpoint, &backend.BaseURL, &backend.Prefix, &backend.Status,
		&capabilitiesRaw, &credentialsRaw, &backend.CredentialSecretID, &backend.FugueManaged,
		&backend.Billable, &lastTestedAt, &backend.LastTestResult, &backend.ErrorMessage,
		&backend.CreatedAt, &backend.UpdatedAt,
		&secretID, &secretTenantID, &secretBackendID, &ciphertext, &keyID,
		&secretCreatedAt, &secretUpdatedAt, &secretLastRotated,
	); err != nil {
		return model.BackupBackend{}, nil, mapDBErr(err)
	}
	if tenantID.Valid {
		backend.TenantID = tenantID.String
	}
	capabilities, err := decodeJSONValue[model.DataBackendCapabilities](capabilitiesRaw)
	if err != nil {
		return model.BackupBackend{}, nil, err
	}
	credentials, err := decodeJSONValue[model.DataBackendCredentials](credentialsRaw)
	if err != nil {
		return model.BackupBackend{}, nil, err
	}
	backend.Capabilities = capabilities
	backend.Credentials = credentials
	if lastTestedAt.Valid {
		backend.LastTestedAt = &lastTestedAt.Time
	}
	backend = model.NormalizeBackupBackend(backend)
	if !secretID.Valid {
		return backend, nil, nil
	}
	secret := &model.BackupBackendSecret{
		ID: secretID.String, TenantID: secretTenantID.String, BackendID: secretBackendID.String,
		Ciphertext: ciphertext.String, KeyID: keyID.String,
	}
	if secretCreatedAt.Valid {
		secret.CreatedAt = secretCreatedAt.Time.UTC()
	}
	if secretUpdatedAt.Valid {
		secret.UpdatedAt = secretUpdatedAt.Time.UTC()
	}
	if secretLastRotated.Valid {
		secret.LastRotated = secretLastRotated.Time.UTC()
	}
	return backend, secret, nil
}

func backupBackendObservationView(backend model.BackupBackend) model.BackupBackend {
	backend = model.NormalizeBackupBackend(backend)
	return model.BackupBackend{
		ID: backend.ID, TenantID: backend.TenantID, Provider: backend.Provider,
		Bucket: backend.Bucket, Region: backend.Region, Endpoint: backend.Endpoint,
		BaseURL: backend.BaseURL, Prefix: backend.Prefix, Status: backend.Status,
		Capabilities: backend.Capabilities, FugueManaged: backend.FugueManaged, Billable: backend.Billable,
	}
}

func newBackupBackendObservation(backend model.BackupBackend, secret *model.BackupBackendSecret) (BackupBackendObservation, error) {
	credentialGeneration, err := backupBackendCredentialGeneration(backend, secret)
	if err != nil {
		return BackupBackendObservation{}, err
	}
	generation, err := backupadapter.BackendGeneration(backupBackendObservationView(backend), credentialGeneration)
	if err != nil {
		return BackupBackendObservation{}, err
	}
	return BackupBackendObservation{
		BackendID: strings.TrimSpace(backend.ID), TenantID: strings.TrimSpace(backend.TenantID), Generation: generation,
	}, nil
}

func backupBackendCredentialGeneration(backend model.BackupBackend, secret *model.BackupBackendSecret) (string, error) {
	backend = model.NormalizeBackupBackend(backend)
	if strings.TrimSpace(backend.ID) == "" {
		return "", ErrInvalidInput
	}
	if secret == nil && strings.TrimSpace(backend.CredentialSecretID) != "" {
		return "", ErrConflict
	}
	if secret != nil && (strings.TrimSpace(secret.ID) == "" || strings.TrimSpace(secret.BackendID) != backend.ID ||
		(strings.TrimSpace(backend.CredentialSecretID) != "" && strings.TrimSpace(secret.ID) != strings.TrimSpace(backend.CredentialSecretID))) {
		return "", ErrConflict
	}
	inlineEncoded, err := json.Marshal(backend.Credentials)
	if err != nil {
		return "", err
	}
	inlineDigest := sha256.Sum256(inlineEncoded)
	payload := struct {
		Version                string `json:"version"`
		BackendID              string `json:"backendId"`
		InlineCredentialDigest string `json:"inlineCredentialDigest"`
		SecretID               string `json:"secretId,omitempty"`
		SecretKeyID            string `json:"secretKeyId,omitempty"`
		SecretCiphertextDigest string `json:"secretCiphertextDigest,omitempty"`
		SecretCreatedAt        string `json:"secretCreatedAt,omitempty"`
		SecretUpdatedAt        string `json:"secretUpdatedAt,omitempty"`
		SecretLastRotatedAt    string `json:"secretLastRotatedAt,omitempty"`
	}{
		Version: "backup-backend-credential-generation-v1", BackendID: backend.ID,
		InlineCredentialDigest: "sha256:" + hex.EncodeToString(inlineDigest[:]),
	}
	if secret != nil {
		ciphertextDigest := sha256.Sum256([]byte(secret.Ciphertext))
		payload.SecretID = strings.TrimSpace(secret.ID)
		payload.SecretKeyID = strings.TrimSpace(secret.KeyID)
		payload.SecretCiphertextDigest = "sha256:" + hex.EncodeToString(ciphertextDigest[:])
		payload.SecretCreatedAt = canonicalBackupObservationTime(secret.CreatedAt)
		payload.SecretUpdatedAt = canonicalBackupObservationTime(secret.UpdatedAt)
		payload.SecretLastRotatedAt = canonicalBackupObservationTime(secret.LastRotated)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func canonicalBackupObservationTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func backupBackendObservationSelectSQL() string {
	return `SELECT
b.id, b.tenant_id, b.name, b.slug, b.provider, b.bucket, b.region, b.endpoint, b.base_url,
b.prefix, b.status, b.capabilities_json, b.credentials_json, b.credential_secret_id,
b.fugue_managed, b.billable, b.last_tested_at, b.last_test_result, b.error_message,
b.created_at, b.updated_at,
s.id, s.tenant_id, s.backend_id, s.ciphertext, s.key_id, s.created_at, s.updated_at, s.last_rotated_at
FROM fugue_backup_backends b
LEFT JOIN fugue_backup_backend_secrets s ON s.backend_id = b.id`
}
