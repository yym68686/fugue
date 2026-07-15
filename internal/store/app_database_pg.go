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

func (s *Store) pgCreateAppDatabaseImportJob(job model.AppDatabaseImportJob) (model.AppDatabaseImportJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppDatabaseImportJob{}, fmt.Errorf("begin database import create transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, job.AppID, true)
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	if isDeletedApp(app) || strings.TrimSpace(app.TenantID) != job.TenantID {
		return model.AppDatabaseImportJob{}, ErrNotFound
	}
	if err := s.pgValidateAppDatabaseImportRunnableTx(ctx, tx, app); err != nil {
		return model.AppDatabaseImportJob{}, err
	}

	now := time.Now().UTC()
	if strings.TrimSpace(job.ID) == "" {
		job.ID = model.NewID("dbimport")
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	logsJSON, err := marshalNullableJSON(job.Logs)
	if err != nil {
		return model.AppDatabaseImportJob{}, err
	}

	created, err := scanAppDatabaseImportJob(tx.QueryRowContext(ctx, `
INSERT INTO fugue_app_database_import_jobs (
	id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256,
	label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id,
	logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
)
SELECT
	$1, a.tenant_id, a.id, u.id, $5, $6,
	$7, $8, $9, $10, $11, $12, $13, $14,
	$15, $16, $17, $18, $19, $20, $21
FROM fugue_apps AS a
JOIN fugue_source_uploads AS u ON u.id = $4 AND u.tenant_id = a.tenant_id
WHERE a.id = $2 AND a.tenant_id = $3
RETURNING id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
`, job.ID, job.AppID, job.TenantID, job.SourceUploadID, job.SourceUploadFilename, job.SourceUploadSHA256, job.Label, job.Format, job.Clean, job.Status, strings.TrimSpace(job.ResultMessage), strings.TrimSpace(job.ErrorMessage), job.RetryCount, job.RetryOfJobID, logsJSON, job.RequestedByType, job.RequestedByID, job.CreatedAt, job.UpdatedAt, job.StartedAt, job.CompletedAt))
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AppDatabaseImportJob{}, fmt.Errorf("commit database import create transaction: %w", err)
	}
	return redactAppDatabaseImportJob(created), nil
}

func (s *Store) pgListAppDatabaseImportJobs(appID string) ([]model.AppDatabaseImportJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := s.pgGetAppMetadata(strings.TrimSpace(appID)); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
FROM fugue_app_database_import_jobs
WHERE app_id = $1
ORDER BY created_at DESC, id DESC
`, strings.TrimSpace(appID))
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()

	jobs := []model.AppDatabaseImportJob{}
	for rows.Next() {
		job, err := scanAppDatabaseImportJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, redactAppDatabaseImportJob(job))
	}
	return jobs, mapDBErr(rows.Err())
}

func (s *Store) pgGetAppDatabaseImportJob(appID, jobID string) (model.AppDatabaseImportJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := scanAppDatabaseImportJob(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
FROM fugue_app_database_import_jobs
WHERE app_id = $1 AND id = $2
`, strings.TrimSpace(appID), strings.TrimSpace(jobID)))
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	return redactAppDatabaseImportJob(job), nil
}

func (s *Store) pgListPendingAppDatabaseImportJobs(limit int) ([]model.AppDatabaseImportJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
FROM fugue_app_database_import_jobs
WHERE status = $1
ORDER BY created_at ASC, id ASC
LIMIT $2
`, model.OperationStatusPending, limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()

	jobs := []model.AppDatabaseImportJob{}
	for rows.Next() {
		job, err := scanAppDatabaseImportJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, redactAppDatabaseImportJob(job))
	}
	return jobs, mapDBErr(rows.Err())
}

func (s *Store) pgClaimAppDatabaseImportJob(jobID string) (model.AppDatabaseImportJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppDatabaseImportJob{}, fmt.Errorf("begin database import claim transaction: %w", err)
	}
	defer tx.Rollback()
	var appID string
	if err := tx.QueryRowContext(ctx, `
SELECT app_id
FROM fugue_app_database_import_jobs
WHERE id = $1
`, strings.TrimSpace(jobID)).Scan(&appID); err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	app, err := s.pgGetAppTx(ctx, tx, strings.TrimSpace(appID), true)
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	if isDeletedApp(app) {
		return model.AppDatabaseImportJob{}, ErrNotFound
	}

	current, err := scanAppDatabaseImportJob(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
FROM fugue_app_database_import_jobs
WHERE id = $1
FOR UPDATE
`, strings.TrimSpace(jobID)))
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	if strings.TrimSpace(current.Status) != model.OperationStatusPending {
		return model.AppDatabaseImportJob{}, ErrConflict
	}

	now := time.Now().UTC()
	if err := s.pgValidateAppDatabaseImportRunnableTx(ctx, tx, app); err != nil {
		if !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
			return model.AppDatabaseImportJob{}, err
		}
		if _, updateErr := tx.ExecContext(ctx, `
UPDATE fugue_app_database_import_jobs
SET status = $2,
	result_message = '',
	error_message = $3,
	started_at = NULL,
	completed_at = $4,
	updated_at = $4
WHERE id = $1 AND status = $5
`, strings.TrimSpace(jobID), model.OperationStatusFailed, ManagedPostgresDatabaseImportConflictMessage, now, model.OperationStatusPending); updateErr != nil {
			return model.AppDatabaseImportJob{}, mapDBErr(updateErr)
		}
		if err := tx.Commit(); err != nil {
			return model.AppDatabaseImportJob{}, fmt.Errorf("commit blocked database import claim transaction: %w", err)
		}
		return model.AppDatabaseImportJob{}, ErrManagedPostgresDatabaseImportConflict
	}
	claimed, err := scanAppDatabaseImportJob(tx.QueryRowContext(ctx, `
UPDATE fugue_app_database_import_jobs
SET status = $2, started_at = $3, updated_at = $3
WHERE id = $1
RETURNING id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
`, strings.TrimSpace(jobID), model.OperationStatusRunning, now))
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AppDatabaseImportJob{}, fmt.Errorf("commit database import claim transaction: %w", err)
	}
	return redactAppDatabaseImportJob(claimed), nil
}

func (s *Store) pgValidateAppDatabaseImportRunnable(appID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin database import validation transaction: %w", err)
	}
	defer tx.Rollback()
	app, err := s.pgGetAppTx(ctx, tx, strings.TrimSpace(appID), true)
	if err != nil {
		return mapDBErr(err)
	}
	if isDeletedApp(app) {
		return ErrNotFound
	}
	if err := s.pgValidateAppDatabaseImportRunnableTx(ctx, tx, app); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit database import validation transaction: %w", err)
	}
	return nil
}

func (s *Store) pgValidateAppDatabaseImportRunnableTx(ctx context.Context, tx *sql.Tx, app model.App) error {
	if appHasSuspendedManagedPostgres(app) {
		return ErrManagedPostgresDatabaseImportConflict
	}
	active, err := s.pgHasActiveManagedPostgresSuspendForAppTx(ctx, tx, app.ID)
	if err != nil {
		return err
	}
	if active {
		return ErrManagedPostgresDatabaseImportConflict
	}
	return nil
}

func (s *Store) pgHasActiveManagedPostgresSuspendForAppTx(ctx context.Context, tx *sql.Tx, appID string) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_operations
	WHERE app_id = $1
	  AND type = $2
	  AND status IN ($3, $4, $5)
)
`,
		strings.TrimSpace(appID),
		model.OperationTypeDatabaseSuspend,
		model.OperationStatusPending,
		model.OperationStatusRunning,
		model.OperationStatusWaitingAgent,
	).Scan(&active); err != nil {
		return false, fmt.Errorf("check active managed postgres suspend for database import: %w", err)
	}
	return active, nil
}

// pgHasActiveAppDatabaseImportJobForManagedPostgresTx uses the shared app row
// lock as its serialization fence. AppDatabaseImportJob has no service ID, so
// any pending/running import for the app blocks suspension of its managed DB.
func (s *Store) pgHasActiveAppDatabaseImportJobForManagedPostgresTx(
	ctx context.Context,
	tx *sql.Tx,
	app model.App,
	_ string,
) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_app_database_import_jobs
	WHERE app_id = $1
	  AND status IN ($2, $3)
)
`, strings.TrimSpace(app.ID), model.OperationStatusPending, model.OperationStatusRunning).Scan(&active); err != nil {
		return false, fmt.Errorf("check active app database import for managed postgres: %w", err)
	}
	return active, nil
}

func (s *Store) pgAppendAppDatabaseImportJobLog(jobID, message string) (model.AppDatabaseImportJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppDatabaseImportJob{}, fmt.Errorf("begin database import log transaction: %w", err)
	}
	defer tx.Rollback()

	current, err := scanAppDatabaseImportJob(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
FROM fugue_app_database_import_jobs
WHERE id = $1
FOR UPDATE
`, strings.TrimSpace(jobID)))
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	now := time.Now().UTC()
	if strings.TrimSpace(message) != "" {
		current.Logs = append(current.Logs, model.AppDatabaseImportJobLog{
			At:      now,
			Message: strings.TrimSpace(message),
		})
	}
	logsJSON, err := marshalNullableJSON(current.Logs)
	if err != nil {
		return model.AppDatabaseImportJob{}, err
	}
	updated, err := scanAppDatabaseImportJob(tx.QueryRowContext(ctx, `
UPDATE fugue_app_database_import_jobs
SET logs_json = $2, updated_at = $3
WHERE id = $1
RETURNING id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
`, strings.TrimSpace(jobID), logsJSON, now))
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AppDatabaseImportJob{}, fmt.Errorf("commit database import log transaction: %w", err)
	}
	return redactAppDatabaseImportJob(updated), nil
}

func (s *Store) pgCompleteAppDatabaseImportJob(jobID, status, message, errorMessage string) (model.AppDatabaseImportJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppDatabaseImportJob{}, fmt.Errorf("begin database import complete transaction: %w", err)
	}
	defer tx.Rollback()

	current, err := scanAppDatabaseImportJob(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
FROM fugue_app_database_import_jobs
WHERE id = $1
FOR UPDATE
`, strings.TrimSpace(jobID)))
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	if strings.TrimSpace(current.Status) != model.OperationStatusRunning {
		return model.AppDatabaseImportJob{}, ErrConflict
	}

	now := time.Now().UTC()
	completed, err := scanAppDatabaseImportJob(tx.QueryRowContext(ctx, `
UPDATE fugue_app_database_import_jobs
SET status = $2, result_message = $3, error_message = $4, completed_at = $5, updated_at = $5
WHERE id = $1
RETURNING id, tenant_id, app_id, source_upload_id, source_upload_filename, source_upload_sha256, label, format, clean, status, result_message, error_message, retry_count, retry_of_job_id, logs_json, requested_by_type, requested_by_id, created_at, updated_at, started_at, completed_at
`, strings.TrimSpace(jobID), status, strings.TrimSpace(message), strings.TrimSpace(errorMessage), now))
	if err != nil {
		return model.AppDatabaseImportJob{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AppDatabaseImportJob{}, fmt.Errorf("commit database import complete transaction: %w", err)
	}
	return redactAppDatabaseImportJob(completed), nil
}

func (s *Store) pgListAppDatabaseAccessGrants(appID string) ([]model.AppDatabaseAccessGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := s.pgGetAppMetadata(strings.TrimSpace(appID)); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, app_id, label, mode, status, token_prefix, token_hash, requested_by_type, requested_by_id, expires_at, revoked_at, last_used_at, created_at, updated_at
FROM fugue_app_database_access_grants
WHERE app_id = $1
ORDER BY created_at DESC, id DESC
`, strings.TrimSpace(appID))
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()

	grants := []model.AppDatabaseAccessGrant{}
	for rows.Next() {
		grant, err := scanAppDatabaseAccessGrant(rows)
		if err != nil {
			return nil, err
		}
		grants = append(grants, redactAppDatabaseAccessGrant(grant))
	}
	return grants, mapDBErr(rows.Err())
}

func (s *Store) pgCreateAppDatabaseAccessGrant(grant model.AppDatabaseAccessGrant, secret string) (model.AppDatabaseAccessGrant, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	if strings.TrimSpace(grant.ID) == "" {
		grant.ID = model.NewID("dbaccess")
	}
	if strings.TrimSpace(grant.Label) == "" {
		grant.Label = "database tunnel"
	}
	if grant.CreatedAt.IsZero() {
		grant.CreatedAt = now
	}
	grant.UpdatedAt = now

	created, err := scanAppDatabaseAccessGrant(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_app_database_access_grants (
	id, tenant_id, app_id, label, mode, status, token_prefix, token_hash,
	requested_by_type, requested_by_id, expires_at, revoked_at, last_used_at, created_at, updated_at
)
SELECT
	$1, a.tenant_id, a.id, $4, $5, $6, $7, $8,
	$9, $10, $11, $12, $13, $14, $15
FROM fugue_apps AS a
WHERE a.id = $2 AND a.tenant_id = $3
RETURNING id, tenant_id, app_id, label, mode, status, token_prefix, token_hash, requested_by_type, requested_by_id, expires_at, revoked_at, last_used_at, created_at, updated_at
`, grant.ID, grant.AppID, grant.TenantID, grant.Label, grant.Mode, grant.Status, grant.TokenPrefix, grant.TokenHash, grant.RequestedByType, grant.RequestedByID, grant.ExpiresAt, grant.RevokedAt, grant.LastUsedAt, grant.CreatedAt, grant.UpdatedAt))
	if err != nil {
		return model.AppDatabaseAccessGrant{}, "", mapDBErr(err)
	}
	return redactAppDatabaseAccessGrant(created), secret, nil
}

func (s *Store) pgRevokeAppDatabaseAccessGrant(appID, grantID string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
UPDATE fugue_app_database_access_grants
SET status = $3, revoked_at = COALESCE(revoked_at, $4), updated_at = $4
WHERE app_id = $1 AND id = $2
`, strings.TrimSpace(appID), strings.TrimSpace(grantID), model.AppDatabaseAccessGrantStatusRevoked, now)
	if err != nil {
		return false, mapDBErr(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *Store) pgAuthenticateAppDatabaseAccessGrant(appID, grantID, secret string) (model.AppDatabaseAccessGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppDatabaseAccessGrant{}, fmt.Errorf("begin database access authentication transaction: %w", err)
	}
	defer tx.Rollback()

	grant, err := scanAppDatabaseAccessGrant(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, app_id, label, mode, status, token_prefix, token_hash, requested_by_type, requested_by_id, expires_at, revoked_at, last_used_at, created_at, updated_at
FROM fugue_app_database_access_grants
WHERE app_id = $1 AND id = $2 AND token_hash = $3
FOR UPDATE
`, strings.TrimSpace(appID), strings.TrimSpace(grantID), model.HashSecret(strings.TrimSpace(secret))))
	if err != nil {
		return model.AppDatabaseAccessGrant{}, mapDBErr(err)
	}
	normalizeAppDatabaseAccessGrantForRead(&grant)
	if grant.Status != model.AppDatabaseAccessGrantStatusActive {
		return model.AppDatabaseAccessGrant{}, ErrConflict
	}

	now := time.Now().UTC()
	grant.LastUsedAt = &now
	grant.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_app_database_access_grants
SET last_used_at = $2, updated_at = $2
WHERE id = $1
`, grant.ID, now); err != nil {
		return model.AppDatabaseAccessGrant{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AppDatabaseAccessGrant{}, fmt.Errorf("commit database access authentication transaction: %w", err)
	}
	return redactAppDatabaseAccessGrant(grant), nil
}

func scanAppDatabaseImportJob(scanner sqlScanner) (model.AppDatabaseImportJob, error) {
	var job model.AppDatabaseImportJob
	var logsJSON []byte
	var startedAt sql.NullTime
	var completedAt sql.NullTime
	if err := scanner.Scan(
		&job.ID,
		&job.TenantID,
		&job.AppID,
		&job.SourceUploadID,
		&job.SourceUploadFilename,
		&job.SourceUploadSHA256,
		&job.Label,
		&job.Format,
		&job.Clean,
		&job.Status,
		&job.ResultMessage,
		&job.ErrorMessage,
		&job.RetryCount,
		&job.RetryOfJobID,
		&logsJSON,
		&job.RequestedByType,
		&job.RequestedByID,
		&job.CreatedAt,
		&job.UpdatedAt,
		&startedAt,
		&completedAt,
	); err != nil {
		return model.AppDatabaseImportJob{}, err
	}
	if startedAt.Valid {
		value := startedAt.Time
		job.StartedAt = &value
	}
	if completedAt.Valid {
		value := completedAt.Time
		job.CompletedAt = &value
	}
	logs, err := decodeJSONValue[[]model.AppDatabaseImportJobLog](logsJSON)
	if err != nil {
		return model.AppDatabaseImportJob{}, err
	}
	job.Logs = logs
	return job, nil
}

func scanAppDatabaseAccessGrant(scanner sqlScanner) (model.AppDatabaseAccessGrant, error) {
	var grant model.AppDatabaseAccessGrant
	var expiresAt sql.NullTime
	var revokedAt sql.NullTime
	var lastUsedAt sql.NullTime
	if err := scanner.Scan(
		&grant.ID,
		&grant.TenantID,
		&grant.AppID,
		&grant.Label,
		&grant.Mode,
		&grant.Status,
		&grant.TokenPrefix,
		&grant.TokenHash,
		&grant.RequestedByType,
		&grant.RequestedByID,
		&expiresAt,
		&revokedAt,
		&lastUsedAt,
		&grant.CreatedAt,
		&grant.UpdatedAt,
	); err != nil {
		return model.AppDatabaseAccessGrant{}, err
	}
	if expiresAt.Valid {
		value := expiresAt.Time
		grant.ExpiresAt = &value
	}
	if revokedAt.Valid {
		value := revokedAt.Time
		grant.RevokedAt = &value
	}
	if lastUsedAt.Valid {
		value := lastUsedAt.Time
		grant.LastUsedAt = &value
	}
	return grant, nil
}
