package store

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	ManagedPostgresDatabaseImportConflictCode      = "managed_postgres_unavailable_for_database_import"
	ManagedPostgresDatabaseImportConflictMessage   = "managed_postgres_unavailable_for_database_import: resume the database or wait for database suspend, resume, or resize to finish before importing"
	ManagedPostgresImportInProgressConflictCode    = "managed_postgres_import_in_progress"
	ManagedPostgresImportInProgressConflictMessage = "managed_postgres_import_in_progress: wait for app-database import to finish before changing database lifecycle or resources"
)

var ErrManagedPostgresDatabaseImportConflict = fmt.Errorf("%w: %s", ErrConflict, ManagedPostgresDatabaseImportConflictMessage)
var ErrManagedPostgresImportInProgressConflict = fmt.Errorf("%w: %s", ErrConflict, ManagedPostgresImportInProgressConflictMessage)

func (s *Store) CreateAppDatabaseImportJob(job model.AppDatabaseImportJob) (model.AppDatabaseImportJob, error) {
	job.AppID = strings.TrimSpace(job.AppID)
	job.TenantID = strings.TrimSpace(job.TenantID)
	job.SourceUploadID = strings.TrimSpace(job.SourceUploadID)
	job.SourceUploadFilename = strings.TrimSpace(job.SourceUploadFilename)
	job.SourceUploadSHA256 = strings.TrimSpace(job.SourceUploadSHA256)
	job.Label = strings.TrimSpace(job.Label)
	job.Format = normalizeAppDatabaseImportFormat(job.Format)
	job.Status = normalizeAppDatabaseImportStatus(job.Status)
	job.RequestedByType = strings.TrimSpace(job.RequestedByType)
	job.RequestedByID = strings.TrimSpace(job.RequestedByID)
	job.RetryOfJobID = strings.TrimSpace(job.RetryOfJobID)
	if job.AppID == "" || job.TenantID == "" || job.SourceUploadID == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	if job.Status == "" {
		job.Status = model.OperationStatusPending
	}
	if job.Format == "" {
		job.Format = model.AppDatabaseImportFormatAuto
	}
	return s.pgCreateAppDatabaseImportJob(job)
}

func (s *Store) ListAppDatabaseImportJobs(appID string) ([]model.AppDatabaseImportJob, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListAppDatabaseImportJobs(appID)
}

func (s *Store) GetAppDatabaseImportJob(appID, jobID string) (model.AppDatabaseImportJob, error) {
	appID = strings.TrimSpace(appID)
	jobID = strings.TrimSpace(jobID)
	if appID == "" || jobID == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	return s.pgGetAppDatabaseImportJob(appID, jobID)
}

func (s *Store) ListPendingAppDatabaseImportJobs(limit int) ([]model.AppDatabaseImportJob, error) {
	if limit <= 0 {
		limit = 10
	}
	return s.pgListPendingAppDatabaseImportJobs(limit)
}

func (s *Store) ClaimAppDatabaseImportJob(jobID string) (model.AppDatabaseImportJob, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	return s.pgClaimAppDatabaseImportJob(jobID)
}

// ValidateAppDatabaseImportRunnable is a defense-in-depth check used immediately
// before an import runner opens a PostgreSQL connection. Create and Claim apply
// the same invariant atomically with their writes.
func (s *Store) ValidateAppDatabaseImportRunnable(appID string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return ErrInvalidInput
	}
	return s.pgValidateAppDatabaseImportRunnable(appID)
}

func (s *Store) AppendAppDatabaseImportJobLog(jobID, message string) (model.AppDatabaseImportJob, error) {
	jobID = strings.TrimSpace(jobID)
	message = strings.TrimSpace(message)
	if jobID == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	return s.pgAppendAppDatabaseImportJobLog(jobID, message)
}

func (s *Store) CompleteAppDatabaseImportJob(jobID, status, message, errorMessage string) (model.AppDatabaseImportJob, error) {
	jobID = strings.TrimSpace(jobID)
	status = normalizeTerminalAppDatabaseImportStatus(status)
	if jobID == "" || status == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	return s.pgCompleteAppDatabaseImportJob(jobID, status, message, errorMessage)
}

func (s *Store) ListAppDatabaseAccessGrants(appID string) ([]model.AppDatabaseAccessGrant, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListAppDatabaseAccessGrants(appID)
}

func (s *Store) CreateAppDatabaseAccessGrant(grant model.AppDatabaseAccessGrant) (model.AppDatabaseAccessGrant, string, error) {
	grant.AppID = strings.TrimSpace(grant.AppID)
	grant.TenantID = strings.TrimSpace(grant.TenantID)
	grant.Label = strings.TrimSpace(grant.Label)
	grant.Mode = normalizeAppDatabaseAccessMode(grant.Mode)
	grant.RequestedByType = strings.TrimSpace(grant.RequestedByType)
	grant.RequestedByID = strings.TrimSpace(grant.RequestedByID)
	if grant.AppID == "" || grant.TenantID == "" || grant.Mode == "" {
		return model.AppDatabaseAccessGrant{}, "", ErrInvalidInput
	}
	secret := model.NewSecret("fugue_db")
	grant.TokenPrefix = model.SecretPrefix(secret)
	grant.TokenHash = model.HashSecret(secret)
	grant.Status = model.AppDatabaseAccessGrantStatusActive
	return s.pgCreateAppDatabaseAccessGrant(grant, secret)
}

func (s *Store) RevokeAppDatabaseAccessGrant(appID, grantID string) (bool, error) {
	appID = strings.TrimSpace(appID)
	grantID = strings.TrimSpace(grantID)
	if appID == "" || grantID == "" {
		return false, ErrInvalidInput
	}
	return s.pgRevokeAppDatabaseAccessGrant(appID, grantID)
}

func (s *Store) AuthenticateAppDatabaseAccessGrant(appID, grantID, secret string) (model.AppDatabaseAccessGrant, error) {
	appID = strings.TrimSpace(appID)
	grantID = strings.TrimSpace(grantID)
	secret = strings.TrimSpace(secret)
	if appID == "" || grantID == "" || secret == "" {
		return model.AppDatabaseAccessGrant{}, ErrInvalidInput
	}
	return s.pgAuthenticateAppDatabaseAccessGrant(appID, grantID, secret)
}

func normalizeAppDatabaseImportFormat(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseImportFormatAuto:
		return model.AppDatabaseImportFormatAuto
	case model.AppDatabaseImportFormatSQL:
		return model.AppDatabaseImportFormatSQL
	case model.AppDatabaseImportFormatCustom:
		return model.AppDatabaseImportFormatCustom
	default:
		return ""
	}
}

func normalizeAppDatabaseImportStatus(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.OperationStatusPending:
		return model.OperationStatusPending
	case model.OperationStatusRunning:
		return model.OperationStatusRunning
	case model.OperationStatusCompleted:
		return model.OperationStatusCompleted
	case model.OperationStatusFailed:
		return model.OperationStatusFailed
	default:
		return ""
	}
}

func normalizeTerminalAppDatabaseImportStatus(raw string) string {
	switch normalizeAppDatabaseImportStatus(raw) {
	case model.OperationStatusCompleted:
		return model.OperationStatusCompleted
	case model.OperationStatusFailed:
		return model.OperationStatusFailed
	default:
		return ""
	}
}

func normalizeAppDatabaseAccessMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseAccessModeReadWrite:
		return model.AppDatabaseAccessModeReadWrite
	case model.AppDatabaseAccessModeReadOnly:
		return model.AppDatabaseAccessModeReadOnly
	default:
		return ""
	}
}

func normalizeAppDatabaseAccessGrantForRead(grant *model.AppDatabaseAccessGrant) {
	if grant == nil {
		return
	}
	grant.Mode = normalizeAppDatabaseAccessMode(grant.Mode)
	switch {
	case grant.RevokedAt != nil:
		grant.Status = model.AppDatabaseAccessGrantStatusRevoked
	case grant.ExpiresAt != nil && time.Now().UTC().After(grant.ExpiresAt.UTC()):
		grant.Status = model.AppDatabaseAccessGrantStatusExpired
	case strings.TrimSpace(grant.Status) == "":
		grant.Status = model.AppDatabaseAccessGrantStatusActive
	}
	if strings.TrimSpace(grant.Label) == "" {
		grant.Label = "database tunnel"
	}
}

func redactAppDatabaseAccessGrant(grant model.AppDatabaseAccessGrant) model.AppDatabaseAccessGrant {
	normalizeAppDatabaseAccessGrantForRead(&grant)
	grant.TokenHash = ""
	return grant
}

func redactAppDatabaseImportJob(job model.AppDatabaseImportJob) model.AppDatabaseImportJob {
	job.Format = normalizeAppDatabaseImportFormat(job.Format)
	job.Status = normalizeAppDatabaseImportStatus(job.Status)
	job.Logs = append([]model.AppDatabaseImportJobLog(nil), job.Logs...)
	return job
}
