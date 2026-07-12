package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/backupschedule"
	"fugue/internal/model"
)

const (
	defaultBackupBackendID            = "backup_backend_fugue_default_r2"
	defaultControlPlaneBackupPolicyID = "backup_policy_control_plane_db_default"
	backupUsageCloudflareR2PriceCode  = "cloudflare-r2-standard-storage"
	backupRunLostErrorCode            = "backup_run_lost"
	backupRunLostErrorMessage         = "backup run lease expired before completion; worker likely restarted or lost ownership"
	defaultBackupDueScannerLimit      = 100
	defaultBackupRunHistoryLimit      = 100
	defaultBackupArtifactHistoryLimit = 100
	defaultBackupRestorePlanListLimit = 100
	defaultBackupRestoreRunListLimit  = 100
)

const pgRepairBackupPolicyScheduleSQL = `
UPDATE fugue_backup_policies
SET schedule = $2, status = $3, disabled_reason = $4, next_run_at = $5, updated_at = $6
WHERE id = $1
  AND enabled = TRUE
  AND status = 'active'
  AND schedule = $7
  AND next_run_at IS NOT DISTINCT FROM $8
  AND last_run_at IS NOT DISTINCT FROM $9
`

type BackupPolicyFilter struct {
	TenantID        string
	ProjectID       string
	AppID           string
	TargetType      string
	IncludeDisabled bool
	PlatformAdmin   bool
	Limit           int
}

type BackupRunFilter struct {
	TenantID      string
	ProjectID     string
	AppID         string
	PolicyID      string
	TargetType    string
	Status        string
	PlatformAdmin bool
	Limit         int
}

type BackupArtifactFilter struct {
	TenantID      string
	ProjectID     string
	AppID         string
	PolicyID      string
	RunID         string
	TargetType    string
	ActiveOnly    bool
	PlatformAdmin bool
	Limit         int
}

type BackupRunUpdate struct {
	Status        *string
	LeaseOwner    *string
	LockedUntil   **time.Time
	HeartbeatAt   **time.Time
	BytesWritten  *int64
	LogicalBytes  *int64
	ArtifactCount *int
	ErrorCode     *string
	ErrorMessage  *string
	NextRetryAt   **time.Time
	StartedAt     **time.Time
	FinishedAt    **time.Time
}

type BackupRunFinish struct {
	Status        string
	BytesWritten  int64
	ArtifactCount int
	ErrorCode     string
	ErrorMessage  string
	FinishedAt    time.Time
}

func ensureBackupDefaults(state *model.State) {
	if state.BackupBackends == nil {
		state.BackupBackends = []model.BackupBackend{}
	}
	if state.BackupBackendSecrets == nil {
		state.BackupBackendSecrets = []model.BackupBackendSecret{}
	}
	if state.BackupPolicies == nil {
		state.BackupPolicies = []model.BackupPolicy{}
	}
	if state.BackupRuns == nil {
		state.BackupRuns = []model.BackupRun{}
	}
	if state.BackupArtifacts == nil {
		state.BackupArtifacts = []model.BackupArtifact{}
	}
	if state.BackupRestorePlans == nil {
		state.BackupRestorePlans = []model.BackupRestorePlan{}
	}
	if state.BackupRestoreRuns == nil {
		state.BackupRestoreRuns = []model.BackupRestoreRun{}
	}
	for idx := range state.BackupBackends {
		state.BackupBackends[idx] = model.NormalizeBackupBackend(state.BackupBackends[idx])
	}
	for idx := range state.BackupPolicies {
		state.BackupPolicies[idx] = model.NormalizeBackupPolicy(state.BackupPolicies[idx])
	}
	for idx := range state.BackupRuns {
		state.BackupRuns[idx] = model.NormalizeBackupRun(state.BackupRuns[idx])
	}
	for idx := range state.BackupArtifacts {
		state.BackupArtifacts[idx] = model.NormalizeBackupArtifact(state.BackupArtifacts[idx])
	}
	for idx := range state.BackupRestorePlans {
		state.BackupRestorePlans[idx] = model.NormalizeBackupRestorePlan(state.BackupRestorePlans[idx])
	}
	for idx := range state.BackupRestoreRuns {
		state.BackupRestoreRuns[idx] = model.NormalizeBackupRestoreRun(state.BackupRestoreRuns[idx])
	}
	ensureDefaultBackupPolicyInState(state)
}

func (s *Store) DefaultBackupBackendID() string {
	return defaultBackupBackendID
}

func (s *Store) DefaultControlPlaneBackupPolicyID() string {
	return defaultControlPlaneBackupPolicyID
}

func (s *Store) SeedDefaultBackupBackendFromEnv() error {
	if s == nil {
		return nil
	}
	if s.usingDatabase() {
		return s.pgSeedDefaultBackupBackendFromEnv()
	}
	return s.withLockedState(true, seedDefaultBackupBackendFromEnvInState)
}

func (s *Store) EnsureDefaultBackupPolicy() error {
	if s == nil {
		return nil
	}
	if s.usingDatabase() {
		return s.pgEnsureDefaultBackupPolicy()
	}
	return s.withLockedState(true, func(state *model.State) error {
		ensureDefaultBackupPolicyInState(state)
		return nil
	})
}

func (s *Store) repairBackupPolicySchedules() error {
	if s == nil {
		return nil
	}
	if s.usingDatabase() {
		return s.pgRepairBackupPolicySchedules()
	}
	return s.withLockedState(true, func(state *model.State) error {
		repairBackupPolicySchedulesInState(state, time.Now().UTC())
		return nil
	})
}

func repairBackupPolicySchedulesInState(state *model.State, now time.Time) {
	if state == nil {
		return
	}
	for idx := range state.BackupPolicies {
		policy, changed := repairBackupPolicySchedule(state.BackupPolicies[idx], now)
		if changed {
			state.BackupPolicies[idx] = policy
		}
	}
}

func repairBackupPolicySchedule(policy model.BackupPolicy, now time.Time) (model.BackupPolicy, bool) {
	policy = model.NormalizeBackupPolicy(policy)
	if !policy.Enabled || policy.Status != model.BackupPolicyStatusActive {
		return policy, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	changed := false
	if policy.Schedule == "" {
		policy.Schedule = model.BackupDefaultSchedule
		changed = true
	}
	anchor := now.UTC()
	if policy.LastRunAt != nil {
		anchor = policy.LastRunAt.UTC()
	} else if !policy.CreatedAt.IsZero() {
		anchor = policy.CreatedAt.UTC()
	}
	next, err := nextBackupRunAfter(policy.Schedule, anchor)
	if err != nil {
		reason := "invalid backup schedule: " + err.Error()
		policy.Status = model.BackupPolicyStatusError
		policy.DisabledReason = reason
		policy.NextRunAt = nil
		changed = true
	} else {
		if policy.NextRunAt == nil {
			policy.NextRunAt = next
			changed = true
		}
		if policy.DisabledReason != "" {
			policy.DisabledReason = ""
			changed = true
		}
	}
	if changed {
		policy.UpdatedAt = now.UTC()
	}
	return model.NormalizeBackupPolicy(policy), changed
}

func seedDefaultBackupBackendFromEnvInState(state *model.State) error {
	backend, credentials, ok := defaultBackupBackendConfigFromEnv()
	if !ok {
		ensureDefaultBackupPolicyInState(state)
		return nil
	}
	index := findBackupBackend(state, defaultBackupBackendID)
	if index < 0 {
		state.BackupBackends = append(state.BackupBackends, backend)
		index = len(state.BackupBackends) - 1
	} else {
		existing := state.BackupBackends[index]
		if existing.CreatedAt.IsZero() {
			existing.CreatedAt = backend.CreatedAt
		}
		existing.Name = backend.Name
		existing.Slug = backend.Slug
		existing.Provider = backend.Provider
		existing.Bucket = backend.Bucket
		existing.Region = backend.Region
		existing.Endpoint = backend.Endpoint
		existing.BaseURL = backend.BaseURL
		existing.Prefix = backend.Prefix
		existing.Status = backend.Status
		existing.Capabilities = backend.Capabilities
		existing.Credentials = redactAccessKeyOnly(credentials)
		existing.FugueManaged = backend.FugueManaged
		existing.Billable = backend.Billable
		existing.UpdatedAt = backend.UpdatedAt
		backend = existing
	}
	if dataBackendCredentialsPresent(credentials) {
		secretID := backend.CredentialSecretID
		secretIndex := findBackupBackendSecret(state, secretID, backend.ID)
		secret := model.BackupBackendSecret{
			ID:        secretID,
			TenantID:  backend.TenantID,
			BackendID: backend.ID,
		}
		if secretIndex >= 0 {
			secret = state.BackupBackendSecrets[secretIndex]
		}
		encrypted, err := encryptBackupBackendSecret(secret, credentials)
		if err != nil {
			return err
		}
		backend.CredentialSecretID = encrypted.ID
		if secretIndex >= 0 {
			state.BackupBackendSecrets[secretIndex] = encrypted
		} else {
			state.BackupBackendSecrets = append(state.BackupBackendSecrets, encrypted)
		}
	}
	state.BackupBackends[index] = model.NormalizeBackupBackend(backend)
	ensureDefaultBackupPolicyInState(state)
	return nil
}

func defaultBackupBackendConfigFromEnv() (model.BackupBackend, model.DataBackendCredentials, bool) {
	dataBackend, credentials, ok := defaultDataBackendConfigFromEnv()
	if !ok {
		return model.BackupBackend{}, model.DataBackendCredentials{}, false
	}
	now := time.Now().UTC()
	backend := model.NormalizeBackupBackend(model.BackupBackend{
		ID:           defaultBackupBackendID,
		Name:         "fugue-default-r2",
		Slug:         "fugue-default-r2",
		Provider:     dataBackend.Provider,
		Bucket:       dataBackend.Bucket,
		Region:       dataBackend.Region,
		Endpoint:     dataBackend.Endpoint,
		BaseURL:      dataBackend.BaseURL,
		Prefix:       backupPrefix(dataBackend.Prefix),
		Status:       "active",
		Capabilities: dataBackend.Capabilities,
		Credentials:  redactAccessKeyOnly(credentials),
		FugueManaged: true,
		Billable:     model.NormalizeDataBackendProvider(dataBackend.Provider) == model.DataBackendProviderCloudflareR2,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	return backend, credentials, true
}

func backupPrefix(prefix string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return "fugue-backups"
	}
	if strings.HasSuffix(prefix, "backups") || strings.Contains(prefix, "backup") {
		return prefix
	}
	return strings.Trim(prefix+"/backups", "/")
}

func ensureDefaultBackupPolicyInState(state *model.State) {
	now := time.Now().UTC()
	status := model.BackupPolicyStatusBlockedNoBackend
	disabledReason := "platform R2 backup backend is not configured"
	backendID := ""
	if idx := findBackupBackend(state, defaultBackupBackendID); idx >= 0 {
		backend := model.NormalizeBackupBackend(state.BackupBackends[idx])
		if strings.EqualFold(backend.Status, "active") {
			backendID = backend.ID
			status = model.BackupPolicyStatusActive
			disabledReason = ""
		}
	}
	policy := model.NormalizeBackupPolicy(model.BackupPolicy{
		ID:             defaultControlPlaneBackupPolicyID,
		Name:           "control-plane-database-hourly",
		Slug:           "control-plane-database-hourly",
		Scope:          model.BackupScopePlatform,
		Target:         model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Component: "control-plane-postgres"},
		BackendID:      backendID,
		Enabled:        true,
		Status:         status,
		DisabledReason: disabledReason,
		Schedule:       model.BackupDefaultSchedule,
		RetainCount:    model.BackupDefaultRetainCount,
		Retention: model.BackupRetentionPolicy{
			RetainCount:   model.BackupDefaultRetainCount,
			ProtectLatest: model.BackupDefaultRetainCount,
		},
		CreatedBy: "system",
		CreatedAt: now,
		UpdatedAt: now,
	})
	index := findBackupPolicy(state, policy.ID)
	if index < 0 {
		if next, err := nextBackupRunAfter(policy.Schedule, now.Add(-time.Hour)); err == nil {
			policy.NextRunAt = next
		} else {
			policy.Status = model.BackupPolicyStatusError
			policy.DisabledReason = "invalid backup schedule: " + err.Error()
		}
		state.BackupPolicies = append(state.BackupPolicies, policy)
		return
	}
	existing := model.NormalizeBackupPolicy(state.BackupPolicies[index])
	existing.Name = policy.Name
	existing.Slug = policy.Slug
	existing.Scope = policy.Scope
	existing.Target = policy.Target
	if existing.BackendID == "" || existing.BackendID == defaultBackupBackendID || backendID == "" {
		existing.BackendID = backendID
	}
	existing.Enabled = true
	if backendID == "" {
		existing.Status = model.BackupPolicyStatusBlockedNoBackend
		existing.DisabledReason = disabledReason
	} else if existing.Status == "" || existing.Status == model.BackupPolicyStatusBlockedNoBackend {
		existing.Status = model.BackupPolicyStatusActive
		existing.DisabledReason = ""
	}
	if existing.Schedule == "" {
		existing.Schedule = model.BackupDefaultSchedule
	}
	if existing.RetainCount <= 0 {
		existing.RetainCount = model.BackupDefaultRetainCount
	}
	if existing.Retention.RetainCount <= 0 {
		existing.Retention.RetainCount = existing.RetainCount
	}
	if existing.Retention.ProtectLatest <= 0 {
		existing.Retention.ProtectLatest = existing.RetainCount
	}
	if existing.NextRunAt == nil {
		if next, err := nextBackupRunAfter(existing.Schedule, now.Add(-time.Hour)); err == nil {
			existing.NextRunAt = next
		} else {
			existing.Status = model.BackupPolicyStatusError
			existing.DisabledReason = "invalid backup schedule: " + err.Error()
		}
	}
	existing.UpdatedAt = now
	state.BackupPolicies[index] = model.NormalizeBackupPolicy(existing)
}

func (s *Store) ListBackupBackends(tenantID string, platformAdmin bool) ([]model.BackupBackend, error) {
	if s.usingDatabase() {
		return s.pgListBackupBackends(tenantID, platformAdmin)
	}
	backends := []model.BackupBackend{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, backend := range state.BackupBackends {
			backend = model.NormalizeBackupBackend(backend)
			if platformAdmin || backend.TenantID == "" || backend.TenantID == tenantID {
				backends = append(backends, model.RedactBackupBackendCredentials(backend))
			}
		}
		sort.Slice(backends, func(i, j int) bool { return backends[i].Name < backends[j].Name })
		return nil
	})
	return backends, err
}

func (s *Store) GetBackupBackend(idOrName, tenantID string, platformAdmin bool) (model.BackupBackend, error) {
	return s.getBackupBackend(idOrName, tenantID, platformAdmin, true)
}

func (s *Store) GetBackupBackendForUse(idOrName, tenantID string, platformAdmin bool) (model.BackupBackend, error) {
	return s.getBackupBackend(idOrName, tenantID, platformAdmin, false)
}

func (s *Store) getBackupBackend(idOrName, tenantID string, platformAdmin bool, redact bool) (model.BackupBackend, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return model.BackupBackend{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetBackupBackend(idOrName, tenantID, platformAdmin, redact)
	}
	var backend model.BackupBackend
	err := s.withLockedState(false, func(state *model.State) error {
		index := findBackupBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		backend = model.NormalizeBackupBackend(state.BackupBackends[index])
		if !redact {
			secretIndex := findBackupBackendSecret(state, backend.CredentialSecretID, backend.ID)
			if secretIndex >= 0 {
				credentials, err := decryptBackupBackendSecret(state.BackupBackendSecrets[secretIndex])
				if err != nil {
					return err
				}
				backend.Credentials = credentials
			}
		} else {
			backend = model.RedactBackupBackendCredentials(backend)
		}
		return nil
	})
	return backend, err
}

func (s *Store) CreateBackupBackend(backend model.BackupBackend) (model.BackupBackend, error) {
	backend = model.NormalizeBackupBackend(backend)
	if backend.Name == "" || backend.Provider == "" {
		return model.BackupBackend{}, ErrInvalidInput
	}
	if err := validateBackupBackend(backend); err != nil {
		return model.BackupBackend{}, err
	}
	if s.usingDatabase() {
		return s.pgCreateBackupBackend(backend)
	}
	credentials := backend.Credentials
	backend.Credentials = redactAccessKeyOnly(credentials)
	err := s.withLockedState(true, func(state *model.State) error {
		if findBackupBackendBySlug(state, backend.TenantID, backend.Slug) >= 0 {
			return ErrConflict
		}
		if backend.ID == "" {
			backend.ID = model.NewID("backup_backend")
		}
		now := time.Now().UTC()
		if backend.CreatedAt.IsZero() {
			backend.CreatedAt = now
		}
		backend.UpdatedAt = now
		if dataBackendCredentialsPresent(credentials) {
			secret, err := encryptBackupBackendSecret(model.BackupBackendSecret{
				ID:        model.NewID("backup_backend_secret"),
				TenantID:  backend.TenantID,
				BackendID: backend.ID,
			}, credentials)
			if err != nil {
				return err
			}
			backend.CredentialSecretID = secret.ID
			state.BackupBackendSecrets = append(state.BackupBackendSecrets, secret)
		}
		state.BackupBackends = append(state.BackupBackends, backend)
		ensureDefaultBackupPolicyInState(state)
		return nil
	})
	if err != nil {
		return model.BackupBackend{}, err
	}
	return model.RedactBackupBackendCredentials(backend), nil
}

func validateBackupBackend(backend model.BackupBackend) error {
	switch model.NormalizeDataBackendProvider(backend.Provider) {
	case model.DataBackendProviderCloudflareR2, model.DataBackendProviderBackblazeB2, model.DataBackendProviderS3, model.DataBackendProviderMinIO:
	default:
		return ErrInvalidInput
	}
	if strings.TrimSpace(backend.Bucket) == "" {
		return ErrInvalidInput
	}
	return nil
}

func (s *Store) RotateBackupBackendCredentials(idOrName, tenantID string, platformAdmin bool, credentials model.DataBackendCredentials) (model.BackupBackend, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" || !dataBackendCredentialsPresent(credentials) {
		return model.BackupBackend{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRotateBackupBackendCredentials(idOrName, tenantID, platformAdmin, credentials)
	}
	var rotated model.BackupBackend
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOwnedBackupBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		backend := model.NormalizeBackupBackend(state.BackupBackends[index])
		secretIndex := findBackupBackendSecret(state, backend.CredentialSecretID, backend.ID)
		secret := model.BackupBackendSecret{
			ID:        model.NewID("backup_backend_secret"),
			TenantID:  backend.TenantID,
			BackendID: backend.ID,
			CreatedAt: now,
		}
		if secretIndex >= 0 {
			secret = state.BackupBackendSecrets[secretIndex]
		}
		encrypted, err := encryptBackupBackendSecret(secret, credentials)
		if err != nil {
			return err
		}
		backend.CredentialSecretID = encrypted.ID
		backend.Credentials = redactAccessKeyOnly(credentials)
		backend.UpdatedAt = now
		if secretIndex >= 0 {
			state.BackupBackendSecrets[secretIndex] = encrypted
		} else {
			state.BackupBackendSecrets = append(state.BackupBackendSecrets, encrypted)
		}
		state.BackupBackends[index] = backend
		rotated = model.RedactBackupBackendCredentials(backend)
		return nil
	})
	return rotated, err
}

func (s *Store) DeleteBackupBackend(idOrName, tenantID string, platformAdmin bool) (model.BackupBackend, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" || idOrName == defaultBackupBackendID || strings.EqualFold(idOrName, "fugue-default-r2") {
		return model.BackupBackend{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteBackupBackend(idOrName, tenantID, platformAdmin)
	}
	var deleted model.BackupBackend
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOwnedBackupBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		for _, policy := range state.BackupPolicies {
			if policy.BackendID == state.BackupBackends[index].ID {
				return ErrConflict
			}
		}
		deleted = state.BackupBackends[index]
		state.BackupBackends = append(state.BackupBackends[:index], state.BackupBackends[index+1:]...)
		state.BackupBackendSecrets = deleteBackupBackendSecretsByBackend(state.BackupBackendSecrets, deleted.ID)
		ensureDefaultBackupPolicyInState(state)
		return nil
	})
	if err != nil {
		return model.BackupBackend{}, err
	}
	return model.RedactBackupBackendCredentials(deleted), nil
}

func (s *Store) RecordBackupBackendTest(idOrName, tenantID string, platformAdmin bool, success bool, message string) (model.BackupBackend, error) {
	if s.usingDatabase() {
		return s.pgRecordBackupBackendTest(idOrName, tenantID, platformAdmin, success, message)
	}
	var updated model.BackupBackend
	err := s.withLockedState(true, func(state *model.State) error {
		index := findOwnedBackupBackendByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		backend := model.NormalizeBackupBackend(state.BackupBackends[index])
		backend.LastTestedAt = &now
		if success {
			backend.LastTestResult = "ok"
			backend.ErrorMessage = ""
		} else {
			backend.LastTestResult = "failed"
			backend.ErrorMessage = strings.TrimSpace(message)
		}
		backend.UpdatedAt = now
		state.BackupBackends[index] = backend
		updated = model.RedactBackupBackendCredentials(backend)
		return nil
	})
	return updated, err
}

func (s *Store) ListBackupPolicies(filter BackupPolicyFilter) ([]model.BackupPolicy, error) {
	filter.TargetType = normalizeBackupTargetTypeFilter(filter.TargetType)
	if s.usingDatabase() {
		return s.pgListBackupPolicies(filter)
	}
	policies := []model.BackupPolicy{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, policy := range state.BackupPolicies {
			policy = model.NormalizeBackupPolicy(policy)
			if !backupPolicyVisible(policy, filter.TenantID, filter.PlatformAdmin) {
				continue
			}
			if !filter.IncludeDisabled && (!policy.Enabled || policy.Status == model.BackupPolicyStatusDisabled) {
				continue
			}
			if filter.ProjectID != "" && policy.ProjectID != filter.ProjectID && policy.Target.ProjectID != filter.ProjectID {
				continue
			}
			if filter.AppID != "" && policy.AppID != filter.AppID && policy.Target.AppID != filter.AppID {
				continue
			}
			if filter.TargetType != "" && policy.Target.Type != filter.TargetType {
				continue
			}
			policies = append(policies, policy)
		}
		sortBackupPolicies(policies)
		policies = limitBackupPolicies(policies, filter.Limit)
		return nil
	})
	return policies, err
}

func (s *Store) GetBackupPolicy(idOrName, tenantID string, platformAdmin bool) (model.BackupPolicy, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return model.BackupPolicy{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetBackupPolicy(idOrName, tenantID, platformAdmin)
	}
	var policy model.BackupPolicy
	err := s.withLockedState(false, func(state *model.State) error {
		index := findBackupPolicyByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		policy = model.NormalizeBackupPolicy(state.BackupPolicies[index])
		return nil
	})
	return policy, err
}

func (s *Store) UpsertBackupPolicy(policy model.BackupPolicy) (model.BackupPolicy, error) {
	policy = model.NormalizeBackupPolicy(policy)
	if policy.Schedule == "" {
		policy.Schedule = model.BackupDefaultSchedule
	}
	if policy.Name == "" || policy.Target.Type == "" {
		return model.BackupPolicy{}, ErrInvalidInput
	}
	if policy.Enabled {
		if err := backupschedule.Validate(policy.Schedule); err != nil {
			return model.BackupPolicy{}, fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, err)
		}
	}
	if s.usingDatabase() {
		return s.pgUpsertBackupPolicy(policy)
	}
	var saved model.BackupPolicy
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		if policy.ID == "" {
			policy.ID = model.NewID("backup_policy")
		}
		if policy.CreatedAt.IsZero() {
			policy.CreatedAt = now
		}
		policy.UpdatedAt = now
		if policy.BackendID != "" && findBackupBackend(state, policy.BackendID) < 0 {
			return ErrNotFound
		}
		index := findBackupPolicy(state, policy.ID)
		if index >= 0 && state.BackupPolicies[index].Schedule != policy.Schedule {
			policy.NextRunAt = nil
		}
		if policy.Enabled && policy.NextRunAt == nil {
			next, err := nextBackupRunAfter(policy.Schedule, now)
			if err != nil {
				return fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, err)
			}
			policy.NextRunAt = next
		} else if !policy.Enabled {
			policy.NextRunAt = nil
		}
		if index < 0 {
			if findBackupPolicyBySlug(state, policy.TenantID, policy.ProjectID, policy.AppID, policy.Slug) >= 0 {
				return ErrConflict
			}
			state.BackupPolicies = append(state.BackupPolicies, policy)
		} else {
			existing := state.BackupPolicies[index]
			if existing.ID == defaultControlPlaneBackupPolicyID && policy.Target.Type != model.BackupTargetControlPlaneDatabase {
				return ErrInvalidInput
			}
			policy.CreatedAt = existing.CreatedAt
			state.BackupPolicies[index] = policy
		}
		saved = policy
		return nil
	})
	return saved, err
}

func (s *Store) SetBackupPolicyEnabled(idOrName, tenantID string, platformAdmin bool, enabled bool, reason string) (model.BackupPolicy, error) {
	if s.usingDatabase() {
		return s.pgSetBackupPolicyEnabled(idOrName, tenantID, platformAdmin, enabled, reason)
	}
	var updated model.BackupPolicy
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackupPolicyByIDNameOrSlug(state, idOrName, tenantID, platformAdmin)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		policy := model.NormalizeBackupPolicy(state.BackupPolicies[index])
		policy.Enabled = enabled
		if enabled {
			if err := backupschedule.Validate(policy.Schedule); err != nil {
				return fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, err)
			}
			if policy.BackendID == "" {
				policy.Status = model.BackupPolicyStatusBlockedNoBackend
				policy.DisabledReason = "backup backend is not configured"
			} else {
				policy.Status = model.BackupPolicyStatusActive
				policy.DisabledReason = ""
			}
			next, err := nextBackupRunAfter(policy.Schedule, now)
			if err != nil {
				return fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, err)
			}
			policy.NextRunAt = next
		} else {
			policy.Status = model.BackupPolicyStatusDisabled
			policy.DisabledReason = strings.TrimSpace(reason)
			policy.NextRunAt = nil
		}
		policy.UpdatedAt = now
		state.BackupPolicies[index] = model.NormalizeBackupPolicy(policy)
		updated = state.BackupPolicies[index]
		return nil
	})
	return updated, err
}

func (s *Store) ListDueBackupPolicies(now time.Time, limit int) ([]model.BackupPolicy, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 {
		limit = defaultBackupDueScannerLimit
	}
	if s.usingDatabase() {
		return s.pgListDueBackupPolicies(now, limit)
	}
	policies := []model.BackupPolicy{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, policy := range state.BackupPolicies {
			policy = model.NormalizeBackupPolicy(policy)
			if !policy.Enabled || policy.Status != model.BackupPolicyStatusActive {
				continue
			}
			dueAt := policy.NextRunAt
			if dueAt == nil {
				var err error
				dueAt, err = nextBackupRunAfter(policy.Schedule, now.Add(-time.Hour))
				if err != nil {
					continue
				}
			}
			if dueAt == nil || dueAt.After(now) {
				continue
			}
			policies = append(policies, policy)
		}
		sort.Slice(policies, func(i, j int) bool {
			left, right := policies[i].NextRunAt, policies[j].NextRunAt
			if left == nil {
				return true
			}
			if right == nil {
				return false
			}
			return left.Before(*right)
		})
		policies = limitBackupPolicies(policies, limit)
		return nil
	})
	return policies, err
}

func (s *Store) CreateBackupRun(run model.BackupRun) (model.BackupRun, error) {
	run = model.NormalizeBackupRun(run)
	if run.Target.Type == "" {
		return model.BackupRun{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateBackupRun(run)
	}
	var created model.BackupRun
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		if run.PolicyID != "" {
			index := findBackupPolicy(state, run.PolicyID)
			if index < 0 {
				return ErrNotFound
			}
			policy := model.NormalizeBackupPolicy(state.BackupPolicies[index])
			if !policy.Enabled || policy.Status != model.BackupPolicyStatusActive {
				return ErrConflict
			}
			if err := validateScheduledBackupPolicyDue(run, policy, now); err != nil {
				return err
			}
			if run.TenantID == "" {
				run.TenantID = policy.TenantID
			}
			if run.ProjectID == "" {
				run.ProjectID = policy.ProjectID
			}
			if run.AppID == "" {
				run.AppID = policy.AppID
			}
			if run.BackendID == "" {
				run.BackendID = policy.BackendID
			}
			if run.Target.Type == "" || run.Target.Type == model.BackupTargetControlPlaneDatabase && policy.Target.Type != "" {
				run.Target = policy.Target
			}
			if run.Version == "" {
				run.Version = policy.Version
			}
		}
		if run.BackendID == "" {
			run.Status = model.BackupRunStatusBlocked
			run.ErrorCode = "backup_backend_missing"
			run.ErrorMessage = "backup backend is not configured"
		} else if findBackupBackend(state, run.BackendID) < 0 {
			return ErrNotFound
		}
		if backupTargetHasActiveRun(state.BackupRuns, run.Target) {
			return ErrConflict
		}
		if run.ID == "" {
			run.ID = model.NewID("backup_run")
		}
		if run.CreatedAt.IsZero() {
			run.CreatedAt = now
		}
		var policyNextRunAt *time.Time
		if run.PolicyID != "" {
			index := findBackupPolicy(state, run.PolicyID)
			if index < 0 {
				return ErrNotFound
			}
			next, err := nextBackupRunAfter(state.BackupPolicies[index].Schedule, now)
			if err != nil {
				return fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, err)
			}
			policyNextRunAt = next
		}
		run.UpdatedAt = now
		run = model.NormalizeBackupRun(run)
		state.BackupRuns = append(state.BackupRuns, run)
		if run.PolicyID != "" {
			if index := findBackupPolicy(state, run.PolicyID); index >= 0 {
				policy := model.NormalizeBackupPolicy(state.BackupPolicies[index])
				policy.LastRunID = run.ID
				policy.LastRunAt = &now
				policy.NextRunAt = policyNextRunAt
				policy.UpdatedAt = now
				state.BackupPolicies[index] = policy
			}
		}
		created = run
		return nil
	})
	return created, err
}

func (s *Store) ClaimBackupRun(id, leaseOwner string, now time.Time, leaseTTL time.Duration) (model.BackupRun, error) {
	id = strings.TrimSpace(id)
	leaseOwner = strings.TrimSpace(leaseOwner)
	if id == "" || leaseOwner == "" || leaseTTL <= 0 {
		return model.BackupRun{}, ErrInvalidInput
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if s.usingDatabase() {
		return s.pgClaimBackupRun(id, leaseOwner, now, leaseTTL)
	}
	var claimed model.BackupRun
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackupRun(state, id)
		if index < 0 {
			return ErrConflict
		}
		run := model.NormalizeBackupRun(state.BackupRuns[index])
		if run.Status != model.BackupRunStatusPending || run.NextRetryAt != nil && run.NextRetryAt.After(now) {
			return ErrConflict
		}
		lockedUntil := now.Add(leaseTTL)
		run.Status = model.BackupRunStatusRunning
		run.LeaseOwner = leaseOwner
		run.LockedUntil = &lockedUntil
		run.HeartbeatAt = &now
		run.StartedAt = &now
		run.UpdatedAt = now
		state.BackupRuns[index] = model.NormalizeBackupRun(run)
		claimed = state.BackupRuns[index]
		return nil
	})
	return claimed, err
}

func (s *Store) HeartbeatBackupRun(id, leaseOwner string, now time.Time, leaseTTL time.Duration) (model.BackupRun, error) {
	id = strings.TrimSpace(id)
	leaseOwner = strings.TrimSpace(leaseOwner)
	if id == "" || leaseOwner == "" || leaseTTL <= 0 {
		return model.BackupRun{}, ErrInvalidInput
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if s.usingDatabase() {
		return s.pgHeartbeatBackupRun(id, leaseOwner, now, leaseTTL)
	}
	var heartbeat model.BackupRun
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackupRun(state, id)
		if index < 0 {
			return ErrConflict
		}
		run := model.NormalizeBackupRun(state.BackupRuns[index])
		if run.Status != model.BackupRunStatusRunning || run.LeaseOwner != leaseOwner || run.LockedUntil == nil || run.LockedUntil.Before(now) {
			return ErrConflict
		}
		lockedUntil := now.Add(leaseTTL)
		run.LockedUntil = &lockedUntil
		run.HeartbeatAt = &now
		run.UpdatedAt = now
		state.BackupRuns[index] = model.NormalizeBackupRun(run)
		heartbeat = state.BackupRuns[index]
		return nil
	})
	return heartbeat, err
}

// BackupRunIsStale reports whether an active backup run has exceeded its
// observed lease or activity grace period. Callers that act on a listed run
// must still use RecoverStaleBackupRun so the observation is checked again
// atomically with the state transition.
func BackupRunIsStale(run model.BackupRun, now time.Time, leaseTTL time.Duration) bool {
	if leaseTTL <= 0 {
		return false
	}
	run = model.NormalizeBackupRun(run)
	now = now.UTC()
	if run.Status != model.BackupRunStatusRunning && run.Status != model.BackupRunStatusPending {
		return false
	}
	if run.Status == model.BackupRunStatusPending && run.Trigger == model.BackupRunTriggerRetry && run.NextRetryAt != nil && run.NextRetryAt.After(now) {
		return false
	}
	deadline, ok := backupRunStaleDeadline(run, leaseTTL)
	return ok && deadline.Before(now)
}

func backupRunStaleDeadline(run model.BackupRun, leaseTTL time.Duration) (time.Time, bool) {
	run = model.NormalizeBackupRun(run)
	if run.Status != model.BackupRunStatusRunning && run.Status != model.BackupRunStatusPending {
		return time.Time{}, false
	}
	if run.LockedUntil != nil {
		return run.LockedUntil.UTC(), true
	}
	lastSeen := run.UpdatedAt
	if run.HeartbeatAt != nil {
		lastSeen = *run.HeartbeatAt
	}
	if run.Status == model.BackupRunStatusPending && run.Trigger == model.BackupRunTriggerRetry && run.NextRetryAt != nil && run.NextRetryAt.After(lastSeen) {
		lastSeen = *run.NextRetryAt
	}
	if lastSeen.IsZero() {
		lastSeen = run.CreatedAt
	}
	if lastSeen.IsZero() {
		return time.Time{}, false
	}
	return lastSeen.Add(leaseTTL), true
}

// RecoverStaleBackupRun atomically fails the exact active-run observation
// supplied by a stale-run scanner. A heartbeat, claim, or competing recovery
// changes at least one compared lease/version field and causes ErrConflict.
func (s *Store) RecoverStaleBackupRun(observed model.BackupRun, now time.Time, leaseTTL time.Duration) (model.BackupRun, error) {
	observed = model.NormalizeBackupRun(observed)
	if strings.TrimSpace(observed.ID) == "" || observed.UpdatedAt.IsZero() || leaseTTL <= 0 ||
		(observed.Status != model.BackupRunStatusRunning && observed.Status != model.BackupRunStatusPending) {
		return model.BackupRun{}, ErrInvalidInput
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if !BackupRunIsStale(observed, now, leaseTTL) {
		return model.BackupRun{}, ErrConflict
	}
	if s.usingDatabase() {
		return s.pgRecoverStaleBackupRun(observed, now)
	}
	var recovered model.BackupRun
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackupRun(state, observed.ID)
		if index < 0 {
			return ErrConflict
		}
		current := model.NormalizeBackupRun(state.BackupRuns[index])
		if !backupRunRecoveryObservationMatches(current, observed) || !BackupRunIsStale(current, now, leaseTTL) {
			return ErrConflict
		}
		current.Status = model.BackupRunStatusFailed
		current.LockedUntil = nil
		current.HeartbeatAt = &now
		current.ErrorCode = backupRunLostErrorCode
		current.ErrorMessage = backupRunLostErrorMessage
		current.FinishedAt = &now
		current.UpdatedAt = now
		state.BackupRuns[index] = model.NormalizeBackupRun(current)
		recovered = state.BackupRuns[index]
		return nil
	})
	return recovered, err
}

func backupRunRecoveryObservationMatches(current, observed model.BackupRun) bool {
	return current.Status == observed.Status &&
		current.LeaseOwner == observed.LeaseOwner &&
		current.UpdatedAt.Equal(observed.UpdatedAt) &&
		backupOptionalTimeEqual(current.LockedUntil, observed.LockedUntil) &&
		backupOptionalTimeEqual(current.HeartbeatAt, observed.HeartbeatAt) &&
		backupOptionalTimeEqual(current.NextRetryAt, observed.NextRetryAt)
}

func backupOptionalTimeEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func (s *Store) FinishBackupRun(id, leaseOwner string, finish BackupRunFinish) (model.BackupRun, error) {
	id = strings.TrimSpace(id)
	leaseOwner = strings.TrimSpace(leaseOwner)
	finish.Status = strings.TrimSpace(strings.ToLower(finish.Status))
	finish.ErrorCode = strings.TrimSpace(finish.ErrorCode)
	finish.ErrorMessage = strings.TrimSpace(finish.ErrorMessage)
	if id == "" || leaseOwner == "" || !backupRunFinishStatus(finish.Status) || finish.BytesWritten < 0 || finish.ArtifactCount < 0 {
		return model.BackupRun{}, ErrInvalidInput
	}
	if finish.FinishedAt.IsZero() {
		finish.FinishedAt = time.Now().UTC()
	} else {
		finish.FinishedAt = finish.FinishedAt.UTC()
	}
	if s.usingDatabase() {
		return s.pgFinishBackupRun(id, leaseOwner, finish)
	}
	var finished model.BackupRun
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackupRun(state, id)
		if index < 0 {
			return ErrConflict
		}
		run := model.NormalizeBackupRun(state.BackupRuns[index])
		if run.Status != model.BackupRunStatusRunning || run.LeaseOwner != leaseOwner || run.LockedUntil == nil || run.LockedUntil.Before(finish.FinishedAt) {
			return ErrConflict
		}
		run.Status = finish.Status
		run.LockedUntil = nil
		run.HeartbeatAt = &finish.FinishedAt
		run.BytesWritten = finish.BytesWritten
		run.ArtifactCount = finish.ArtifactCount
		run.ErrorCode = finish.ErrorCode
		run.ErrorMessage = finish.ErrorMessage
		run.FinishedAt = &finish.FinishedAt
		run.UpdatedAt = finish.FinishedAt
		state.BackupRuns[index] = model.NormalizeBackupRun(run)
		finished = state.BackupRuns[index]
		if finished.Status == model.BackupRunStatusSucceeded && finished.PolicyID != "" {
			if policyIndex := findBackupPolicy(state, finished.PolicyID); policyIndex >= 0 {
				policy := model.NormalizeBackupPolicy(state.BackupPolicies[policyIndex])
				policy.LastSuccessfulRunID = finished.ID
				policy.LastSuccessfulAt = &finish.FinishedAt
				policy.UpdatedAt = finish.FinishedAt
				state.BackupPolicies[policyIndex] = policy
			}
			applyBackupRetentionInState(state, finished.PolicyID)
		}
		return nil
	})
	return finished, err
}

func (s *Store) ListBackupRuns(filter BackupRunFilter) ([]model.BackupRun, error) {
	filter.TargetType = normalizeBackupTargetTypeFilter(filter.TargetType)
	if s.usingDatabase() {
		return s.pgListBackupRuns(filter)
	}
	runs := []model.BackupRun{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, run := range state.BackupRuns {
			run = model.NormalizeBackupRun(run)
			if !backupRunVisible(run, filter.TenantID, filter.PlatformAdmin) {
				continue
			}
			if filter.ProjectID != "" && run.ProjectID != filter.ProjectID && run.Target.ProjectID != filter.ProjectID {
				continue
			}
			if filter.AppID != "" && run.AppID != filter.AppID && run.Target.AppID != filter.AppID {
				continue
			}
			if filter.PolicyID != "" && run.PolicyID != filter.PolicyID {
				continue
			}
			if filter.TargetType != "" && run.Target.Type != filter.TargetType {
				continue
			}
			if filter.Status != "" && run.Status != filter.Status {
				continue
			}
			runs = append(runs, run)
		}
		sortBackupRuns(runs)
		runs = limitBackupRuns(runs, filter.Limit)
		return nil
	})
	return runs, err
}

// ListDueBackupRetryRuns filters and orders due retries before applying the
// scan limit so newer future work cannot starve an older runnable retry.
func (s *Store) ListDueBackupRetryRuns(now time.Time, limit int) ([]model.BackupRun, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if limit <= 0 {
		limit = defaultBackupDueScannerLimit
	}
	if s.usingDatabase() {
		return s.pgListDueBackupRetryRuns(now, limit)
	}
	runs := []model.BackupRun{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, run := range state.BackupRuns {
			run = model.NormalizeBackupRun(run)
			if run.Status != model.BackupRunStatusPending || run.Trigger != model.BackupRunTriggerRetry || run.NextRetryAt == nil || run.NextRetryAt.After(now) {
				continue
			}
			runs = append(runs, run)
		}
		sort.Slice(runs, func(i, j int) bool {
			if !runs[i].NextRetryAt.Equal(*runs[j].NextRetryAt) {
				return runs[i].NextRetryAt.Before(*runs[j].NextRetryAt)
			}
			return runs[i].CreatedAt.Before(runs[j].CreatedAt)
		})
		if len(runs) > limit {
			runs = runs[:limit]
		}
		return nil
	})
	return runs, err
}

// ListStaleBackupRuns filters and orders expired observations before applying
// the scan limit. RecoverStaleBackupRun remains the authoritative CAS.
func (s *Store) ListStaleBackupRuns(now time.Time, leaseTTL time.Duration, limit int) ([]model.BackupRun, error) {
	if leaseTTL <= 0 {
		return nil, ErrInvalidInput
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if limit <= 0 {
		limit = defaultBackupDueScannerLimit
	}
	if s.usingDatabase() {
		return s.pgListStaleBackupRuns(now, leaseTTL, limit)
	}
	runs := []model.BackupRun{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, run := range state.BackupRuns {
			run = model.NormalizeBackupRun(run)
			if BackupRunIsStale(run, now, leaseTTL) {
				runs = append(runs, run)
			}
		}
		sort.Slice(runs, func(i, j int) bool {
			left, _ := backupRunStaleDeadline(runs[i], leaseTTL)
			right, _ := backupRunStaleDeadline(runs[j], leaseTTL)
			if !left.Equal(right) {
				return left.Before(right)
			}
			return runs[i].CreatedAt.Before(runs[j].CreatedAt)
		})
		if len(runs) > limit {
			runs = runs[:limit]
		}
		return nil
	})
	return runs, err
}

func (s *Store) GetBackupRun(id string, tenantID string, platformAdmin bool) (model.BackupRun, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.BackupRun{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetBackupRun(id, tenantID, platformAdmin)
	}
	var run model.BackupRun
	err := s.withLockedState(false, func(state *model.State) error {
		index := findBackupRun(state, id)
		if index < 0 {
			return ErrNotFound
		}
		candidate := model.NormalizeBackupRun(state.BackupRuns[index])
		if !backupRunVisible(candidate, tenantID, platformAdmin) {
			return ErrNotFound
		}
		run = candidate
		return nil
	})
	return run, err
}

func (s *Store) UpdateBackupRun(id string, update BackupRunUpdate) (model.BackupRun, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.BackupRun{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateBackupRun(id, update)
	}
	var updated model.BackupRun
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackupRun(state, id)
		if index < 0 {
			return ErrNotFound
		}
		run := model.NormalizeBackupRun(state.BackupRuns[index])
		applyBackupRunUpdate(&run, update)
		run.UpdatedAt = time.Now().UTC()
		state.BackupRuns[index] = model.NormalizeBackupRun(run)
		updated = state.BackupRuns[index]
		return nil
	})
	return updated, err
}

func (s *Store) CreateBackupArtifact(artifact model.BackupArtifact) (model.BackupArtifact, error) {
	return s.createBackupArtifact(artifact, "")
}

// CreateBackupArtifactForRun persists an artifact only while the creating
// worker still owns an unexpired lease for the linked run.
func (s *Store) CreateBackupArtifactForRun(artifact model.BackupArtifact, leaseOwner string) (model.BackupArtifact, error) {
	leaseOwner = strings.TrimSpace(leaseOwner)
	artifact.RunID = strings.TrimSpace(artifact.RunID)
	if artifact.RunID == "" || leaseOwner == "" {
		return model.BackupArtifact{}, ErrInvalidInput
	}
	return s.createBackupArtifact(artifact, leaseOwner)
}

func (s *Store) createBackupArtifact(artifact model.BackupArtifact, leaseOwner string) (model.BackupArtifact, error) {
	artifact = model.NormalizeBackupArtifact(artifact)
	if artifact.Kind == "" || artifact.Target.Type == "" {
		return model.BackupArtifact{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateBackupArtifact(artifact, leaseOwner)
	}
	var created model.BackupArtifact
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		if leaseOwner != "" {
			runIndex := findBackupRun(state, artifact.RunID)
			if runIndex < 0 {
				return ErrConflict
			}
			run := model.NormalizeBackupRun(state.BackupRuns[runIndex])
			if run.Status != model.BackupRunStatusRunning || run.LeaseOwner != leaseOwner || run.LockedUntil == nil || run.LockedUntil.Before(now) {
				return ErrConflict
			}
			if artifact.PolicyID == "" {
				artifact.PolicyID = run.PolicyID
			} else if artifact.PolicyID != run.PolicyID {
				return ErrConflict
			}
		}
		if artifact.ID == "" {
			artifact.ID = model.NewID("backup_artifact")
		}
		if artifact.CreatedAt.IsZero() {
			artifact.CreatedAt = now
		}
		artifact.Manifest.ArtifactID = artifact.ID
		artifact.Manifest.RunID = artifact.RunID
		artifact.Manifest.PolicyID = artifact.PolicyID
		artifact.Manifest.Target = artifact.Target
		artifact.Manifest.Kind = artifact.Kind
		artifact.Manifest.Version = artifact.Version
		artifact.Manifest.ObjectKey = artifact.ObjectKey
		artifact.Manifest.ManifestObjectKey = artifact.ManifestObjectKey
		artifact.Manifest.SizeBytes = artifact.SizeBytes
		artifact.Manifest.LogicalBytes = artifact.LogicalBytes
		artifact.Manifest.SHA256 = artifact.SHA256
		if artifact.Manifest.CreatedAt.IsZero() {
			artifact.Manifest.CreatedAt = artifact.CreatedAt
		}
		artifact.Manifest = model.NormalizeBackupManifest(artifact.Manifest)
		artifact.ManifestDigest = digestBackupManifest(artifact.Manifest)
		artifact = model.NormalizeBackupArtifact(artifact)
		state.BackupArtifacts = append(state.BackupArtifacts, artifact)
		if artifact.RunID != "" {
			if index := findBackupRun(state, artifact.RunID); index >= 0 {
				run := model.NormalizeBackupRun(state.BackupRuns[index])
				run.ArtifactCount++
				run.BytesWritten += artifact.SizeBytes
				run.LogicalBytes += artifact.LogicalBytes
				run.UpdatedAt = now
				state.BackupRuns[index] = run
			}
		}
		created = artifact
		return nil
	})
	return created, err
}

func (s *Store) ListBackupArtifacts(filter BackupArtifactFilter) ([]model.BackupArtifact, error) {
	filter.TargetType = normalizeBackupTargetTypeFilter(filter.TargetType)
	if s.usingDatabase() {
		return s.pgListBackupArtifacts(filter)
	}
	artifacts := []model.BackupArtifact{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, artifact := range state.BackupArtifacts {
			artifact = model.NormalizeBackupArtifact(artifact)
			if !backupArtifactVisible(artifact, filter.TenantID, filter.PlatformAdmin) {
				continue
			}
			if filter.ActiveOnly && artifact.Status != model.BackupArtifactStatusActive {
				continue
			}
			if filter.ProjectID != "" && artifact.ProjectID != filter.ProjectID && artifact.Target.ProjectID != filter.ProjectID {
				continue
			}
			if filter.AppID != "" && artifact.AppID != filter.AppID && artifact.Target.AppID != filter.AppID {
				continue
			}
			if filter.PolicyID != "" && artifact.PolicyID != filter.PolicyID {
				continue
			}
			if filter.RunID != "" && artifact.RunID != filter.RunID {
				continue
			}
			if filter.TargetType != "" && artifact.Target.Type != filter.TargetType {
				continue
			}
			artifacts = append(artifacts, artifact)
		}
		sortBackupArtifacts(artifacts)
		artifacts = limitBackupArtifacts(artifacts, filter.Limit)
		return nil
	})
	return artifacts, err
}

func (s *Store) GetBackupArtifact(id string, tenantID string, platformAdmin bool) (model.BackupArtifact, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.BackupArtifact{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetBackupArtifact(id, tenantID, platformAdmin)
	}
	var artifact model.BackupArtifact
	err := s.withLockedState(false, func(state *model.State) error {
		index := findBackupArtifact(state, id)
		if index < 0 {
			return ErrNotFound
		}
		candidate := model.NormalizeBackupArtifact(state.BackupArtifacts[index])
		if !backupArtifactVisible(candidate, tenantID, platformAdmin) {
			return ErrNotFound
		}
		artifact = candidate
		return nil
	})
	return artifact, err
}

func (s *Store) MarkBackupArtifactDeleted(id, tenantID string, platformAdmin bool) (model.BackupArtifact, error) {
	id = strings.TrimSpace(id)
	tenantID = strings.TrimSpace(tenantID)
	if id == "" {
		return model.BackupArtifact{}, ErrInvalidInput
	}
	if !platformAdmin && tenantID == "" {
		return model.BackupArtifact{}, ErrNotFound
	}
	if s.usingDatabase() {
		return s.pgMarkBackupArtifactDeleted(id, tenantID, platformAdmin)
	}
	var deleted model.BackupArtifact
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackupArtifact(state, id)
		if index < 0 {
			return ErrNotFound
		}
		artifact := model.NormalizeBackupArtifact(state.BackupArtifacts[index])
		if !platformAdmin && artifact.TenantID != tenantID {
			return ErrNotFound
		}
		if artifact.Protected {
			return ErrConflict
		}
		now := time.Now().UTC()
		artifact.Status = model.BackupArtifactStatusDeleted
		artifact.DeletedAt = &now
		state.BackupArtifacts[index] = artifact
		deleted = artifact
		return nil
	})
	return deleted, err
}

func (s *Store) CreateBackupRestorePlan(plan model.BackupRestorePlan) (model.BackupRestorePlan, error) {
	plan.ArtifactID = strings.TrimSpace(plan.ArtifactID)
	if plan.ArtifactID == "" {
		return model.BackupRestorePlan{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateBackupRestorePlan(plan)
	}
	var created model.BackupRestorePlan
	err := s.withLockedState(true, func(state *model.State) error {
		artifactIndex := findBackupArtifact(state, plan.ArtifactID)
		if artifactIndex < 0 {
			return ErrNotFound
		}
		artifact := model.NormalizeBackupArtifact(state.BackupArtifacts[artifactIndex])
		now := time.Now().UTC()
		if plan.ID == "" {
			plan.ID = model.NewID("backup_restore_plan")
		}
		if backupTargetIsEmpty(plan.Target) {
			plan.Target = artifact.Target
		} else if strings.TrimSpace(plan.Target.Type) == "" {
			return ErrInvalidInput
		}
		if plan.TenantID == "" {
			plan.TenantID = artifact.TenantID
		}
		if plan.ProjectID == "" {
			plan.ProjectID = artifact.ProjectID
		}
		if plan.AppID == "" {
			plan.AppID = artifact.AppID
		}
		if plan.CreatedAt.IsZero() {
			plan.CreatedAt = now
		}
		plan.UpdatedAt = now
		plan.Mode = model.NormalizeBackupRestoreMode(plan.Mode)
		if len(plan.Phases) == 0 {
			plan.Phases = defaultRestorePlanPhases(plan)
		}
		plan = model.NormalizeBackupRestorePlan(plan)
		state.BackupRestorePlans = append(state.BackupRestorePlans, plan)
		created = plan
		return nil
	})
	return created, err
}

func (s *Store) ListBackupRestorePlans(tenantID string, platformAdmin bool, limit int) ([]model.BackupRestorePlan, error) {
	if s.usingDatabase() {
		return s.pgListBackupRestorePlans(tenantID, platformAdmin, limit)
	}
	plans := []model.BackupRestorePlan{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, plan := range state.BackupRestorePlans {
			plan = model.NormalizeBackupRestorePlan(plan)
			if !backupRestorePlanVisible(plan, tenantID, platformAdmin) {
				continue
			}
			plans = append(plans, plan)
		}
		sort.Slice(plans, func(i, j int) bool { return plans[i].CreatedAt.After(plans[j].CreatedAt) })
		plans = limitBackupRestorePlans(plans, limit)
		return nil
	})
	return plans, err
}

func (s *Store) GetBackupRestorePlan(id string, tenantID string, platformAdmin bool) (model.BackupRestorePlan, error) {
	if s.usingDatabase() {
		return s.pgGetBackupRestorePlan(id, tenantID, platformAdmin)
	}
	var plan model.BackupRestorePlan
	err := s.withLockedState(false, func(state *model.State) error {
		index := findBackupRestorePlan(state, id)
		if index < 0 {
			return ErrNotFound
		}
		candidate := model.NormalizeBackupRestorePlan(state.BackupRestorePlans[index])
		if !backupRestorePlanVisible(candidate, tenantID, platformAdmin) {
			return ErrNotFound
		}
		plan = candidate
		return nil
	})
	return plan, err
}

func (s *Store) CreateBackupRestoreRun(run model.BackupRestoreRun) (model.BackupRestoreRun, error) {
	run = model.NormalizeBackupRestoreRun(run)
	if run.PlanID == "" {
		return model.BackupRestoreRun{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateBackupRestoreRun(run)
	}
	var created model.BackupRestoreRun
	err := s.withLockedState(true, func(state *model.State) error {
		planIndex := findBackupRestorePlan(state, run.PlanID)
		if planIndex < 0 {
			return ErrNotFound
		}
		plan := model.NormalizeBackupRestorePlan(state.BackupRestorePlans[planIndex])
		now := time.Now().UTC()
		if run.ID == "" {
			run.ID = model.NewID("backup_restore_run")
		}
		run.TenantID = firstNonEmpty(run.TenantID, plan.TenantID)
		run.ProjectID = firstNonEmpty(run.ProjectID, plan.ProjectID)
		run.AppID = firstNonEmpty(run.AppID, plan.AppID)
		run.ArtifactID = firstNonEmpty(run.ArtifactID, plan.ArtifactID)
		run.Mode = firstNonEmpty(run.Mode, plan.Mode)
		if run.CreatedAt.IsZero() {
			run.CreatedAt = now
		}
		run.UpdatedAt = now
		if len(run.Phases) == 0 {
			run.Phases = plan.Phases
		}
		run = model.NormalizeBackupRestoreRun(run)
		state.BackupRestoreRuns = append(state.BackupRestoreRuns, run)
		plan.Status = model.BackupRestoreStatusRunning
		plan.UpdatedAt = now
		state.BackupRestorePlans[planIndex] = plan
		created = run
		return nil
	})
	return created, err
}

func (s *Store) ListBackupRestoreRuns(tenantID string, platformAdmin bool, limit int) ([]model.BackupRestoreRun, error) {
	if s.usingDatabase() {
		return s.pgListBackupRestoreRuns(tenantID, platformAdmin, limit)
	}
	runs := []model.BackupRestoreRun{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, run := range state.BackupRestoreRuns {
			run = model.NormalizeBackupRestoreRun(run)
			if !backupRestoreRunVisible(run, tenantID, platformAdmin) {
				continue
			}
			runs = append(runs, run)
		}
		sort.Slice(runs, func(i, j int) bool { return runs[i].CreatedAt.After(runs[j].CreatedAt) })
		runs = limitBackupRestoreRuns(runs, limit)
		return nil
	})
	return runs, err
}

func (s *Store) BackupUsage(tenantID string, platformAdmin bool) (model.BackupUsage, error) {
	if s.usingDatabase() {
		return s.pgBackupUsage(tenantID, platformAdmin)
	}
	usage := model.BackupUsage{
		TenantID:              tenantID,
		Provider:              model.DataBackendProviderCloudflareR2,
		CloudflareR2PriceCode: backupUsageCloudflareR2PriceCode,
		MarkupPercent:         model.BackupR2MarkupPercent,
		EffectiveMultiplier:   1.05,
		Currency:              "USD",
		UpdatedAt:             time.Now().UTC(),
	}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, artifact := range state.BackupArtifacts {
			artifact = model.NormalizeBackupArtifact(artifact)
			if artifact.Status != model.BackupArtifactStatusActive || !artifact.Billable {
				continue
			}
			if !platformAdmin && artifact.TenantID != tenantID {
				continue
			}
			usage.BillableBytes += artifact.SizeBytes
		}
		usage = model.NormalizeBackupUsage(usage)
		return nil
	})
	return usage, err
}

func applyBackupRetentionInState(state *model.State, policyID string) {
	policyIndex := findBackupPolicy(state, policyID)
	if policyIndex < 0 {
		return
	}
	policy := model.NormalizeBackupPolicy(state.BackupPolicies[policyIndex])
	retain := policy.RetainCount
	if retain <= 0 {
		retain = policy.Retention.RetainCount
	}
	if retain <= 0 {
		return
	}
	var candidates []int
	for idx, artifact := range state.BackupArtifacts {
		artifact = model.NormalizeBackupArtifact(artifact)
		if artifact.PolicyID != policyID || artifact.Status != model.BackupArtifactStatusActive || artifact.Protected {
			continue
		}
		candidates = append(candidates, idx)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return state.BackupArtifacts[candidates[i]].CreatedAt.After(state.BackupArtifacts[candidates[j]].CreatedAt)
	})
	if len(candidates) <= retain {
		return
	}
	now := time.Now().UTC()
	for _, idx := range candidates[retain:] {
		state.BackupArtifacts[idx].Status = model.BackupArtifactStatusExpired
		state.BackupArtifacts[idx].DeletedAt = &now
	}
}

func applyBackupRunUpdate(run *model.BackupRun, update BackupRunUpdate) {
	if update.Status != nil {
		run.Status = strings.TrimSpace(strings.ToLower(*update.Status))
	}
	if update.LeaseOwner != nil {
		run.LeaseOwner = strings.TrimSpace(*update.LeaseOwner)
	}
	if update.LockedUntil != nil {
		run.LockedUntil = *update.LockedUntil
	}
	if update.HeartbeatAt != nil {
		run.HeartbeatAt = *update.HeartbeatAt
	}
	if update.BytesWritten != nil {
		run.BytesWritten = *update.BytesWritten
	}
	if update.LogicalBytes != nil {
		run.LogicalBytes = *update.LogicalBytes
	}
	if update.ArtifactCount != nil {
		run.ArtifactCount = *update.ArtifactCount
	}
	if update.ErrorCode != nil {
		run.ErrorCode = strings.TrimSpace(*update.ErrorCode)
	}
	if update.ErrorMessage != nil {
		run.ErrorMessage = strings.TrimSpace(*update.ErrorMessage)
	}
	if update.NextRetryAt != nil {
		run.NextRetryAt = *update.NextRetryAt
	}
	if update.StartedAt != nil {
		run.StartedAt = *update.StartedAt
	}
	if update.FinishedAt != nil {
		run.FinishedAt = *update.FinishedAt
	}
}

func validateScheduledBackupPolicyDue(run model.BackupRun, policy model.BackupPolicy, now time.Time) error {
	if run.Trigger != model.BackupRunTriggerScheduled {
		return nil
	}
	if policy.NextRunAt == nil || policy.NextRunAt.After(now) {
		return ErrConflict
	}
	return nil
}

func backupRunFinishStatus(status string) bool {
	switch status {
	case model.BackupRunStatusSucceeded, model.BackupRunStatusFailed, model.BackupRunStatusCanceled:
		return true
	default:
		return false
	}
}

func nextBackupRunAfter(schedule string, after time.Time) (*time.Time, error) {
	next, err := backupschedule.Next(schedule, after)
	if err != nil {
		return nil, err
	}
	return &next, nil
}

func backupTargetHasActiveRun(runs []model.BackupRun, target model.BackupTarget) bool {
	target = model.NormalizeBackupTarget(target)
	for _, run := range runs {
		run = model.NormalizeBackupRun(run)
		if run.Status != model.BackupRunStatusPending && run.Status != model.BackupRunStatusRunning {
			continue
		}
		if backupTargetsEqual(run.Target, target) {
			return true
		}
	}
	return false
}

func normalizeBackupTargetTypeFilter(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return model.NormalizeBackupTargetType(raw)
}

func backupTargetsEqual(left, right model.BackupTarget) bool {
	left = model.NormalizeBackupTarget(left)
	right = model.NormalizeBackupTarget(right)
	return left.Type == right.Type &&
		left.TenantID == right.TenantID &&
		left.ProjectID == right.ProjectID &&
		left.AppID == right.AppID &&
		left.WorkspaceID == right.WorkspaceID &&
		left.RuntimeID == right.RuntimeID &&
		left.ServiceName == right.ServiceName &&
		left.Database == right.Database &&
		left.Component == right.Component
}

func backupTargetIsEmpty(target model.BackupTarget) bool {
	return strings.TrimSpace(target.Type) == "" &&
		strings.TrimSpace(target.TenantID) == "" &&
		strings.TrimSpace(target.ProjectID) == "" &&
		strings.TrimSpace(target.AppID) == "" &&
		strings.TrimSpace(target.WorkspaceID) == "" &&
		strings.TrimSpace(target.RuntimeID) == "" &&
		strings.TrimSpace(target.Name) == "" &&
		strings.TrimSpace(target.ServiceName) == "" &&
		strings.TrimSpace(target.Database) == "" &&
		strings.TrimSpace(target.Component) == ""
}

func digestBackupManifest(manifest model.BackupManifest) string {
	manifest = model.NormalizeBackupManifest(manifest)
	raw, _ := json.Marshal(manifest)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func defaultRestorePlanPhases(plan model.BackupRestorePlan) []model.BackupRestorePhase {
	switch plan.Target.Type {
	case model.BackupTargetPersistentStorage:
		return []model.BackupRestorePhase{
			{Name: "provision-new-pvc", Status: model.BackupRestoreStatusPlanned, Message: "restore file archive into a new persistent volume claim"},
			{Name: "verify-manifest", Status: model.BackupRestoreStatusPlanned, Message: "verify manifest entries, file checksums, ownership, and permissions from the archive headers"},
			{Name: "cutover-deploy", Status: model.BackupRestoreStatusPlanned, Message: "switch the app storage reference through a normal deploy operation"},
			{Name: "retain-old-pvc", Status: model.BackupRestoreStatusPlanned, Message: "retain the previous PVC as rollback source until explicit deletion or retention expiry"},
		}
	case model.BackupTargetDataWorkspace:
		return []model.BackupRestorePhase{
			{Name: "resolve-data-snapshot", Status: model.BackupRestoreStatusPlanned, Message: "resolve the protected Data Workspace snapshot and manifest digest"},
			{Name: "materialize-snapshot", Status: model.BackupRestoreStatusPlanned, Message: "materialize the snapshot into the target workspace or restore workspace"},
			{Name: "verify-manifest-digest", Status: model.BackupRestoreStatusPlanned, Message: "verify manifest digest, file count, and total bytes"},
		}
	case model.BackupTargetRegistry:
		return []model.BackupRestorePhase{
			{Name: "quiesce-registry-gc", Status: model.BackupRestoreStatusPlanned, Message: "ensure registry GC is not running during restore"},
			{Name: "restore-registry-data", Status: model.BackupRestoreStatusPlanned, Message: "restore registry archive or verify external registry backup reference"},
			{Name: "verify-registry-catalog", Status: model.BackupRestoreStatusPlanned, Message: "verify registry catalog and protected workload digests"},
		}
	}
	switch plan.Mode {
	case model.BackupRestoreModeOfflineControlPlane:
		return []model.BackupRestorePhase{
			{Name: "download-artifact", Status: model.BackupRestoreStatusPlanned, Message: "download backup artifact and manifest"},
			{Name: "stop-control-plane", Status: model.BackupRestoreStatusPlanned, Message: "stop writers before restoring"},
			{Name: "restore-postgres", Status: model.BackupRestoreStatusPlanned, Message: "restore control-plane PostgreSQL dump"},
			{Name: "verify-store", Status: model.BackupRestoreStatusPlanned, Message: "verify store fingerprint and invariants"},
		}
	case model.BackupRestoreModeReplace:
		return []model.BackupRestorePhase{
			{Name: "protective-backup", Status: model.BackupRestoreStatusPlanned, Message: "create a protective backup before destructive replacement"},
			{Name: "restore-target", Status: model.BackupRestoreStatusPlanned, Message: "restore backup into the existing target"},
			{Name: "verify-target", Status: model.BackupRestoreStatusPlanned, Message: "verify restored target"},
		}
	case model.BackupRestoreModeClone:
		return []model.BackupRestorePhase{
			{Name: "provision-clone", Status: model.BackupRestoreStatusPlanned, Message: "provision an isolated clone target"},
			{Name: "restore-target", Status: model.BackupRestoreStatusPlanned, Message: "restore backup into clone target"},
			{Name: "verify-target", Status: model.BackupRestoreStatusPlanned, Message: "verify restored clone"},
		}
	default:
		return []model.BackupRestorePhase{
			{Name: "validate-artifact", Status: model.BackupRestoreStatusPlanned, Message: "validate manifest, checksum, and target compatibility"},
			{Name: "prepare-restore", Status: model.BackupRestoreStatusPlanned, Message: "prepare restore command sequence"},
		}
	}
}

func encryptBackupBackendSecret(secret model.BackupBackendSecret, credentials model.DataBackendCredentials) (model.BackupBackendSecret, error) {
	dataSecret, err := encryptDataBackendSecret(model.DataBackendSecret{
		ID:          secret.ID,
		TenantID:    secret.TenantID,
		BackendID:   secret.BackendID,
		Ciphertext:  secret.Ciphertext,
		KeyID:       secret.KeyID,
		CreatedAt:   secret.CreatedAt,
		UpdatedAt:   secret.UpdatedAt,
		LastRotated: secret.LastRotated,
	}, credentials)
	if err != nil {
		return model.BackupBackendSecret{}, err
	}
	return model.BackupBackendSecret{
		ID:          dataSecret.ID,
		TenantID:    dataSecret.TenantID,
		BackendID:   dataSecret.BackendID,
		Ciphertext:  dataSecret.Ciphertext,
		KeyID:       dataSecret.KeyID,
		CreatedAt:   dataSecret.CreatedAt,
		UpdatedAt:   dataSecret.UpdatedAt,
		LastRotated: dataSecret.LastRotated,
	}, nil
}

func decryptBackupBackendSecret(secret model.BackupBackendSecret) (model.DataBackendCredentials, error) {
	return decryptDataBackendSecret(model.DataBackendSecret{
		ID:          secret.ID,
		TenantID:    secret.TenantID,
		BackendID:   secret.BackendID,
		Ciphertext:  secret.Ciphertext,
		KeyID:       secret.KeyID,
		CreatedAt:   secret.CreatedAt,
		UpdatedAt:   secret.UpdatedAt,
		LastRotated: secret.LastRotated,
	})
}

func backupPolicyVisible(policy model.BackupPolicy, tenantID string, platformAdmin bool) bool {
	if platformAdmin {
		return true
	}
	return tenantID != "" && policy.TenantID == tenantID
}

func backupRunVisible(run model.BackupRun, tenantID string, platformAdmin bool) bool {
	if platformAdmin {
		return true
	}
	return tenantID != "" && run.TenantID == tenantID
}

func backupArtifactVisible(artifact model.BackupArtifact, tenantID string, platformAdmin bool) bool {
	if platformAdmin {
		return true
	}
	return tenantID != "" && artifact.TenantID == tenantID
}

func backupRestorePlanVisible(plan model.BackupRestorePlan, tenantID string, platformAdmin bool) bool {
	if platformAdmin {
		return true
	}
	return tenantID != "" && plan.TenantID == tenantID
}

func backupRestoreRunVisible(run model.BackupRestoreRun, tenantID string, platformAdmin bool) bool {
	if platformAdmin {
		return true
	}
	return tenantID != "" && run.TenantID == tenantID
}

func sortBackupPolicies(policies []model.BackupPolicy) {
	sort.Slice(policies, func(i, j int) bool {
		if policies[i].Scope != policies[j].Scope {
			return policies[i].Scope < policies[j].Scope
		}
		return policies[i].Name < policies[j].Name
	})
}

func sortBackupRuns(runs []model.BackupRun) {
	sort.Slice(runs, func(i, j int) bool { return runs[i].CreatedAt.After(runs[j].CreatedAt) })
}

func sortBackupArtifacts(artifacts []model.BackupArtifact) {
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].CreatedAt.After(artifacts[j].CreatedAt) })
}

func limitBackupPolicies(policies []model.BackupPolicy, limit int) []model.BackupPolicy {
	if limit <= 0 || limit > len(policies) {
		return policies
	}
	return policies[:limit]
}

func limitBackupRuns(runs []model.BackupRun, limit int) []model.BackupRun {
	if limit <= 0 {
		limit = defaultBackupRunHistoryLimit
	}
	if limit > len(runs) {
		return runs
	}
	return runs[:limit]
}

func limitBackupArtifacts(artifacts []model.BackupArtifact, limit int) []model.BackupArtifact {
	if limit <= 0 {
		limit = defaultBackupArtifactHistoryLimit
	}
	if limit > len(artifacts) {
		return artifacts
	}
	return artifacts[:limit]
}

func limitBackupRestorePlans(plans []model.BackupRestorePlan, limit int) []model.BackupRestorePlan {
	if limit <= 0 {
		limit = defaultBackupRestorePlanListLimit
	}
	if limit > len(plans) {
		return plans
	}
	return plans[:limit]
}

func limitBackupRestoreRuns(runs []model.BackupRestoreRun, limit int) []model.BackupRestoreRun {
	if limit <= 0 {
		limit = defaultBackupRestoreRunListLimit
	}
	if limit > len(runs) {
		return runs
	}
	return runs[:limit]
}

func findBackupBackend(state *model.State, id string) int {
	for idx, backend := range state.BackupBackends {
		if backend.ID == id {
			return idx
		}
	}
	return -1
}

func findBackupBackendBySlug(state *model.State, tenantID, slug string) int {
	slug = model.Slugify(slug)
	for idx, backend := range state.BackupBackends {
		if backend.TenantID == tenantID && backend.Slug == slug {
			return idx
		}
	}
	return -1
}

func findBackupBackendByIDNameOrSlug(state *model.State, value, tenantID string, platformAdmin bool) int {
	value = strings.TrimSpace(value)
	slug := model.Slugify(value)
	for idx, backend := range state.BackupBackends {
		backend = model.NormalizeBackupBackend(backend)
		if !platformAdmin && backend.TenantID != "" && backend.TenantID != tenantID {
			continue
		}
		if backend.ID == value || backend.Name == value || backend.Slug == slug {
			return idx
		}
	}
	return -1
}

func findOwnedBackupBackendByIDNameOrSlug(state *model.State, value, tenantID string, platformAdmin bool) int {
	if platformAdmin {
		return findBackupBackendByIDNameOrSlug(state, value, tenantID, true)
	}
	value = strings.TrimSpace(value)
	tenantID = strings.TrimSpace(tenantID)
	if value == "" || tenantID == "" {
		return -1
	}
	slug := model.Slugify(value)
	for idx, candidate := range state.BackupBackends {
		backend := model.NormalizeBackupBackend(candidate)
		if backend.TenantID != tenantID {
			continue
		}
		if backend.ID == value || backend.Name == value || backend.Slug == slug {
			return idx
		}
	}
	return -1
}

func findBackupBackendSecret(state *model.State, id, backendID string) int {
	for idx, secret := range state.BackupBackendSecrets {
		if id != "" && secret.ID == id {
			return idx
		}
		if backendID != "" && secret.BackendID == backendID {
			return idx
		}
	}
	return -1
}

func deleteBackupBackendSecretsByBackend(secrets []model.BackupBackendSecret, backendID string) []model.BackupBackendSecret {
	filtered := secrets[:0]
	for _, secret := range secrets {
		if secret.BackendID == backendID {
			continue
		}
		filtered = append(filtered, secret)
	}
	return filtered
}

func deleteBackupStateByTenant(state *model.State, tenantID string) {
	state.BackupBackends = filterBackupBackends(state.BackupBackends, func(backend model.BackupBackend) bool {
		return backend.TenantID != tenantID
	})
	state.BackupBackendSecrets = filterBackupBackendSecrets(state.BackupBackendSecrets, func(secret model.BackupBackendSecret) bool {
		return secret.TenantID != tenantID
	})
	state.BackupPolicies = filterBackupPolicies(state.BackupPolicies, func(policy model.BackupPolicy) bool {
		return policy.TenantID != tenantID
	})
	state.BackupRuns = filterBackupRuns(state.BackupRuns, func(run model.BackupRun) bool {
		return run.TenantID != tenantID
	})
	state.BackupArtifacts = filterBackupArtifacts(state.BackupArtifacts, func(artifact model.BackupArtifact) bool {
		return artifact.TenantID != tenantID
	})
	state.BackupRestorePlans = filterBackupRestorePlans(state.BackupRestorePlans, func(plan model.BackupRestorePlan) bool {
		return plan.TenantID != tenantID
	})
	state.BackupRestoreRuns = filterBackupRestoreRuns(state.BackupRestoreRuns, func(run model.BackupRestoreRun) bool {
		return run.TenantID != tenantID
	})
	ensureDefaultBackupPolicyInState(state)
}

func deleteBackupStateByProject(state *model.State, projectID string, appIDs []string) {
	appIDSet := map[string]struct{}{}
	for _, appID := range appIDs {
		appID = strings.TrimSpace(appID)
		if appID != "" {
			appIDSet[appID] = struct{}{}
		}
	}
	state.BackupPolicies = filterBackupPolicies(state.BackupPolicies, func(policy model.BackupPolicy) bool {
		return policy.ProjectID != projectID && policy.Target.ProjectID != projectID && !backupAppIDInSet(policy.AppID, policy.Target.AppID, appIDSet)
	})
	state.BackupRuns = filterBackupRuns(state.BackupRuns, func(run model.BackupRun) bool {
		return run.ProjectID != projectID && run.Target.ProjectID != projectID && !backupAppIDInSet(run.AppID, run.Target.AppID, appIDSet)
	})
	state.BackupArtifacts = filterBackupArtifacts(state.BackupArtifacts, func(artifact model.BackupArtifact) bool {
		return artifact.ProjectID != projectID && artifact.Target.ProjectID != projectID && !backupAppIDInSet(artifact.AppID, artifact.Target.AppID, appIDSet)
	})
	state.BackupRestorePlans = filterBackupRestorePlans(state.BackupRestorePlans, func(plan model.BackupRestorePlan) bool {
		return plan.ProjectID != projectID && plan.Target.ProjectID != projectID && !backupAppIDInSet(plan.AppID, plan.Target.AppID, appIDSet)
	})
	state.BackupRestoreRuns = filterBackupRestoreRuns(state.BackupRestoreRuns, func(run model.BackupRestoreRun) bool {
		return run.ProjectID != projectID && !backupAppIDInSet(run.AppID, "", appIDSet)
	})
}

func backupAppIDInSet(primary, target string, appIDs map[string]struct{}) bool {
	if len(appIDs) == 0 {
		return false
	}
	if _, ok := appIDs[strings.TrimSpace(primary)]; ok {
		return true
	}
	if _, ok := appIDs[strings.TrimSpace(target)]; ok {
		return true
	}
	return false
}

func filterBackupBackends(items []model.BackupBackend, keep func(model.BackupBackend) bool) []model.BackupBackend {
	filtered := items[:0]
	for _, item := range items {
		if keep(model.NormalizeBackupBackend(item)) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func filterBackupBackendSecrets(items []model.BackupBackendSecret, keep func(model.BackupBackendSecret) bool) []model.BackupBackendSecret {
	filtered := items[:0]
	for _, item := range items {
		if keep(item) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func filterBackupPolicies(items []model.BackupPolicy, keep func(model.BackupPolicy) bool) []model.BackupPolicy {
	filtered := items[:0]
	for _, item := range items {
		if keep(model.NormalizeBackupPolicy(item)) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func filterBackupRuns(items []model.BackupRun, keep func(model.BackupRun) bool) []model.BackupRun {
	filtered := items[:0]
	for _, item := range items {
		if keep(model.NormalizeBackupRun(item)) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func filterBackupArtifacts(items []model.BackupArtifact, keep func(model.BackupArtifact) bool) []model.BackupArtifact {
	filtered := items[:0]
	for _, item := range items {
		if keep(model.NormalizeBackupArtifact(item)) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func filterBackupRestorePlans(items []model.BackupRestorePlan, keep func(model.BackupRestorePlan) bool) []model.BackupRestorePlan {
	filtered := items[:0]
	for _, item := range items {
		if keep(model.NormalizeBackupRestorePlan(item)) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func filterBackupRestoreRuns(items []model.BackupRestoreRun, keep func(model.BackupRestoreRun) bool) []model.BackupRestoreRun {
	filtered := items[:0]
	for _, item := range items {
		if keep(model.NormalizeBackupRestoreRun(item)) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func findBackupPolicy(state *model.State, id string) int {
	for idx, policy := range state.BackupPolicies {
		if policy.ID == id {
			return idx
		}
	}
	return -1
}

func findBackupPolicyBySlug(state *model.State, tenantID, projectID, appID, slug string) int {
	slug = model.Slugify(slug)
	for idx, policy := range state.BackupPolicies {
		if policy.TenantID == tenantID && policy.ProjectID == projectID && policy.AppID == appID && policy.Slug == slug {
			return idx
		}
	}
	return -1
}

func findBackupPolicyByIDNameOrSlug(state *model.State, value, tenantID string, platformAdmin bool) int {
	value = strings.TrimSpace(value)
	slug := model.Slugify(value)
	for idx, policy := range state.BackupPolicies {
		policy = model.NormalizeBackupPolicy(policy)
		if !backupPolicyVisible(policy, tenantID, platformAdmin) {
			continue
		}
		if policy.ID == value || policy.Name == value || policy.Slug == slug {
			return idx
		}
	}
	return -1
}

func findBackupRun(state *model.State, id string) int {
	for idx, run := range state.BackupRuns {
		if run.ID == id {
			return idx
		}
	}
	return -1
}

func findBackupArtifact(state *model.State, id string) int {
	for idx, artifact := range state.BackupArtifacts {
		if artifact.ID == id {
			return idx
		}
	}
	return -1
}

func findBackupRestorePlan(state *model.State, id string) int {
	for idx, plan := range state.BackupRestorePlans {
		if plan.ID == id {
			return idx
		}
	}
	return -1
}

func scanBackupBackend(scanner sqlRowScanner) (model.BackupBackend, error) {
	var backend model.BackupBackend
	var capabilitiesRaw, credentialsRaw []byte
	var tenantID sql.NullString
	var lastTestedAt sql.NullTime
	if err := scanner.Scan(&backend.ID, &tenantID, &backend.Name, &backend.Slug, &backend.Provider, &backend.Bucket, &backend.Region, &backend.Endpoint, &backend.BaseURL, &backend.Prefix, &backend.Status, &capabilitiesRaw, &credentialsRaw, &backend.CredentialSecretID, &backend.FugueManaged, &backend.Billable, &lastTestedAt, &backend.LastTestResult, &backend.ErrorMessage, &backend.CreatedAt, &backend.UpdatedAt); err != nil {
		return model.BackupBackend{}, mapDBErr(err)
	}
	if tenantID.Valid {
		backend.TenantID = tenantID.String
	}
	capabilities, err := decodeJSONValue[model.DataBackendCapabilities](capabilitiesRaw)
	if err != nil {
		return model.BackupBackend{}, err
	}
	credentials, err := decodeJSONValue[model.DataBackendCredentials](credentialsRaw)
	if err != nil {
		return model.BackupBackend{}, err
	}
	backend.Capabilities = capabilities
	backend.Credentials = credentials
	if lastTestedAt.Valid {
		backend.LastTestedAt = &lastTestedAt.Time
	}
	return model.NormalizeBackupBackend(backend), nil
}

func scanBackupPolicy(scanner sqlRowScanner) (model.BackupPolicy, error) {
	var policy model.BackupPolicy
	var tenantID, projectID, appID, backendID sql.NullString
	var targetRaw, retentionRaw []byte
	var lastRunAt, lastSuccessfulAt, nextRunAt sql.NullTime
	if err := scanner.Scan(&policy.ID, &tenantID, &projectID, &appID, &policy.Name, &policy.Slug, &policy.Scope, &targetRaw, &backendID, &policy.Enabled, &policy.Status, &policy.DisabledReason, &policy.Schedule, &policy.RetainCount, &retentionRaw, &policy.Version, &policy.LastRunID, &policy.LastSuccessfulRunID, &lastRunAt, &lastSuccessfulAt, &nextRunAt, &policy.CreatedBy, &policy.CreatedAt, &policy.UpdatedAt); err != nil {
		return model.BackupPolicy{}, mapDBErr(err)
	}
	if tenantID.Valid {
		policy.TenantID = tenantID.String
	}
	if projectID.Valid {
		policy.ProjectID = projectID.String
	}
	if appID.Valid {
		policy.AppID = appID.String
	}
	if backendID.Valid {
		policy.BackendID = backendID.String
	}
	target, err := decodeJSONValue[model.BackupTarget](targetRaw)
	if err != nil {
		return model.BackupPolicy{}, err
	}
	retention, err := decodeJSONValue[model.BackupRetentionPolicy](retentionRaw)
	if err != nil {
		return model.BackupPolicy{}, err
	}
	policy.Target = target
	policy.Retention = retention
	if lastRunAt.Valid {
		policy.LastRunAt = &lastRunAt.Time
	}
	if lastSuccessfulAt.Valid {
		policy.LastSuccessfulAt = &lastSuccessfulAt.Time
	}
	if nextRunAt.Valid {
		policy.NextRunAt = &nextRunAt.Time
	}
	return model.NormalizeBackupPolicy(policy), nil
}

func scanBackupRun(scanner sqlRowScanner) (model.BackupRun, error) {
	var run model.BackupRun
	var tenantID, projectID, appID, policyID, backendID sql.NullString
	var targetRaw []byte
	var lockedUntil, heartbeatAt, nextRetryAt, startedAt, finishedAt sql.NullTime
	if err := scanner.Scan(&run.ID, &policyID, &tenantID, &projectID, &appID, &targetRaw, &backendID, &run.Trigger, &run.Version, &run.Status, &run.Attempt, &run.RetryCount, &run.RequestedByType, &run.RequestedByID, &run.LeaseOwner, &lockedUntil, &heartbeatAt, &run.BytesWritten, &run.LogicalBytes, &run.ArtifactCount, &run.ErrorCode, &run.ErrorMessage, &nextRetryAt, &run.CreatedAt, &run.UpdatedAt, &startedAt, &finishedAt); err != nil {
		return model.BackupRun{}, mapDBErr(err)
	}
	if policyID.Valid {
		run.PolicyID = policyID.String
	}
	if tenantID.Valid {
		run.TenantID = tenantID.String
	}
	if projectID.Valid {
		run.ProjectID = projectID.String
	}
	if appID.Valid {
		run.AppID = appID.String
	}
	if backendID.Valid {
		run.BackendID = backendID.String
	}
	target, err := decodeJSONValue[model.BackupTarget](targetRaw)
	if err != nil {
		return model.BackupRun{}, err
	}
	run.Target = target
	if lockedUntil.Valid {
		run.LockedUntil = &lockedUntil.Time
	}
	if heartbeatAt.Valid {
		run.HeartbeatAt = &heartbeatAt.Time
	}
	if nextRetryAt.Valid {
		run.NextRetryAt = &nextRetryAt.Time
	}
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		run.FinishedAt = &finishedAt.Time
	}
	return model.NormalizeBackupRun(run), nil
}

func scanBackupArtifact(scanner sqlRowScanner) (model.BackupArtifact, error) {
	var artifact model.BackupArtifact
	var tenantID, projectID, appID, policyID, runID, backendID sql.NullString
	var targetRaw, manifestRaw []byte
	var deletedAt sql.NullTime
	if err := scanner.Scan(&artifact.ID, &runID, &policyID, &tenantID, &projectID, &appID, &targetRaw, &backendID, &artifact.Kind, &artifact.Version, &artifact.ObjectKey, &artifact.ManifestObjectKey, &artifact.SHA256, &artifact.SizeBytes, &artifact.LogicalBytes, &artifact.Status, &artifact.Protected, &artifact.Billable, &artifact.BillingClass, &artifact.ManifestDigest, &manifestRaw, &artifact.CreatedAt, &deletedAt); err != nil {
		return model.BackupArtifact{}, mapDBErr(err)
	}
	if runID.Valid {
		artifact.RunID = runID.String
	}
	if policyID.Valid {
		artifact.PolicyID = policyID.String
	}
	if tenantID.Valid {
		artifact.TenantID = tenantID.String
	}
	if projectID.Valid {
		artifact.ProjectID = projectID.String
	}
	if appID.Valid {
		artifact.AppID = appID.String
	}
	if backendID.Valid {
		artifact.BackendID = backendID.String
	}
	target, err := decodeJSONValue[model.BackupTarget](targetRaw)
	if err != nil {
		return model.BackupArtifact{}, err
	}
	manifest, err := decodeJSONValue[model.BackupManifest](manifestRaw)
	if err != nil {
		return model.BackupArtifact{}, err
	}
	artifact.Target = target
	artifact.Manifest = manifest
	if deletedAt.Valid {
		artifact.DeletedAt = &deletedAt.Time
	}
	return model.NormalizeBackupArtifact(artifact), nil
}

func scanBackupRestorePlan(scanner sqlRowScanner) (model.BackupRestorePlan, error) {
	var plan model.BackupRestorePlan
	var tenantID, projectID, appID sql.NullString
	var targetRaw, warningsRaw, phasesRaw []byte
	if err := scanner.Scan(&plan.ID, &tenantID, &projectID, &appID, &plan.ArtifactID, &targetRaw, &plan.Mode, &plan.Status, &warningsRaw, &phasesRaw, &plan.CreatedByType, &plan.CreatedByID, &plan.CreatedAt, &plan.UpdatedAt); err != nil {
		return model.BackupRestorePlan{}, mapDBErr(err)
	}
	if tenantID.Valid {
		plan.TenantID = tenantID.String
	}
	if projectID.Valid {
		plan.ProjectID = projectID.String
	}
	if appID.Valid {
		plan.AppID = appID.String
	}
	target, err := decodeJSONValue[model.BackupTarget](targetRaw)
	if err != nil {
		return model.BackupRestorePlan{}, err
	}
	warnings, err := decodeJSONValue[[]string](warningsRaw)
	if err != nil {
		return model.BackupRestorePlan{}, err
	}
	phases, err := decodeJSONValue[[]model.BackupRestorePhase](phasesRaw)
	if err != nil {
		return model.BackupRestorePlan{}, err
	}
	plan.Target = target
	plan.Warnings = warnings
	plan.Phases = phases
	return model.NormalizeBackupRestorePlan(plan), nil
}

func scanBackupRestoreRun(scanner sqlRowScanner) (model.BackupRestoreRun, error) {
	var run model.BackupRestoreRun
	var tenantID, projectID, appID sql.NullString
	var phasesRaw []byte
	var startedAt, finishedAt sql.NullTime
	if err := scanner.Scan(&run.ID, &run.PlanID, &tenantID, &projectID, &appID, &run.ArtifactID, &run.Mode, &run.Status, &phasesRaw, &run.ErrorCode, &run.ErrorMessage, &run.RequestedByType, &run.RequestedByID, &run.CreatedAt, &run.UpdatedAt, &startedAt, &finishedAt); err != nil {
		return model.BackupRestoreRun{}, mapDBErr(err)
	}
	if tenantID.Valid {
		run.TenantID = tenantID.String
	}
	if projectID.Valid {
		run.ProjectID = projectID.String
	}
	if appID.Valid {
		run.AppID = appID.String
	}
	phases, err := decodeJSONValue[[]model.BackupRestorePhase](phasesRaw)
	if err != nil {
		return model.BackupRestoreRun{}, err
	}
	run.Phases = phases
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		run.FinishedAt = &finishedAt.Time
	}
	return model.NormalizeBackupRestoreRun(run), nil
}

func (s *Store) pgListBackupBackends(tenantID string, platformAdmin bool) ([]model.BackupBackend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupBackendSelectSQL()
	args := []any{}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` WHERE tenant_id IS NULL OR tenant_id = $1`
	}
	query += ` ORDER BY name ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	backends := []model.BackupBackend{}
	for rows.Next() {
		backend, err := scanBackupBackend(rows)
		if err != nil {
			return nil, err
		}
		backends = append(backends, model.RedactBackupBackendCredentials(backend))
	}
	return backends, mapDBErr(rows.Err())
}

func (s *Store) pgGetBackupBackend(idOrName, tenantID string, platformAdmin bool, redact bool) (model.BackupBackend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	slug := model.Slugify(idOrName)
	query := backupBackendSelectSQL() + ` WHERE (id = $1 OR name = $1 OR slug = $2)`
	args := []any{idOrName, slug}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND (tenant_id IS NULL OR tenant_id = $3)`
	}
	backend, err := scanBackupBackend(s.db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.BackupBackend{}, err
	}
	return s.pgLoadBackupBackendCredentials(ctx, backend, redact)
}

func (s *Store) pgGetBackupBackendForMutation(idOrName, tenantID string, platformAdmin bool, redact bool) (model.BackupBackend, error) {
	if platformAdmin {
		return s.pgGetBackupBackend(idOrName, tenantID, true, redact)
	}
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return model.BackupBackend{}, ErrNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	slug := model.Slugify(idOrName)
	backend, err := scanBackupBackend(s.db.QueryRowContext(ctx, backupBackendSelectSQL()+`
 WHERE (id = $1 OR name = $1 OR slug = $2) AND tenant_id = $3`, idOrName, slug, tenantID))
	if err != nil {
		return model.BackupBackend{}, err
	}
	return s.pgLoadBackupBackendCredentials(ctx, backend, redact)
}

func (s *Store) pgLoadBackupBackendCredentials(ctx context.Context, backend model.BackupBackend, redact bool) (model.BackupBackend, error) {
	if !redact {
		var secret model.BackupBackendSecret
		err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at
FROM fugue_backup_backend_secrets WHERE backend_id = $1
`, backend.ID).Scan(&secret.ID, &secret.TenantID, &secret.BackendID, &secret.Ciphertext, &secret.KeyID, &secret.CreatedAt, &secret.UpdatedAt, &secret.LastRotated)
		if err == nil {
			credentials, decryptErr := decryptBackupBackendSecret(secret)
			if decryptErr != nil {
				return model.BackupBackend{}, decryptErr
			}
			backend.CredentialSecretID = secret.ID
			backend.Credentials = credentials
			return backend, nil
		}
		if !errors.Is(mapDBErr(err), ErrNotFound) {
			return model.BackupBackend{}, mapDBErr(err)
		}
		return backend, nil
	}
	return model.RedactBackupBackendCredentials(backend), nil
}

func (s *Store) pgCreateBackupBackend(backend model.BackupBackend) (model.BackupBackend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend = model.NormalizeBackupBackend(backend)
	credentials := backend.Credentials
	backend.Credentials = redactAccessKeyOnly(credentials)
	if backend.ID == "" {
		backend.ID = model.NewID("backup_backend")
	}
	now := time.Now().UTC()
	if backend.CreatedAt.IsZero() {
		backend.CreatedAt = now
	}
	backend.UpdatedAt = now
	capabilitiesJSON, err := marshalJSON(backend.Capabilities)
	if err != nil {
		return model.BackupBackend{}, err
	}
	credentialsJSON, err := marshalJSON(backend.Credentials)
	if err != nil {
		return model.BackupBackend{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BackupBackend{}, err
	}
	defer tx.Rollback()
	created, err := scanBackupBackend(tx.QueryRowContext(ctx, `
INSERT INTO fugue_backup_backends (id, tenant_id, name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, credential_secret_id, fugue_managed, billable, last_tested_at, last_test_result, error_message, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, '', $14, $15, NULL, '', '', $16, $17)
RETURNING `+backupBackendReturningColumns(), backend.ID, nullIfEmpty(backend.TenantID), backend.Name, backend.Slug, backend.Provider, backend.Bucket, backend.Region, backend.Endpoint, backend.BaseURL, backend.Prefix, backend.Status, capabilitiesJSON, credentialsJSON, backend.FugueManaged, backend.Billable, backend.CreatedAt, backend.UpdatedAt))
	if err != nil {
		return model.BackupBackend{}, mapDBErr(err)
	}
	if dataBackendCredentialsPresent(credentials) {
		secret, err := encryptBackupBackendSecret(model.BackupBackendSecret{ID: model.NewID("backup_backend_secret"), TenantID: backend.TenantID, BackendID: backend.ID}, credentials)
		if err != nil {
			return model.BackupBackend{}, err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_backup_backend_secrets (id, tenant_id, backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`, secret.ID, nullIfEmpty(secret.TenantID), secret.BackendID, secret.Ciphertext, secret.KeyID, secret.CreatedAt, secret.UpdatedAt, secret.LastRotated); err != nil {
			return model.BackupBackend{}, mapDBErr(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fugue_backup_backends SET credential_secret_id = $2 WHERE id = $1`, backend.ID, secret.ID); err != nil {
			return model.BackupBackend{}, mapDBErr(err)
		}
		created.CredentialSecretID = secret.ID
	}
	if err := tx.Commit(); err != nil {
		return model.BackupBackend{}, err
	}
	if err := s.pgEnsureDefaultBackupPolicy(); err != nil {
		return model.BackupBackend{}, err
	}
	return model.RedactBackupBackendCredentials(created), nil
}

func (s *Store) pgSeedDefaultBackupBackendFromEnv() error {
	backend, credentials, ok := defaultBackupBackendConfigFromEnv()
	if !ok {
		return s.pgEnsureDefaultBackupPolicy()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	capabilitiesJSON, err := marshalJSON(backend.Capabilities)
	if err != nil {
		return err
	}
	credentialsJSON, err := marshalJSON(backend.Credentials)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_backup_backends (id, tenant_id, name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, credential_secret_id, fugue_managed, billable, last_tested_at, last_test_result, error_message, created_at, updated_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, '', $13, $14, NULL, '', '', $15, $15)
ON CONFLICT (id) DO UPDATE SET
	name = EXCLUDED.name,
	slug = EXCLUDED.slug,
	provider = EXCLUDED.provider,
	bucket = EXCLUDED.bucket,
	region = EXCLUDED.region,
	endpoint = EXCLUDED.endpoint,
	base_url = EXCLUDED.base_url,
	prefix = EXCLUDED.prefix,
	status = EXCLUDED.status,
	capabilities_json = EXCLUDED.capabilities_json,
	credentials_json = EXCLUDED.credentials_json,
	fugue_managed = EXCLUDED.fugue_managed,
	billable = EXCLUDED.billable,
	updated_at = EXCLUDED.updated_at
`, backend.ID, backend.Name, backend.Slug, backend.Provider, backend.Bucket, backend.Region, backend.Endpoint, backend.BaseURL, backend.Prefix, backend.Status, capabilitiesJSON, credentialsJSON, backend.FugueManaged, backend.Billable, backend.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	if dataBackendCredentialsPresent(credentials) {
		var existing model.BackupBackendSecret
		err := tx.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at
FROM fugue_backup_backend_secrets WHERE backend_id = $1
`, backend.ID).Scan(&existing.ID, &existing.TenantID, &existing.BackendID, &existing.Ciphertext, &existing.KeyID, &existing.CreatedAt, &existing.UpdatedAt, &existing.LastRotated)
		if err != nil && !errors.Is(mapDBErr(err), ErrNotFound) {
			return mapDBErr(err)
		}
		if existing.ID == "" {
			existing = model.BackupBackendSecret{ID: model.NewID("backup_backend_secret"), BackendID: backend.ID}
		}
		secret, err := encryptBackupBackendSecret(existing, credentials)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_backup_backend_secrets (id, tenant_id, backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, $7)
ON CONFLICT (backend_id) DO UPDATE SET
	ciphertext = EXCLUDED.ciphertext,
	key_id = EXCLUDED.key_id,
	updated_at = EXCLUDED.updated_at,
	last_rotated_at = EXCLUDED.last_rotated_at
`, secret.ID, secret.BackendID, secret.Ciphertext, secret.KeyID, secret.CreatedAt, secret.UpdatedAt, secret.LastRotated); err != nil {
			return mapDBErr(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fugue_backup_backends SET credential_secret_id = $2 WHERE id = $1`, backend.ID, secret.ID); err != nil {
			return mapDBErr(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.pgEnsureDefaultBackupPolicy()
}

func (s *Store) pgRepairBackupPolicySchedules() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, backupPolicySelectSQL()+` WHERE enabled = TRUE AND status = 'active'`)
	if err != nil {
		return mapDBErr(err)
	}
	policies := []model.BackupPolicy{}
	for rows.Next() {
		policy, scanErr := scanBackupPolicy(rows)
		if scanErr != nil {
			rows.Close()
			return scanErr
		}
		policies = append(policies, policy)
	}
	if err := rows.Close(); err != nil {
		return mapDBErr(err)
	}
	if err := rows.Err(); err != nil {
		return mapDBErr(err)
	}
	now := time.Now().UTC()
	for _, policy := range policies {
		if _, err := s.pgRepairBackupPolicyScheduleCAS(ctx, policy, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) pgRepairNullBackupPolicySchedules(ctx context.Context, now time.Time, limit int) error {
	if limit <= 0 {
		limit = defaultBackupDueScannerLimit
	}
	rows, err := s.db.QueryContext(ctx, backupPolicySelectSQL()+`
 WHERE enabled = TRUE AND status = 'active' AND next_run_at IS NULL
 ORDER BY created_at ASC, id ASC
 LIMIT $1`, limit)
	if err != nil {
		return mapDBErr(err)
	}
	policies := []model.BackupPolicy{}
	for rows.Next() {
		policy, scanErr := scanBackupPolicy(rows)
		if scanErr != nil {
			rows.Close()
			return scanErr
		}
		policies = append(policies, policy)
	}
	if err := rows.Close(); err != nil {
		return mapDBErr(err)
	}
	if err := rows.Err(); err != nil {
		return mapDBErr(err)
	}
	for _, policy := range policies {
		if _, err := s.pgRepairBackupPolicyScheduleCAS(ctx, policy, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) pgRepairBackupPolicyScheduleCAS(ctx context.Context, policy model.BackupPolicy, now time.Time) (bool, error) {
	repaired, changed := repairBackupPolicySchedule(policy, now)
	if !changed {
		return false, nil
	}
	result, err := s.db.ExecContext(ctx, pgRepairBackupPolicyScheduleSQL,
		repaired.ID,
		repaired.Schedule,
		repaired.Status,
		repaired.DisabledReason,
		repaired.NextRunAt,
		repaired.UpdatedAt,
		policy.Schedule,
		policy.NextRunAt,
		policy.LastRunAt,
	)
	if err != nil {
		return false, mapDBErr(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, mapDBErr(err)
	}
	return rowsAffected == 1, nil
}

func (s *Store) pgEnsureDefaultBackupPolicy() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var backendID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM fugue_backup_backends WHERE id = $1 AND status = 'active'`, defaultBackupBackendID).Scan(&backendID); err != nil && !errors.Is(mapDBErr(err), ErrNotFound) {
		return mapDBErr(err)
	}
	now := time.Now().UTC()
	status := model.BackupPolicyStatusBlockedNoBackend
	reason := "platform R2 backup backend is not configured"
	backend := ""
	if backendID.Valid && backendID.String != "" {
		backend = backendID.String
		status = model.BackupPolicyStatusActive
		reason = ""
	}
	policy := model.NormalizeBackupPolicy(model.BackupPolicy{
		ID:             defaultControlPlaneBackupPolicyID,
		Name:           "control-plane-database-hourly",
		Slug:           "control-plane-database-hourly",
		Scope:          model.BackupScopePlatform,
		Target:         model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Component: "control-plane-postgres"},
		BackendID:      backend,
		Enabled:        true,
		Status:         status,
		DisabledReason: reason,
		Schedule:       model.BackupDefaultSchedule,
		RetainCount:    model.BackupDefaultRetainCount,
		Retention: model.BackupRetentionPolicy{
			RetainCount:   model.BackupDefaultRetainCount,
			ProtectLatest: model.BackupDefaultRetainCount,
		},
		CreatedBy: "system",
		CreatedAt: now,
		UpdatedAt: now,
	})
	if policy.NextRunAt == nil {
		next, err := nextBackupRunAfter(policy.Schedule, now.Add(-time.Hour))
		if err != nil {
			return fmt.Errorf("initialize default backup schedule: %w", err)
		}
		policy.NextRunAt = next
	}
	targetJSON, err := marshalJSON(policy.Target)
	if err != nil {
		return err
	}
	retentionJSON, err := marshalJSON(policy.Retention)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO fugue_backup_policies (id, tenant_id, project_id, app_id, name, slug, scope, target_type, target_tenant_id, target_project_id, target_app_id, target_json, backend_id, enabled, status, disabled_reason, schedule, retain_count, retention_json, version, last_run_id, last_successful_run_id, last_run_at, last_successful_at, next_run_at, created_by, created_at, updated_at)
VALUES ($1, NULL, NULL, NULL, $2, $3, $4, $5, NULL, NULL, NULL, $6, NULLIF($7, ''), $8, $9, $10, $11, $12, $13, '', '', '', NULL, NULL, $14, $15, $16, $17)
ON CONFLICT (id) DO UPDATE SET
	name = EXCLUDED.name,
	slug = EXCLUDED.slug,
	scope = EXCLUDED.scope,
	target_type = EXCLUDED.target_type,
	target_json = EXCLUDED.target_json,
	backend_id = CASE WHEN fugue_backup_policies.backend_id IS NULL OR fugue_backup_policies.backend_id = '' OR EXCLUDED.backend_id IS NULL THEN EXCLUDED.backend_id ELSE fugue_backup_policies.backend_id END,
	enabled = TRUE,
	status = CASE WHEN EXCLUDED.backend_id IS NULL THEN $9 WHEN fugue_backup_policies.status = 'blocked_no_backend' THEN 'active' ELSE fugue_backup_policies.status END,
	disabled_reason = CASE WHEN EXCLUDED.backend_id IS NULL THEN $10 WHEN fugue_backup_policies.status = 'blocked_no_backend' THEN '' ELSE fugue_backup_policies.disabled_reason END,
	schedule = CASE WHEN fugue_backup_policies.schedule = '' THEN EXCLUDED.schedule ELSE fugue_backup_policies.schedule END,
	retain_count = CASE WHEN fugue_backup_policies.retain_count <= 0 THEN EXCLUDED.retain_count ELSE fugue_backup_policies.retain_count END,
	retention_json = CASE WHEN fugue_backup_policies.retention_json IS NULL THEN EXCLUDED.retention_json ELSE fugue_backup_policies.retention_json END,
	next_run_at = fugue_backup_policies.next_run_at,
	updated_at = EXCLUDED.updated_at
`, policy.ID, policy.Name, policy.Slug, policy.Scope, policy.Target.Type, targetJSON, backend, policy.Enabled, policy.Status, policy.DisabledReason, policy.Schedule, policy.RetainCount, retentionJSON, policy.NextRunAt, policy.CreatedBy, policy.CreatedAt, policy.UpdatedAt)
	return mapDBErr(err)
}

func (s *Store) pgRotateBackupBackendCredentials(idOrName, tenantID string, platformAdmin bool, credentials model.DataBackendCredentials) (model.BackupBackend, error) {
	backend, err := s.pgGetBackupBackendForMutation(idOrName, tenantID, platformAdmin, false)
	if err != nil {
		return model.BackupBackend{}, err
	}
	if !platformAdmin && (tenantID == "" || backend.TenantID != tenantID) {
		return model.BackupBackend{}, ErrNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BackupBackend{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	credentialsJSON, err := marshalJSON(redactAccessKeyOnly(credentials))
	if err != nil {
		return model.BackupBackend{}, err
	}
	rotated, err := scanBackupBackend(tx.QueryRowContext(ctx, `
UPDATE fugue_backup_backends SET credentials_json = $2, updated_at = $3 WHERE id = $1
RETURNING `+backupBackendReturningColumns(), backend.ID, credentialsJSON, now))
	if err != nil {
		return model.BackupBackend{}, mapDBErr(err)
	}
	var existing model.BackupBackendSecret
	err = tx.QueryRowContext(ctx, `
SELECT id, COALESCE(tenant_id, ''), backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at
FROM fugue_backup_backend_secrets WHERE backend_id = $1
`, backend.ID).Scan(&existing.ID, &existing.TenantID, &existing.BackendID, &existing.Ciphertext, &existing.KeyID, &existing.CreatedAt, &existing.UpdatedAt, &existing.LastRotated)
	if err != nil && !errors.Is(mapDBErr(err), ErrNotFound) {
		return model.BackupBackend{}, mapDBErr(err)
	}
	if errors.Is(mapDBErr(err), ErrNotFound) {
		existing = model.BackupBackendSecret{ID: model.NewID("backup_backend_secret"), TenantID: backend.TenantID, BackendID: backend.ID, CreatedAt: now}
	}
	existing.LastRotated = now
	secret, err := encryptBackupBackendSecret(existing, credentials)
	if err != nil {
		return model.BackupBackend{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_backup_backend_secrets (id, tenant_id, backend_id, ciphertext, key_id, created_at, updated_at, last_rotated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (backend_id) DO UPDATE SET ciphertext = EXCLUDED.ciphertext, key_id = EXCLUDED.key_id, updated_at = EXCLUDED.updated_at, last_rotated_at = EXCLUDED.last_rotated_at
`, secret.ID, nullIfEmpty(secret.TenantID), secret.BackendID, secret.Ciphertext, secret.KeyID, secret.CreatedAt, secret.UpdatedAt, secret.LastRotated); err != nil {
		return model.BackupBackend{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE fugue_backup_backends SET credential_secret_id = $2 WHERE id = $1`, backend.ID, secret.ID); err != nil {
		return model.BackupBackend{}, mapDBErr(err)
	}
	rotated.CredentialSecretID = secret.ID
	if err := tx.Commit(); err != nil {
		return model.BackupBackend{}, err
	}
	return model.RedactBackupBackendCredentials(rotated), nil
}

func (s *Store) pgDeleteBackupBackend(idOrName, tenantID string, platformAdmin bool) (model.BackupBackend, error) {
	backend, err := s.pgGetBackupBackendForMutation(idOrName, tenantID, platformAdmin, false)
	if err != nil {
		return model.BackupBackend{}, err
	}
	if !platformAdmin && (tenantID == "" || backend.TenantID != tenantID) {
		return model.BackupBackend{}, ErrNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var inUse bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_backup_policies WHERE backend_id = $1)`, backend.ID).Scan(&inUse); err != nil {
		return model.BackupBackend{}, mapDBErr(err)
	}
	if inUse {
		return model.BackupBackend{}, ErrConflict
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM fugue_backup_backends WHERE id = $1`, backend.ID); err != nil {
		return model.BackupBackend{}, mapDBErr(err)
	}
	if err := s.pgEnsureDefaultBackupPolicy(); err != nil {
		return model.BackupBackend{}, err
	}
	return model.RedactBackupBackendCredentials(backend), nil
}

func (s *Store) pgRecordBackupBackendTest(idOrName, tenantID string, platformAdmin bool, success bool, message string) (model.BackupBackend, error) {
	backend, err := s.pgGetBackupBackendForMutation(idOrName, tenantID, platformAdmin, false)
	if err != nil {
		return model.BackupBackend{}, err
	}
	if !platformAdmin && (tenantID == "" || backend.TenantID != tenantID) {
		return model.BackupBackend{}, ErrNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := "failed"
	errorMessage := strings.TrimSpace(message)
	if success {
		result = "ok"
		errorMessage = ""
	}
	now := time.Now().UTC()
	updated, err := scanBackupBackend(s.db.QueryRowContext(ctx, `
UPDATE fugue_backup_backends SET last_tested_at = $2, last_test_result = $3, error_message = $4, updated_at = $2
WHERE id = $1
RETURNING `+backupBackendReturningColumns(), backend.ID, now, result, errorMessage))
	if err != nil {
		return model.BackupBackend{}, err
	}
	return model.RedactBackupBackendCredentials(updated), nil
}

func (s *Store) pgListBackupPolicies(filter BackupPolicyFilter) ([]model.BackupPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupPolicySelectSQL()
	clauses, args := backupPolicyFilterClauses(filter)
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}
	query += ` ORDER BY scope ASC, name ASC`
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		query += fmt.Sprintf(` LIMIT $%d`, len(args))
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	policies := []model.BackupPolicy{}
	for rows.Next() {
		policy, err := scanBackupPolicy(rows)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	return policies, mapDBErr(rows.Err())
}

func (s *Store) pgGetBackupPolicy(idOrName, tenantID string, platformAdmin bool) (model.BackupPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	slug := model.Slugify(idOrName)
	query := backupPolicySelectSQL() + ` WHERE (id = $1 OR name = $1 OR slug = $2)`
	args := []any{idOrName, slug}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $3`
	}
	return scanBackupPolicy(s.db.QueryRowContext(ctx, query, args...))
}

func (s *Store) pgUpsertBackupPolicy(policy model.BackupPolicy) (model.BackupPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	policy = model.NormalizeBackupPolicy(policy)
	if policy.Schedule == "" {
		policy.Schedule = model.BackupDefaultSchedule
	}
	var freshNextRunAt *time.Time
	if policy.Enabled {
		var scheduleErr error
		freshNextRunAt, scheduleErr = nextBackupRunAfter(policy.Schedule, time.Now().UTC())
		if scheduleErr != nil {
			return model.BackupPolicy{}, fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, scheduleErr)
		}
	}
	if policy.ID == "" {
		policy.ID = model.NewID("backup_policy")
	}
	now := time.Now().UTC()
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = now
	}
	policy.UpdatedAt = now
	if policy.Enabled && policy.NextRunAt == nil {
		policy.NextRunAt = freshNextRunAt
	} else if !policy.Enabled {
		policy.NextRunAt = nil
	}
	targetJSON, err := marshalJSON(policy.Target)
	if err != nil {
		return model.BackupPolicy{}, err
	}
	retentionJSON, err := marshalJSON(policy.Retention)
	if err != nil {
		return model.BackupPolicy{}, err
	}
	return scanBackupPolicy(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_backup_policies (id, tenant_id, project_id, app_id, name, slug, scope, target_type, target_tenant_id, target_project_id, target_app_id, target_json, backend_id, enabled, status, disabled_reason, schedule, retain_count, retention_json, version, last_run_id, last_successful_run_id, last_run_at, last_successful_at, next_run_at, created_by, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULLIF($13, ''), $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)
ON CONFLICT (id) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id,
	project_id = EXCLUDED.project_id,
	app_id = EXCLUDED.app_id,
	name = EXCLUDED.name,
	slug = EXCLUDED.slug,
	scope = EXCLUDED.scope,
	target_type = EXCLUDED.target_type,
	target_tenant_id = EXCLUDED.target_tenant_id,
	target_project_id = EXCLUDED.target_project_id,
	target_app_id = EXCLUDED.target_app_id,
	target_json = EXCLUDED.target_json,
	backend_id = EXCLUDED.backend_id,
	enabled = EXCLUDED.enabled,
	status = EXCLUDED.status,
	disabled_reason = EXCLUDED.disabled_reason,
	schedule = EXCLUDED.schedule,
	retain_count = EXCLUDED.retain_count,
	retention_json = EXCLUDED.retention_json,
	version = EXCLUDED.version,
	next_run_at = CASE
		WHEN EXCLUDED.enabled = FALSE THEN NULL
		WHEN fugue_backup_policies.schedule IS DISTINCT FROM EXCLUDED.schedule THEN $29
		ELSE EXCLUDED.next_run_at
	END,
	updated_at = EXCLUDED.updated_at
RETURNING `+backupPolicyReturningColumns(), policy.ID, nullIfEmpty(policy.TenantID), nullIfEmpty(policy.ProjectID), nullIfEmpty(policy.AppID), policy.Name, policy.Slug, policy.Scope, policy.Target.Type, nullIfEmpty(policy.Target.TenantID), nullIfEmpty(policy.Target.ProjectID), nullIfEmpty(policy.Target.AppID), targetJSON, policy.BackendID, policy.Enabled, policy.Status, policy.DisabledReason, policy.Schedule, policy.RetainCount, retentionJSON, policy.Version, policy.LastRunID, policy.LastSuccessfulRunID, policy.LastRunAt, policy.LastSuccessfulAt, policy.NextRunAt, policy.CreatedBy, policy.CreatedAt, policy.UpdatedAt, freshNextRunAt))
}

func (s *Store) pgSetBackupPolicyEnabled(idOrName, tenantID string, platformAdmin bool, enabled bool, reason string) (model.BackupPolicy, error) {
	policy, err := s.pgGetBackupPolicy(idOrName, tenantID, platformAdmin)
	if err != nil {
		return model.BackupPolicy{}, err
	}
	now := time.Now().UTC()
	policy.Enabled = enabled
	if enabled {
		if err := backupschedule.Validate(policy.Schedule); err != nil {
			return model.BackupPolicy{}, fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, err)
		}
		if policy.BackendID == "" {
			policy.Status = model.BackupPolicyStatusBlockedNoBackend
			policy.DisabledReason = "backup backend is not configured"
		} else {
			policy.Status = model.BackupPolicyStatusActive
			policy.DisabledReason = ""
		}
		next, err := nextBackupRunAfter(policy.Schedule, now)
		if err != nil {
			return model.BackupPolicy{}, fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, err)
		}
		policy.NextRunAt = next
	} else {
		policy.Status = model.BackupPolicyStatusDisabled
		policy.DisabledReason = strings.TrimSpace(reason)
		policy.NextRunAt = nil
	}
	return s.pgUpsertBackupPolicy(policy)
}

func (s *Store) pgListDueBackupPolicies(now time.Time, limit int) ([]model.BackupPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if limit <= 0 {
		limit = defaultBackupDueScannerLimit
	}
	if err := s.pgRepairNullBackupPolicySchedules(ctx, now, limit); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, backupPolicySelectSQL()+`
 WHERE enabled = TRUE AND status = 'active' AND next_run_at IS NOT NULL AND next_run_at <= $1
 ORDER BY next_run_at ASC
 LIMIT $2`, now, limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	policies := []model.BackupPolicy{}
	for rows.Next() {
		policy, err := scanBackupPolicy(rows)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	return policies, mapDBErr(rows.Err())
}

func (s *Store) pgCreateBackupRun(run model.BackupRun) (model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run = model.NormalizeBackupRun(run)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BackupRun{}, err
	}
	defer tx.Rollback()
	policySchedule := model.BackupDefaultSchedule
	var now time.Time
	if run.PolicyID != "" {
		policy, err := scanBackupPolicy(tx.QueryRowContext(ctx, backupPolicySelectSQL()+` WHERE id = $1 FOR UPDATE`, run.PolicyID))
		if err != nil {
			return model.BackupRun{}, err
		}
		if !policy.Enabled || policy.Status != model.BackupPolicyStatusActive {
			return model.BackupRun{}, ErrConflict
		}
		now = time.Now().UTC()
		if err := validateScheduledBackupPolicyDue(run, policy, now); err != nil {
			return model.BackupRun{}, err
		}
		if run.TenantID == "" {
			run.TenantID = policy.TenantID
		}
		if run.ProjectID == "" {
			run.ProjectID = policy.ProjectID
		}
		if run.AppID == "" {
			run.AppID = policy.AppID
		}
		if run.BackendID == "" {
			run.BackendID = policy.BackendID
		}
		run.Target = policy.Target
		if run.Version == "" {
			run.Version = policy.Version
		}
		policySchedule = policy.Schedule
	}
	if run.BackendID == "" {
		run.Status = model.BackupRunStatusBlocked
		run.ErrorCode = "backup_backend_missing"
		run.ErrorMessage = "backup backend is not configured"
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_backup_runs WHERE status IN ('pending', 'running') AND target_type = $1 AND COALESCE(target_tenant_id, '') = $2 AND COALESCE(target_project_id, '') = $3 AND COALESCE(target_app_id, '') = $4)`, run.Target.Type, run.Target.TenantID, run.Target.ProjectID, run.Target.AppID).Scan(&active); err != nil {
		return model.BackupRun{}, mapDBErr(err)
	}
	if active {
		return model.BackupRun{}, ErrConflict
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if run.ID == "" {
		run.ID = model.NewID("backup_run")
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = now
	}
	run.UpdatedAt = now
	targetJSON, err := marshalJSON(run.Target)
	if err != nil {
		return model.BackupRun{}, err
	}
	created, err := scanBackupRun(tx.QueryRowContext(ctx, `
INSERT INTO fugue_backup_runs (id, policy_id, tenant_id, project_id, app_id, target_type, target_tenant_id, target_project_id, target_app_id, target_json, backend_id, trigger, version, status, attempt, retry_count, requested_by_type, requested_by_id, lease_owner, locked_until, heartbeat_at, bytes_written, logical_bytes, artifact_count, error_code, error_message, next_retry_at, created_at, updated_at, started_at, finished_at)
VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, ''), $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)
RETURNING `+backupRunReturningColumns(), run.ID, run.PolicyID, nullIfEmpty(run.TenantID), nullIfEmpty(run.ProjectID), nullIfEmpty(run.AppID), run.Target.Type, nullIfEmpty(run.Target.TenantID), nullIfEmpty(run.Target.ProjectID), nullIfEmpty(run.Target.AppID), targetJSON, run.BackendID, run.Trigger, run.Version, run.Status, run.Attempt, run.RetryCount, run.RequestedByType, run.RequestedByID, run.LeaseOwner, run.LockedUntil, run.HeartbeatAt, run.BytesWritten, run.LogicalBytes, run.ArtifactCount, run.ErrorCode, run.ErrorMessage, run.NextRetryAt, run.CreatedAt, run.UpdatedAt, run.StartedAt, run.FinishedAt))
	if err != nil {
		return model.BackupRun{}, mapDBErr(err)
	}
	if run.PolicyID != "" {
		next, err := nextBackupRunAfter(policySchedule, now)
		if err != nil {
			return model.BackupRun{}, fmt.Errorf("%w: invalid backup schedule: %v", ErrInvalidInput, err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fugue_backup_policies SET last_run_id = $2, last_run_at = $3, next_run_at = $4, updated_at = $3 WHERE id = $1`, run.PolicyID, run.ID, now, next); err != nil {
			return model.BackupRun{}, mapDBErr(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return model.BackupRun{}, err
	}
	return created, nil
}

func (s *Store) pgClaimBackupRun(id, leaseOwner string, now time.Time, leaseTTL time.Duration) (model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lockedUntil := now.Add(leaseTTL)
	claimed, err := scanBackupRun(s.db.QueryRowContext(ctx, `
UPDATE fugue_backup_runs
SET status = 'running', lease_owner = $2, locked_until = $3, heartbeat_at = $4, updated_at = $4, started_at = $4
WHERE id = $1 AND status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= $4)
RETURNING `+backupRunReturningColumns(), id, leaseOwner, lockedUntil, now))
	if errors.Is(err, ErrNotFound) {
		return model.BackupRun{}, ErrConflict
	}
	return claimed, err
}

func (s *Store) pgHeartbeatBackupRun(id, leaseOwner string, now time.Time, leaseTTL time.Duration) (model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lockedUntil := now.Add(leaseTTL)
	heartbeat, err := scanBackupRun(s.db.QueryRowContext(ctx, `
UPDATE fugue_backup_runs
SET locked_until = $3, heartbeat_at = $4, updated_at = $4
WHERE id = $1 AND status = 'running' AND lease_owner = $2 AND locked_until IS NOT NULL AND locked_until >= $4
RETURNING `+backupRunReturningColumns(), id, leaseOwner, lockedUntil, now))
	if errors.Is(err, ErrNotFound) {
		return model.BackupRun{}, ErrConflict
	}
	return heartbeat, err
}

func (s *Store) pgRecoverStaleBackupRun(observed model.BackupRun, now time.Time) (model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	recovered, err := scanBackupRun(s.db.QueryRowContext(ctx, `
UPDATE fugue_backup_runs
SET status = 'failed', locked_until = NULL, heartbeat_at = $8, error_code = $9, error_message = $10, updated_at = $8, finished_at = $8
WHERE id = $1
  AND status = $2
  AND lease_owner = $3
  AND updated_at = $4
  AND locked_until IS NOT DISTINCT FROM $5
  AND heartbeat_at IS NOT DISTINCT FROM $6
  AND next_retry_at IS NOT DISTINCT FROM $7
RETURNING `+backupRunReturningColumns(), observed.ID, observed.Status, observed.LeaseOwner, observed.UpdatedAt, observed.LockedUntil, observed.HeartbeatAt, observed.NextRetryAt, now, backupRunLostErrorCode, backupRunLostErrorMessage))
	if errors.Is(err, ErrNotFound) {
		return model.BackupRun{}, ErrConflict
	}
	return recovered, err
}

func (s *Store) pgFinishBackupRun(id, leaseOwner string, finish BackupRunFinish) (model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BackupRun{}, err
	}
	defer tx.Rollback()
	finished, err := scanBackupRun(tx.QueryRowContext(ctx, `
UPDATE fugue_backup_runs
SET status = $3, locked_until = NULL, heartbeat_at = $4, bytes_written = $5, artifact_count = $6, error_code = $7, error_message = $8, updated_at = $4, finished_at = $4
WHERE id = $1 AND status = 'running' AND lease_owner = $2 AND locked_until IS NOT NULL AND locked_until >= $4
RETURNING `+backupRunReturningColumns(), id, leaseOwner, finish.Status, finish.FinishedAt, finish.BytesWritten, finish.ArtifactCount, finish.ErrorCode, finish.ErrorMessage))
	if errors.Is(err, ErrNotFound) {
		return model.BackupRun{}, ErrConflict
	}
	if err != nil {
		return model.BackupRun{}, err
	}
	if finished.Status == model.BackupRunStatusSucceeded && finished.PolicyID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE fugue_backup_policies SET last_successful_run_id = $2, last_successful_at = $3, updated_at = $3 WHERE id = $1`, finished.PolicyID, finished.ID, finish.FinishedAt); err != nil {
			return model.BackupRun{}, mapDBErr(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return model.BackupRun{}, err
	}
	if finished.Status == model.BackupRunStatusSucceeded && finished.PolicyID != "" {
		_ = s.pgApplyBackupRetention(finished.PolicyID)
	}
	return finished, nil
}

func (s *Store) pgListBackupRuns(filter BackupRunFilter) ([]model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupRunSelectSQL()
	clauses, args := backupRunFilterClauses(filter)
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}
	query += ` ORDER BY created_at DESC`
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultBackupRunHistoryLimit
	}
	args = append(args, limit)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	runs := []model.BackupRun{}
	for rows.Next() {
		run, err := scanBackupRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, mapDBErr(rows.Err())
}

func (s *Store) pgListDueBackupRetryRuns(now time.Time, limit int) ([]model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, backupRunSelectSQL()+`
 WHERE status = 'pending'
   AND trigger = 'retry'
   AND next_retry_at IS NOT NULL
   AND next_retry_at <= $1
 ORDER BY next_retry_at ASC, created_at ASC
 LIMIT $2`, now, limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	runs := []model.BackupRun{}
	for rows.Next() {
		run, err := scanBackupRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, mapDBErr(rows.Err())
}

func (s *Store) pgListStaleBackupRuns(now time.Time, leaseTTL time.Duration, limit int) ([]model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lastSeenSQL := `CASE
     WHEN status = 'pending' AND trigger = 'retry' THEN GREATEST(COALESCE(heartbeat_at, updated_at), COALESCE(next_retry_at, updated_at))
     ELSE COALESCE(heartbeat_at, updated_at)
   END`
	deadlineSQL := `CASE
     WHEN locked_until IS NOT NULL THEN locked_until
     ELSE (` + lastSeenSQL + `) + ($2 * INTERVAL '1 microsecond')
   END`
	query := backupRunSelectSQL() + `
 WHERE status IN ('running', 'pending')
   AND NOT (status = 'pending' AND trigger = 'retry' AND next_retry_at IS NOT NULL AND next_retry_at > $1)
   AND (` + deadlineSQL + `) < $1
 ORDER BY (` + deadlineSQL + `) ASC, created_at ASC
 LIMIT $3`
	rows, err := s.db.QueryContext(ctx, query, now, leaseTTL.Microseconds(), limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	runs := []model.BackupRun{}
	for rows.Next() {
		run, err := scanBackupRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, mapDBErr(rows.Err())
}

func (s *Store) pgGetBackupRun(id string, tenantID string, platformAdmin bool) (model.BackupRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupRunSelectSQL() + ` WHERE id = $1`
	args := []any{id}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $2`
	}
	return scanBackupRun(s.db.QueryRowContext(ctx, query, args...))
}

func (s *Store) pgUpdateBackupRun(id string, update BackupRunUpdate) (model.BackupRun, error) {
	current, err := s.pgGetBackupRun(id, "", true)
	if err != nil {
		return model.BackupRun{}, err
	}
	applyBackupRunUpdate(&current, update)
	current.UpdatedAt = time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanBackupRun(s.db.QueryRowContext(ctx, `
UPDATE fugue_backup_runs SET status = $2, lease_owner = $3, locked_until = $4, heartbeat_at = $5, bytes_written = $6, logical_bytes = $7, artifact_count = $8, error_code = $9, error_message = $10, next_retry_at = $11, updated_at = $12, started_at = $13, finished_at = $14
WHERE id = $1
RETURNING `+backupRunReturningColumns(), current.ID, current.Status, current.LeaseOwner, current.LockedUntil, current.HeartbeatAt, current.BytesWritten, current.LogicalBytes, current.ArtifactCount, current.ErrorCode, current.ErrorMessage, current.NextRetryAt, current.UpdatedAt, current.StartedAt, current.FinishedAt))
}

func (s *Store) pgCreateBackupArtifact(artifact model.BackupArtifact, leaseOwner string) (model.BackupArtifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	artifact = model.NormalizeBackupArtifact(artifact)
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BackupArtifact{}, err
	}
	defer tx.Rollback()
	if leaseOwner != "" {
		var runPolicyID sql.NullString
		err := tx.QueryRowContext(ctx, `
SELECT policy_id
FROM fugue_backup_runs
WHERE id = $1 AND status = 'running' AND lease_owner = $2 AND locked_until IS NOT NULL AND locked_until >= $3
FOR UPDATE`, artifact.RunID, leaseOwner, now).Scan(&runPolicyID)
		if errors.Is(err, sql.ErrNoRows) {
			return model.BackupArtifact{}, ErrConflict
		}
		if err != nil {
			return model.BackupArtifact{}, mapDBErr(err)
		}
		linkedPolicyID := ""
		if runPolicyID.Valid {
			linkedPolicyID = runPolicyID.String
		}
		if artifact.PolicyID == "" {
			artifact.PolicyID = linkedPolicyID
		} else if artifact.PolicyID != linkedPolicyID {
			return model.BackupArtifact{}, ErrConflict
		}
	}
	if artifact.ID == "" {
		artifact.ID = model.NewID("backup_artifact")
	}
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = now
	}
	artifact.Manifest.ArtifactID = artifact.ID
	artifact.Manifest.RunID = artifact.RunID
	artifact.Manifest.PolicyID = artifact.PolicyID
	artifact.Manifest.Target = artifact.Target
	artifact.Manifest.Kind = artifact.Kind
	artifact.Manifest.Version = artifact.Version
	artifact.Manifest.ObjectKey = artifact.ObjectKey
	artifact.Manifest.ManifestObjectKey = artifact.ManifestObjectKey
	artifact.Manifest.SizeBytes = artifact.SizeBytes
	artifact.Manifest.LogicalBytes = artifact.LogicalBytes
	artifact.Manifest.SHA256 = artifact.SHA256
	if artifact.Manifest.CreatedAt.IsZero() {
		artifact.Manifest.CreatedAt = artifact.CreatedAt
	}
	artifact.Manifest = model.NormalizeBackupManifest(artifact.Manifest)
	artifact.ManifestDigest = digestBackupManifest(artifact.Manifest)
	targetJSON, err := marshalJSON(artifact.Target)
	if err != nil {
		return model.BackupArtifact{}, err
	}
	manifestJSON, err := marshalJSON(artifact.Manifest)
	if err != nil {
		return model.BackupArtifact{}, err
	}
	created, err := scanBackupArtifact(tx.QueryRowContext(ctx, `
INSERT INTO fugue_backup_artifacts (id, run_id, policy_id, tenant_id, project_id, app_id, target_type, target_tenant_id, target_project_id, target_app_id, target_json, backend_id, kind, version, object_key, manifest_object_key, sha256, size_bytes, logical_bytes, status, protected, billable, billing_class, manifest_digest, manifest_json, created_at, deleted_at)
VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, ''), $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)
RETURNING `+backupArtifactReturningColumns(), artifact.ID, artifact.RunID, artifact.PolicyID, nullIfEmpty(artifact.TenantID), nullIfEmpty(artifact.ProjectID), nullIfEmpty(artifact.AppID), artifact.Target.Type, nullIfEmpty(artifact.Target.TenantID), nullIfEmpty(artifact.Target.ProjectID), nullIfEmpty(artifact.Target.AppID), targetJSON, artifact.BackendID, artifact.Kind, artifact.Version, artifact.ObjectKey, artifact.ManifestObjectKey, artifact.SHA256, artifact.SizeBytes, artifact.LogicalBytes, artifact.Status, artifact.Protected, artifact.Billable, artifact.BillingClass, artifact.ManifestDigest, manifestJSON, artifact.CreatedAt, artifact.DeletedAt))
	if err != nil {
		return model.BackupArtifact{}, mapDBErr(err)
	}
	if artifact.RunID != "" {
		query := `UPDATE fugue_backup_runs SET artifact_count = artifact_count + 1, bytes_written = bytes_written + $2, logical_bytes = logical_bytes + $3, updated_at = $4 WHERE id = $1`
		args := []any{artifact.RunID, artifact.SizeBytes, artifact.LogicalBytes, now}
		if leaseOwner != "" {
			query += ` AND status = 'running' AND lease_owner = $5 AND locked_until IS NOT NULL AND locked_until >= $4`
			args = append(args, leaseOwner)
		}
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return model.BackupArtifact{}, mapDBErr(err)
		}
		if leaseOwner != "" {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return model.BackupArtifact{}, err
			}
			if rowsAffected != 1 {
				return model.BackupArtifact{}, ErrConflict
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return model.BackupArtifact{}, err
	}
	return created, nil
}

func (s *Store) pgListBackupArtifacts(filter BackupArtifactFilter) ([]model.BackupArtifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupArtifactSelectSQL()
	clauses, args := backupArtifactFilterClauses(filter)
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}
	query += ` ORDER BY created_at DESC`
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultBackupArtifactHistoryLimit
	}
	args = append(args, limit)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	artifacts := []model.BackupArtifact{}
	for rows.Next() {
		artifact, err := scanBackupArtifact(rows)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, mapDBErr(rows.Err())
}

func (s *Store) pgGetBackupArtifact(id string, tenantID string, platformAdmin bool) (model.BackupArtifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupArtifactSelectSQL() + ` WHERE id = $1`
	args := []any{id}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $2`
	}
	return scanBackupArtifact(s.db.QueryRowContext(ctx, query, args...))
}

func (s *Store) pgMarkBackupArtifactDeleted(id, tenantID string, platformAdmin bool) (model.BackupArtifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	query := `UPDATE fugue_backup_artifacts SET status = 'deleted', deleted_at = $2 WHERE id = $1 AND protected = FALSE`
	args := []any{id, now}
	if !platformAdmin {
		query += ` AND tenant_id = $3`
		args = append(args, tenantID)
	}
	query += ` RETURNING ` + backupArtifactReturningColumns()
	artifact, err := scanBackupArtifact(s.db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.BackupArtifact{}, mapDBErr(err)
	}
	return artifact, nil
}

func (s *Store) pgApplyBackupRetention(policyID string) error {
	policy, err := s.pgGetBackupPolicy(policyID, "", true)
	if err != nil {
		return err
	}
	retain := policy.RetainCount
	if retain <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM fugue_backup_artifacts WHERE policy_id = $1 AND status = 'active' AND protected = FALSE ORDER BY created_at DESC OFFSET $2`, policyID, retain)
	if err != nil {
		return mapDBErr(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return mapDBErr(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return mapDBErr(err)
	}
	for _, id := range ids {
		if _, err := s.pgMarkBackupArtifactDeleted(id, "", true); err != nil && !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return nil
}

func (s *Store) pgCreateBackupRestorePlan(plan model.BackupRestorePlan) (model.BackupRestorePlan, error) {
	plan.ArtifactID = strings.TrimSpace(plan.ArtifactID)
	artifact, err := s.pgGetBackupArtifact(plan.ArtifactID, "", true)
	if err != nil {
		return model.BackupRestorePlan{}, err
	}
	now := time.Now().UTC()
	if plan.ID == "" {
		plan.ID = model.NewID("backup_restore_plan")
	}
	if backupTargetIsEmpty(plan.Target) {
		plan.Target = artifact.Target
	} else if strings.TrimSpace(plan.Target.Type) == "" {
		return model.BackupRestorePlan{}, ErrInvalidInput
	}
	plan.TenantID = firstNonEmpty(plan.TenantID, artifact.TenantID)
	plan.ProjectID = firstNonEmpty(plan.ProjectID, artifact.ProjectID)
	plan.AppID = firstNonEmpty(plan.AppID, artifact.AppID)
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = now
	}
	plan.UpdatedAt = now
	plan.Mode = model.NormalizeBackupRestoreMode(plan.Mode)
	if len(plan.Phases) == 0 {
		plan.Phases = defaultRestorePlanPhases(plan)
	}
	plan = model.NormalizeBackupRestorePlan(plan)
	targetJSON, err := marshalJSON(plan.Target)
	if err != nil {
		return model.BackupRestorePlan{}, err
	}
	warningsJSON, err := marshalJSON(plan.Warnings)
	if err != nil {
		return model.BackupRestorePlan{}, err
	}
	phasesJSON, err := marshalJSON(plan.Phases)
	if err != nil {
		return model.BackupRestorePlan{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanBackupRestorePlan(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_backup_restore_plans (id, tenant_id, project_id, app_id, artifact_id, target_type, target_json, mode, status, warnings_json, phases_json, created_by_type, created_by_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
RETURNING `+backupRestorePlanReturningColumns(), plan.ID, nullIfEmpty(plan.TenantID), nullIfEmpty(plan.ProjectID), nullIfEmpty(plan.AppID), plan.ArtifactID, plan.Target.Type, targetJSON, plan.Mode, plan.Status, warningsJSON, phasesJSON, plan.CreatedByType, plan.CreatedByID, plan.CreatedAt, plan.UpdatedAt))
}

func (s *Store) pgListBackupRestorePlans(tenantID string, platformAdmin bool, limit int) ([]model.BackupRestorePlan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupRestorePlanSelectSQL()
	args := []any{}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` WHERE tenant_id = $1`
	}
	query += ` ORDER BY created_at DESC`
	if limit <= 0 {
		limit = defaultBackupRestorePlanListLimit
	}
	args = append(args, limit)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	plans := []model.BackupRestorePlan{}
	for rows.Next() {
		plan, err := scanBackupRestorePlan(rows)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, mapDBErr(rows.Err())
}

func (s *Store) pgGetBackupRestorePlan(id string, tenantID string, platformAdmin bool) (model.BackupRestorePlan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupRestorePlanSelectSQL() + ` WHERE id = $1`
	args := []any{id}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $2`
	}
	return scanBackupRestorePlan(s.db.QueryRowContext(ctx, query, args...))
}

func (s *Store) pgCreateBackupRestoreRun(run model.BackupRestoreRun) (model.BackupRestoreRun, error) {
	plan, err := s.pgGetBackupRestorePlan(run.PlanID, "", true)
	if err != nil {
		return model.BackupRestoreRun{}, err
	}
	now := time.Now().UTC()
	if run.ID == "" {
		run.ID = model.NewID("backup_restore_run")
	}
	run.TenantID = firstNonEmpty(run.TenantID, plan.TenantID)
	run.ProjectID = firstNonEmpty(run.ProjectID, plan.ProjectID)
	run.AppID = firstNonEmpty(run.AppID, plan.AppID)
	run.ArtifactID = firstNonEmpty(run.ArtifactID, plan.ArtifactID)
	run.Mode = firstNonEmpty(run.Mode, plan.Mode)
	if run.CreatedAt.IsZero() {
		run.CreatedAt = now
	}
	run.UpdatedAt = now
	if len(run.Phases) == 0 {
		run.Phases = plan.Phases
	}
	run = model.NormalizeBackupRestoreRun(run)
	phasesJSON, err := marshalJSON(run.Phases)
	if err != nil {
		return model.BackupRestoreRun{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanBackupRestoreRun(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_backup_restore_runs (id, plan_id, tenant_id, project_id, app_id, artifact_id, mode, status, phases_json, error_code, error_message, requested_by_type, requested_by_id, created_at, updated_at, started_at, finished_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
RETURNING `+backupRestoreRunReturningColumns(), run.ID, run.PlanID, nullIfEmpty(run.TenantID), nullIfEmpty(run.ProjectID), nullIfEmpty(run.AppID), run.ArtifactID, run.Mode, run.Status, phasesJSON, run.ErrorCode, run.ErrorMessage, run.RequestedByType, run.RequestedByID, run.CreatedAt, run.UpdatedAt, run.StartedAt, run.FinishedAt))
}

func (s *Store) pgListBackupRestoreRuns(tenantID string, platformAdmin bool, limit int) ([]model.BackupRestoreRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := backupRestoreRunSelectSQL()
	args := []any{}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` WHERE tenant_id = $1`
	}
	query += ` ORDER BY created_at DESC`
	if limit <= 0 {
		limit = defaultBackupRestoreRunListLimit
	}
	args = append(args, limit)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	runs := []model.BackupRestoreRun{}
	for rows.Next() {
		run, err := scanBackupRestoreRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, mapDBErr(rows.Err())
}

func (s *Store) pgBackupUsage(tenantID string, platformAdmin bool) (model.BackupUsage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := `SELECT COALESCE(SUM(size_bytes), 0) FROM fugue_backup_artifacts WHERE status = 'active' AND billable = TRUE`
	args := []any{}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $1`
	}
	var bytes int64
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&bytes); err != nil {
		return model.BackupUsage{}, mapDBErr(err)
	}
	return model.NormalizeBackupUsage(model.BackupUsage{
		TenantID:              tenantID,
		Provider:              model.DataBackendProviderCloudflareR2,
		BillableBytes:         bytes,
		CloudflareR2PriceCode: backupUsageCloudflareR2PriceCode,
		MarkupPercent:         model.BackupR2MarkupPercent,
		EffectiveMultiplier:   1.05,
		Currency:              "USD",
		UpdatedAt:             time.Now().UTC(),
	}), nil
}

func backupBackendSelectSQL() string {
	return `SELECT ` + backupBackendReturningColumns() + ` FROM fugue_backup_backends`
}

func backupBackendReturningColumns() string {
	return `id, tenant_id, name, slug, provider, bucket, region, endpoint, base_url, prefix, status, capabilities_json, credentials_json, credential_secret_id, fugue_managed, billable, last_tested_at, last_test_result, error_message, created_at, updated_at`
}

func backupPolicySelectSQL() string {
	return `SELECT ` + backupPolicyReturningColumns() + ` FROM fugue_backup_policies`
}

func backupPolicyReturningColumns() string {
	return `id, tenant_id, project_id, app_id, name, slug, scope, target_json, backend_id, enabled, status, disabled_reason, schedule, retain_count, retention_json, version, last_run_id, last_successful_run_id, last_run_at, last_successful_at, next_run_at, created_by, created_at, updated_at`
}

func backupRunSelectSQL() string {
	return `SELECT ` + backupRunReturningColumns() + ` FROM fugue_backup_runs`
}

func backupRunReturningColumns() string {
	return `id, policy_id, tenant_id, project_id, app_id, target_json, backend_id, trigger, version, status, attempt, retry_count, requested_by_type, requested_by_id, lease_owner, locked_until, heartbeat_at, bytes_written, logical_bytes, artifact_count, error_code, error_message, next_retry_at, created_at, updated_at, started_at, finished_at`
}

func backupArtifactSelectSQL() string {
	return `SELECT ` + backupArtifactReturningColumns() + ` FROM fugue_backup_artifacts`
}

func backupArtifactReturningColumns() string {
	return `id, run_id, policy_id, tenant_id, project_id, app_id, target_json, backend_id, kind, version, object_key, manifest_object_key, sha256, size_bytes, logical_bytes, status, protected, billable, billing_class, manifest_digest, manifest_json, created_at, deleted_at`
}

func backupRestorePlanSelectSQL() string {
	return `SELECT ` + backupRestorePlanReturningColumns() + ` FROM fugue_backup_restore_plans`
}

func backupRestorePlanReturningColumns() string {
	return `id, tenant_id, project_id, app_id, artifact_id, target_json, mode, status, warnings_json, phases_json, created_by_type, created_by_id, created_at, updated_at`
}

func backupRestoreRunSelectSQL() string {
	return `SELECT ` + backupRestoreRunReturningColumns() + ` FROM fugue_backup_restore_runs`
}

func backupRestoreRunReturningColumns() string {
	return `id, plan_id, tenant_id, project_id, app_id, artifact_id, mode, status, phases_json, error_code, error_message, requested_by_type, requested_by_id, created_at, updated_at, started_at, finished_at`
}

func backupPolicyFilterClauses(filter BackupPolicyFilter) ([]string, []any) {
	var clauses []string
	var args []any
	if !filter.PlatformAdmin {
		args = append(args, filter.TenantID)
		clauses = append(clauses, fmt.Sprintf(`tenant_id = $%d`, len(args)))
	}
	if !filter.IncludeDisabled {
		clauses = append(clauses, `enabled = TRUE`)
		clauses = append(clauses, `status <> 'disabled'`)
	}
	if filter.ProjectID != "" {
		args = append(args, filter.ProjectID)
		clauses = append(clauses, fmt.Sprintf(`(project_id = $%d OR target_project_id = $%d)`, len(args), len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf(`(app_id = $%d OR target_app_id = $%d)`, len(args), len(args)))
	}
	if filter.TargetType != "" {
		args = append(args, filter.TargetType)
		clauses = append(clauses, fmt.Sprintf(`target_type = $%d`, len(args)))
	}
	return clauses, args
}

func backupRunFilterClauses(filter BackupRunFilter) ([]string, []any) {
	var clauses []string
	var args []any
	if !filter.PlatformAdmin {
		args = append(args, filter.TenantID)
		clauses = append(clauses, fmt.Sprintf(`tenant_id = $%d`, len(args)))
	}
	if filter.ProjectID != "" {
		args = append(args, filter.ProjectID)
		clauses = append(clauses, fmt.Sprintf(`(project_id = $%d OR target_project_id = $%d)`, len(args), len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf(`(app_id = $%d OR target_app_id = $%d)`, len(args), len(args)))
	}
	if filter.PolicyID != "" {
		args = append(args, filter.PolicyID)
		clauses = append(clauses, fmt.Sprintf(`policy_id = $%d`, len(args)))
	}
	if filter.TargetType != "" {
		args = append(args, filter.TargetType)
		clauses = append(clauses, fmt.Sprintf(`target_type = $%d`, len(args)))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		clauses = append(clauses, fmt.Sprintf(`status = $%d`, len(args)))
	}
	return clauses, args
}

func backupArtifactFilterClauses(filter BackupArtifactFilter) ([]string, []any) {
	var clauses []string
	var args []any
	if !filter.PlatformAdmin {
		args = append(args, filter.TenantID)
		clauses = append(clauses, fmt.Sprintf(`tenant_id = $%d`, len(args)))
	}
	if filter.ActiveOnly {
		clauses = append(clauses, `status = 'active'`)
	}
	if filter.ProjectID != "" {
		args = append(args, filter.ProjectID)
		clauses = append(clauses, fmt.Sprintf(`(project_id = $%d OR target_project_id = $%d)`, len(args), len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf(`(app_id = $%d OR target_app_id = $%d)`, len(args), len(args)))
	}
	if filter.PolicyID != "" {
		args = append(args, filter.PolicyID)
		clauses = append(clauses, fmt.Sprintf(`policy_id = $%d`, len(args)))
	}
	if filter.RunID != "" {
		args = append(args, filter.RunID)
		clauses = append(clauses, fmt.Sprintf(`run_id = $%d`, len(args)))
	}
	if filter.TargetType != "" {
		args = append(args, filter.TargetType)
		clauses = append(clauses, fmt.Sprintf(`target_type = $%d`, len(args)))
	}
	return clauses, args
}
