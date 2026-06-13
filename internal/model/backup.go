package model

import (
	"sort"
	"strings"
	"time"
)

const (
	BackupScopePlatform = "platform"
	BackupScopeTenant   = "tenant"
	BackupScopeProject  = "project"
	BackupScopeApp      = "app"

	BackupTargetControlPlaneDatabase = "control-plane-db"
	BackupTargetAppDatabase          = "app-database"
	BackupTargetPersistentStorage    = "persistent-storage"
	BackupTargetDataWorkspace        = "data-workspace"
	BackupTargetRegistry             = "registry"
	BackupTargetPlatformComponent    = "platform-component"

	BackupPolicyStatusActive           = "active"
	BackupPolicyStatusDisabled         = "disabled"
	BackupPolicyStatusBlockedNoBackend = "blocked_no_backend"
	BackupPolicyStatusError            = "error"

	BackupRunTriggerManual    = "manual"
	BackupRunTriggerScheduled = "scheduled"
	BackupRunTriggerRetry     = "retry"

	BackupRunStatusPending   = "pending"
	BackupRunStatusRunning   = "running"
	BackupRunStatusSucceeded = "succeeded"
	BackupRunStatusFailed    = "failed"
	BackupRunStatusCanceled  = "canceled"
	BackupRunStatusBlocked   = "blocked"

	BackupArtifactStatusActive  = "active"
	BackupArtifactStatusDeleted = "deleted"
	BackupArtifactStatusExpired = "expired"

	BackupArtifactKindManifest           = "manifest"
	BackupArtifactKindControlPlanePGDump = "control-plane-pg-dump"
	BackupArtifactKindAppPGDump          = "app-pg-dump"
	BackupArtifactKindFileArchive        = "file-archive"
	BackupArtifactKindDataSnapshot       = "data-snapshot"
	BackupArtifactKindRegistryArchive    = "registry-archive"

	BackupRestoreModePlanOnly            = "plan-only"
	BackupRestoreModeClone               = "clone"
	BackupRestoreModeReplace             = "replace"
	BackupRestoreModeOfflineControlPlane = "offline-control-plane"

	BackupRestoreStatusPlanned   = "planned"
	BackupRestoreStatusReady     = "ready"
	BackupRestoreStatusRunning   = "running"
	BackupRestoreStatusSucceeded = "succeeded"
	BackupRestoreStatusFailed    = "failed"
	BackupRestoreStatusCanceled  = "canceled"

	BackupDefaultSchedule    = "0 * * * *"
	BackupDefaultRetainCount = 3

	BackupR2MarkupPercent = 5
)

type BackupBackend struct {
	ID                 string                  `json:"id"`
	TenantID           string                  `json:"tenant_id,omitempty"`
	Name               string                  `json:"name"`
	Slug               string                  `json:"slug"`
	Provider           string                  `json:"provider"`
	Bucket             string                  `json:"bucket,omitempty"`
	Region             string                  `json:"region,omitempty"`
	Endpoint           string                  `json:"endpoint,omitempty"`
	BaseURL            string                  `json:"base_url,omitempty"`
	Prefix             string                  `json:"prefix,omitempty"`
	Status             string                  `json:"status"`
	Capabilities       DataBackendCapabilities `json:"capabilities"`
	Credentials        DataBackendCredentials  `json:"credentials,omitempty"`
	CredentialSecretID string                  `json:"credential_secret_id,omitempty"`
	FugueManaged       bool                    `json:"fugue_managed"`
	Billable           bool                    `json:"billable"`
	LastTestedAt       *time.Time              `json:"last_tested_at,omitempty"`
	LastTestResult     string                  `json:"last_test_result,omitempty"`
	ErrorMessage       string                  `json:"error_message,omitempty"`
	CreatedAt          time.Time               `json:"created_at"`
	UpdatedAt          time.Time               `json:"updated_at"`
}

type BackupBackendSecret struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id,omitempty"`
	BackendID   string    `json:"backend_id"`
	Ciphertext  string    `json:"ciphertext"`
	KeyID       string    `json:"key_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	LastRotated time.Time `json:"last_rotated_at,omitempty"`
}

type BackupTarget struct {
	Type        string `json:"type"`
	TenantID    string `json:"tenant_id,omitempty"`
	ProjectID   string `json:"project_id,omitempty"`
	AppID       string `json:"app_id,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	RuntimeID   string `json:"runtime_id,omitempty"`
	Name        string `json:"name,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
	Database    string `json:"database,omitempty"`
	Component   string `json:"component,omitempty"`
}

type BackupRetentionPolicy struct {
	RetainCount   int `json:"retain_count,omitempty"`
	RetainDays    int `json:"retain_days,omitempty"`
	ProtectLatest int `json:"protect_latest,omitempty"`
}

type BackupPolicy struct {
	ID                  string                `json:"id"`
	TenantID            string                `json:"tenant_id,omitempty"`
	ProjectID           string                `json:"project_id,omitempty"`
	AppID               string                `json:"app_id,omitempty"`
	Name                string                `json:"name"`
	Slug                string                `json:"slug"`
	Scope               string                `json:"scope"`
	Target              BackupTarget          `json:"target"`
	BackendID           string                `json:"backend_id,omitempty"`
	Enabled             bool                  `json:"enabled"`
	Status              string                `json:"status"`
	DisabledReason      string                `json:"disabled_reason,omitempty"`
	Schedule            string                `json:"schedule,omitempty"`
	RetainCount         int                   `json:"retain_count,omitempty"`
	Retention           BackupRetentionPolicy `json:"retention,omitempty"`
	Version             string                `json:"version,omitempty"`
	LastRunID           string                `json:"last_run_id,omitempty"`
	LastSuccessfulRunID string                `json:"last_successful_run_id,omitempty"`
	LastRunAt           *time.Time            `json:"last_run_at,omitempty"`
	LastSuccessfulAt    *time.Time            `json:"last_successful_at,omitempty"`
	NextRunAt           *time.Time            `json:"next_run_at,omitempty"`
	CreatedBy           string                `json:"created_by,omitempty"`
	CreatedAt           time.Time             `json:"created_at"`
	UpdatedAt           time.Time             `json:"updated_at"`
}

type BackupRun struct {
	ID              string       `json:"id"`
	PolicyID        string       `json:"policy_id,omitempty"`
	TenantID        string       `json:"tenant_id,omitempty"`
	ProjectID       string       `json:"project_id,omitempty"`
	AppID           string       `json:"app_id,omitempty"`
	Target          BackupTarget `json:"target"`
	BackendID       string       `json:"backend_id,omitempty"`
	Trigger         string       `json:"trigger"`
	Version         string       `json:"version,omitempty"`
	Status          string       `json:"status"`
	Attempt         int          `json:"attempt"`
	RetryCount      int          `json:"retry_count,omitempty"`
	RequestedByType string       `json:"requested_by_type,omitempty"`
	RequestedByID   string       `json:"requested_by_id,omitempty"`
	LeaseOwner      string       `json:"lease_owner,omitempty"`
	LockedUntil     *time.Time   `json:"locked_until,omitempty"`
	HeartbeatAt     *time.Time   `json:"heartbeat_at,omitempty"`
	BytesWritten    int64        `json:"bytes_written"`
	LogicalBytes    int64        `json:"logical_bytes"`
	ArtifactCount   int          `json:"artifact_count"`
	ErrorCode       string       `json:"error_code,omitempty"`
	ErrorMessage    string       `json:"error_message,omitempty"`
	NextRetryAt     *time.Time   `json:"next_retry_at,omitempty"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
	StartedAt       *time.Time   `json:"started_at,omitempty"`
	FinishedAt      *time.Time   `json:"finished_at,omitempty"`
}

type BackupManifestFile struct {
	Path      string    `json:"path"`
	Kind      string    `json:"kind,omitempty"`
	Size      int64     `json:"size,omitempty"`
	SHA256    string    `json:"sha256,omitempty"`
	ObjectKey string    `json:"object_key,omitempty"`
	MTime     time.Time `json:"mtime,omitempty"`
}

type BackupArtifactEncryption struct {
	Algorithm string `json:"algorithm,omitempty"`
	KeyID     string `json:"key_id,omitempty"`
}

type BackupManifest struct {
	SchemaVersion     string                    `json:"schema_version"`
	ArtifactID        string                    `json:"artifact_id,omitempty"`
	RunID             string                    `json:"run_id,omitempty"`
	PolicyID          string                    `json:"policy_id,omitempty"`
	Target            BackupTarget              `json:"target"`
	Kind              string                    `json:"kind"`
	Version           string                    `json:"version,omitempty"`
	Format            string                    `json:"format,omitempty"`
	Compression       string                    `json:"compression,omitempty"`
	ObjectKey         string                    `json:"object_key,omitempty"`
	ManifestObjectKey string                    `json:"manifest_object_key,omitempty"`
	SizeBytes         int64                     `json:"size_bytes,omitempty"`
	LogicalBytes      int64                     `json:"logical_bytes,omitempty"`
	SHA256            string                    `json:"sha256,omitempty"`
	StoreFingerprint  string                    `json:"store_fingerprint,omitempty"`
	Invariants        map[string]string         `json:"invariants,omitempty"`
	Metadata          map[string]string         `json:"metadata,omitempty"`
	Encryption        *BackupArtifactEncryption `json:"encryption,omitempty"`
	Files             []BackupManifestFile      `json:"files,omitempty"`
	CreatedAt         time.Time                 `json:"created_at"`
}

type BackupArtifact struct {
	ID                string         `json:"id"`
	RunID             string         `json:"run_id,omitempty"`
	PolicyID          string         `json:"policy_id,omitempty"`
	TenantID          string         `json:"tenant_id,omitempty"`
	ProjectID         string         `json:"project_id,omitempty"`
	AppID             string         `json:"app_id,omitempty"`
	Target            BackupTarget   `json:"target"`
	BackendID         string         `json:"backend_id,omitempty"`
	Kind              string         `json:"kind"`
	Version           string         `json:"version,omitempty"`
	ObjectKey         string         `json:"object_key,omitempty"`
	ManifestObjectKey string         `json:"manifest_object_key,omitempty"`
	SHA256            string         `json:"sha256,omitempty"`
	SizeBytes         int64          `json:"size_bytes"`
	LogicalBytes      int64          `json:"logical_bytes"`
	Status            string         `json:"status"`
	Protected         bool           `json:"protected"`
	Billable          bool           `json:"billable"`
	BillingClass      string         `json:"billing_class,omitempty"`
	ManifestDigest    string         `json:"manifest_digest,omitempty"`
	Manifest          BackupManifest `json:"manifest"`
	CreatedAt         time.Time      `json:"created_at"`
	DeletedAt         *time.Time     `json:"deleted_at,omitempty"`
}

type BackupRestorePhase struct {
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	Message     string     `json:"message,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type BackupRestorePlan struct {
	ID            string               `json:"id"`
	TenantID      string               `json:"tenant_id,omitempty"`
	ProjectID     string               `json:"project_id,omitempty"`
	AppID         string               `json:"app_id,omitempty"`
	ArtifactID    string               `json:"artifact_id"`
	Target        BackupTarget         `json:"target"`
	Mode          string               `json:"mode"`
	Status        string               `json:"status"`
	Warnings      []string             `json:"warnings,omitempty"`
	Phases        []BackupRestorePhase `json:"phases,omitempty"`
	CreatedByType string               `json:"created_by_type,omitempty"`
	CreatedByID   string               `json:"created_by_id,omitempty"`
	CreatedAt     time.Time            `json:"created_at"`
	UpdatedAt     time.Time            `json:"updated_at"`
}

type BackupRestoreRun struct {
	ID              string               `json:"id"`
	PlanID          string               `json:"plan_id"`
	TenantID        string               `json:"tenant_id,omitempty"`
	ProjectID       string               `json:"project_id,omitempty"`
	AppID           string               `json:"app_id,omitempty"`
	ArtifactID      string               `json:"artifact_id"`
	Mode            string               `json:"mode"`
	Status          string               `json:"status"`
	Phases          []BackupRestorePhase `json:"phases,omitempty"`
	ErrorCode       string               `json:"error_code,omitempty"`
	ErrorMessage    string               `json:"error_message,omitempty"`
	RequestedByType string               `json:"requested_by_type,omitempty"`
	RequestedByID   string               `json:"requested_by_id,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
	StartedAt       *time.Time           `json:"started_at,omitempty"`
	FinishedAt      *time.Time           `json:"finished_at,omitempty"`
}

type BackupUsage struct {
	TenantID              string    `json:"tenant_id,omitempty"`
	BackendID             string    `json:"backend_id,omitempty"`
	Provider              string    `json:"provider"`
	BillableBytes         int64     `json:"billable_bytes"`
	CloudflareR2PriceCode string    `json:"cloudflare_r2_price_code,omitempty"`
	MarkupPercent         int       `json:"markup_percent"`
	EffectiveMultiplier   float64   `json:"effective_multiplier"`
	Currency              string    `json:"currency"`
	UpdatedAt             time.Time `json:"updated_at"`
}

type BackupPosture struct {
	Target               BackupTarget `json:"target"`
	Status               string       `json:"status"`
	Message              string       `json:"message,omitempty"`
	PolicyID             string       `json:"policy_id,omitempty"`
	LastSuccessfulRunID  string       `json:"last_successful_run_id,omitempty"`
	LastSuccessfulAt     *time.Time   `json:"last_successful_at,omitempty"`
	BillableBytes        int64        `json:"billable_bytes,omitempty"`
	Externalized         bool         `json:"externalized,omitempty"`
	ExternallyBackedUp   bool         `json:"externally_backed_up,omitempty"`
	CNPGBackupIntegrated bool         `json:"cnpg_backup_integrated,omitempty"`
	RestoreDrillStatus   string       `json:"restore_drill_status,omitempty"`
}

func NormalizeBackupTarget(target BackupTarget) BackupTarget {
	target.Type = NormalizeBackupTargetType(target.Type)
	target.TenantID = strings.TrimSpace(target.TenantID)
	target.ProjectID = strings.TrimSpace(target.ProjectID)
	target.AppID = strings.TrimSpace(target.AppID)
	target.WorkspaceID = strings.TrimSpace(target.WorkspaceID)
	target.RuntimeID = strings.TrimSpace(target.RuntimeID)
	target.Name = strings.TrimSpace(target.Name)
	target.ServiceName = strings.TrimSpace(target.ServiceName)
	target.Database = strings.TrimSpace(target.Database)
	target.Component = strings.TrimSpace(target.Component)
	return target
}

func NormalizeBackupTargetType(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", BackupTargetControlPlaneDatabase, "control-plane-postgres", "platform-db", "control-plane-database":
		return BackupTargetControlPlaneDatabase
	case BackupTargetAppDatabase, "app-db", "managed-postgres":
		return BackupTargetAppDatabase
	case BackupTargetPersistentStorage, "persistent-files", "pvc", "volume":
		return BackupTargetPersistentStorage
	case BackupTargetDataWorkspace, "workspace":
		return BackupTargetDataWorkspace
	case BackupTargetRegistry, "image-registry":
		return BackupTargetRegistry
	case BackupTargetPlatformComponent, "platform", "component":
		return BackupTargetPlatformComponent
	default:
		return strings.TrimSpace(strings.ToLower(raw))
	}
}

func NormalizeBackupScope(raw string, target BackupTarget) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case BackupScopePlatform:
		return BackupScopePlatform
	case BackupScopeTenant:
		return BackupScopeTenant
	case BackupScopeProject:
		return BackupScopeProject
	case BackupScopeApp:
		return BackupScopeApp
	}
	target = NormalizeBackupTarget(target)
	switch {
	case target.Type == BackupTargetControlPlaneDatabase || target.Type == BackupTargetRegistry || target.Type == BackupTargetPlatformComponent:
		return BackupScopePlatform
	case target.AppID != "" || target.Type == BackupTargetAppDatabase || target.Type == BackupTargetPersistentStorage:
		return BackupScopeApp
	case target.ProjectID != "" || target.Type == BackupTargetDataWorkspace:
		return BackupScopeProject
	case target.TenantID != "":
		return BackupScopeTenant
	default:
		return BackupScopePlatform
	}
}

func NormalizeBackupBackend(backend BackupBackend) BackupBackend {
	backend.TenantID = strings.TrimSpace(backend.TenantID)
	backend.Name = strings.TrimSpace(backend.Name)
	if backend.Name == "" {
		backend.Name = "backup-backend"
	}
	backend.Slug = Slugify(firstNonEmpty(backend.Slug, backend.Name))
	backend.Provider = NormalizeDataBackendProvider(backend.Provider)
	backend.Bucket = strings.TrimSpace(backend.Bucket)
	backend.Region = strings.TrimSpace(backend.Region)
	backend.Endpoint = strings.TrimRight(strings.TrimSpace(backend.Endpoint), "/")
	backend.BaseURL = strings.TrimRight(strings.TrimSpace(backend.BaseURL), "/")
	backend.Prefix = strings.Trim(strings.TrimSpace(backend.Prefix), "/")
	backend.Status = strings.TrimSpace(strings.ToLower(backend.Status))
	if backend.Status == "" {
		backend.Status = "active"
	}
	if backend.Capabilities == (DataBackendCapabilities{}) {
		backend.Capabilities = DataBackendCapabilitiesForProvider(backend.Provider)
	}
	backend.Credentials.AccessKeyID = strings.TrimSpace(backend.Credentials.AccessKeyID)
	backend.Credentials.SecretAccessKey = strings.TrimSpace(backend.Credentials.SecretAccessKey)
	backend.Credentials.Token = strings.TrimSpace(backend.Credentials.Token)
	backend.CredentialSecretID = strings.TrimSpace(backend.CredentialSecretID)
	backend.LastTestResult = strings.TrimSpace(strings.ToLower(backend.LastTestResult))
	backend.ErrorMessage = strings.TrimSpace(backend.ErrorMessage)
	return backend
}

func RedactBackupBackendCredentials(backend BackupBackend) BackupBackend {
	backend.Credentials = DataBackendCredentials{AccessKeyID: strings.TrimSpace(backend.Credentials.AccessKeyID)}
	backend.CredentialSecretID = ""
	return backend
}

func BackupBackendAsDataBackend(backend BackupBackend) DataBackend {
	backend = NormalizeBackupBackend(backend)
	return DataBackend{
		ID:                 backend.ID,
		TenantID:           backend.TenantID,
		Name:               backend.Name,
		Slug:               backend.Slug,
		Provider:           backend.Provider,
		Bucket:             backend.Bucket,
		Region:             backend.Region,
		Endpoint:           backend.Endpoint,
		BaseURL:            backend.BaseURL,
		Prefix:             backend.Prefix,
		Status:             backend.Status,
		Capabilities:       backend.Capabilities,
		Credentials:        backend.Credentials,
		CredentialSecretID: backend.CredentialSecretID,
		CreatedAt:          backend.CreatedAt,
		UpdatedAt:          backend.UpdatedAt,
	}
}

func BackupBackendFromDataBackend(backend DataBackend) BackupBackend {
	backend = RedactDataBackendCredentials(backend)
	return NormalizeBackupBackend(BackupBackend{
		ID:                 strings.Replace(backend.ID, "data_backend_", "backup_backend_", 1),
		TenantID:           backend.TenantID,
		Name:               backend.Name,
		Slug:               backend.Slug,
		Provider:           backend.Provider,
		Bucket:             backend.Bucket,
		Region:             backend.Region,
		Endpoint:           backend.Endpoint,
		BaseURL:            backend.BaseURL,
		Prefix:             backend.Prefix,
		Status:             backend.Status,
		Capabilities:       backend.Capabilities,
		Credentials:        backend.Credentials,
		CredentialSecretID: backend.CredentialSecretID,
		FugueManaged:       strings.TrimSpace(backend.TenantID) == "",
		Billable:           strings.TrimSpace(backend.TenantID) == "" && NormalizeDataBackendProvider(backend.Provider) == DataBackendProviderCloudflareR2,
		CreatedAt:          backend.CreatedAt,
		UpdatedAt:          backend.UpdatedAt,
	})
}

func NormalizeBackupPolicy(policy BackupPolicy) BackupPolicy {
	policy.TenantID = strings.TrimSpace(policy.TenantID)
	policy.ProjectID = strings.TrimSpace(policy.ProjectID)
	policy.AppID = strings.TrimSpace(policy.AppID)
	policy.Name = strings.TrimSpace(policy.Name)
	if policy.Name == "" {
		policy.Name = "backup-policy"
	}
	policy.Slug = Slugify(firstNonEmpty(policy.Slug, policy.Name))
	policy.Target = NormalizeBackupTarget(policy.Target)
	policy.Scope = NormalizeBackupScope(policy.Scope, policy.Target)
	policy.BackendID = strings.TrimSpace(policy.BackendID)
	policy.Status = strings.TrimSpace(strings.ToLower(policy.Status))
	if policy.Status == "" {
		if policy.Enabled {
			policy.Status = BackupPolicyStatusActive
		} else {
			policy.Status = BackupPolicyStatusDisabled
		}
	}
	policy.DisabledReason = strings.TrimSpace(policy.DisabledReason)
	policy.Schedule = strings.TrimSpace(policy.Schedule)
	policy.Version = strings.TrimSpace(policy.Version)
	if policy.RetainCount <= 0 {
		policy.RetainCount = policy.Retention.RetainCount
	}
	if policy.RetainCount <= 0 && policy.Target.Type == BackupTargetControlPlaneDatabase {
		policy.RetainCount = BackupDefaultRetainCount
	}
	if policy.Retention.RetainCount <= 0 {
		policy.Retention.RetainCount = policy.RetainCount
	}
	if policy.Retention.ProtectLatest <= 0 && policy.RetainCount > 0 {
		policy.Retention.ProtectLatest = policy.RetainCount
	}
	policy.LastRunID = strings.TrimSpace(policy.LastRunID)
	policy.LastSuccessfulRunID = strings.TrimSpace(policy.LastSuccessfulRunID)
	policy.CreatedBy = strings.TrimSpace(policy.CreatedBy)
	return policy
}

func NormalizeBackupRun(run BackupRun) BackupRun {
	run.PolicyID = strings.TrimSpace(run.PolicyID)
	run.TenantID = strings.TrimSpace(run.TenantID)
	run.ProjectID = strings.TrimSpace(run.ProjectID)
	run.AppID = strings.TrimSpace(run.AppID)
	run.Target = NormalizeBackupTarget(run.Target)
	run.BackendID = strings.TrimSpace(run.BackendID)
	run.Trigger = strings.TrimSpace(strings.ToLower(run.Trigger))
	if run.Trigger == "" {
		run.Trigger = BackupRunTriggerManual
	}
	run.Version = strings.TrimSpace(run.Version)
	run.Status = strings.TrimSpace(strings.ToLower(run.Status))
	if run.Status == "" {
		run.Status = BackupRunStatusPending
	}
	if run.Attempt <= 0 {
		run.Attempt = 1
	}
	run.RequestedByType = strings.TrimSpace(run.RequestedByType)
	run.RequestedByID = strings.TrimSpace(run.RequestedByID)
	run.LeaseOwner = strings.TrimSpace(run.LeaseOwner)
	run.ErrorCode = strings.TrimSpace(run.ErrorCode)
	run.ErrorMessage = strings.TrimSpace(run.ErrorMessage)
	return run
}

func NormalizeBackupManifest(manifest BackupManifest) BackupManifest {
	manifest.SchemaVersion = strings.TrimSpace(manifest.SchemaVersion)
	if manifest.SchemaVersion == "" {
		manifest.SchemaVersion = "fugue.backup/v1"
	}
	manifest.ArtifactID = strings.TrimSpace(manifest.ArtifactID)
	manifest.RunID = strings.TrimSpace(manifest.RunID)
	manifest.PolicyID = strings.TrimSpace(manifest.PolicyID)
	manifest.Target = NormalizeBackupTarget(manifest.Target)
	manifest.Kind = NormalizeBackupArtifactKind(manifest.Kind)
	manifest.Version = strings.TrimSpace(manifest.Version)
	manifest.Format = strings.TrimSpace(manifest.Format)
	manifest.Compression = strings.TrimSpace(manifest.Compression)
	manifest.ObjectKey = strings.Trim(strings.TrimSpace(manifest.ObjectKey), "/")
	manifest.ManifestObjectKey = strings.Trim(strings.TrimSpace(manifest.ManifestObjectKey), "/")
	manifest.SHA256 = strings.TrimSpace(strings.ToLower(manifest.SHA256))
	manifest.StoreFingerprint = strings.TrimSpace(manifest.StoreFingerprint)
	manifest.Invariants = compactStringMap(manifest.Invariants)
	manifest.Metadata = compactStringMap(manifest.Metadata)
	files := make([]BackupManifestFile, 0, len(manifest.Files))
	for _, file := range manifest.Files {
		file.Path = strings.TrimSpace(file.Path)
		file.Kind = strings.TrimSpace(file.Kind)
		file.SHA256 = strings.TrimSpace(strings.ToLower(file.SHA256))
		file.ObjectKey = strings.Trim(strings.TrimSpace(file.ObjectKey), "/")
		if file.Path == "" && file.ObjectKey == "" {
			continue
		}
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	manifest.Files = files
	return manifest
}

func NormalizeBackupArtifactKind(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", BackupArtifactKindManifest:
		return BackupArtifactKindManifest
	case BackupArtifactKindControlPlanePGDump, "control-plane-dump", "pg_dump":
		return BackupArtifactKindControlPlanePGDump
	case BackupArtifactKindAppPGDump, "app-dump":
		return BackupArtifactKindAppPGDump
	case BackupArtifactKindFileArchive, "files":
		return BackupArtifactKindFileArchive
	case BackupArtifactKindDataSnapshot, "data-workspace-snapshot":
		return BackupArtifactKindDataSnapshot
	case BackupArtifactKindRegistryArchive, "registry":
		return BackupArtifactKindRegistryArchive
	default:
		return strings.TrimSpace(strings.ToLower(raw))
	}
}

func NormalizeBackupArtifact(artifact BackupArtifact) BackupArtifact {
	artifact.RunID = strings.TrimSpace(artifact.RunID)
	artifact.PolicyID = strings.TrimSpace(artifact.PolicyID)
	artifact.TenantID = strings.TrimSpace(artifact.TenantID)
	artifact.ProjectID = strings.TrimSpace(artifact.ProjectID)
	artifact.AppID = strings.TrimSpace(artifact.AppID)
	artifact.Target = NormalizeBackupTarget(artifact.Target)
	artifact.BackendID = strings.TrimSpace(artifact.BackendID)
	artifact.Kind = NormalizeBackupArtifactKind(artifact.Kind)
	artifact.Version = strings.TrimSpace(artifact.Version)
	artifact.ObjectKey = strings.Trim(strings.TrimSpace(artifact.ObjectKey), "/")
	artifact.ManifestObjectKey = strings.Trim(strings.TrimSpace(artifact.ManifestObjectKey), "/")
	artifact.SHA256 = strings.TrimSpace(strings.ToLower(artifact.SHA256))
	artifact.Status = strings.TrimSpace(strings.ToLower(artifact.Status))
	if artifact.Status == "" {
		artifact.Status = BackupArtifactStatusActive
	}
	artifact.BillingClass = strings.TrimSpace(artifact.BillingClass)
	artifact.ManifestDigest = strings.TrimSpace(strings.ToLower(artifact.ManifestDigest))
	artifact.Manifest = NormalizeBackupManifest(artifact.Manifest)
	return artifact
}

func NormalizeBackupRestorePlan(plan BackupRestorePlan) BackupRestorePlan {
	plan.TenantID = strings.TrimSpace(plan.TenantID)
	plan.ProjectID = strings.TrimSpace(plan.ProjectID)
	plan.AppID = strings.TrimSpace(plan.AppID)
	plan.ArtifactID = strings.TrimSpace(plan.ArtifactID)
	plan.Target = NormalizeBackupTarget(plan.Target)
	plan.Mode = NormalizeBackupRestoreMode(plan.Mode)
	plan.Status = strings.TrimSpace(strings.ToLower(plan.Status))
	if plan.Status == "" {
		plan.Status = BackupRestoreStatusPlanned
	}
	plan.Warnings = compactSortedStrings(plan.Warnings)
	for idx := range plan.Phases {
		plan.Phases[idx].Name = strings.TrimSpace(plan.Phases[idx].Name)
		plan.Phases[idx].Status = strings.TrimSpace(strings.ToLower(plan.Phases[idx].Status))
		plan.Phases[idx].Message = strings.TrimSpace(plan.Phases[idx].Message)
	}
	return plan
}

func NormalizeBackupRestoreRun(run BackupRestoreRun) BackupRestoreRun {
	run.PlanID = strings.TrimSpace(run.PlanID)
	run.TenantID = strings.TrimSpace(run.TenantID)
	run.ProjectID = strings.TrimSpace(run.ProjectID)
	run.AppID = strings.TrimSpace(run.AppID)
	run.ArtifactID = strings.TrimSpace(run.ArtifactID)
	run.Mode = NormalizeBackupRestoreMode(run.Mode)
	run.Status = strings.TrimSpace(strings.ToLower(run.Status))
	if run.Status == "" {
		run.Status = BackupRestoreStatusPending()
	}
	for idx := range run.Phases {
		run.Phases[idx].Name = strings.TrimSpace(run.Phases[idx].Name)
		run.Phases[idx].Status = strings.TrimSpace(strings.ToLower(run.Phases[idx].Status))
		run.Phases[idx].Message = strings.TrimSpace(run.Phases[idx].Message)
	}
	return run
}

func BackupRestoreStatusPending() string {
	return BackupRestoreStatusPlanned
}

func NormalizeBackupRestoreMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", BackupRestoreModePlanOnly, "plan":
		return BackupRestoreModePlanOnly
	case BackupRestoreModeClone:
		return BackupRestoreModeClone
	case BackupRestoreModeReplace, "destructive-replace":
		return BackupRestoreModeReplace
	case BackupRestoreModeOfflineControlPlane, "offline":
		return BackupRestoreModeOfflineControlPlane
	default:
		return strings.TrimSpace(strings.ToLower(raw))
	}
}

func NormalizeBackupUsage(usage BackupUsage) BackupUsage {
	usage.TenantID = strings.TrimSpace(usage.TenantID)
	usage.BackendID = strings.TrimSpace(usage.BackendID)
	usage.Provider = NormalizeDataBackendProvider(usage.Provider)
	if usage.MarkupPercent == 0 {
		usage.MarkupPercent = BackupR2MarkupPercent
	}
	if usage.EffectiveMultiplier == 0 {
		usage.EffectiveMultiplier = 1 + float64(usage.MarkupPercent)/100
	}
	if strings.TrimSpace(usage.Currency) == "" {
		usage.Currency = "USD"
	}
	return usage
}

func compactStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := map[string]string{}
	for key, value := range input {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
