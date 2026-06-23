package model

import "time"

type AppMoveImpact struct {
	AppID           string                  `json:"app_id"`
	TargetRuntimeID string                  `json:"target_runtime_id"`
	DryRun          bool                    `json:"dry_run"`
	Pass            bool                    `json:"pass"`
	Blockers        []string                `json:"blockers,omitempty"`
	RollbackRef     string                  `json:"rollback_ref,omitempty"`
	OperationChain  []string                `json:"operation_chain,omitempty"`
	Volumes         []AppMoveVolumeImpact   `json:"volumes,omitempty"`
	Databases       []AppMoveDatabaseImpact `json:"databases,omitempty"`
	Services        []string                `json:"services,omitempty"`
	Routes          []string                `json:"routes,omitempty"`
	DNS             []string                `json:"dns,omitempty"`
	Checks          []StoreInvariantCheck   `json:"checks,omitempty"`
	GeneratedAt     time.Time               `json:"generated_at"`
}

type AppMoveVolumeImpact struct {
	Mode             string `json:"mode,omitempty"`
	StorageClassName string `json:"storage_class_name,omitempty"`
	ClaimName        string `json:"claim_name,omitempty"`
	MountCount       int    `json:"mount_count"`
	Strategy         string `json:"strategy"`
}

type AppMoveDatabaseImpact struct {
	ServiceName          string `json:"service_name,omitempty"`
	CurrentRuntimeID     string `json:"current_runtime_id,omitempty"`
	TargetRuntimeID      string `json:"target_runtime_id,omitempty"`
	BackupStatus         string `json:"backup_status"`
	RestoreStatus        string `json:"restore_status"`
	GrantVerification    string `json:"grant_verification"`
	RequiresLocalization bool   `json:"requires_localization"`
}

type AppMoveDryRunResponse struct {
	Impact AppMoveImpact `json:"impact"`
	App    App           `json:"app"`
}

type ManagedPostgresStatus struct {
	AppID               string    `json:"app_id"`
	Enabled             bool      `json:"enabled"`
	ServiceName         string    `json:"service_name,omitempty"`
	Owner               string    `json:"owner,omitempty"`
	RuntimeID           string    `json:"runtime_id,omitempty"`
	FailoverRuntimeID   string    `json:"failover_runtime_id,omitempty"`
	StorageSize         string    `json:"storage_size,omitempty"`
	StorageClassName    string    `json:"storage_class_name,omitempty"`
	Instances           int       `json:"instances,omitempty"`
	SynchronousReplicas int       `json:"synchronous_replicas,omitempty"`
	PrimaryNodeName     string    `json:"primary_node_name,omitempty"`
	LastBackup          string    `json:"last_backup,omitempty"`
	LastRestore         string    `json:"last_restore,omitempty"`
	BackupStatus        string    `json:"backup_status"`
	RestoreStatus       string    `json:"restore_status"`
	GrantVerification   string    `json:"grant_verification"`
	GeneratedAt         time.Time `json:"generated_at"`
}

type ManagedPostgresStatusResponse struct {
	Status ManagedPostgresStatus `json:"status"`
	App    App                   `json:"app"`
}

type AppDatabaseImportRequest struct {
	Label  string `json:"label,omitempty"`
	Clean  bool   `json:"clean,omitempty"`
	Format string `json:"format,omitempty"`
}

type AppDatabaseImportMultipartRequest struct {
	Request AppDatabaseImportRequest `json:"request"`
	Dump    string                   `json:"dump"`
}

type AppDatabaseImportJobLog struct {
	At      time.Time `json:"at"`
	Message string    `json:"message"`
}

type AppDatabaseImportJob struct {
	ID                   string                    `json:"id"`
	AppID                string                    `json:"app_id"`
	TenantID             string                    `json:"tenant_id"`
	SourceUploadID       string                    `json:"source_upload_id"`
	SourceUploadFilename string                    `json:"source_upload_filename,omitempty"`
	SourceUploadSHA256   string                    `json:"source_upload_sha256,omitempty"`
	Label                string                    `json:"label,omitempty"`
	Format               string                    `json:"format,omitempty"`
	Clean                bool                      `json:"clean,omitempty"`
	Status               string                    `json:"status"`
	ResultMessage        string                    `json:"result_message,omitempty"`
	ErrorMessage         string                    `json:"error_message,omitempty"`
	RetryCount           int                       `json:"retry_count"`
	RetryOfJobID         string                    `json:"retry_of_job_id,omitempty"`
	RequestedByType      string                    `json:"requested_by_type,omitempty"`
	RequestedByID        string                    `json:"requested_by_id,omitempty"`
	Logs                 []AppDatabaseImportJobLog `json:"logs,omitempty"`
	CreatedAt            time.Time                 `json:"created_at"`
	UpdatedAt            time.Time                 `json:"updated_at"`
	StartedAt            *time.Time                `json:"started_at,omitempty"`
	CompletedAt          *time.Time                `json:"completed_at,omitempty"`
}

type AppDatabaseImportResponse struct {
	App App                   `json:"app"`
	Job *AppDatabaseImportJob `json:"job,omitempty"`
}

type AppDatabaseImportRetryRequest struct {
	JobID string `json:"job_id,omitempty"`
}

type AppDatabaseAccessGrant struct {
	ID              string     `json:"id"`
	AppID           string     `json:"app_id"`
	TenantID        string     `json:"tenant_id"`
	Label           string     `json:"label,omitempty"`
	Mode            string     `json:"mode"`
	Status          string     `json:"status"`
	TokenPrefix     string     `json:"token_prefix,omitempty"`
	TokenHash       string     `json:"token_hash,omitempty"`
	RequestedByType string     `json:"requested_by_type,omitempty"`
	RequestedByID   string     `json:"requested_by_id,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	RevokedAt       *time.Time `json:"revoked_at,omitempty"`
	LastUsedAt      *time.Time `json:"last_used_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type AppDatabaseAccessGrantCreateRequest struct {
	Label            string `json:"label,omitempty"`
	Mode             string `json:"mode,omitempty"`
	ExpiresInMinutes int    `json:"expires_in_minutes,omitempty"`
}

type AppDatabaseAccessGrantCreateResponse struct {
	Grant  AppDatabaseAccessGrant `json:"grant"`
	Secret string                 `json:"secret"`
}

type AppDatabaseAccessResponse struct {
	App    App                      `json:"app"`
	Grants []AppDatabaseAccessGrant `json:"grants"`
}

type AppDatabaseAccessRevokeResponse struct {
	Removed bool `json:"removed"`
}

type PlatformFailureDrillRequest struct {
	DryRun bool   `json:"dry_run,omitempty"`
	Target string `json:"target,omitempty"`
}

type PlatformFailureDrillReport struct {
	ID            string                 `json:"id"`
	DryRun        bool                   `json:"dry_run"`
	Target        string                 `json:"target"`
	GeneratedAt   time.Time              `json:"generated_at"`
	Status        string                 `json:"status"`
	BlockRollout  bool                   `json:"block_rollout"`
	Checks        []StoreInvariantCheck  `json:"checks"`
	Backlog       []string               `json:"backlog,omitempty"`
	ReportRef     string                 `json:"report_ref,omitempty"`
	AutonomyState PlatformAutonomyStatus `json:"autonomy_state"`
}

type PlatformFailureDrillResponse struct {
	Report PlatformFailureDrillReport `json:"report"`
}

type KeyRotationPreflightRequest struct {
	DryRun        bool   `json:"dry_run,omitempty"`
	Stage         bool   `json:"stage,omitempty"`
	ConfirmRevoke bool   `json:"confirm_revoke,omitempty"`
	NewKeyID      string `json:"new_key_id,omitempty"`
	PreviousKeyID string `json:"previous_key_id,omitempty"`
}

type KeyRotationNodeAcceptance struct {
	NodeKind          string `json:"node_kind"`
	NodeID            string `json:"node_id"`
	Healthy           bool   `json:"healthy"`
	ServingGeneration string `json:"serving_generation,omitempty"`
	LKGGeneration     string `json:"lkg_generation,omitempty"`
	Accepted          bool   `json:"accepted"`
	Reason            string `json:"reason,omitempty"`
}

type KeyRotationPreflight struct {
	GeneratedAt       time.Time                   `json:"generated_at"`
	DryRun            bool                        `json:"dry_run"`
	Stage             bool                        `json:"stage"`
	ConfirmRevoke     bool                        `json:"confirm_revoke"`
	CurrentKeyID      string                      `json:"current_key_id,omitempty"`
	NewKeyID          string                      `json:"new_key_id,omitempty"`
	PreviousKeyID     string                      `json:"previous_key_id,omitempty"`
	RevokedKeyIDs     []string                    `json:"revoked_key_ids,omitempty"`
	CanStage          bool                        `json:"can_stage"`
	CanRevokePrevious bool                        `json:"can_revoke_previous"`
	BlockRollout      bool                        `json:"block_rollout"`
	Checks            []StoreInvariantCheck       `json:"checks"`
	Nodes             []KeyRotationNodeAcceptance `json:"nodes"`
}

type KeyRotationPreflightResponse struct {
	Preflight KeyRotationPreflight `json:"preflight"`
}
