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
	AppID             string    `json:"app_id"`
	Enabled           bool      `json:"enabled"`
	ServiceName       string    `json:"service_name,omitempty"`
	Owner             string    `json:"owner,omitempty"`
	RuntimeID         string    `json:"runtime_id,omitempty"`
	FailoverRuntimeID string    `json:"failover_runtime_id,omitempty"`
	LastBackup        string    `json:"last_backup,omitempty"`
	LastRestore       string    `json:"last_restore,omitempty"`
	BackupStatus      string    `json:"backup_status"`
	RestoreStatus     string    `json:"restore_status"`
	GrantVerification string    `json:"grant_verification"`
	GeneratedAt       time.Time `json:"generated_at"`
}

type ManagedPostgresStatusResponse struct {
	Status ManagedPostgresStatus `json:"status"`
	App    App                   `json:"app"`
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
