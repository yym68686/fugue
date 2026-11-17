package model

import "time"

// A job's result is observation evidence, never authority to delete objects.
// Execution always re-reads references and applies the engine's safety checks.
type StorageMaintenanceJob struct {
	ID                  string                   `json:"id"`
	Kind                string                   `json:"kind"`
	ScopeID             string                   `json:"scope_id,omitempty"`
	Execute             bool                     `json:"execute"`
	Status              string                   `json:"status"`
	Attempts            int                      `json:"attempts"`
	CreatedAt           time.Time                `json:"created_at"`
	UpdatedAt           time.Time                `json:"updated_at"`
	RetryAt             *time.Time               `json:"retry_at,omitempty"`
	Error               string                   `json:"error,omitempty"`
	Result              map[string]any           `json:"result,omitempty"`
	WorkspaceRetirement *DataWorkspaceRetirement `json:"workspace_retirement,omitempty"`
}

type DataWorkspaceRetirementOptions struct {
	Execute                bool `json:"execute"`
	CancelPlannedTransfers bool `json:"cancel_planned_transfers"`
	RevokeGrants           bool `json:"revoke_grants"`
	EvictCaches            bool `json:"evict_caches"`
}

// The workspace and its manifests remain as a tombstone until reclamation is
// complete. Task identity and authorization survive final metadata deletion.
type DataWorkspaceRetirement struct {
	TenantID        string                         `json:"tenant_id"`
	ProjectID       string                         `json:"project_id,omitempty"`
	Name            string                         `json:"name"`
	ActorID         string                         `json:"actor_id,omitempty"`
	ActorType       string                         `json:"actor_type,omitempty"`
	Options         DataWorkspaceRetirementOptions `json:"options"`
	Phase           string                         `json:"phase"`
	ReleasedAt      *time.Time                     `json:"released_at,omitempty"`
	ReclaimAfter    *time.Time                     `json:"reclaim_after,omitempty"`
	DeletedObjects  int64                          `json:"deleted_objects"`
	DeletedBytes    int64                          `json:"deleted_bytes"`
	MetadataDeleted bool                           `json:"metadata_deleted"`
	Cursor          int                            `json:"cursor"`
	InventoryDigest string                         `json:"inventory_digest,omitempty"`
	SharedIndices   []int                          `json:"shared_indices,omitempty"`
	BackendIDs      []string                       `json:"backend_ids,omitempty"`
}
