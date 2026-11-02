package model

import "time"

// ObjectStore is an explicitly provisioned resource, independent of app releases
// and of backup/DataWorkspace blob garbage collection.
type ObjectStore struct {
	ID              string                       `json:"id"`
	TenantID        string                       `json:"tenant_id"`
	ProjectID       string                       `json:"project_id"`
	Name            string                       `json:"name"`
	Provider        string                       `json:"provider"`
	Bucket          string                       `json:"bucket"`
	Endpoint        string                       `json:"endpoint"`
	Region          string                       `json:"region"`
	Status          string                       `json:"status"`
	QuotaBytes      int64                        `json:"quota_bytes"`
	QuotaMode       string                       `json:"quota_mode"`
	UsedBytes       int64                        `json:"used_bytes"`
	ObjectCount     int64                        `json:"object_count"`
	UsageMeasuredAt *time.Time                   `json:"usage_measured_at,omitempty"`
	Deletion        *ObjectStoreDeletionProgress `json:"deletion,omitempty"`
	CreatedAt       time.Time                    `json:"created_at"`
	UpdatedAt       time.Time                    `json:"updated_at"`
}

type ObjectStorageCredential struct {
	ID          string    `json:"id"`
	StoreID     string    `json:"store_id"`
	AppID       string    `json:"app_id"`
	Name        string    `json:"name"`
	Permission  string    `json:"permission"`
	Status      string    `json:"status"`
	AccessKeyID string    `json:"access_key_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// These records are persistence-only. HTTP handlers return the public nested
// credential/configuration status, never a record containing encrypted secrets.
type ObjectStorageCredentialRecord struct {
	Credential ObjectStorageCredential `json:"credential"`
	Secret     DataBackendSecret       `json:"secret"`
}
type ObjectStorageConfig struct {
	AccountID string            `json:"account_id"`
	Secret    DataBackendSecret `json:"secret"`
	UpdatedAt time.Time         `json:"updated_at"`
}
type ObjectStorageState struct {
	Config      *ObjectStorageConfig            `json:"config,omitempty"`
	Stores      []ObjectStore                   `json:"stores,omitempty"`
	Credentials []ObjectStorageCredentialRecord `json:"credentials,omitempty"`
}

type ObjectStoreDeleteInventory struct {
	ObjectCount int64      `json:"object_count"`
	Bytes       int64      `json:"bytes"`
	Source      string     `json:"source"`
	Complete    bool       `json:"complete"`
	MeasuredAt  *time.Time `json:"measured_at,omitempty"`
	PlanSHA256  string     `json:"plan_sha256"`
}

// Counters are lower bounds: a process can stop after a provider mutation but
// before persisting its receipt. Completion always uses provider state.
type ObjectStoreDeletionProgress struct {
	StartedAt               time.Time `json:"started_at"`
	DeletedObjects          int64     `json:"deleted_objects"`
	AbortedMultipartUploads int64     `json:"aborted_multipart_uploads"`
}

type ObjectStoreDeleteResponse struct {
	Store     ObjectStore                `json:"store"`
	Inventory ObjectStoreDeleteInventory `json:"inventory"`
	Deleted   bool                       `json:"deleted"`
}
