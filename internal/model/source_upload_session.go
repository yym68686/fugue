package model

import "time"

// SourceUploadSession is a receipt, not a configuration store. Chunks only
// contain hashes; source bytes and download capabilities never appear here.
type SourceUploadSession struct {
	SchemaVersion  int            `json:"schema_version"`
	ID             string         `json:"id"`
	RequestID      string         `json:"request_id"`
	TenantID       string         `json:"tenant_id"`
	ProjectID      string         `json:"project_id,omitempty"`
	ActorType      string         `json:"actor_type,omitempty"`
	ActorID        string         `json:"actor_id,omitempty"`
	Filename       string         `json:"filename"`
	SizeBytes      int64          `json:"size_bytes"`
	SHA256         string         `json:"sha256"`
	ChunkSize      int            `json:"chunk_size"`
	Chunks         map[int]string `json:"chunks,omitempty"`
	State          string         `json:"state"`
	UploadID       string         `json:"upload_id,omitempty"`
	RequestHash    string         `json:"request_hash,omitempty"`
	OperationIDs   []string       `json:"operation_ids,omitempty"`
	AppIDs         []string       `json:"app_ids,omitempty"`
	ResponseStatus int            `json:"response_status,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	ExpiresAt      time.Time      `json:"expires_at"`
}
