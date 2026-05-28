package model

import (
	"sort"
	"strings"
	"time"
)

const (
	DataBlobUploadModeSingle    = "single"
	DataBlobUploadModeMultipart = "multipart"

	DataBackendProviderFugueManaged = "fugue-managed"
	DataBackendProviderCloudflareR2 = "cloudflare-r2"
	DataBackendProviderBackblazeB2  = "backblaze-b2"
	DataBackendProviderS3           = "s3"
	DataBackendProviderHuggingFace  = "hugging-face"
	DataBackendProviderMinIO        = "minio"

	DataTransferDirectionUpload   = "upload"
	DataTransferDirectionDownload = "download"
	DataTransferDirectionMigrate  = "migrate"
	DataTransferDirectionPrewarm  = "prewarm"

	DataTransferStatusPlanned   = "planned"
	DataTransferStatusRunning   = "running"
	DataTransferStatusCompleted = "completed"
	DataTransferStatusFailed    = "failed"
	DataTransferStatusCanceled  = "canceled"

	DataGrantStatusActive  = "active"
	DataGrantStatusRevoked = "revoked"
	DataGrantStatusExpired = "expired"

	DataAssetModeReadMostly = "read-mostly"
	DataAssetModeAppend     = "append"

	DataManifestEntryKindFile    = "file"
	DataManifestEntryKindDir     = "dir"
	DataManifestEntryKindSymlink = "symlink"
)

type DataBackendCapabilities struct {
	MultipartUpload     bool `json:"multipart_upload"`
	MultipartDownload   bool `json:"multipart_download"`
	RangeDownload       bool `json:"range_download"`
	PresignedUpload     bool `json:"presigned_upload"`
	PresignedDownload   bool `json:"presigned_download"`
	BestEffortResume    bool `json:"best_effort_resume"`
	StrongResume        bool `json:"strong_resume"`
	S3Compatible        bool `json:"s3_compatible"`
	FugueManagedBlobAPI bool `json:"fugue_managed_blob_api"`
}

type DataBackendCredentials struct {
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	Token           string `json:"token,omitempty"`
}

type DataBackend struct {
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
	CreatedAt          time.Time               `json:"created_at"`
	UpdatedAt          time.Time               `json:"updated_at"`
}

type DataBackendSecret struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id,omitempty"`
	BackendID   string    `json:"backend_id"`
	Ciphertext  string    `json:"ciphertext"`
	KeyID       string    `json:"key_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	LastRotated time.Time `json:"last_rotated_at,omitempty"`
}

type DataAsset struct {
	Name            string   `json:"name" yaml:"name"`
	Path            string   `json:"path" yaml:"path"`
	MaterializePath string   `json:"materialize_path,omitempty" yaml:"materialize_path,omitempty"`
	Mode            string   `json:"mode,omitempty" yaml:"mode,omitempty"`
	Required        bool     `json:"required,omitempty" yaml:"required,omitempty"`
	Include         []string `json:"include,omitempty" yaml:"include,omitempty"`
	Ignore          []string `json:"ignore,omitempty" yaml:"ignore,omitempty"`
}

type DataWorkspace struct {
	ID               string      `json:"id"`
	TenantID         string      `json:"tenant_id,omitempty"`
	ProjectID        string      `json:"project_id,omitempty"`
	Name             string      `json:"name"`
	Slug             string      `json:"slug"`
	DefaultRegion    string      `json:"default_region,omitempty"`
	StorageBackendID string      `json:"storage_backend_id,omitempty"`
	QuotaBytes       int64       `json:"quota_bytes,omitempty"`
	UsedBytes        int64       `json:"used_bytes"`
	Assets           []DataAsset `json:"assets,omitempty"`
	CreatedAt        time.Time   `json:"created_at"`
	UpdatedAt        time.Time   `json:"updated_at"`
}

type DataManifestEntry struct {
	AssetName    string    `json:"asset_name"`
	RelativePath string    `json:"relative_path"`
	Kind         string    `json:"kind"`
	Size         int64     `json:"size"`
	Mode         int64     `json:"mode,omitempty"`
	MTime        time.Time `json:"mtime,omitempty"`
	SHA256       string    `json:"sha256,omitempty"`
	ObjectKey    string    `json:"object_key,omitempty"`
	ETag         string    `json:"etag,omitempty"`
	LinkTarget   string    `json:"link_target,omitempty"`
}

type DataManifest struct {
	WorkspaceID string              `json:"workspace_id,omitempty"`
	SnapshotID  string              `json:"snapshot_id,omitempty"`
	Digest      string              `json:"digest,omitempty"`
	FileCount   int                 `json:"file_count"`
	TotalBytes  int64               `json:"total_bytes"`
	Entries     []DataManifestEntry `json:"entries"`
}

type DataTransferPart struct {
	PartNumber  int32     `json:"part_number"`
	Offset      int64     `json:"offset,omitempty"`
	Size        int64     `json:"size,omitempty"`
	UploadURL   string    `json:"upload_url,omitempty"`
	DownloadURL string    `json:"download_url,omitempty"`
	ETag        string    `json:"etag,omitempty"`
	Completed   bool      `json:"completed,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

type DataTransferPlanBlob struct {
	SHA256      string             `json:"sha256"`
	Size        int64              `json:"size"`
	ObjectKey   string             `json:"object_key"`
	UploadURL   string             `json:"upload_url,omitempty"`
	DownloadURL string             `json:"download_url,omitempty"`
	UploadMode  string             `json:"upload_mode,omitempty"`
	UploadID    string             `json:"upload_id,omitempty"`
	PartSize    int64              `json:"part_size,omitempty"`
	Parts       []DataTransferPart `json:"parts,omitempty"`
	Exists      bool               `json:"exists"`
	ExpiresAt   time.Time          `json:"expires_at,omitempty"`
}

type DataSnapshot struct {
	ID             string       `json:"id"`
	TenantID       string       `json:"tenant_id,omitempty"`
	ProjectID      string       `json:"project_id,omitempty"`
	WorkspaceID    string       `json:"workspace_id"`
	Version        string       `json:"version"`
	Message        string       `json:"message,omitempty"`
	ManifestDigest string       `json:"manifest_digest"`
	Manifest       DataManifest `json:"manifest"`
	AssetCount     int          `json:"asset_count"`
	FileCount      int          `json:"file_count"`
	TotalBytes     int64        `json:"total_bytes"`
	CreatedBy      string       `json:"created_by,omitempty"`
	CreatedAt      time.Time    `json:"created_at"`
	DeletedAt      *time.Time   `json:"deleted_at,omitempty"`
}

type DataTransfer struct {
	ID           string                 `json:"id"`
	TenantID     string                 `json:"tenant_id,omitempty"`
	WorkspaceID  string                 `json:"workspace_id"`
	SnapshotID   string                 `json:"snapshot_id,omitempty"`
	Version      string                 `json:"version,omitempty"`
	Message      string                 `json:"message,omitempty"`
	Direction    string                 `json:"direction"`
	Status       string                 `json:"status"`
	Source       string                 `json:"source,omitempty"`
	Target       string                 `json:"target,omitempty"`
	Manifest     DataManifest           `json:"manifest,omitempty"`
	PlanBlobs    []DataTransferPlanBlob `json:"plan_blobs,omitempty"`
	PartSize     int64                  `json:"part_size,omitempty"`
	ExpiresAt    *time.Time             `json:"expires_at,omitempty"`
	BytesTotal   int64                  `json:"bytes_total"`
	BytesDone    int64                  `json:"bytes_done"`
	FilesTotal   int                    `json:"files_total"`
	FilesDone    int                    `json:"files_done"`
	ErrorCode    string                 `json:"error_code,omitempty"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	StartedAt    *time.Time             `json:"started_at,omitempty"`
	FinishedAt   *time.Time             `json:"finished_at,omitempty"`
}

type DataGrant struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"tenant_id,omitempty"`
	WorkspaceID string     `json:"workspace_id"`
	SnapshotID  string     `json:"snapshot_id,omitempty"`
	AssetName   string     `json:"asset_name,omitempty"`
	Mode        string     `json:"mode"`
	Status      string     `json:"status"`
	TokenPrefix string     `json:"token_prefix,omitempty"`
	TokenHash   string     `json:"token_hash,omitempty"`
	CreatedBy   string     `json:"created_by,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
}

type DataGCSweepCandidate struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size,omitempty"`
	References   int       `json:"references"`
	LastModified time.Time `json:"last_modified,omitempty"`
	Reason       string    `json:"reason,omitempty"`
}

type DataGCSweepResult struct {
	WorkspaceID   string                 `json:"workspace_id"`
	BackendID     string                 `json:"backend_id"`
	DryRun        bool                   `json:"dry_run"`
	RetentionDays int                    `json:"retention_days"`
	Cutoff        time.Time              `json:"cutoff"`
	Scanned       int                    `json:"scanned"`
	Deleted       int                    `json:"deleted"`
	DeletedBytes  int64                  `json:"deleted_bytes"`
	Candidates    []DataGCSweepCandidate `json:"candidates,omitempty"`
}

func NormalizeDataBackendProvider(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", DataBackendProviderFugueManaged:
		return DataBackendProviderFugueManaged
	case DataBackendProviderCloudflareR2, "r2", "cloudflare":
		return DataBackendProviderCloudflareR2
	case DataBackendProviderBackblazeB2, "b2", "backblaze":
		return DataBackendProviderBackblazeB2
	case DataBackendProviderS3, "aws-s3", "aws":
		return DataBackendProviderS3
	case DataBackendProviderHuggingFace, "hf", "huggingface":
		return DataBackendProviderHuggingFace
	case DataBackendProviderMinIO, "minio-self-host", "self-host":
		return DataBackendProviderMinIO
	default:
		return strings.TrimSpace(strings.ToLower(raw))
	}
}

func DataBackendCapabilitiesForProvider(provider string) DataBackendCapabilities {
	switch NormalizeDataBackendProvider(provider) {
	case DataBackendProviderCloudflareR2, DataBackendProviderBackblazeB2, DataBackendProviderS3, DataBackendProviderMinIO:
		return DataBackendCapabilities{
			MultipartUpload:   true,
			MultipartDownload: true,
			RangeDownload:     true,
			PresignedUpload:   true,
			PresignedDownload: true,
			StrongResume:      true,
			S3Compatible:      true,
		}
	case DataBackendProviderHuggingFace:
		return DataBackendCapabilities{
			RangeDownload:     true,
			PresignedDownload: true,
			BestEffortResume:  true,
		}
	default:
		return DataBackendCapabilities{
			MultipartUpload:     true,
			MultipartDownload:   true,
			RangeDownload:       true,
			PresignedUpload:     true,
			PresignedDownload:   true,
			StrongResume:        true,
			FugueManagedBlobAPI: true,
		}
	}
}

func NormalizeDataAsset(asset DataAsset) DataAsset {
	asset.Name = strings.TrimSpace(asset.Name)
	asset.Path = strings.TrimSpace(asset.Path)
	asset.MaterializePath = strings.TrimSpace(asset.MaterializePath)
	asset.Mode = strings.TrimSpace(asset.Mode)
	if asset.Mode == "" {
		asset.Mode = DataAssetModeReadMostly
	}
	if asset.MaterializePath == "" {
		asset.MaterializePath = asset.Path
	}
	asset.Include = compactSortedStrings(asset.Include)
	asset.Ignore = compactSortedStrings(asset.Ignore)
	return asset
}

func NormalizeDataManifest(manifest DataManifest) DataManifest {
	entries := make([]DataManifestEntry, 0, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		entry.AssetName = strings.TrimSpace(entry.AssetName)
		entry.RelativePath = strings.Trim(strings.TrimSpace(entry.RelativePath), "/")
		entry.Kind = strings.TrimSpace(entry.Kind)
		entry.SHA256 = strings.TrimSpace(strings.ToLower(entry.SHA256))
		entry.ObjectKey = strings.TrimSpace(entry.ObjectKey)
		if entry.ObjectKey == "" && entry.SHA256 != "" {
			entry.ObjectKey = DataObjectKey(entry.SHA256)
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].AssetName == entries[j].AssetName {
			return entries[i].RelativePath < entries[j].RelativePath
		}
		return entries[i].AssetName < entries[j].AssetName
	})
	manifest.Entries = entries
	manifest.FileCount = 0
	manifest.TotalBytes = 0
	for _, entry := range entries {
		if entry.Kind == DataManifestEntryKindFile {
			manifest.FileCount++
			manifest.TotalBytes += entry.Size
		}
	}
	return manifest
}

func DataObjectKey(sha256Digest string) string {
	digest := strings.TrimSpace(strings.ToLower(sha256Digest))
	if len(digest) >= 4 {
		return "blobs/sha256/" + digest[:2] + "/" + digest[2:4] + "/" + digest
	}
	return "blobs/sha256/" + digest
}

func RedactDataBackendCredentials(backend DataBackend) DataBackend {
	if backend.Credentials.SecretAccessKey != "" {
		backend.Credentials.SecretAccessKey = "redacted"
	}
	if backend.Credentials.Token != "" {
		backend.Credentials.Token = "redacted"
	}
	return backend
}

func compactSortedStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
