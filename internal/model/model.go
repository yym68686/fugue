package model

import (
	"strings"
	"time"
)

const (
	RuntimeTypeManagedShared = "managed-shared"
	RuntimeTypeManagedOwned  = "managed-owned"
	RuntimeTypeExternalOwned = "external-owned"

	RuntimeAccessModePrivate        = "private"
	RuntimeAccessModePlatformShared = "platform-shared"

	RuntimePoolModeDedicated      = "dedicated"
	RuntimePoolModeInternalShared = "internal-shared"

	MachineConnectionModeAgent   = "agent"
	MachineConnectionModeCluster = "cluster"

	AppSourceTypeGitHubPublic = "github-public"
	AppSourceTypeUpload       = "upload"

	AppBuildStrategyAuto       = "auto"
	AppBuildStrategyStaticSite = "static-site"
	AppBuildStrategyDockerfile = "dockerfile"
	AppBuildStrategyBuildpacks = "buildpacks"
	AppBuildStrategyNixpacks   = "nixpacks"

	BackingServiceTypePostgres = "postgres"

	BackingServiceProvisionerManaged  = "managed"
	BackingServiceProvisionerExternal = "external"

	BackingServiceStatusActive  = "active"
	BackingServiceStatusDeleted = "deleted"

	RuntimeStatusPending = "pending"
	RuntimeStatusActive  = "active"
	RuntimeStatusOffline = "offline"

	APIKeyStatusActive   = "active"
	APIKeyStatusDisabled = "disabled"

	NodeKeyStatusActive  = "active"
	NodeKeyStatusRevoked = "revoked"

	OperationTypeImport  = "import"
	OperationTypeDeploy  = "deploy"
	OperationTypeScale   = "scale"
	OperationTypeMigrate = "migrate"
	OperationTypeDelete  = "delete"

	OperationStatusPending      = "pending"
	OperationStatusRunning      = "running"
	OperationStatusWaitingAgent = "waiting-agent"
	OperationStatusCompleted    = "completed"
	OperationStatusFailed       = "failed"

	IdempotencyScopeAppImportGitHub = "app.import_github"

	IdempotencyStatusPending   = "pending"
	IdempotencyStatusCompleted = "completed"

	ExecutionModeManaged = "managed"
	ExecutionModeAgent   = "agent"

	ActorTypeBootstrap = "bootstrap"
	ActorTypeAPIKey    = "api-key"
	ActorTypeNodeKey   = "node-key"
	ActorTypeRuntime   = "runtime"

	OperationRequestedByGitHubSyncController = "fugue-controller/github-sync"

	ClusterNodeWorkloadKindApp            = "app"
	ClusterNodeWorkloadKindBackingService = "backing_service"

	DefaultAppWorkspaceMountPath = "/workspace"
	AppWorkspaceInternalDirName  = ".fugue-workspace-state"
)

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Project struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	Name        string    `json:"name"`
	Slug        string    `json:"slug"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type APIKey struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Label      string     `json:"label"`
	Prefix     string     `json:"prefix"`
	Hash       string     `json:"hash"`
	Status     string     `json:"status"`
	Scopes     []string   `json:"scopes"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	DisabledAt *time.Time `json:"disabled_at,omitempty"`
}

type EnrollmentToken struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Label      string     `json:"label"`
	Prefix     string     `json:"prefix"`
	Hash       string     `json:"hash"`
	ExpiresAt  time.Time  `json:"expires_at"`
	UsedAt     *time.Time `json:"used_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

type NodeKey struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Label      string     `json:"label"`
	Prefix     string     `json:"prefix"`
	Hash       string     `json:"hash"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
}

type Runtime struct {
	ID                string            `json:"id"`
	TenantID          string            `json:"tenant_id,omitempty"`
	Name              string            `json:"name"`
	MachineName       string            `json:"machine_name,omitempty"`
	Type              string            `json:"type"`
	AccessMode        string            `json:"access_mode,omitempty"`
	PoolMode          string            `json:"pool_mode,omitempty"`
	ConnectionMode    string            `json:"connection_mode,omitempty"`
	Status            string            `json:"status"`
	Endpoint          string            `json:"endpoint,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	NodeKeyID         string            `json:"node_key_id,omitempty"`
	ClusterNodeName   string            `json:"cluster_node_name,omitempty"`
	FingerprintPrefix string            `json:"fingerprint_prefix,omitempty"`
	FingerprintHash   string            `json:"fingerprint_hash,omitempty"`
	AgentKeyPrefix    string            `json:"agent_key_prefix,omitempty"`
	AgentKeyHash      string            `json:"agent_key_hash,omitempty"`
	LastSeenAt        *time.Time        `json:"last_seen_at,omitempty"`
	LastHeartbeatAt   *time.Time        `json:"last_heartbeat_at,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type Machine struct {
	ID                string            `json:"id"`
	TenantID          string            `json:"tenant_id,omitempty"`
	Name              string            `json:"name"`
	ConnectionMode    string            `json:"connection_mode"`
	Status            string            `json:"status"`
	Endpoint          string            `json:"endpoint,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	NodeKeyID         string            `json:"node_key_id,omitempty"`
	RuntimeID         string            `json:"runtime_id,omitempty"`
	RuntimeName       string            `json:"runtime_name,omitempty"`
	ClusterNodeName   string            `json:"cluster_node_name,omitempty"`
	FingerprintPrefix string            `json:"fingerprint_prefix,omitempty"`
	FingerprintHash   string            `json:"fingerprint_hash,omitempty"`
	LastSeenAt        *time.Time        `json:"last_seen_at,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type RuntimeAccessGrant struct {
	RuntimeID string    `json:"runtime_id"`
	TenantID  string    `json:"tenant_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ClusterNodeCondition struct {
	Status           string     `json:"status"`
	Reason           string     `json:"reason,omitempty"`
	Message          string     `json:"message,omitempty"`
	LastTransitionAt *time.Time `json:"last_transition_at,omitempty"`
}

type ClusterNodeCPUStats struct {
	CapacityMilliCores    *int64   `json:"capacity_millicores,omitempty"`
	AllocatableMilliCores *int64   `json:"allocatable_millicores,omitempty"`
	UsedMilliCores        *int64   `json:"used_millicores,omitempty"`
	UsagePercent          *float64 `json:"usage_percent,omitempty"`
}

type ClusterNodeMemoryStats struct {
	CapacityBytes    *int64   `json:"capacity_bytes,omitempty"`
	AllocatableBytes *int64   `json:"allocatable_bytes,omitempty"`
	UsedBytes        *int64   `json:"used_bytes,omitempty"`
	UsagePercent     *float64 `json:"usage_percent,omitempty"`
}

type ClusterNodeStorageStats struct {
	CapacityBytes    *int64   `json:"capacity_bytes,omitempty"`
	AllocatableBytes *int64   `json:"allocatable_bytes,omitempty"`
	UsedBytes        *int64   `json:"used_bytes,omitempty"`
	UsagePercent     *float64 `json:"usage_percent,omitempty"`
}

type ResourceUsage struct {
	CPUMilliCores         *int64 `json:"cpu_millicores,omitempty"`
	MemoryBytes           *int64 `json:"memory_bytes,omitempty"`
	EphemeralStorageBytes *int64 `json:"ephemeral_storage_bytes,omitempty"`
}

type ClusterNodeWorkloadPod struct {
	Name  string `json:"name"`
	Phase string `json:"phase,omitempty"`
}

type ClusterNodeWorkload struct {
	Kind        string                   `json:"kind"`
	ID          string                   `json:"id"`
	Name        string                   `json:"name"`
	TenantID    string                   `json:"tenant_id,omitempty"`
	ProjectID   string                   `json:"project_id,omitempty"`
	RuntimeID   string                   `json:"runtime_id,omitempty"`
	ServiceType string                   `json:"service_type,omitempty"`
	OwnerAppID  string                   `json:"owner_app_id,omitempty"`
	Namespace   string                   `json:"namespace,omitempty"`
	Pods        []ClusterNodeWorkloadPod `json:"pods,omitempty"`
	PodCount    int                      `json:"pod_count"`
}

type ClusterNode struct {
	Name             string                          `json:"name"`
	Status           string                          `json:"status"`
	Roles            []string                        `json:"roles,omitempty"`
	InternalIP       string                          `json:"internal_ip,omitempty"`
	ExternalIP       string                          `json:"external_ip,omitempty"`
	PublicIP         string                          `json:"public_ip,omitempty"`
	Region           string                          `json:"region,omitempty"`
	Zone             string                          `json:"zone,omitempty"`
	KubeletVersion   string                          `json:"kubelet_version,omitempty"`
	OSImage          string                          `json:"os_image,omitempty"`
	KernelVersion    string                          `json:"kernel_version,omitempty"`
	ContainerRuntime string                          `json:"container_runtime,omitempty"`
	Conditions       map[string]ClusterNodeCondition `json:"conditions,omitempty"`
	CPU              *ClusterNodeCPUStats            `json:"cpu,omitempty"`
	Memory           *ClusterNodeMemoryStats         `json:"memory,omitempty"`
	EphemeralStorage *ClusterNodeStorageStats        `json:"ephemeral_storage,omitempty"`
	RuntimeID        string                          `json:"runtime_id,omitempty"`
	TenantID         string                          `json:"tenant_id,omitempty"`
	Workloads        []ClusterNodeWorkload           `json:"workloads,omitempty"`
	CreatedAt        *time.Time                      `json:"created_at,omitempty"`
}

type AppSource struct {
	Type              string `json:"type"`
	RepoURL           string `json:"repo_url,omitempty"`
	RepoBranch        string `json:"repo_branch,omitempty"`
	UploadID          string `json:"upload_id,omitempty"`
	UploadFilename    string `json:"upload_filename,omitempty"`
	ArchiveSHA256     string `json:"archive_sha256,omitempty"`
	ArchiveSizeBytes  int64  `json:"archive_size_bytes,omitempty"`
	SourceDir         string `json:"source_dir,omitempty"`
	BuildStrategy     string `json:"build_strategy,omitempty"`
	CommitSHA         string `json:"commit_sha,omitempty"`
	CommitCommittedAt string `json:"commit_committed_at,omitempty"`
	DockerfilePath    string `json:"dockerfile_path,omitempty"`
	BuildContextDir   string `json:"build_context_dir,omitempty"`
	ImageNameSuffix   string `json:"image_name_suffix,omitempty"`
	ComposeService    string `json:"compose_service,omitempty"`
	DetectedProvider  string `json:"detected_provider,omitempty"`
	DetectedStack     string `json:"detected_stack,omitempty"`
}

type AppTechnology struct {
	Kind   string `json:"kind"`
	Slug   string `json:"slug"`
	Name   string `json:"name"`
	Source string `json:"source,omitempty"`
}

type AppRoute struct {
	Hostname    string `json:"hostname,omitempty"`
	BaseDomain  string `json:"base_domain,omitempty"`
	PublicURL   string `json:"public_url,omitempty"`
	ServicePort int    `json:"service_port,omitempty"`
}

const (
	AppDomainStatusPending  = "pending"
	AppDomainStatusVerified = "verified"
)

const (
	AppDomainTLSStatusPending = "pending"
	AppDomainTLSStatusReady   = "ready"
	AppDomainTLSStatusError   = "error"
)

func NormalizeAppDomainTLSStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case AppDomainTLSStatusPending:
		return AppDomainTLSStatusPending
	case AppDomainTLSStatusReady:
		return AppDomainTLSStatusReady
	case AppDomainTLSStatusError:
		return AppDomainTLSStatusError
	default:
		return ""
	}
}

type AppDomain struct {
	Hostname             string     `json:"hostname"`
	AppID                string     `json:"app_id,omitempty"`
	TenantID             string     `json:"tenant_id,omitempty"`
	Status               string     `json:"status"`
	TLSStatus            string     `json:"tls_status,omitempty"`
	VerificationTXTName  string     `json:"verification_txt_name,omitempty"`
	VerificationTXTValue string     `json:"verification_txt_value,omitempty"`
	RouteTarget          string     `json:"route_target,omitempty"`
	LastMessage          string     `json:"last_message,omitempty"`
	TLSLastMessage       string     `json:"tls_last_message,omitempty"`
	LastCheckedAt        *time.Time `json:"last_checked_at,omitempty"`
	VerifiedAt           *time.Time `json:"verified_at,omitempty"`
	TLSLastCheckedAt     *time.Time `json:"tls_last_checked_at,omitempty"`
	TLSReadyAt           *time.Time `json:"tls_ready_at,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

type AppSpec struct {
	Image        string            `json:"image"`
	Command      []string          `json:"command,omitempty"`
	Args         []string          `json:"args,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	Ports        []int             `json:"ports,omitempty"`
	Replicas     int               `json:"replicas"`
	RuntimeID    string            `json:"runtime_id"`
	Files        []AppFile         `json:"files,omitempty"`
	Workspace    *AppWorkspaceSpec `json:"workspace,omitempty"`
	Postgres     *AppPostgresSpec  `json:"postgres,omitempty"`
	RestartToken string            `json:"restart_token,omitempty"`
}

type AppFile struct {
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
	Secret  bool   `json:"secret,omitempty"`
	Mode    int32  `json:"mode,omitempty"`
}

type AppWorkspaceSpec struct {
	MountPath   string `json:"mount_path,omitempty"`
	StoragePath string `json:"storage_path,omitempty"`
	ResetToken  string `json:"reset_token,omitempty"`
}

type AppPostgresSpec struct {
	Image       string `json:"image,omitempty"`
	Database    string `json:"database,omitempty"`
	User        string `json:"user,omitempty"`
	Password    string `json:"password,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
	StoragePath string `json:"storage_path,omitempty"`
}

type BackingServiceSpec struct {
	Postgres *AppPostgresSpec `json:"postgres,omitempty"`
}

type BackingService struct {
	ID                      string             `json:"id"`
	TenantID                string             `json:"tenant_id"`
	ProjectID               string             `json:"project_id"`
	OwnerAppID              string             `json:"owner_app_id,omitempty"`
	Name                    string             `json:"name"`
	Description             string             `json:"description,omitempty"`
	Type                    string             `json:"type"`
	Provisioner             string             `json:"provisioner"`
	Status                  string             `json:"status"`
	Spec                    BackingServiceSpec `json:"spec"`
	CurrentResourceUsage    *ResourceUsage     `json:"current_resource_usage,omitempty"`
	CurrentRuntimeStartedAt *time.Time         `json:"current_runtime_started_at,omitempty"`
	CurrentRuntimeReadyAt   *time.Time         `json:"current_runtime_ready_at,omitempty"`
	CreatedAt               time.Time          `json:"created_at"`
	UpdatedAt               time.Time          `json:"updated_at"`
}

type ServiceBinding struct {
	ID        string            `json:"id"`
	TenantID  string            `json:"tenant_id"`
	AppID     string            `json:"app_id"`
	ServiceID string            `json:"service_id"`
	Alias     string            `json:"alias,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type AppStatus struct {
	Phase                   string     `json:"phase"`
	CurrentRuntimeID        string     `json:"current_runtime_id,omitempty"`
	CurrentReplicas         int        `json:"current_replicas"`
	CurrentReleaseStartedAt *time.Time `json:"current_release_started_at,omitempty"`
	CurrentReleaseReadyAt   *time.Time `json:"current_release_ready_at,omitempty"`
	LastOperationID         string     `json:"last_operation_id,omitempty"`
	LastMessage             string     `json:"last_message,omitempty"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

type App struct {
	ID                   string           `json:"id"`
	TenantID             string           `json:"tenant_id"`
	ProjectID            string           `json:"project_id"`
	Name                 string           `json:"name"`
	Description          string           `json:"description"`
	Source               *AppSource       `json:"source,omitempty"`
	Route                *AppRoute        `json:"route,omitempty"`
	Spec                 AppSpec          `json:"spec"`
	Status               AppStatus        `json:"status"`
	CurrentResourceUsage *ResourceUsage   `json:"current_resource_usage,omitempty"`
	Bindings             []ServiceBinding `json:"bindings,omitempty"`
	BackingServices      []BackingService `json:"backing_services,omitempty"`
	TechStack            []AppTechnology  `json:"tech_stack,omitempty"`
	CreatedAt            time.Time        `json:"created_at"`
	UpdatedAt            time.Time        `json:"updated_at"`
}

type IdempotencyRecord struct {
	Scope       string    `json:"scope"`
	TenantID    string    `json:"tenant_id"`
	Key         string    `json:"key"`
	RequestHash string    `json:"request_hash"`
	Status      string    `json:"status"`
	AppID       string    `json:"app_id,omitempty"`
	OperationID string    `json:"operation_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type SourceUpload struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	Filename      string    `json:"filename,omitempty"`
	ContentType   string    `json:"content_type,omitempty"`
	SHA256        string    `json:"sha256"`
	SizeBytes     int64     `json:"size_bytes"`
	DownloadToken string    `json:"-"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Operation struct {
	ID                string     `json:"id"`
	TenantID          string     `json:"tenant_id"`
	Type              string     `json:"type"`
	Status            string     `json:"status"`
	ExecutionMode     string     `json:"execution_mode"`
	RequestedByType   string     `json:"requested_by_type"`
	RequestedByID     string     `json:"requested_by_id"`
	AppID             string     `json:"app_id"`
	SourceRuntimeID   string     `json:"source_runtime_id,omitempty"`
	TargetRuntimeID   string     `json:"target_runtime_id,omitempty"`
	DesiredReplicas   *int       `json:"desired_replicas,omitempty"`
	DesiredSpec       *AppSpec   `json:"desired_spec,omitempty"`
	DesiredSource     *AppSource `json:"desired_source,omitempty"`
	ResultMessage     string     `json:"result_message,omitempty"`
	ManifestPath      string     `json:"manifest_path,omitempty"`
	AssignedRuntimeID string     `json:"assigned_runtime_id,omitempty"`
	ErrorMessage      string     `json:"error_message,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
}

type AuditEvent struct {
	ID         string            `json:"id"`
	TenantID   string            `json:"tenant_id,omitempty"`
	ActorType  string            `json:"actor_type"`
	ActorID    string            `json:"actor_id"`
	Action     string            `json:"action"`
	TargetType string            `json:"target_type"`
	TargetID   string            `json:"target_id,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
}

type Principal struct {
	ActorType string
	ActorID   string
	TenantID  string
	Scopes    map[string]struct{}
}

func (p Principal) HasScope(scope string) bool {
	if _, ok := p.Scopes["platform.admin"]; ok {
		return true
	}
	if _, ok := p.Scopes["*"]; ok {
		return true
	}
	_, ok := p.Scopes[scope]
	return ok
}

func (p Principal) IsPlatformAdmin() bool {
	return p.HasScope("platform.admin")
}

func NormalizeRuntimePoolMode(runtimeType, poolMode string) string {
	switch strings.TrimSpace(poolMode) {
	case RuntimePoolModeInternalShared:
		if runtimeType == RuntimeTypeManagedOwned {
			return RuntimePoolModeInternalShared
		}
		return RuntimePoolModeDedicated
	case RuntimePoolModeDedicated:
		return RuntimePoolModeDedicated
	default:
		return RuntimePoolModeDedicated
	}
}

type State struct {
	Version          string               `json:"version"`
	Tenants          []Tenant             `json:"tenants"`
	Projects         []Project            `json:"projects"`
	APIKeys          []APIKey             `json:"api_keys"`
	EnrollmentTokens []EnrollmentToken    `json:"enrollment_tokens"`
	NodeKeys         []NodeKey            `json:"node_keys"`
	Machines         []Machine            `json:"machines"`
	Runtimes         []Runtime            `json:"runtimes"`
	RuntimeGrants    []RuntimeAccessGrant `json:"runtime_grants"`
	Apps             []App                `json:"apps"`
	AppDomains       []AppDomain          `json:"app_domains"`
	BackingServices  []BackingService     `json:"backing_services"`
	ServiceBindings  []ServiceBinding     `json:"service_bindings"`
	Operations       []Operation          `json:"operations"`
	AuditEvents      []AuditEvent         `json:"audit_events"`
	Idempotency      []IdempotencyRecord  `json:"idempotency"`
}
