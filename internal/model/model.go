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
	RuntimeAccessModePublic         = "public"
	RuntimeAccessModePlatformShared = "platform-shared"

	RuntimePoolModeDedicated      = "dedicated"
	RuntimePoolModeInternalShared = "internal-shared"

	ProjectRuntimeReservationModeExclusive = "exclusive"

	MachineConnectionModeAgent   = "agent"
	MachineConnectionModeCluster = "cluster"

	NodeKeyScopeTenantRuntime = "tenant-runtime"
	NodeKeyScopePlatformNode  = "platform-node"

	MachineScopeTenantRuntime = "tenant-runtime"
	MachineScopePlatformNode  = "platform-node"

	MachineControlPlaneRoleNone      = "none"
	MachineControlPlaneRoleCandidate = "candidate"
	MachineControlPlaneRoleMember    = "member"

	AppSourceTypeGitHubPublic  = "github-public"
	AppSourceTypeGitHubPrivate = "github-private"
	AppSourceTypeDockerImage   = "docker-image"
	AppSourceTypeUpload        = "upload"

	AppBuildStrategyAuto       = "auto"
	AppBuildStrategyStaticSite = "static-site"
	AppBuildStrategyDockerfile = "dockerfile"
	AppBuildStrategyBuildpacks = "buildpacks"
	AppBuildStrategyNixpacks   = "nixpacks"

	AppNetworkModeBackground = "background"
	AppNetworkModeInternal   = "internal"

	AppNetworkPolicyModeRestricted = "restricted"

	AppGeneratedEnvGenerateRandom    = "random"
	AppGeneratedEnvEncodingBase64URL = "base64url"
	AppGeneratedEnvEncodingBase64    = "base64"
	AppGeneratedEnvEncodingHex       = "hex"
	DefaultAppGeneratedEnvBytes      = 32

	WorkloadClassCritical = "critical"
	WorkloadClassService  = "service"
	WorkloadClassDemo     = "demo"
	WorkloadClassBatch    = "batch"

	AppRightSizingModeDisabled  = "disabled"
	AppRightSizingModeRecommend = "recommend"
	AppRightSizingModeAuto      = "auto"

	BackingServiceTypePostgres = "postgres"

	BackingServiceProvisionerManaged  = "managed"
	BackingServiceProvisionerExternal = "external"

	BackingServiceStatusActive  = "active"
	BackingServiceStatusDeleted = "deleted"

	RuntimeStatusPending = "pending"
	RuntimeStatusActive  = "active"
	RuntimeStatusOffline = "offline"

	DefaultManagedRuntimeID = "runtime_managed_shared"

	APIKeyStatusActive   = "active"
	APIKeyStatusDisabled = "disabled"

	NodeKeyStatusActive  = "active"
	NodeKeyStatusRevoked = "revoked"

	NodeUpdaterStatusActive  = "active"
	NodeUpdaterStatusRevoked = "revoked"

	NodeUpdateTaskTypeRefreshJoinConfig = "refresh-join-config"
	NodeUpdateTaskTypeUpgradeK3SAgent   = "upgrade-k3s-agent"
	NodeUpdateTaskTypeUpgradeUpdater    = "upgrade-node-updater"
	NodeUpdateTaskTypeRestartK3SAgent   = "restart-k3s-agent"
	NodeUpdateTaskTypeDiagnoseNode      = "diagnose-node"

	NodeUpdateTaskStatusPending   = "pending"
	NodeUpdateTaskStatusRunning   = "running"
	NodeUpdateTaskStatusCompleted = "completed"
	NodeUpdateTaskStatusFailed    = "failed"
	NodeUpdateTaskStatusCanceled  = "canceled"

	OperationTypeImport             = "import"
	OperationTypeDeploy             = "deploy"
	OperationTypeScale              = "scale"
	OperationTypeMigrate            = "migrate"
	OperationTypeFailover           = "failover"
	OperationTypeDatabaseSwitchover = "database-switchover"
	OperationTypeDatabaseLocalize   = "database-localize"
	OperationTypeDelete             = "delete"

	OperationStatusPending      = "pending"
	OperationStatusRunning      = "running"
	OperationStatusWaitingAgent = "waiting-agent"
	OperationStatusCompleted    = "completed"
	OperationStatusFailed       = "failed"

	IdempotencyScopeAppImportGitHub = "app.import_github"
	IdempotencyScopeAppImportImage  = "app.import_image"

	IdempotencyStatusPending   = "pending"
	IdempotencyStatusCompleted = "completed"

	ExecutionModeManaged = "managed"
	ExecutionModeAgent   = "agent"

	ActorTypeBootstrap   = "bootstrap"
	ActorTypeAPIKey      = "api-key"
	ActorTypeNodeKey     = "node-key"
	ActorTypeNodeUpdater = "node-updater"
	ActorTypeRuntime     = "runtime"
	ActorTypeWorkload    = "workload"
	ActorTypeSystem      = "system"

	OperationRequestedByGitHubSyncController = "fugue-controller/github-sync"
	OperationRequestedByAutoFailover         = "fugue-controller/auto-failover"

	ClusterNodeWorkloadKindApp            = "app"
	ClusterNodeWorkloadKindBackingService = "backing_service"

	DefaultAppWorkspaceMountPath             = "/workspace"
	AppWorkspaceInternalDirName              = ".fugue-workspace-state"
	AppPersistentStorageInternalDirName      = ".fugue-persistent-storage-state"
	AppPersistentStorageModeDedicatedPVC     = "dedicated_pvc"
	AppPersistentStorageModeMovableRWO       = "movable_rwo"
	AppPersistentStorageModeSharedProjectRWX = "shared_project_rwx"
	AppPersistentStorageMountKindDirectory   = "directory"
	AppPersistentStorageMountKindFile        = "file"
	AppVolumeReplicationModeDisabled         = "disabled"
	AppVolumeReplicationModeManual           = "manual"
	AppVolumeReplicationModeScheduled        = "scheduled"
	DefaultAppVolumeReplicationSchedule      = "*/5 * * * *"
	DefaultAppImageMirrorLimit               = 1
)

func NormalizeGitHubAppSourceType(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case AppSourceTypeGitHubPublic:
		return AppSourceTypeGitHubPublic
	case AppSourceTypeGitHubPrivate:
		return AppSourceTypeGitHubPrivate
	default:
		return ""
	}
}

func IsGitHubAppSourceType(raw string) bool {
	return NormalizeGitHubAppSourceType(raw) != ""
}

func ResolveGitHubAppSourceType(raw string, hasRepoAuth bool) string {
	if normalized := NormalizeGitHubAppSourceType(raw); normalized != "" {
		return normalized
	}
	if hasRepoAuth {
		return AppSourceTypeGitHubPrivate
	}
	return AppSourceTypeGitHubPublic
}

func EffectiveAppImageMirrorLimit(value int) int {
	if value <= 0 {
		return DefaultAppImageMirrorLimit
	}
	return value
}

func ApplyAppSpecDefaults(spec *AppSpec) {
	if spec == nil {
		return
	}
	spec.NetworkMode = NormalizeAppNetworkMode(spec.NetworkMode)
	if spec.NetworkPolicy != nil {
		normalized := NormalizeAppNetworkPolicySpec(*spec.NetworkPolicy)
		spec.NetworkPolicy = &normalized
	}
	spec.GeneratedEnv = NormalizeAppGeneratedEnvSpecs(spec.GeneratedEnv)
	spec.WorkloadClass = NormalizeWorkloadClass(spec.WorkloadClass)
	if spec.RightSizing != nil {
		normalized := NormalizeAppRightSizingSpec(*spec.RightSizing)
		spec.RightSizing = &normalized
	}
	spec.ImageMirrorLimit = EffectiveAppImageMirrorLimit(spec.ImageMirrorLimit)
}

func NormalizeWorkloadClass(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case WorkloadClassCritical:
		return WorkloadClassCritical
	case WorkloadClassDemo:
		return WorkloadClassDemo
	case WorkloadClassBatch:
		return WorkloadClassBatch
	case WorkloadClassService, "":
		return WorkloadClassService
	default:
		return ""
	}
}

func EffectiveWorkloadClass(spec AppSpec) string {
	if normalized := NormalizeWorkloadClass(spec.WorkloadClass); normalized != "" {
		return normalized
	}
	if AppUsesBackgroundNetwork(spec) {
		return WorkloadClassBatch
	}
	return WorkloadClassService
}

func NormalizeAppRightSizingMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case AppRightSizingModeAuto:
		return AppRightSizingModeAuto
	case AppRightSizingModeRecommend:
		return AppRightSizingModeRecommend
	case AppRightSizingModeDisabled, "":
		return AppRightSizingModeDisabled
	default:
		return ""
	}
}

func NormalizeAppRightSizingSpec(spec AppRightSizingSpec) AppRightSizingSpec {
	out := spec
	rawMode := strings.TrimSpace(out.Mode)
	out.Mode = NormalizeAppRightSizingMode(out.Mode)
	if out.Mode == "" && rawMode == "" {
		out.Mode = AppRightSizingModeDisabled
	}
	if out.WindowHours <= 0 {
		out.WindowHours = 168
	}
	if out.WindowHours > 168 {
		out.WindowHours = 168
	}
	if out.MinSamples <= 0 {
		out.MinSamples = 12
	}
	return out
}

func NormalizeAppNetworkMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case AppNetworkModeBackground:
		return AppNetworkModeBackground
	case AppNetworkModeInternal:
		return AppNetworkModeInternal
	default:
		return ""
	}
}

func AppUsesBackgroundNetwork(spec AppSpec) bool {
	return NormalizeAppNetworkMode(spec.NetworkMode) == AppNetworkModeBackground
}

func AppUsesInternalNetwork(spec AppSpec) bool {
	return NormalizeAppNetworkMode(spec.NetworkMode) == AppNetworkModeInternal
}

func NormalizeAppNetworkPolicyMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case AppNetworkPolicyModeRestricted:
		return AppNetworkPolicyModeRestricted
	default:
		return ""
	}
}

func NormalizeAppNetworkPolicySpec(spec AppNetworkPolicySpec) AppNetworkPolicySpec {
	if spec.Egress != nil {
		egress := NormalizeAppNetworkPolicyDirectionSpec(*spec.Egress)
		spec.Egress = &egress
	}
	if spec.Ingress != nil {
		ingress := NormalizeAppNetworkPolicyDirectionSpec(*spec.Ingress)
		spec.Ingress = &ingress
	}
	return spec
}

func NormalizeAppNetworkPolicyDirectionSpec(spec AppNetworkPolicyDirectionSpec) AppNetworkPolicyDirectionSpec {
	spec.Mode = NormalizeAppNetworkPolicyMode(spec.Mode)
	if len(spec.AllowApps) > 0 {
		peers := make([]AppNetworkPolicyAppPeer, 0, len(spec.AllowApps))
		for _, peer := range spec.AllowApps {
			peer.AppID = strings.TrimSpace(peer.AppID)
			ports := make([]int, 0, len(peer.Ports))
			for _, port := range peer.Ports {
				if port > 0 && port <= 65535 {
					ports = append(ports, port)
				}
			}
			peer.Ports = ports
			if peer.AppID != "" {
				peers = append(peers, peer)
			}
		}
		spec.AllowApps = peers
	}
	return spec
}

func NormalizeAppGeneratedEnvGenerate(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case AppGeneratedEnvGenerateRandom, "":
		return AppGeneratedEnvGenerateRandom
	default:
		return ""
	}
}

func NormalizeAppGeneratedEnvEncoding(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case AppGeneratedEnvEncodingBase64URL, "":
		return AppGeneratedEnvEncodingBase64URL
	case AppGeneratedEnvEncodingBase64:
		return AppGeneratedEnvEncodingBase64
	case AppGeneratedEnvEncodingHex:
		return AppGeneratedEnvEncodingHex
	default:
		return ""
	}
}

func NormalizeAppGeneratedEnvSpec(spec AppGeneratedEnvSpec) AppGeneratedEnvSpec {
	spec.Generate = NormalizeAppGeneratedEnvGenerate(spec.Generate)
	spec.Encoding = NormalizeAppGeneratedEnvEncoding(spec.Encoding)
	if spec.Length <= 0 {
		spec.Length = DefaultAppGeneratedEnvBytes
	}
	return spec
}

func NormalizeAppGeneratedEnvSpecs(in map[string]AppGeneratedEnvSpec) map[string]AppGeneratedEnvSpec {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]AppGeneratedEnvSpec, len(in))
	for key, spec := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = NormalizeAppGeneratedEnvSpec(spec)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func AppManagedRouteEnabled(spec AppSpec) bool {
	return !AppUsesBackgroundNetwork(spec) && !AppUsesInternalNetwork(spec)
}

func AppServicePort(spec AppSpec) int {
	if AppUsesBackgroundNetwork(spec) {
		return 0
	}
	for _, port := range spec.Ports {
		if port > 0 {
			return port
		}
	}
	return 0
}

func AppHasClusterService(spec AppSpec) bool {
	return AppServicePort(spec) > 0
}

func AppPublicServicePort(spec AppSpec) int {
	if !AppManagedRouteEnabled(spec) {
		return 0
	}
	return AppServicePort(spec)
}

func AppExposesPublicService(spec AppSpec) bool {
	return AppPublicServicePort(spec) > 0
}

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Project struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenant_id"`
	Name             string    `json:"name"`
	Slug             string    `json:"slug"`
	Description      string    `json:"description"`
	DefaultRuntimeID string    `json:"default_runtime_id,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type ProjectRuntimeReservation struct {
	TenantID  string    `json:"tenant_id"`
	ProjectID string    `json:"project_id"`
	RuntimeID string    `json:"runtime_id"`
	Mode      string    `json:"mode"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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
	Scope      string     `json:"scope"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
}

type Runtime struct {
	ID                string              `json:"id"`
	TenantID          string              `json:"tenant_id,omitempty"`
	Name              string              `json:"name"`
	MachineName       string              `json:"machine_name,omitempty"`
	Type              string              `json:"type"`
	AccessMode        string              `json:"access_mode,omitempty"`
	PublicOffer       *RuntimePublicOffer `json:"public_offer,omitempty"`
	PoolMode          string              `json:"pool_mode,omitempty"`
	ConnectionMode    string              `json:"connection_mode,omitempty"`
	Status            string              `json:"status"`
	Endpoint          string              `json:"endpoint,omitempty"`
	Labels            map[string]string   `json:"labels,omitempty"`
	NodeKeyID         string              `json:"node_key_id,omitempty"`
	ClusterNodeName   string              `json:"cluster_node_name,omitempty"`
	FingerprintPrefix string              `json:"fingerprint_prefix,omitempty"`
	FingerprintHash   string              `json:"fingerprint_hash,omitempty"`
	AgentKeyPrefix    string              `json:"agent_key_prefix,omitempty"`
	AgentKeyHash      string              `json:"agent_key_hash,omitempty"`
	LastSeenAt        *time.Time          `json:"last_seen_at,omitempty"`
	LastHeartbeatAt   *time.Time          `json:"last_heartbeat_at,omitempty"`
	CreatedAt         time.Time           `json:"created_at"`
	UpdatedAt         time.Time           `json:"updated_at"`
}

type RuntimePublicOffer struct {
	ReferenceBundle                 BillingResourceSpec `json:"reference_bundle"`
	ReferenceMonthlyPriceMicroCents int64               `json:"reference_monthly_price_microcents,omitempty"`
	Free                            bool                `json:"free,omitempty"`
	FreeCPU                         bool                `json:"free_cpu,omitempty"`
	FreeMemory                      bool                `json:"free_memory,omitempty"`
	FreeStorage                     bool                `json:"free_storage,omitempty"`
	PriceBook                       BillingPriceBook    `json:"price_book"`
	UpdatedAt                       time.Time           `json:"updated_at"`
}

type Machine struct {
	ID                string            `json:"id"`
	TenantID          string            `json:"tenant_id,omitempty"`
	Name              string            `json:"name"`
	Scope             string            `json:"scope,omitempty"`
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
	Policy            MachinePolicy     `json:"policy"`
	LastSeenAt        *time.Time        `json:"last_seen_at,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type MachinePolicy struct {
	AllowBuilds             bool   `json:"allow_builds"`
	AllowSharedPool         bool   `json:"allow_shared_pool"`
	DesiredControlPlaneRole string `json:"desired_control_plane_role,omitempty"`
}

type NodeUpdater struct {
	ID                string            `json:"id"`
	TenantID          string            `json:"tenant_id,omitempty"`
	NodeKeyID         string            `json:"node_key_id,omitempty"`
	MachineID         string            `json:"machine_id,omitempty"`
	RuntimeID         string            `json:"runtime_id,omitempty"`
	ClusterNodeName   string            `json:"cluster_node_name,omitempty"`
	Status            string            `json:"status"`
	TokenPrefix       string            `json:"token_prefix,omitempty"`
	TokenHash         string            `json:"token_hash,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Capabilities      []string          `json:"capabilities,omitempty"`
	UpdaterVersion    string            `json:"updater_version,omitempty"`
	JoinScriptVersion string            `json:"join_script_version,omitempty"`
	K3SVersion        string            `json:"k3s_version,omitempty"`
	OS                string            `json:"os,omitempty"`
	Arch              string            `json:"arch,omitempty"`
	LastError         string            `json:"last_error,omitempty"`
	LastSeenAt        *time.Time        `json:"last_seen_at,omitempty"`
	LastHeartbeatAt   *time.Time        `json:"last_heartbeat_at,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type NodeUpdateTask struct {
	ID              string              `json:"id"`
	TenantID        string              `json:"tenant_id,omitempty"`
	NodeUpdaterID   string              `json:"node_updater_id"`
	MachineID       string              `json:"machine_id,omitempty"`
	RuntimeID       string              `json:"runtime_id,omitempty"`
	NodeKeyID       string              `json:"node_key_id,omitempty"`
	ClusterNodeName string              `json:"cluster_node_name,omitempty"`
	Type            string              `json:"type"`
	Status          string              `json:"status"`
	Payload         map[string]string   `json:"payload,omitempty"`
	ResultMessage   string              `json:"result_message,omitempty"`
	ErrorMessage    string              `json:"error_message,omitempty"`
	Logs            []NodeUpdateTaskLog `json:"logs,omitempty"`
	RequestedByType string              `json:"requested_by_type,omitempty"`
	RequestedByID   string              `json:"requested_by_id,omitempty"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
	ClaimedAt       *time.Time          `json:"claimed_at,omitempty"`
	CompletedAt     *time.Time          `json:"completed_at,omitempty"`
}

type NodeUpdateTaskLog struct {
	At      time.Time `json:"at"`
	Message string    `json:"message"`
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
	CapacityMilliCores        *int64   `json:"capacity_millicores,omitempty"`
	AllocatableMilliCores     *int64   `json:"allocatable_millicores,omitempty"`
	UsedMilliCores            *int64   `json:"used_millicores,omitempty"`
	UsagePercent              *float64 `json:"usage_percent,omitempty"`
	RequestedMilliCores       *int64   `json:"requested_millicores,omitempty"`
	RequestPercent            *float64 `json:"request_percent,omitempty"`
	SchedulableFreeMilliCores *int64   `json:"schedulable_free_millicores,omitempty"`
}

type ClusterNodeMemoryStats struct {
	CapacityBytes        *int64   `json:"capacity_bytes,omitempty"`
	AllocatableBytes     *int64   `json:"allocatable_bytes,omitempty"`
	UsedBytes            *int64   `json:"used_bytes,omitempty"`
	UsagePercent         *float64 `json:"usage_percent,omitempty"`
	RequestedBytes       *int64   `json:"requested_bytes,omitempty"`
	RequestPercent       *float64 `json:"request_percent,omitempty"`
	SchedulableFreeBytes *int64   `json:"schedulable_free_bytes,omitempty"`
}

type ClusterNodeStorageStats struct {
	CapacityBytes        *int64   `json:"capacity_bytes,omitempty"`
	AllocatableBytes     *int64   `json:"allocatable_bytes,omitempty"`
	UsedBytes            *int64   `json:"used_bytes,omitempty"`
	UsagePercent         *float64 `json:"usage_percent,omitempty"`
	RequestedBytes       *int64   `json:"requested_bytes,omitempty"`
	RequestPercent       *float64 `json:"request_percent,omitempty"`
	SchedulableFreeBytes *int64   `json:"schedulable_free_bytes,omitempty"`
}

type ResourceUsage struct {
	CPUMilliCores         *int64 `json:"cpu_millicores,omitempty"`
	MemoryBytes           *int64 `json:"memory_bytes,omitempty"`
	EphemeralStorageBytes *int64 `json:"ephemeral_storage_bytes,omitempty"`
}

type ResourceUsageSample struct {
	ID                    string    `json:"id,omitempty"`
	TenantID              string    `json:"tenant_id,omitempty"`
	ProjectID             string    `json:"project_id,omitempty"`
	TargetKind            string    `json:"target_kind"`
	TargetID              string    `json:"target_id"`
	TargetName            string    `json:"target_name,omitempty"`
	ServiceType           string    `json:"service_type,omitempty"`
	ObservedAt            time.Time `json:"observed_at"`
	CPUMilliCores         *int64    `json:"cpu_millicores,omitempty"`
	MemoryBytes           *int64    `json:"memory_bytes,omitempty"`
	EphemeralStorageBytes *int64    `json:"ephemeral_storage_bytes,omitempty"`
}

type ResourceRightSizingPolicy struct {
	WindowHours      int     `json:"window_hours"`
	MinSamples       int     `json:"min_samples"`
	CPUPercentile    float64 `json:"cpu_percentile"`
	CPUMultiplier    float64 `json:"cpu_multiplier"`
	CPUFloorMilli    int64   `json:"cpu_floor_millicores"`
	MemoryPercentile float64 `json:"memory_percentile"`
	MemoryMultiplier float64 `json:"memory_multiplier"`
	MemoryFloorMiB   int64   `json:"memory_floor_mebibytes"`
}

type ResourceRightSizingRecommendation struct {
	TargetKind           string                    `json:"target_kind"`
	TargetID             string                    `json:"target_id"`
	TargetName           string                    `json:"target_name,omitempty"`
	ServiceType          string                    `json:"service_type,omitempty"`
	WorkloadClass        string                    `json:"workload_class,omitempty"`
	WindowHours          int                       `json:"window_hours"`
	SampleCount          int                       `json:"sample_count"`
	Current              *ResourceSpec             `json:"current,omitempty"`
	Recommended          *ResourceSpec             `json:"recommended,omitempty"`
	Policy               ResourceRightSizingPolicy `json:"policy"`
	Ready                bool                      `json:"ready"`
	AlreadyCurrent       bool                      `json:"already_current"`
	Reason               string                    `json:"reason,omitempty"`
	ObservedAt           *time.Time                `json:"observed_at,omitempty"`
	LastSampleObservedAt *time.Time                `json:"last_sample_observed_at,omitempty"`
	PeakCPUUsageMilli    *int64                    `json:"peak_cpu_usage_millicores,omitempty"`
	PeakMemoryUsageBytes *int64                    `json:"peak_memory_usage_bytes,omitempty"`
}

type AppRightSizingRecommendation struct {
	App             ResourceRightSizingRecommendation   `json:"app"`
	BackingServices []ResourceRightSizingRecommendation `json:"backing_services,omitempty"`
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
	Machine          *ClusterNodeMachine             `json:"machine,omitempty"`
	Policy           *ClusterNodePolicy              `json:"policy,omitempty"`
	Workloads        []ClusterNodeWorkload           `json:"workloads,omitempty"`
	CreatedAt        *time.Time                      `json:"created_at,omitempty"`
}

type ClusterNodeMachine struct {
	ID             string `json:"id"`
	Scope          string `json:"scope"`
	ConnectionMode string `json:"connection_mode"`
	Status         string `json:"status"`
	TenantID       string `json:"tenant_id,omitempty"`
	RuntimeID      string `json:"runtime_id,omitempty"`
	NodeKeyID      string `json:"node_key_id,omitempty"`
}

type ClusterNodePolicy struct {
	AllowBuilds               bool   `json:"allow_builds"`
	AllowSharedPool           bool   `json:"allow_shared_pool"`
	NodeMode                  string `json:"node_mode,omitempty"`
	DesiredControlPlaneRole   string `json:"desired_control_plane_role,omitempty"`
	EffectiveBuilds           bool   `json:"effective_builds"`
	EffectiveSharedPool       bool   `json:"effective_shared_pool"`
	EffectiveControlPlaneRole string `json:"effective_control_plane_role,omitempty"`
}

type ControlPlaneComponent struct {
	Component         string            `json:"component"`
	DeploymentName    string            `json:"deployment_name"`
	Image             string            `json:"image"`
	ImageRepository   string            `json:"image_repository"`
	ImageTag          string            `json:"image_tag"`
	ObservedImageTags []string          `json:"observed_image_tags,omitempty"`
	ObservedPods      []ControlPlanePod `json:"observed_pods,omitempty"`
	Status            string            `json:"status"`
	DesiredReplicas   int               `json:"desired_replicas"`
	ReadyReplicas     int               `json:"ready_replicas"`
	UpdatedReplicas   int               `json:"updated_replicas"`
	AvailableReplicas int               `json:"available_replicas"`
}

type ControlPlanePod struct {
	Name            string     `json:"name"`
	NodeName        string     `json:"node_name,omitempty"`
	Phase           string     `json:"phase,omitempty"`
	Ready           bool       `json:"ready"`
	Image           string     `json:"image,omitempty"`
	ImageRepository string     `json:"image_repository,omitempty"`
	ImageTag        string     `json:"image_tag,omitempty"`
	StartTime       *time.Time `json:"start_time,omitempty"`
}

type ControlPlaneStatus struct {
	Namespace       string                   `json:"namespace"`
	ReleaseInstance string                   `json:"release_instance"`
	Version         string                   `json:"version"`
	LiveVersion     string                   `json:"live_version,omitempty"`
	Status          string                   `json:"status"`
	ObservedAt      time.Time                `json:"observed_at"`
	Components      []ControlPlaneComponent  `json:"components"`
	DeployWorkflow  *ControlPlaneWorkflowRun `json:"deploy_workflow,omitempty"`
}

type ControlPlaneWorkflowRun struct {
	Repository string     `json:"repository"`
	Workflow   string     `json:"workflow"`
	Status     string     `json:"status"`
	Conclusion string     `json:"conclusion,omitempty"`
	RunNumber  int        `json:"run_number,omitempty"`
	Event      string     `json:"event,omitempty"`
	HeadBranch string     `json:"head_branch,omitempty"`
	HeadSHA    string     `json:"head_sha"`
	HTMLURL    string     `json:"html_url,omitempty"`
	CreatedAt  *time.Time `json:"created_at,omitempty"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
	ObservedAt time.Time  `json:"observed_at"`
	Error      string     `json:"error,omitempty"`
}

type ClusterPodOwner struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type ClusterPodContainer struct {
	Name         string `json:"name"`
	Image        string `json:"image"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restart_count"`
	State        string `json:"state"`
	Reason       string `json:"reason,omitempty"`
	Message      string `json:"message,omitempty"`
}

type ClusterPod struct {
	Namespace  string                `json:"namespace"`
	Name       string                `json:"name"`
	Phase      string                `json:"phase"`
	NodeName   string                `json:"node_name,omitempty"`
	PodIP      string                `json:"pod_ip,omitempty"`
	HostIP     string                `json:"host_ip,omitempty"`
	QOSClass   string                `json:"qos_class,omitempty"`
	Owner      *ClusterPodOwner      `json:"owner,omitempty"`
	Labels     map[string]string     `json:"labels,omitempty"`
	Ready      bool                  `json:"ready"`
	StartTime  *time.Time            `json:"start_time,omitempty"`
	Containers []ClusterPodContainer `json:"containers"`
}

type ClusterEvent struct {
	Namespace       string     `json:"namespace"`
	Name            string     `json:"name"`
	Type            string     `json:"type"`
	Reason          string     `json:"reason"`
	Message         string     `json:"message"`
	ObjectKind      string     `json:"object_kind"`
	ObjectName      string     `json:"object_name"`
	ObjectNamespace string     `json:"object_namespace,omitempty"`
	Count           int32      `json:"count,omitempty"`
	FirstTimestamp  *time.Time `json:"first_timestamp,omitempty"`
	LastTimestamp   *time.Time `json:"last_timestamp,omitempty"`
	EventTime       *time.Time `json:"event_time,omitempty"`
}

type ClusterDNSAnswer struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type ClusterDNSResolveResult struct {
	Name       string             `json:"name"`
	Server     string             `json:"server,omitempty"`
	RecordType string             `json:"record_type"`
	Answers    []ClusterDNSAnswer `json:"answers"`
	ObservedAt time.Time          `json:"observed_at"`
	Error      string             `json:"error,omitempty"`
}

type ClusterNetworkConnectResult struct {
	Target            string    `json:"target"`
	Network           string    `json:"network,omitempty"`
	Success           bool      `json:"success"`
	DurationMillis    int64     `json:"duration_ms"`
	RemoteAddr        string    `json:"remote_addr,omitempty"`
	ResolvedAddresses []string  `json:"resolved_addresses,omitempty"`
	ObservedAt        time.Time `json:"observed_at,omitempty"`
	Error             string    `json:"error,omitempty"`
}

type ClusterTLSPeerCertificate struct {
	Subject     string    `json:"subject"`
	Issuer      string    `json:"issuer"`
	SHA256      string    `json:"sha256"`
	DNSNames    []string  `json:"dns_names,omitempty"`
	IPAddresses []string  `json:"ip_addresses,omitempty"`
	NotBefore   time.Time `json:"not_before,omitempty"`
	NotAfter    time.Time `json:"not_after,omitempty"`
}

type ClusterTLSProbeResult struct {
	Target             string                      `json:"target"`
	ServerName         string                      `json:"server_name,omitempty"`
	Success            bool                        `json:"success"`
	DurationMillis     int64                       `json:"duration_ms"`
	Version            string                      `json:"version,omitempty"`
	CipherSuite        string                      `json:"cipher_suite,omitempty"`
	NegotiatedProtocol string                      `json:"negotiated_protocol,omitempty"`
	Verified           bool                        `json:"verified,omitempty"`
	VerificationError  string                      `json:"verification_error,omitempty"`
	ObservedAt         time.Time                   `json:"observed_at,omitempty"`
	PeerCertificates   []ClusterTLSPeerCertificate `json:"peer_certificates,omitempty"`
	Error              string                      `json:"error,omitempty"`
}

type ClusterWebSocketProbeAttempt struct {
	Target         string            `json:"target"`
	URL            string            `json:"url,omitempty"`
	Status         string            `json:"status,omitempty"`
	StatusCode     int               `json:"status_code,omitempty"`
	Upgraded       bool              `json:"upgraded"`
	DurationMillis int64             `json:"duration_ms"`
	Headers        map[string]string `json:"headers,omitempty"`
	BodyPreview    string            `json:"body_preview,omitempty"`
	Error          string            `json:"error,omitempty"`
}

type ClusterWebSocketProbeResult struct {
	AppID           string                       `json:"app_id"`
	AppName         string                       `json:"app_name"`
	Path            string                       `json:"path"`
	RouteConfigured bool                         `json:"route_configured"`
	Service         ClusterWebSocketProbeAttempt `json:"service"`
	PublicRoute     ClusterWebSocketProbeAttempt `json:"public_route"`
	ConclusionCode  string                       `json:"conclusion_code"`
	Conclusion      string                       `json:"conclusion"`
	ObservedAt      time.Time                    `json:"observed_at"`
}

type ClusterWorkloadCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type ClusterWorkloadContainer struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

type AppRuntimePodGroup struct {
	OwnerKind         string                     `json:"owner_kind"`
	OwnerName         string                     `json:"owner_name"`
	Parent            *ClusterPodOwner           `json:"parent,omitempty"`
	Revision          string                     `json:"revision,omitempty"`
	CreatedAt         *time.Time                 `json:"created_at,omitempty"`
	DesiredReplicas   *int32                     `json:"desired_replicas,omitempty"`
	ReadyReplicas     *int32                     `json:"ready_replicas,omitempty"`
	AvailableReplicas *int32                     `json:"available_replicas,omitempty"`
	CurrentReplicas   *int32                     `json:"current_replicas,omitempty"`
	Containers        []ClusterWorkloadContainer `json:"containers,omitempty"`
	Pods              []ClusterPod               `json:"pods"`
}

type AppRuntimePodInventory struct {
	Component string               `json:"component"`
	Namespace string               `json:"namespace"`
	Selector  string               `json:"selector"`
	Container string               `json:"container"`
	Groups    []AppRuntimePodGroup `json:"groups"`
	Warnings  []string             `json:"warnings,omitempty"`
}

type ClusterWorkloadDetail struct {
	APIVersion        string                     `json:"api_version"`
	Kind              string                     `json:"kind"`
	Namespace         string                     `json:"namespace"`
	Name              string                     `json:"name"`
	Selector          string                     `json:"selector,omitempty"`
	Labels            map[string]string          `json:"labels,omitempty"`
	Annotations       map[string]string          `json:"annotations,omitempty"`
	NodeSelector      map[string]string          `json:"node_selector,omitempty"`
	Tolerations       []string                   `json:"tolerations,omitempty"`
	Containers        []ClusterWorkloadContainer `json:"containers,omitempty"`
	InitContainers    []ClusterWorkloadContainer `json:"init_containers,omitempty"`
	DesiredReplicas   *int32                     `json:"desired_replicas,omitempty"`
	ReadyReplicas     *int32                     `json:"ready_replicas,omitempty"`
	UpdatedReplicas   *int32                     `json:"updated_replicas,omitempty"`
	AvailableReplicas *int32                     `json:"available_replicas,omitempty"`
	CurrentReplicas   *int32                     `json:"current_replicas,omitempty"`
	Conditions        []ClusterWorkloadCondition `json:"conditions,omitempty"`
	Pods              []ClusterPod               `json:"pods,omitempty"`
	Manifest          map[string]any             `json:"manifest,omitempty"`
}

type ClusterServicePort struct {
	Name       string `json:"name,omitempty"`
	Port       int32  `json:"port"`
	Protocol   string `json:"protocol,omitempty"`
	TargetPort string `json:"target_port,omitempty"`
}

type ClusterServiceEndpoint struct {
	IP              string `json:"ip,omitempty"`
	NodeName        string `json:"node_name,omitempty"`
	Ready           bool   `json:"ready"`
	TargetKind      string `json:"target_kind,omitempty"`
	TargetName      string `json:"target_name,omitempty"`
	TargetNamespace string `json:"target_namespace,omitempty"`
	Pod             string `json:"pod,omitempty"`
}

type ClusterServiceDetail struct {
	Namespace    string                   `json:"namespace"`
	Name         string                   `json:"name"`
	Type         string                   `json:"type,omitempty"`
	ClusterIP    string                   `json:"cluster_ip,omitempty"`
	ExternalName string                   `json:"external_name,omitempty"`
	Selector     map[string]string        `json:"selector,omitempty"`
	Labels       map[string]string        `json:"labels,omitempty"`
	Annotations  map[string]string        `json:"annotations,omitempty"`
	Ports        []ClusterServicePort     `json:"ports,omitempty"`
	Endpoints    []ClusterServiceEndpoint `json:"endpoints,omitempty"`
}

type ClusterRolloutStatus struct {
	Kind              string                     `json:"kind"`
	Namespace         string                     `json:"namespace"`
	Name              string                     `json:"name"`
	Status            string                     `json:"status"`
	DesiredReplicas   *int32                     `json:"desired_replicas,omitempty"`
	ReadyReplicas     *int32                     `json:"ready_replicas,omitempty"`
	UpdatedReplicas   *int32                     `json:"updated_replicas,omitempty"`
	AvailableReplicas *int32                     `json:"available_replicas,omitempty"`
	Message           string                     `json:"message,omitempty"`
	Conditions        []ClusterWorkloadCondition `json:"conditions,omitempty"`
	ObservedAt        time.Time                  `json:"observed_at,omitempty"`
}

type AppSource struct {
	Type              string   `json:"type"`
	RepoURL           string   `json:"repo_url,omitempty"`
	RepoBranch        string   `json:"repo_branch,omitempty"`
	RepoAuthToken     string   `json:"repo_auth_token,omitempty"`
	ImageRef          string   `json:"image_ref,omitempty"`
	ResolvedImageRef  string   `json:"resolved_image_ref,omitempty"`
	UploadID          string   `json:"upload_id,omitempty"`
	UploadFilename    string   `json:"upload_filename,omitempty"`
	ArchiveSHA256     string   `json:"archive_sha256,omitempty"`
	ArchiveSizeBytes  int64    `json:"archive_size_bytes,omitempty"`
	SourceDir         string   `json:"source_dir,omitempty"`
	BuildStrategy     string   `json:"build_strategy,omitempty"`
	CommitSHA         string   `json:"commit_sha,omitempty"`
	CommitCommittedAt string   `json:"commit_committed_at,omitempty"`
	DockerfilePath    string   `json:"dockerfile_path,omitempty"`
	BuildContextDir   string   `json:"build_context_dir,omitempty"`
	ImageNameSuffix   string   `json:"image_name_suffix,omitempty"`
	ComposeService    string   `json:"compose_service,omitempty"`
	ComposeDependsOn  []string `json:"compose_depends_on,omitempty"`
	DetectedProvider  string   `json:"detected_provider,omitempty"`
	DetectedStack     string   `json:"detected_stack,omitempty"`
}

func CloneAppSource(in *AppSource) *AppSource {
	if in == nil {
		return nil
	}
	out := *in
	if len(in.ComposeDependsOn) > 0 {
		out.ComposeDependsOn = append([]string(nil), in.ComposeDependsOn...)
	}
	return &out
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

type AppInternalService struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Host      string `json:"host,omitempty"`
	Port      int    `json:"port,omitempty"`
}

type AppEnvEntry struct {
	Key       string   `json:"key"`
	Value     string   `json:"value"`
	Source    string   `json:"source,omitempty"`
	SourceRef string   `json:"source_ref,omitempty"`
	Overrides []string `json:"overrides,omitempty"`
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
	Image             string                         `json:"image"`
	Command           []string                       `json:"command,omitempty"`
	Args              []string                       `json:"args,omitempty"`
	Env               map[string]string              `json:"env,omitempty"`
	GeneratedEnv      map[string]AppGeneratedEnvSpec `json:"generated_env,omitempty"`
	NetworkMode       string                         `json:"network_mode,omitempty"`
	NetworkPolicy     *AppNetworkPolicySpec          `json:"network_policy,omitempty"`
	WorkloadClass     string                         `json:"workload_class,omitempty"`
	Ports             []int                          `json:"ports,omitempty"`
	Replicas          int                            `json:"replicas"`
	Resources         *ResourceSpec                  `json:"resources,omitempty"`
	RightSizing       *AppRightSizingSpec            `json:"right_sizing,omitempty"`
	RuntimeID         string                         `json:"runtime_id"`
	Files             []AppFile                      `json:"files,omitempty"`
	Workspace         *AppWorkspaceSpec              `json:"workspace,omitempty"`
	PersistentStorage *AppPersistentStorageSpec      `json:"persistent_storage,omitempty"`
	VolumeReplication *AppVolumeReplicationSpec      `json:"volume_replication,omitempty"`
	Postgres          *AppPostgresSpec               `json:"postgres,omitempty"`
	Failover          *AppFailoverSpec               `json:"failover,omitempty"`
	ImageMirrorLimit  int                            `json:"image_mirror_limit,omitempty"`
	RestartToken      string                         `json:"restart_token,omitempty"`
}

type AppNetworkPolicySpec struct {
	Egress  *AppNetworkPolicyDirectionSpec `json:"egress,omitempty" yaml:"egress,omitempty"`
	Ingress *AppNetworkPolicyDirectionSpec `json:"ingress,omitempty" yaml:"ingress,omitempty"`
}

type AppNetworkPolicyDirectionSpec struct {
	Mode                string                    `json:"mode,omitempty" yaml:"mode,omitempty"`
	AllowDNS            bool                      `json:"allow_dns,omitempty" yaml:"allow_dns,omitempty"`
	AllowPublicInternet bool                      `json:"allow_public_internet,omitempty" yaml:"allow_public_internet,omitempty"`
	AllowApps           []AppNetworkPolicyAppPeer `json:"allow_apps,omitempty" yaml:"allow_apps,omitempty"`
}

type AppNetworkPolicyAppPeer struct {
	AppID string `json:"app_id,omitempty" yaml:"app_id,omitempty"`
	Ports []int  `json:"ports,omitempty" yaml:"ports,omitempty"`
}

type AppGeneratedEnvSpec struct {
	Generate string `json:"generate,omitempty" yaml:"generate,omitempty"`
	Encoding string `json:"encoding,omitempty" yaml:"encoding,omitempty"`
	Length   int    `json:"length,omitempty" yaml:"length,omitempty"`
}

type AppRightSizingSpec struct {
	Mode        string `json:"mode,omitempty"`
	WindowHours int    `json:"window_hours,omitempty"`
	MinSamples  int    `json:"min_samples,omitempty"`
}

type AppFile struct {
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
	Secret  bool   `json:"secret,omitempty"`
	Mode    int32  `json:"mode,omitempty"`
}

type AppWorkspaceSpec struct {
	MountPath        string `json:"mount_path,omitempty"`
	StoragePath      string `json:"storage_path,omitempty"`
	StorageSize      string `json:"storage_size,omitempty"`
	StorageClassName string `json:"storage_class_name,omitempty"`
	ResetToken       string `json:"reset_token,omitempty"`
}

type AppPersistentStorageSpec struct {
	Mode             string                      `json:"mode,omitempty"`
	StoragePath      string                      `json:"storage_path,omitempty"`
	StorageSize      string                      `json:"storage_size,omitempty"`
	StorageClassName string                      `json:"storage_class_name,omitempty"`
	ClaimName        string                      `json:"claim_name,omitempty"`
	SharedSubPath    string                      `json:"shared_sub_path,omitempty"`
	ResetToken       string                      `json:"reset_token,omitempty"`
	Mounts           []AppPersistentStorageMount `json:"mounts,omitempty"`
}

type AppPersistentStorageMount struct {
	Kind        string `json:"kind,omitempty"`
	Path        string `json:"path,omitempty"`
	SeedContent string `json:"seed_content,omitempty"`
	Secret      bool   `json:"secret,omitempty"`
	Mode        int32  `json:"mode,omitempty"`
}

type AppVolumeReplicationSpec struct {
	Mode     string `json:"mode,omitempty"`
	Schedule string `json:"schedule,omitempty"`
}

type AppFailoverSpec struct {
	TargetRuntimeID string `json:"target_runtime_id,omitempty"`
	Auto            bool   `json:"auto,omitempty"`
}

type AppPostgresSpec struct {
	Image                            string        `json:"image,omitempty"`
	Database                         string        `json:"database,omitempty"`
	User                             string        `json:"user,omitempty"`
	Password                         string        `json:"password,omitempty"`
	ServiceName                      string        `json:"service_name,omitempty"`
	RuntimeID                        string        `json:"runtime_id,omitempty"`
	FailoverTargetRuntimeID          string        `json:"failover_target_runtime_id,omitempty"`
	PrimaryNodeName                  string        `json:"primary_node_name,omitempty"`
	PrimaryPlacementPendingRebalance bool          `json:"primary_placement_pending_rebalance,omitempty"`
	StorageSize                      string        `json:"storage_size,omitempty"`
	StorageClassName                 string        `json:"storage_class_name,omitempty"`
	Instances                        int           `json:"instances,omitempty"`
	SynchronousReplicas              int           `json:"synchronous_replicas,omitempty"`
	Resources                        *ResourceSpec `json:"resources,omitempty"`
}

func PostgresRWServiceName(serviceName string) string {
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return ""
	}
	return serviceName + "-rw"
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
	ID                   string              `json:"id"`
	TenantID             string              `json:"tenant_id"`
	ProjectID            string              `json:"project_id"`
	Name                 string              `json:"name"`
	Description          string              `json:"description"`
	Source               *AppSource          `json:"source,omitempty"`
	OriginSource         *AppSource          `json:"origin_source,omitempty"`
	BuildSource          *AppSource          `json:"build_source,omitempty"`
	Route                *AppRoute           `json:"route,omitempty"`
	InternalService      *AppInternalService `json:"internal_service,omitempty"`
	Spec                 AppSpec             `json:"spec"`
	Status               AppStatus           `json:"status"`
	CurrentResourceUsage *ResourceUsage      `json:"current_resource_usage,omitempty"`
	Bindings             []ServiceBinding    `json:"bindings,omitempty"`
	BackingServices      []BackingService    `json:"backing_services,omitempty"`
	TechStack            []AppTechnology     `json:"tech_stack,omitempty"`
	CreatedAt            time.Time           `json:"created_at"`
	UpdatedAt            time.Time           `json:"updated_at"`
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

type SourceUploadReference struct {
	OperationID      string    `json:"operation_id"`
	OperationType    string    `json:"operation_type"`
	OperationStatus  string    `json:"operation_status"`
	AppID            string    `json:"app_id,omitempty"`
	AppName          string    `json:"app_name,omitempty"`
	BuildStrategy    string    `json:"build_strategy,omitempty"`
	SourceDir        string    `json:"source_dir,omitempty"`
	DockerfilePath   string    `json:"dockerfile_path,omitempty"`
	BuildContextDir  string    `json:"build_context_dir,omitempty"`
	ResolvedImageRef string    `json:"resolved_image_ref,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type SourceUploadInspection struct {
	Upload     SourceUpload            `json:"upload"`
	References []SourceUploadReference `json:"references"`
}

type Operation struct {
	ID                  string     `json:"id"`
	TenantID            string     `json:"tenant_id"`
	Type                string     `json:"type"`
	Status              string     `json:"status"`
	ExecutionMode       string     `json:"execution_mode"`
	RequestedByType     string     `json:"requested_by_type"`
	RequestedByID       string     `json:"requested_by_id"`
	AppID               string     `json:"app_id"`
	SourceRuntimeID     string     `json:"source_runtime_id,omitempty"`
	TargetRuntimeID     string     `json:"target_runtime_id,omitempty"`
	DesiredReplicas     *int       `json:"desired_replicas,omitempty"`
	DesiredSpec         *AppSpec   `json:"desired_spec,omitempty"`
	DesiredSource       *AppSource `json:"desired_source,omitempty"`
	DesiredOriginSource *AppSource `json:"desired_origin_source,omitempty"`
	ResultMessage       string     `json:"result_message,omitempty"`
	ManifestPath        string     `json:"manifest_path,omitempty"`
	AssignedRuntimeID   string     `json:"assigned_runtime_id,omitempty"`
	ErrorMessage        string     `json:"error_message,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
}

func SetAppSourceState(app *App, origin, build *AppSource) {
	if app == nil {
		return
	}
	app.OriginSource = CloneAppSource(origin)
	app.BuildSource = CloneAppSource(build)
	if app.BuildSource == nil && app.OriginSource != nil {
		app.BuildSource = CloneAppSource(app.OriginSource)
	}
	app.Source = CloneAppSource(app.BuildSource)
}

func NormalizeAppSourceState(app *App) {
	if app == nil {
		return
	}
	origin := CloneAppSource(app.OriginSource)
	build := CloneAppSource(app.BuildSource)
	if build == nil {
		build = CloneAppSource(app.Source)
	}
	if origin == nil {
		origin = CloneAppSource(app.Source)
	}
	if origin == nil {
		origin = CloneAppSource(build)
	}
	SetAppSourceState(app, origin, build)
}

func AppOriginSource(app App) *AppSource {
	normalized := app
	NormalizeAppSourceState(&normalized)
	return CloneAppSource(normalized.OriginSource)
}

func AppBuildSource(app App) *AppSource {
	normalized := app
	NormalizeAppSourceState(&normalized)
	return CloneAppSource(normalized.BuildSource)
}

func SetOperationSourceState(op *Operation, build, origin *AppSource) {
	if op == nil {
		return
	}
	op.DesiredSource = CloneAppSource(build)
	op.DesiredOriginSource = CloneAppSource(origin)
}

func NormalizeOperationSourceState(op *Operation) {
	if op == nil {
		return
	}
	op.DesiredSource = CloneAppSource(op.DesiredSource)
	if op.DesiredOriginSource == nil && op.DesiredSource != nil {
		op.DesiredOriginSource = CloneAppSource(op.DesiredSource)
		return
	}
	op.DesiredOriginSource = CloneAppSource(op.DesiredOriginSource)
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
	ProjectID string
	AppID     string
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

func (p Principal) AllowsProject(projectID string) bool {
	if p.IsPlatformAdmin() {
		return true
	}
	projectID = strings.TrimSpace(projectID)
	if strings.TrimSpace(p.ProjectID) == "" || projectID == "" {
		return true
	}
	return p.ProjectID == projectID
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

func NormalizeNodeKeyScope(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case NodeKeyScopePlatformNode:
		return NodeKeyScopePlatformNode
	case "", NodeKeyScopeTenantRuntime:
		return NodeKeyScopeTenantRuntime
	default:
		return ""
	}
}

func NormalizeMachineScope(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case MachineScopePlatformNode:
		return MachineScopePlatformNode
	case "", MachineScopeTenantRuntime:
		return MachineScopeTenantRuntime
	default:
		return ""
	}
}

func NormalizeMachineControlPlaneRole(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case MachineControlPlaneRoleCandidate:
		return MachineControlPlaneRoleCandidate
	case MachineControlPlaneRoleMember:
		return MachineControlPlaneRoleMember
	case "", MachineControlPlaneRoleNone:
		return MachineControlPlaneRoleNone
	default:
		return ""
	}
}

type State struct {
	Version                    string                      `json:"version"`
	Tenants                    []Tenant                    `json:"tenants"`
	Projects                   []Project                   `json:"projects"`
	ProjectDeleteRequests      map[string]time.Time        `json:"project_delete_requests,omitempty"`
	ProjectRuntimeReservations []ProjectRuntimeReservation `json:"project_runtime_reservations,omitempty"`
	APIKeys                    []APIKey                    `json:"api_keys"`
	EnrollmentTokens           []EnrollmentToken           `json:"enrollment_tokens"`
	NodeKeys                   []NodeKey                   `json:"node_keys"`
	Machines                   []Machine                   `json:"machines"`
	NodeUpdaters               []NodeUpdater               `json:"node_updaters,omitempty"`
	NodeUpdateTasks            []NodeUpdateTask            `json:"node_update_tasks,omitempty"`
	Runtimes                   []Runtime                   `json:"runtimes"`
	RuntimeGrants              []RuntimeAccessGrant        `json:"runtime_grants"`
	Apps                       []App                       `json:"apps"`
	AppDomains                 []AppDomain                 `json:"app_domains"`
	EdgeRoutePolicies          []EdgeRoutePolicy           `json:"edge_route_policies,omitempty"`
	BackingServices            []BackingService            `json:"backing_services"`
	ServiceBindings            []ServiceBinding            `json:"service_bindings"`
	Operations                 []Operation                 `json:"operations"`
	AuditEvents                []AuditEvent                `json:"audit_events"`
	Idempotency                []IdempotencyRecord         `json:"idempotency"`
	TenantBilling              []TenantBilling             `json:"tenant_billing"`
	BillingEvents              []TenantBillingEvent        `json:"billing_events"`
	ResourceUsageSamples       []ResourceUsageSample       `json:"resource_usage_samples,omitempty"`
}
