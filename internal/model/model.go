package model

import (
	"path"
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

	MachineDedicatedModeNone     = "none"
	MachineDedicatedModeEdge     = "edge"
	MachineDedicatedModeDNS      = "dns"
	MachineDedicatedModeInternal = "internal"

	AppSourceTypeGitHubPublic  = "github-public"
	AppSourceTypeGitHubPrivate = "github-private"
	AppSourceTypeDockerImage   = "docker-image"
	AppSourceTypeUpload        = "upload"

	AppSourceSyncProviderGitHub = "github"

	AppSourceSyncPhaseOK        = "ok"
	AppSourceSyncPhaseDegraded  = "degraded"
	AppSourceSyncPhaseSuspended = "suspended"

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

	AppDatabaseAccessModeReadWrite = "read-write"
	AppDatabaseAccessModeReadOnly  = "read-only"

	AppDatabaseAccessGrantStatusActive  = "active"
	AppDatabaseAccessGrantStatusRevoked = "revoked"
	AppDatabaseAccessGrantStatusExpired = "expired"

	AppDatabaseImportFormatAuto   = "auto"
	AppDatabaseImportFormatSQL    = "sql"
	AppDatabaseImportFormatCustom = "custom"

	NodeKeyStatusActive  = "active"
	NodeKeyStatusRevoked = "revoked"

	NodeUpdaterStatusActive  = "active"
	NodeUpdaterStatusRevoked = "revoked"

	NodeUpdaterCurrentVersion = "v17"

	NodeUpdateTaskTypeRefreshJoinConfig   = "refresh-join-config"
	NodeUpdateTaskTypeUpgradeK3SAgent     = "upgrade-k3s-agent"
	NodeUpdateTaskTypeUpgradeUpdater      = "upgrade-node-updater"
	NodeUpdateTaskTypeRestartK3SAgent     = "restart-k3s-agent"
	NodeUpdateTaskTypeDiagnoseNode        = "diagnose-node"
	NodeUpdateTaskTypeInstallNFSClient    = "install-nfs-client-tools"
	NodeUpdateTaskTypePrepullSystemImages = "prepull-system-images"
	NodeUpdateTaskTypePrepullAppImages    = "prepull-app-images"
	NodeUpdateTaskTypeReplicateAppImage   = "replicate-app-image"
	NodeUpdateTaskTypeVerifyImageCache    = "verify-image-cache"
	NodeUpdateTaskTypePruneImageCache     = "prune-image-cache"
	NodeUpdateTaskTypeReportImageCache    = "report-image-cache-inventory"
	NodeUpdateTaskTypeReportLocalPV       = "report-lvm-localpv-inventory"
	NodeUpdateTaskTypeDecommissionLocalPV = "decommission-lvm-localpv"
	NodeUpdateTaskTypeVerifySystemdEscape = "verify-systemd-escape-hatch"

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
	OperationTypeDataPrewarm        = "data-prewarm"
	OperationTypeDelete             = "delete"

	OperationStatusPending      = "pending"
	OperationStatusRunning      = "running"
	OperationStatusWaitingAgent = "waiting-agent"
	OperationStatusCompleted    = "completed"
	OperationStatusFailed       = "failed"

	ImageLocationStatusPresent = "present"
	ImageLocationStatusMissing = "missing"
	ImageLocationStatusPulling = "pulling"
	ImageLocationStatusFailed  = "failed"

	ImageLifecycleImporting = "importing"
	ImageLifecycleAvailable = "available"
	ImageLifecycleDeleting  = "deleting"
	ImageLifecycleDeleted   = "deleted"
	ImageLifecycleLost      = "lost"

	ImageReplicaStatusPlanned   = "planned"
	ImageReplicaStatusCopying   = "copying"
	ImageReplicaStatusVerifying = "verifying"
	ImageReplicaStatusPresent   = "present"
	ImageReplicaStatusStale     = "stale"
	ImageReplicaStatusDraining  = "draining"
	ImageReplicaStatusDeleting  = "deleting"
	ImageReplicaStatusMissing   = "missing"
	ImageReplicaStatusFailed    = "failed"

	ImagePinReasonCurrentDeploy   = "current_deploy"
	ImagePinReasonRollbackWindow  = "rollback_window"
	ImagePinReasonImportResult    = "import_result"
	ImagePinReasonUserPin         = "user_pin"
	ImagePinReasonRetention       = "retention"
	ImagePinReasonReplicationSeed = "replication_seed"

	ImageReplicationTaskStatusPending   = "pending"
	ImageReplicationTaskStatusRunning   = "running"
	ImageReplicationTaskStatusCompleted = "completed"
	ImageReplicationTaskStatusFailed    = "failed"
	ImageReplicationTaskStatusCanceled  = "canceled"

	ImageReplicationPriorityDeployBlocking = "deploy_blocking"
	ImageReplicationPriorityRepair         = "repair"
	ImageReplicationPriorityWarmup         = "warmup"
	ImageReplicationPriorityRebalance      = "rebalance"

	ImageCachePruneModeObserve = "observe"
	ImageCachePruneModeDryRun  = "dry-run"
	ImageCachePruneModeDelete  = "delete"

	ImageCachePrunePlanStatusPlanned   = "planned"
	ImageCachePrunePlanStatusScheduled = "scheduled"
	ImageCachePrunePlanStatusCompleted = "completed"
	ImageCachePrunePlanStatusFailed    = "failed"

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
	OperationRequestedByImageTracking        = "fugue-controller/image-tracking"
	OperationRequestedByAutoFailover         = "fugue-controller/auto-failover"
	OperationRequestedByImageRebuild         = "fugue-controller/image-rebuild"
	OperationRequestedByOOMRightSizing       = "fugue-api/oom-right-sizing"
	OperationRequestedByRightSizing          = "fugue-api/right-sizing"
	OperationRequestedByRightSizingDownscale = "fugue-api/right-sizing/downscale"

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
	AppRolloutIntentOnlineLifecycleUpdate    = "online_lifecycle_update"
	AppRolloutIntentOnlineImageUpdate        = "online_image_update"
	AppRolloutIntentOnlineRestart            = "online_restart"
	AppRolloutIntentOnlineResourceUpdate     = "online_resource_update"
	AppRolloutIntentOnlineConfigUpdate       = "online_config_update"
	AppVolumeReplicationModeDisabled         = "disabled"
	AppVolumeReplicationModeManual           = "manual"
	AppVolumeReplicationModeScheduled        = "scheduled"
	DefaultAppVolumeReplicationSchedule      = "*/5 * * * *"
	DefaultAppImageMirrorLimit               = 1
	MaxAppTerminationGracePeriodSeconds      = 24 * 60 * 60

	AppReleaseRoleStable    = "stable"
	AppReleaseRoleCandidate = "candidate"
	AppReleaseRolePrevious  = "previous"
	AppReleaseRoleRetired   = "retired"

	AppReleaseStatusCreating = "creating"
	AppReleaseStatusReady    = "ready"
	AppReleaseStatusServing  = "serving"
	AppReleaseStatusFailed   = "failed"
	AppReleaseStatusRetired  = "retired"

	AppTrafficModeSingle   = "single"
	AppTrafficModeCanary   = "canary"
	AppTrafficModeWeighted = "weighted"
	AppTrafficModePaused   = "paused"

	AppReleaseGateStatusPass = "pass"
	AppReleaseGateStatusWarn = "warn"
	AppReleaseGateStatusFail = "fail"

	AppReleaseProbeKindHTTP       = "http"
	AppReleaseProbeKindHTTPStream = "http_stream"
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
	spec.SSH = NormalizeAppSSHSpec(spec.SSH)
	spec.GeneratedEnv = NormalizeAppGeneratedEnvSpecs(spec.GeneratedEnv)
	spec.WorkloadClass = NormalizeWorkloadClass(spec.WorkloadClass)
	rightSizing := AppRightSizingSpec{Mode: AppRightSizingModeAuto}
	if spec.RightSizing != nil {
		rightSizing = *spec.RightSizing
	}
	normalizedRightSizing := NormalizeAppRightSizingSpec(rightSizing)
	spec.RightSizing = &normalizedRightSizing
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
	ID                string                `json:"id"`
	TenantID          string                `json:"tenant_id,omitempty"`
	Name              string                `json:"name"`
	MachineName       string                `json:"machine_name,omitempty"`
	Type              string                `json:"type"`
	AccessMode        string                `json:"access_mode,omitempty"`
	PublicOffer       *RuntimePublicOffer   `json:"public_offer,omitempty"`
	PoolMode          string                `json:"pool_mode,omitempty"`
	ConnectionMode    string                `json:"connection_mode,omitempty"`
	Status            string                `json:"status"`
	Endpoint          string                `json:"endpoint,omitempty"`
	Labels            map[string]string     `json:"labels,omitempty"`
	DataCache         *RuntimeDataCacheSpec `json:"data_cache,omitempty"`
	NodeKeyID         string                `json:"node_key_id,omitempty"`
	ClusterNodeName   string                `json:"cluster_node_name,omitempty"`
	FingerprintPrefix string                `json:"fingerprint_prefix,omitempty"`
	FingerprintHash   string                `json:"fingerprint_hash,omitempty"`
	AgentKeyPrefix    string                `json:"agent_key_prefix,omitempty"`
	AgentKeyHash      string                `json:"agent_key_hash,omitempty"`
	LastSeenAt        *time.Time            `json:"last_seen_at,omitempty"`
	LastHeartbeatAt   *time.Time            `json:"last_heartbeat_at,omitempty"`
	CreatedAt         time.Time             `json:"created_at"`
	UpdatedAt         time.Time             `json:"updated_at"`
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

type RuntimeDataCacheSpec struct {
	Enabled          bool              `json:"enabled,omitempty"`
	RootPath         string            `json:"root_path,omitempty"`
	MaxBytes         int64             `json:"max_bytes,omitempty"`
	ReserveFreeBytes int64             `json:"reserve_free_bytes,omitempty"`
	LocalityHints    map[string]string `json:"locality_hints,omitempty"`
}

type RuntimeDataCacheMetadata struct {
	ID                   string    `json:"id"`
	TenantID             string    `json:"tenant_id,omitempty"`
	RuntimeID            string    `json:"runtime_id"`
	WorkspaceID          string    `json:"workspace_id"`
	SnapshotID           string    `json:"snapshot_id,omitempty"`
	BackendID            string    `json:"backend_id,omitempty"`
	LocalPath            string    `json:"local_path,omitempty"`
	Bytes                int64     `json:"bytes,omitempty"`
	RequiredFreeBytes    int64     `json:"required_free_bytes,omitempty"`
	AvailableBytes       int64     `json:"available_bytes,omitempty"`
	LocalityHint         string    `json:"locality_hint,omitempty"`
	EstimatedEgressBytes int64     `json:"estimated_egress_bytes,omitempty"`
	Status               string    `json:"status"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
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
	AllowAppRuntime          bool   `json:"allow_app_runtime"`
	AllowBuilds              bool   `json:"allow_builds"`
	AllowSharedPool          bool   `json:"allow_shared_pool"`
	AllowEdge                bool   `json:"allow_edge"`
	AllowDNS                 bool   `json:"allow_dns"`
	AllowInternalMaintenance bool   `json:"allow_internal_maintenance"`
	DesiredControlPlaneRole  string `json:"desired_control_plane_role,omitempty"`
}

type NodeUpdater struct {
	ID                  string            `json:"id"`
	TenantID            string            `json:"tenant_id,omitempty"`
	NodeKeyID           string            `json:"node_key_id,omitempty"`
	MachineID           string            `json:"machine_id,omitempty"`
	RuntimeID           string            `json:"runtime_id,omitempty"`
	ClusterNodeName     string            `json:"cluster_node_name,omitempty"`
	Status              string            `json:"status"`
	TokenPrefix         string            `json:"token_prefix,omitempty"`
	TokenHash           string            `json:"token_hash,omitempty"`
	Labels              map[string]string `json:"labels,omitempty"`
	Capabilities        []string          `json:"capabilities,omitempty"`
	UpdaterVersion      string            `json:"updater_version,omitempty"`
	JoinScriptVersion   string            `json:"join_script_version,omitempty"`
	K3SVersion          string            `json:"k3s_version,omitempty"`
	K3SServer           string            `json:"k3s_server,omitempty"`
	K3SFallbackServers  string            `json:"k3s_fallback_servers,omitempty"`
	RegistryMirror      string            `json:"registry_mirror,omitempty"`
	LabelsHash          string            `json:"labels_hash,omitempty"`
	TaintsHash          string            `json:"taints_hash,omitempty"`
	EdgeEnvGeneration   string            `json:"edge_env_generation,omitempty"`
	DNSEnvGeneration    string            `json:"dns_env_generation,omitempty"`
	ConfigHash          string            `json:"config_hash,omitempty"`
	DiscoveryGeneration string            `json:"discovery_generation,omitempty"`
	OS                  string            `json:"os,omitempty"`
	Arch                string            `json:"arch,omitempty"`
	LastError           string            `json:"last_error,omitempty"`
	LastSeenAt          *time.Time        `json:"last_seen_at,omitempty"`
	LastHeartbeatAt     *time.Time        `json:"last_heartbeat_at,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
}

type NodeUpdaterDesiredState struct {
	GeneratedAt     time.Time                  `json:"generated_at"`
	NodeUpdater     NodeUpdater                `json:"node_updater"`
	DiscoveryBundle DiscoveryBundle            `json:"discovery_bundle"`
	NodePolicy      *ClusterNodePolicyStatus   `json:"node_policy,omitempty"`
	EdgeCredential  *NodeUpdaterEdgeCredential `json:"edge_credential,omitempty"`
	Warnings        []string                   `json:"warnings,omitempty"`
}

type NodeUpdaterEdgeCredential struct {
	EdgeID          string `json:"edge_id"`
	EdgeGroupID     string `json:"edge_group_id"`
	WorkloadMode    string `json:"workload_mode,omitempty"`
	Country         string `json:"country,omitempty"`
	Region          string `json:"region,omitempty"`
	PublicIPv4      string `json:"public_ipv4,omitempty"`
	PublicIPv6      string `json:"public_ipv6,omitempty"`
	Token           string `json:"token,omitempty"`
	TokenPrefix     string `json:"token_prefix,omitempty"`
	DesiredStateURL string `json:"desired_state_url,omitempty"`
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
	ImageFilesystem  *ClusterNodeStorageStats        `json:"image_filesystem,omitempty"`
	RuntimeID        string                          `json:"runtime_id,omitempty"`
	TenantID         string                          `json:"tenant_id,omitempty"`
	Machine          *ClusterNodeMachine             `json:"machine,omitempty"`
	Policy           *ClusterNodePolicy              `json:"policy,omitempty"`
	Workloads        []ClusterNodeWorkload           `json:"workloads,omitempty"`
	CreatedAt        *time.Time                      `json:"created_at,omitempty"`
}

type ClusterNodeTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect,omitempty"`
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
	AllowAppRuntime              bool   `json:"allow_app_runtime"`
	AllowBuilds                  bool   `json:"allow_builds"`
	AllowSharedPool              bool   `json:"allow_shared_pool"`
	AllowEdge                    bool   `json:"allow_edge"`
	AllowDNS                     bool   `json:"allow_dns"`
	AllowInternalMaintenance     bool   `json:"allow_internal_maintenance"`
	DedicatedMode                string `json:"dedicated_mode"`
	NodeMode                     string `json:"node_mode,omitempty"`
	NodeHealth                   string `json:"node_health,omitempty"`
	DesiredControlPlaneRole      string `json:"desired_control_plane_role,omitempty"`
	EffectiveAppRuntime          bool   `json:"effective_app_runtime"`
	EffectiveBuilds              bool   `json:"effective_builds"`
	EffectiveSharedPool          bool   `json:"effective_shared_pool"`
	EffectiveEdge                bool   `json:"effective_edge"`
	EffectiveDNS                 bool   `json:"effective_dns"`
	EffectiveInternalMaintenance bool   `json:"effective_internal_maintenance"`
	EffectiveDedicatedMode       string `json:"effective_dedicated_mode"`
	EffectiveSchedulable         bool   `json:"effective_schedulable"`
	EffectiveControlPlaneRole    string `json:"effective_control_plane_role,omitempty"`
}

type ClusterNodePolicyStatus struct {
	NodeName            string                          `json:"node_name"`
	RuntimeID           string                          `json:"runtime_id,omitempty"`
	TenantID            string                          `json:"tenant_id,omitempty"`
	MachineID           string                          `json:"machine_id,omitempty"`
	Policy              *ClusterNodePolicy              `json:"policy,omitempty"`
	Labels              map[string]string               `json:"labels,omitempty"`
	Taints              []ClusterNodeTaint              `json:"taints,omitempty"`
	Conditions          map[string]ClusterNodeCondition `json:"conditions,omitempty"`
	Ready               bool                            `json:"ready"`
	DiskPressure        bool                            `json:"disk_pressure"`
	FilesystemPressure  bool                            `json:"filesystem_pressure"`
	FilesystemUsage     *float64                        `json:"filesystem_usage_percent,omitempty"`
	FilesystemReason    string                          `json:"filesystem_pressure_reason,omitempty"`
	NodeSchedulable     bool                            `json:"node_schedulable"`
	Reconciled          bool                            `json:"reconciled"`
	ReconcileReasons    []string                        `json:"reconcile_reasons,omitempty"`
	ReconcileError      string                          `json:"reconcile_error,omitempty"`
	BlockRollout        bool                            `json:"block_rollout"`
	GateReason          string                          `json:"gate_reason,omitempty"`
	SuggestedFixCommand string                          `json:"suggested_fix_command,omitempty"`
}

type ClusterNodePolicyStatusSummary struct {
	Total              int `json:"total"`
	Reconciled         int `json:"reconciled"`
	Drifted            int `json:"drifted"`
	Ready              int `json:"ready"`
	DiskPressure       int `json:"disk_pressure"`
	FilesystemPressure int `json:"filesystem_pressure"`
	BlockedByHealth    int `json:"blocked_by_health"`
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

type ControlPlaneWarning struct {
	Severity  string `json:"severity"`
	Namespace string `json:"namespace,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Name      string `json:"name"`
	Status    string `json:"status,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Message   string `json:"message,omitempty"`
}

type ControlPlaneStatus struct {
	Namespace       string                   `json:"namespace"`
	ReleaseInstance string                   `json:"release_instance"`
	Version         string                   `json:"version"`
	LiveVersion     string                   `json:"live_version,omitempty"`
	Status          string                   `json:"status"`
	ObservedAt      time.Time                `json:"observed_at"`
	Components      []ControlPlaneComponent  `json:"components"`
	Warnings        []ControlPlaneWarning    `json:"warnings,omitempty"`
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
	ImageID      string `json:"image_id,omitempty"`
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
	Hostname       string `json:"hostname,omitempty"`
	PathPrefix     string `json:"path_prefix,omitempty"`
	BaseDomain     string `json:"base_domain,omitempty"`
	PublicURL      string `json:"public_url,omitempty"`
	ServicePort    int    `json:"service_port,omitempty"`
	DomainName     string `json:"domain_name,omitempty"`
	EntrypointName string `json:"entrypoint_name,omitempty"`
}

type ProjectRouteDomain struct {
	Name         string `json:"name,omitempty"`
	Hostname     string `json:"hostname,omitempty"`
	Host         string `json:"host,omitempty"`
	TLS          string `json:"tls,omitempty"`
	OwnerService string `json:"owner_service,omitempty"`
	OwnerAppID   string `json:"owner_app_id,omitempty"`
}

type ProjectRouteEntrypointRoute struct {
	Path        string `json:"path,omitempty"`
	PathPrefix  string `json:"path_prefix,omitempty"`
	Service     string `json:"service,omitempty"`
	AppID       string `json:"app_id,omitempty"`
	StripPrefix bool   `json:"strip_prefix,omitempty"`
	Rewrite     string `json:"rewrite,omitempty"`
}

type ProjectRouteEntrypoint struct {
	Name   string                        `json:"name,omitempty"`
	Domain string                        `json:"domain,omitempty"`
	Routes []ProjectRouteEntrypointRoute `json:"routes,omitempty"`
}

type ProjectRouteBinding struct {
	Hostname       string `json:"hostname,omitempty"`
	PathPrefix     string `json:"path_prefix,omitempty"`
	PublicURL      string `json:"public_url,omitempty"`
	DomainName     string `json:"domain_name,omitempty"`
	EntrypointName string `json:"entrypoint_name,omitempty"`
	Service        string `json:"service,omitempty"`
	AppID          string `json:"app_id,omitempty"`
	AppName        string `json:"app_name,omitempty"`
	ServicePort    int    `json:"service_port,omitempty"`
	TLS            string `json:"tls,omitempty"`
	StripPrefix    bool   `json:"strip_prefix,omitempty"`
	Rewrite        string `json:"rewrite,omitempty"`
}

type ProjectRouteTable struct {
	TenantID    string                   `json:"tenant_id,omitempty"`
	ProjectID   string                   `json:"project_id,omitempty"`
	Domains     []ProjectRouteDomain     `json:"domains,omitempty"`
	Entrypoints []ProjectRouteEntrypoint `json:"entrypoints,omitempty"`
	Bindings    []ProjectRouteBinding    `json:"bindings,omitempty"`
	Legacy      bool                     `json:"legacy,omitempty"`
	CreatedAt   time.Time                `json:"created_at,omitempty"`
	UpdatedAt   time.Time                `json:"updated_at,omitempty"`
}

func NormalizeProjectRouteDomain(domain ProjectRouteDomain) ProjectRouteDomain {
	domain.Name = strings.TrimSpace(domain.Name)
	domain.Hostname = strings.TrimSpace(strings.ToLower(firstNonEmptyString(domain.Hostname, domain.Host)))
	domain.Host = domain.Hostname
	domain.TLS = strings.TrimSpace(strings.ToLower(domain.TLS))
	domain.OwnerService = strings.TrimSpace(domain.OwnerService)
	domain.OwnerAppID = strings.TrimSpace(domain.OwnerAppID)
	return domain
}

func NormalizeProjectRouteEntrypoint(entrypoint ProjectRouteEntrypoint) ProjectRouteEntrypoint {
	entrypoint.Name = strings.TrimSpace(entrypoint.Name)
	entrypoint.Domain = strings.TrimSpace(entrypoint.Domain)
	routes := make([]ProjectRouteEntrypointRoute, 0, len(entrypoint.Routes))
	for _, route := range entrypoint.Routes {
		route.Service = strings.TrimSpace(route.Service)
		route.AppID = strings.TrimSpace(route.AppID)
		route.PathPrefix = NormalizeAppRoutePathPrefix(firstNonEmptyString(route.PathPrefix, route.Path))
		route.Path = route.PathPrefix
		route.Rewrite = strings.TrimSpace(route.Rewrite)
		if route.Service == "" && route.AppID == "" {
			continue
		}
		routes = append(routes, route)
	}
	entrypoint.Routes = routes
	return entrypoint
}

func NormalizeProjectRouteTable(table ProjectRouteTable) ProjectRouteTable {
	table.TenantID = strings.TrimSpace(table.TenantID)
	table.ProjectID = strings.TrimSpace(table.ProjectID)
	domains := make([]ProjectRouteDomain, 0, len(table.Domains))
	for _, domain := range table.Domains {
		normalized := NormalizeProjectRouteDomain(domain)
		if normalized.Name == "" && normalized.Hostname == "" {
			continue
		}
		domains = append(domains, normalized)
	}
	entrypoints := make([]ProjectRouteEntrypoint, 0, len(table.Entrypoints))
	for _, entrypoint := range table.Entrypoints {
		normalized := NormalizeProjectRouteEntrypoint(entrypoint)
		if normalized.Name == "" && normalized.Domain == "" && len(normalized.Routes) == 0 {
			continue
		}
		entrypoints = append(entrypoints, normalized)
	}
	table.Domains = domains
	table.Entrypoints = entrypoints
	table.Bindings = nil
	return table
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func NormalizeAppRoutePathPrefix(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "/"
	}
	if idx := strings.IndexAny(raw, "?#"); idx >= 0 {
		raw = raw[:idx]
	}
	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}
	if strings.HasSuffix(raw, "/*") {
		raw = strings.TrimSuffix(raw, "/*")
		if raw == "" {
			return "/"
		}
	}
	cleaned := path.Clean(raw)
	if cleaned == "." || cleaned == "" {
		return "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

func NormalizeAppRoute(route AppRoute) AppRoute {
	route.Hostname = strings.TrimSpace(strings.ToLower(route.Hostname))
	route.PathPrefix = NormalizeAppRoutePathPrefix(route.PathPrefix)
	route.BaseDomain = strings.TrimSpace(strings.ToLower(route.BaseDomain))
	route.PublicURL = strings.TrimSpace(route.PublicURL)
	route.DomainName = strings.TrimSpace(route.DomainName)
	route.EntrypointName = strings.TrimSpace(route.EntrypointName)
	return route
}

func AppRoutePublicURL(hostname, pathPrefix string) string {
	hostname = strings.TrimSpace(strings.ToLower(hostname))
	if hostname == "" {
		return ""
	}
	pathPrefix = NormalizeAppRoutePathPrefix(pathPrefix)
	if pathPrefix == "/" {
		return "https://" + hostname
	}
	return "https://" + hostname + pathPrefix
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
	AppDomainDNSStatusPending = "pending"
	AppDomainDNSStatusReady   = "ready"
	AppDomainDNSStatusError   = "error"
)

const (
	AppDomainDNSRecordKindNone      = "none"
	AppDomainDNSRecordKindCNAME     = "cname"
	AppDomainDNSRecordKindFlattened = "flattened"
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

func NormalizeAppDomainDNSStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case AppDomainDNSStatusPending:
		return AppDomainDNSStatusPending
	case AppDomainDNSStatusReady:
		return AppDomainDNSStatusReady
	case AppDomainDNSStatusError:
		return AppDomainDNSStatusError
	default:
		return ""
	}
}

func NormalizeAppDomainDNSRecordKind(kind string) string {
	switch strings.TrimSpace(strings.ToLower(kind)) {
	case AppDomainDNSRecordKindCNAME:
		return AppDomainDNSRecordKindCNAME
	case AppDomainDNSRecordKindFlattened:
		return AppDomainDNSRecordKindFlattened
	case AppDomainDNSRecordKindNone:
		return AppDomainDNSRecordKindNone
	default:
		return ""
	}
}

type AppDomain struct {
	Hostname             string     `json:"hostname"`
	AppID                string     `json:"app_id,omitempty"`
	TenantID             string     `json:"tenant_id,omitempty"`
	Status               string     `json:"status"`
	DNSStatus            string     `json:"dns_status,omitempty"`
	DNSRecordKind        string     `json:"dns_record_kind,omitempty"`
	TLSStatus            string     `json:"tls_status,omitempty"`
	VerificationTXTName  string     `json:"verification_txt_name,omitempty"`
	VerificationTXTValue string     `json:"verification_txt_value,omitempty"`
	RouteTarget          string     `json:"route_target,omitempty"`
	LastMessage          string     `json:"last_message,omitempty"`
	DNSLastMessage       string     `json:"dns_last_message,omitempty"`
	TLSLastMessage       string     `json:"tls_last_message,omitempty"`
	LastCheckedAt        *time.Time `json:"last_checked_at,omitempty"`
	DNSLastCheckedAt     *time.Time `json:"dns_last_checked_at,omitempty"`
	VerifiedAt           *time.Time `json:"verified_at,omitempty"`
	TLSLastCheckedAt     *time.Time `json:"tls_last_checked_at,omitempty"`
	TLSReadyAt           *time.Time `json:"tls_ready_at,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

type EdgeTLSCertificate struct {
	Hostname              string     `json:"hostname"`
	TenantID              string     `json:"tenant_id,omitempty"`
	AppID                 string     `json:"app_id,omitempty"`
	CertificatePEM        string     `json:"certificate_pem,omitempty"`
	PrivateKeyPEM         string     `json:"private_key_pem,omitempty"`
	MetadataJSON          string     `json:"metadata_json,omitempty"`
	IssuerStorage         string     `json:"issuer_storage,omitempty"`
	CertificateSHA256     string     `json:"certificate_sha256,omitempty"`
	NotAfter              *time.Time `json:"not_after,omitempty"`
	UploadedByEdgeID      string     `json:"uploaded_by_edge_id,omitempty"`
	UploadedByEdgeGroupID string     `json:"uploaded_by_edge_group_id,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

type AppSpec struct {
	Image                         string                         `json:"image"`
	Command                       []string                       `json:"command,omitempty"`
	Args                          []string                       `json:"args,omitempty"`
	Env                           map[string]string              `json:"env,omitempty"`
	GeneratedEnv                  map[string]AppGeneratedEnvSpec `json:"generated_env,omitempty"`
	SSH                           *AppSSHSpec                    `json:"ssh,omitempty"`
	NetworkMode                   string                         `json:"network_mode,omitempty"`
	NetworkPolicy                 *AppNetworkPolicySpec          `json:"network_policy,omitempty"`
	WorkloadClass                 string                         `json:"workload_class,omitempty"`
	Ports                         []int                          `json:"ports,omitempty"`
	Replicas                      int                            `json:"replicas"`
	Resources                     *ResourceSpec                  `json:"resources,omitempty"`
	RightSizing                   *AppRightSizingSpec            `json:"right_sizing,omitempty"`
	RuntimeID                     string                         `json:"runtime_id"`
	Files                         []AppFile                      `json:"files,omitempty"`
	Workspace                     *AppWorkspaceSpec              `json:"workspace,omitempty"`
	Data                          *AppDataMaterializationSpec    `json:"data,omitempty"`
	PersistentStorage             *AppPersistentStorageSpec      `json:"persistent_storage,omitempty"`
	VolumeReplication             *AppVolumeReplicationSpec      `json:"volume_replication,omitempty"`
	Postgres                      *AppPostgresSpec               `json:"postgres,omitempty"`
	Failover                      *AppFailoverSpec               `json:"failover,omitempty"`
	ImageMirrorLimit              int                            `json:"image_mirror_limit,omitempty"`
	TerminationGracePeriodSeconds int64                          `json:"termination_grace_period_seconds,omitempty"`
	RestartToken                  string                         `json:"restart_token,omitempty"`
	RolloutIntent                 string                         `json:"-"`
}

type AppNetworkPolicySpec struct {
	Egress  *AppNetworkPolicyDirectionSpec `json:"egress,omitempty" yaml:"egress,omitempty"`
	Ingress *AppNetworkPolicyDirectionSpec `json:"ingress,omitempty" yaml:"ingress,omitempty"`
}

type AppDataMaterializationSpec struct {
	Workspaces           []AppDataWorkspaceMaterialization `json:"workspaces,omitempty" yaml:"workspaces,omitempty"`
	Prewarm              bool                              `json:"prewarm,omitempty" yaml:"prewarm,omitempty"`
	FailOnMissing        bool                              `json:"fail_on_missing,omitempty" yaml:"fail_on_missing,omitempty"`
	LocalityHint         string                            `json:"locality_hint,omitempty" yaml:"locality_hint,omitempty"`
	RequiredFreeBytes    int64                             `json:"required_free_bytes,omitempty" yaml:"required_free_bytes,omitempty"`
	MaxEgressBytes       int64                             `json:"max_egress_bytes,omitempty" yaml:"max_egress_bytes,omitempty"`
	EstimatedEgressBytes int64                             `json:"estimated_egress_bytes,omitempty" yaml:"estimated_egress_bytes,omitempty"`
	EgressEstimate       *DataEgressEstimate               `json:"egress_estimate,omitempty" yaml:"egress_estimate,omitempty"`
}

type AppDataWorkspaceMaterialization struct {
	WorkspaceID          string   `json:"workspace_id,omitempty" yaml:"workspace_id,omitempty"`
	Workspace            string   `json:"workspace,omitempty" yaml:"workspace,omitempty"`
	Version              string   `json:"version,omitempty" yaml:"version,omitempty"`
	Assets               []string `json:"assets,omitempty" yaml:"assets,omitempty"`
	TargetPath           string   `json:"target_path,omitempty" yaml:"target_path,omitempty"`
	Mode                 string   `json:"mode,omitempty" yaml:"mode,omitempty"`
	Required             bool     `json:"required,omitempty" yaml:"required,omitempty"`
	LocalityHint         string   `json:"locality_hint,omitempty" yaml:"locality_hint,omitempty"`
	EstimatedEgressBytes int64    `json:"estimated_egress_bytes,omitempty" yaml:"estimated_egress_bytes,omitempty"`
}

type DataEgressEstimate struct {
	SourceRegion          string `json:"source_region,omitempty" yaml:"source_region,omitempty"`
	TargetRegion          string `json:"target_region,omitempty" yaml:"target_region,omitempty"`
	Bytes                 int64  `json:"bytes,omitempty" yaml:"bytes,omitempty"`
	CrossRegion           bool   `json:"cross_region,omitempty" yaml:"cross_region,omitempty"`
	EstimatedMicroCents   int64  `json:"estimated_micro_cents,omitempty" yaml:"estimated_micro_cents,omitempty"`
	ProviderCostAvailable bool   `json:"provider_cost_available,omitempty" yaml:"provider_cost_available,omitempty"`
}

type DataMaterializationPlan struct {
	Workspaces           []AppDataWorkspaceMaterialization `json:"workspaces,omitempty" yaml:"workspaces,omitempty"`
	RequiredBytes        int64                             `json:"required_bytes,omitempty" yaml:"required_bytes,omitempty"`
	RequiredFreeBytes    int64                             `json:"required_free_bytes,omitempty" yaml:"required_free_bytes,omitempty"`
	AvailableBytes       int64                             `json:"available_bytes,omitempty" yaml:"available_bytes,omitempty"`
	DiskOK               bool                              `json:"disk_ok" yaml:"disk_ok"`
	LocalityHint         string                            `json:"locality_hint,omitempty" yaml:"locality_hint,omitempty"`
	EstimatedEgressBytes int64                             `json:"estimated_egress_bytes,omitempty" yaml:"estimated_egress_bytes,omitempty"`
	EgressEstimate       *DataEgressEstimate               `json:"egress_estimate,omitempty" yaml:"egress_estimate,omitempty"`
}

func PlanAppDataMaterialization(spec AppDataMaterializationSpec, availableBytes int64) DataMaterializationPlan {
	var requiredBytes int64
	for _, workspace := range spec.Workspaces {
		if workspace.EstimatedEgressBytes > 0 {
			requiredBytes += workspace.EstimatedEgressBytes
		}
	}
	if requiredBytes == 0 && spec.EstimatedEgressBytes > 0 {
		requiredBytes = spec.EstimatedEgressBytes
	}
	requiredFreeBytes := spec.RequiredFreeBytes
	if requiredFreeBytes == 0 {
		requiredFreeBytes = requiredBytes
	}
	estimatedEgressBytes := spec.EstimatedEgressBytes
	if estimatedEgressBytes == 0 {
		estimatedEgressBytes = requiredBytes
	}
	plan := DataMaterializationPlan{
		Workspaces:           append([]AppDataWorkspaceMaterialization(nil), spec.Workspaces...),
		RequiredBytes:        requiredBytes,
		RequiredFreeBytes:    requiredFreeBytes,
		AvailableBytes:       availableBytes,
		DiskOK:               requiredFreeBytes <= 0 || availableBytes >= requiredFreeBytes,
		LocalityHint:         strings.TrimSpace(spec.LocalityHint),
		EstimatedEgressBytes: estimatedEgressBytes,
		EgressEstimate:       spec.EgressEstimate,
	}
	return plan
}

type AppNetworkPolicyDirectionSpec struct {
	Mode                 string                    `json:"mode,omitempty" yaml:"mode,omitempty"`
	AllowDNS             bool                      `json:"allow_dns,omitempty" yaml:"allow_dns,omitempty"`
	AllowPublicInternet  bool                      `json:"allow_public_internet,omitempty" yaml:"allow_public_internet,omitempty"`
	AllowBackingServices bool                      `json:"allow_backing_services,omitempty" yaml:"allow_backing_services,omitempty"`
	AllowApps            []AppNetworkPolicyAppPeer `json:"allow_apps,omitempty" yaml:"allow_apps,omitempty"`
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

type AppSourceSyncStatus struct {
	Provider            string     `json:"provider,omitempty"`
	Phase               string     `json:"phase,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures,omitempty"`
	LastCheckedAt       *time.Time `json:"last_checked_at,omitempty"`
	LastSuccessAt       *time.Time `json:"last_success_at,omitempty"`
	LastErrorAt         *time.Time `json:"last_error_at,omitempty"`
	LastErrorCode       string     `json:"last_error_code,omitempty"`
	LastErrorMessage    string     `json:"last_error_message,omitempty"`
	NextCheckAt         *time.Time `json:"next_check_at,omitempty"`
	SuspendedAt         *time.Time `json:"suspended_at,omitempty"`
	NeedsUserAction     bool       `json:"needs_user_action,omitempty"`
}

func CloneAppSourceSyncStatus(in *AppSourceSyncStatus) *AppSourceSyncStatus {
	if in == nil {
		return nil
	}
	out := *in
	if in.LastCheckedAt != nil {
		value := *in.LastCheckedAt
		out.LastCheckedAt = &value
	}
	if in.LastSuccessAt != nil {
		value := *in.LastSuccessAt
		out.LastSuccessAt = &value
	}
	if in.LastErrorAt != nil {
		value := *in.LastErrorAt
		out.LastErrorAt = &value
	}
	if in.NextCheckAt != nil {
		value := *in.NextCheckAt
		out.NextCheckAt = &value
	}
	if in.SuspendedAt != nil {
		value := *in.SuspendedAt
		out.SuspendedAt = &value
	}
	return &out
}

type AppStatus struct {
	Phase                   string               `json:"phase"`
	CurrentRuntimeID        string               `json:"current_runtime_id,omitempty"`
	CurrentReplicas         int                  `json:"current_replicas"`
	CurrentReleaseStartedAt *time.Time           `json:"current_release_started_at,omitempty"`
	CurrentReleaseReadyAt   *time.Time           `json:"current_release_ready_at,omitempty"`
	LastOperationID         string               `json:"last_operation_id,omitempty"`
	LastMessage             string               `json:"last_message,omitempty"`
	UpdatedAt               time.Time            `json:"updated_at"`
	SourceSync              *AppSourceSyncStatus `json:"source_sync,omitempty"`
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

type AppRelease struct {
	ID               string     `json:"id"`
	TenantID         string     `json:"tenant_id"`
	AppID            string     `json:"app_id"`
	Role             string     `json:"role"`
	SourceRef        string     `json:"source_ref,omitempty"`
	ResolvedImageRef string     `json:"resolved_image_ref,omitempty"`
	UpstreamURL      string     `json:"upstream_url,omitempty"`
	RuntimeID        string     `json:"runtime_id,omitempty"`
	DeploymentName   string     `json:"deployment_name,omitempty"`
	ServiceName      string     `json:"service_name,omitempty"`
	Status           string     `json:"status"`
	StatusReason     string     `json:"status_reason,omitempty"`
	SpecSnapshot     *AppSpec   `json:"spec_snapshot,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	ReadyAt          *time.Time `json:"ready_at,omitempty"`
	PromotedAt       *time.Time `json:"promoted_at,omitempty"`
	RetiredAt        *time.Time `json:"retired_at,omitempty"`
}

type AppReleaseFilter struct {
	TenantID       string
	AppID          string
	Role           string
	IncludeRetired bool
	PlatformAdmin  bool
}

type AppTrafficPolicy struct {
	ID                 string    `json:"id"`
	TenantID           string    `json:"tenant_id"`
	AppID              string    `json:"app_id"`
	Mode               string    `json:"mode"`
	StableReleaseID    string    `json:"stable_release_id,omitempty"`
	CandidateReleaseID string    `json:"candidate_release_id,omitempty"`
	StableWeight       int       `json:"stable_weight"`
	CandidateWeight    int       `json:"candidate_weight"`
	StickyHeader       string    `json:"sticky_header,omitempty"`
	StickyCookie       string    `json:"sticky_cookie,omitempty"`
	UpdatedByType      string    `json:"updated_by_type,omitempty"`
	UpdatedByID        string    `json:"updated_by_id,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type AppReleaseGatePolicy struct {
	WindowSeconds              int               `json:"window_seconds,omitempty"`
	MinCandidateRequests       int               `json:"min_candidate_requests,omitempty"`
	Max5xxRate                 float64           `json:"max_5xx_rate,omitempty"`
	MaxEdgeUpstreamErrorRate   float64           `json:"max_edge_upstream_error_rate,omitempty"`
	MaxP95TTFBMilliseconds     int               `json:"max_p95_ttfb_ms,omitempty"`
	MaxP99DurationMilliseconds int               `json:"max_p99_duration_ms,omitempty"`
	Probes                     []AppReleaseProbe `json:"probes,omitempty"`
}

type AppReleaseProbe struct {
	Name                          string            `json:"name,omitempty"`
	Kind                          string            `json:"kind,omitempty"`
	Method                        string            `json:"method,omitempty"`
	Path                          string            `json:"path"`
	Headers                       map[string]string `json:"headers,omitempty"`
	Body                          string            `json:"body,omitempty"`
	ExpectedStatus                int               `json:"expected_status,omitempty"`
	ExpectedContentType           string            `json:"expected_content_type,omitempty"`
	ExpectedBodyContains          string            `json:"expected_body_contains,omitempty"`
	ExpectStreamEventBeforeMillis int               `json:"expect_stream_event_before_ms,omitempty"`
	TimeoutMilliseconds           int               `json:"timeout_ms,omitempty"`
	MaxTTFBMilliseconds           int               `json:"max_ttfb_ms,omitempty"`
	MaxDurationMilliseconds       int               `json:"max_duration_ms,omitempty"`
}

type AppReleaseGateResult struct {
	Status       string                  `json:"status"`
	ReleaseID    string                  `json:"release_id,omitempty"`
	Role         string                  `json:"role,omitempty"`
	Window       string                  `json:"window,omitempty"`
	Evidence     []string                `json:"evidence,omitempty"`
	Warnings     []string                `json:"warnings,omitempty"`
	Failures     []string                `json:"failures,omitempty"`
	ProbeResults []AppReleaseProbeResult `json:"probe_results,omitempty"`
	Metrics      map[string]any          `json:"metrics,omitempty"`
	EvaluatedAt  time.Time               `json:"evaluated_at"`
}

type AppReleaseProbeResult struct {
	Name           string `json:"name,omitempty"`
	Path           string `json:"path,omitempty"`
	Status         string `json:"status"`
	StatusCode     int    `json:"status_code,omitempty"`
	DurationMillis int64  `json:"duration_ms,omitempty"`
	TTFBMillis     int64  `json:"ttfb_ms,omitempty"`
	Error          string `json:"error,omitempty"`
	Evidence       string `json:"evidence,omitempty"`
}

type ImageLocation struct {
	ID                string     `json:"id"`
	TenantID          string     `json:"tenant_id,omitempty"`
	AppID             string     `json:"app_id,omitempty"`
	ImageRef          string     `json:"image_ref"`
	Digest            string     `json:"digest,omitempty"`
	SourceOperationID string     `json:"source_operation_id,omitempty"`
	NodeID            string     `json:"node_id,omitempty"`
	RuntimeID         string     `json:"runtime_id,omitempty"`
	ClusterNodeName   string     `json:"cluster_node_name,omitempty"`
	CacheEndpoint     string     `json:"cache_endpoint,omitempty"`
	Status            string     `json:"status"`
	LastSeenAt        *time.Time `json:"last_seen_at,omitempty"`
	SizeBytes         int64      `json:"size_bytes,omitempty"`
	LastError         string     `json:"last_error,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type ImageLocationFilter struct {
	TenantID        string
	AppID           string
	ImageRef        string
	Digest          string
	Status          string
	NodeID          string
	RuntimeID       string
	ClusterNodeName string
	PlatformAdmin   bool
}

type Image struct {
	ID                       string    `json:"id"`
	TenantID                 string    `json:"tenant_id,omitempty"`
	AppID                    string    `json:"app_id,omitempty"`
	ImageRef                 string    `json:"image_ref"`
	CanonicalDigest          string    `json:"canonical_digest,omitempty"`
	MediaType                string    `json:"media_type,omitempty"`
	ManifestJSON             string    `json:"manifest_json,omitempty"`
	ManifestSizeBytes        int64     `json:"manifest_size_bytes,omitempty"`
	BlobBytes                int64     `json:"blob_bytes,omitempty"`
	SourceOperationID        string    `json:"source_operation_id,omitempty"`
	LifecycleState           string    `json:"lifecycle_state"`
	RequiredReplicaCount     int       `json:"required_replica_count,omitempty"`
	MinAvailableReplicaCount int       `json:"min_available_replica_count,omitempty"`
	CreatedAt                time.Time `json:"created_at"`
	UpdatedAt                time.Time `json:"updated_at"`
}

type DistributedImageRetentionPlan struct {
	TenantID             string                   `json:"tenant_id,omitempty"`
	AppID                string                   `json:"app_id"`
	AppName              string                   `json:"app_name,omitempty"`
	EffectiveLimit       int                      `json:"effective_limit"`
	KeepImageIDs         []string                 `json:"keep_image_ids"`
	DropImageIDs         []string                 `json:"drop_image_ids"`
	ImageDecisions       []ImageRetentionDecision `json:"image_decisions"`
	WouldDeletePins      int                      `json:"would_delete_pins,omitempty"`
	WouldCancelTasks     int                      `json:"would_cancel_tasks,omitempty"`
	WouldNormalizeImages int                      `json:"would_normalize_images,omitempty"`
}

type ImageRetentionDecision struct {
	ImageID           string     `json:"image_id"`
	ImageRef          string     `json:"image_ref"`
	SourceOperationID string     `json:"source_operation_id,omitempty"`
	LifecycleState    string     `json:"lifecycle_state,omitempty"`
	LastDeployedAt    *time.Time `json:"last_deployed_at,omitempty"`
	CurrentWorkload   bool       `json:"current_workload"`
	ActiveOperation   bool       `json:"active_operation"`
	UserPinned        bool       `json:"user_pinned"`
	Rank              int        `json:"rank"`
	Keep              bool       `json:"keep"`
	Reason            string     `json:"reason"`
}

type ImageFilter struct {
	TenantID        string
	AppID           string
	ImageRef        string
	CanonicalDigest string
	LifecycleState  string
	PlatformAdmin   bool
}

type ImageAlias struct {
	ID        string    `json:"id"`
	ImageID   string    `json:"image_id"`
	TenantID  string    `json:"tenant_id,omitempty"`
	AliasRef  string    `json:"alias_ref"`
	Digest    string    `json:"digest,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ImageAliasFilter struct {
	ImageID       string
	TenantID      string
	AliasRef      string
	Digest        string
	PlatformAdmin bool
}

type ImageReplica struct {
	ID              string     `json:"id"`
	ImageID         string     `json:"image_id"`
	TenantID        string     `json:"tenant_id,omitempty"`
	AppID           string     `json:"app_id,omitempty"`
	Digest          string     `json:"digest,omitempty"`
	NodeID          string     `json:"node_id,omitempty"`
	RuntimeID       string     `json:"runtime_id,omitempty"`
	ClusterNodeName string     `json:"cluster_node_name,omitempty"`
	CacheEndpoint   string     `json:"cache_endpoint,omitempty"`
	FailureDomain   string     `json:"failure_domain,omitempty"`
	Status          string     `json:"status"`
	SourceReplicaID string     `json:"source_replica_id,omitempty"`
	LastVerifiedAt  *time.Time `json:"last_verified_at,omitempty"`
	LeaseExpiresAt  *time.Time `json:"lease_expires_at,omitempty"`
	SizeBytes       int64      `json:"size_bytes,omitempty"`
	LastError       string     `json:"last_error,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type ImageReplicaFilter struct {
	ImageID         string
	TenantID        string
	AppID           string
	Digest          string
	NodeID          string
	RuntimeID       string
	ClusterNodeName string
	Status          string
	PlatformAdmin   bool
}

type ImagePin struct {
	ID          string     `json:"id"`
	ImageID     string     `json:"image_id"`
	TenantID    string     `json:"tenant_id,omitempty"`
	AppID       string     `json:"app_id,omitempty"`
	OperationID string     `json:"operation_id,omitempty"`
	Reason      string     `json:"reason"`
	MinReplicas int        `json:"min_replicas,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

type ImagePinFilter struct {
	ImageID       string
	TenantID      string
	AppID         string
	OperationID   string
	Reason        string
	PlatformAdmin bool
}

type ImageReplicationTask struct {
	ID                    string     `json:"id"`
	ImageID               string     `json:"image_id"`
	TenantID              string     `json:"tenant_id,omitempty"`
	AppID                 string     `json:"app_id,omitempty"`
	SourceReplicaID       string     `json:"source_replica_id,omitempty"`
	SourceCacheEndpoint   string     `json:"source_cache_endpoint,omitempty"`
	TargetNodeID          string     `json:"target_node_id,omitempty"`
	TargetRuntimeID       string     `json:"target_runtime_id,omitempty"`
	TargetClusterNodeName string     `json:"target_cluster_node_name,omitempty"`
	Priority              string     `json:"priority"`
	Status                string     `json:"status"`
	Attempts              int        `json:"attempts,omitempty"`
	LastError             string     `json:"last_error,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
	StartedAt             *time.Time `json:"started_at,omitempty"`
	CompletedAt           *time.Time `json:"completed_at,omitempty"`
}

type ImageReplicationTaskFilter struct {
	ImageID               string
	TenantID              string
	AppID                 string
	SourceReplicaID       string
	TargetNodeID          string
	TargetRuntimeID       string
	TargetClusterNodeName string
	Priority              string
	Status                string
	PlatformAdmin         bool
}

type ImageCacheNodeInventory struct {
	ID                      string                         `json:"id"`
	NodeID                  string                         `json:"node_id,omitempty"`
	ClusterNodeName         string                         `json:"cluster_node_name,omitempty"`
	RuntimeID               string                         `json:"runtime_id,omitempty"`
	CacheEndpoint           string                         `json:"cache_endpoint,omitempty"`
	StorePath               string                         `json:"store_path,omitempty"`
	FilesystemTotalBytes    int64                          `json:"filesystem_total_bytes,omitempty"`
	FilesystemFreeBytes     int64                          `json:"filesystem_free_bytes,omitempty"`
	FilesystemUsedPercent   float64                        `json:"filesystem_used_percent,omitempty"`
	CacheBytes              int64                          `json:"cache_bytes,omitempty"`
	ManifestCount           int                            `json:"manifest_count,omitempty"`
	BlobCount               int                            `json:"blob_count,omitempty"`
	UnreferencedBlobCount   int                            `json:"unreferenced_blob_count,omitempty"`
	UnreferencedBlobBytes   int64                          `json:"unreferenced_blob_bytes,omitempty"`
	UnreferencedBlobs       []ImageCachePruneBlobCandidate `json:"unreferenced_blobs,omitempty"`
	PinCount                int                            `json:"pin_count,omitempty"`
	ObservedAt              time.Time                      `json:"observed_at"`
	ReportedByNodeUpdaterID string                         `json:"reported_by_node_updater_id,omitempty"`
	Status                  string                         `json:"status,omitempty"`
	LastError               string                         `json:"last_error,omitempty"`
	CreatedAt               time.Time                      `json:"created_at"`
	UpdatedAt               time.Time                      `json:"updated_at"`
	SnapshotComplete        bool                           `json:"-"`
}

type ImageCacheNodeInventoryFilter struct {
	NodeID          string
	ClusterNodeName string
	RuntimeID       string
	StaleAfter      time.Time
}

type ImageCacheManifest struct {
	ID                string     `json:"id"`
	NodeID            string     `json:"node_id,omitempty"`
	ClusterNodeName   string     `json:"cluster_node_name,omitempty"`
	RuntimeID         string     `json:"runtime_id,omitempty"`
	ImageRef          string     `json:"image_ref,omitempty"`
	Repo              string     `json:"repo"`
	Target            string     `json:"target"`
	Digest            string     `json:"digest,omitempty"`
	MediaType         string     `json:"media_type,omitempty"`
	ManifestSizeBytes int64      `json:"manifest_size_bytes,omitempty"`
	TotalBlobBytes    int64      `json:"total_blob_bytes,omitempty"`
	ReferencedBlobs   []string   `json:"referenced_blobs,omitempty"`
	CreatedAtObserved *time.Time `json:"created_at_observed,omitempty"`
	LastSeenAt        time.Time  `json:"last_seen_at"`
	PinnedLocally     bool       `json:"pinned_locally,omitempty"`
	Present           bool       `json:"present"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type ImageCacheManifestFilter struct {
	NodeID          string
	ClusterNodeName string
	RuntimeID       string
	Repo            string
	Target          string
	Digest          string
	SeenAfter       time.Time
	PresentOnly     bool
}

type ImageCachePruneCandidate struct {
	ImageRef            string   `json:"image_ref,omitempty"`
	NodeName            string   `json:"node_name,omitempty"`
	Repo                string   `json:"repo"`
	Target              string   `json:"target"`
	Digest              string   `json:"digest,omitempty"`
	Reason              string   `json:"reason,omitempty"`
	SkipReason          string   `json:"skip_reason,omitempty"`
	SkipDetails         []string `json:"skip_details,omitempty"`
	Protected           bool     `json:"protected"`
	PlannedDeleteBytes  int64    `json:"planned_delete_bytes,omitempty"`
	ReferencedBlobs     []string `json:"referenced_blobs,omitempty"`
	ReferencedBlobCount int      `json:"referenced_blob_count,omitempty"`
	ReferencedBlobBytes int64    `json:"referenced_blob_bytes,omitempty"`
	MatchedImageIDs     []string `json:"matched_image_ids,omitempty"`
	MatchedPinIDs       []string `json:"matched_pin_ids,omitempty"`
	MatchedTaskIDs      []string `json:"matched_task_ids,omitempty"`
	MatchedWorkloadRefs []string `json:"matched_workload_refs,omitempty"`
	MatchedReplicaIDs   []string `json:"matched_replica_ids,omitempty"`
	LastSeenAt          string   `json:"last_seen_at,omitempty"`
	CreatedAtObserved   string   `json:"created_at_observed,omitempty"`
}

type ImageCachePruneBlobCandidate struct {
	NodeName           string `json:"node_name,omitempty"`
	Digest             string `json:"digest"`
	SizeBytes          int64  `json:"size_bytes,omitempty"`
	Reason             string `json:"reason,omitempty"`
	PlannedDeleteBytes int64  `json:"planned_delete_bytes,omitempty"`
	LastSeenAt         string `json:"last_seen_at,omitempty"`
}

type ImageCachePrunePlan struct {
	ID                     string                         `json:"id"`
	NodeID                 string                         `json:"node_id,omitempty"`
	ClusterNodeName        string                         `json:"cluster_node_name,omitempty"`
	RuntimeID              string                         `json:"runtime_id,omitempty"`
	Mode                   string                         `json:"mode"`
	CandidateManifestCount int                            `json:"candidate_manifest_count"`
	ProtectedManifestCount int                            `json:"protected_manifest_count"`
	CandidateBlobCount     int                            `json:"candidate_blob_count,omitempty"`
	CandidateBlobBytes     int64                          `json:"candidate_blob_bytes,omitempty"`
	ProtectedBlobCount     int                            `json:"protected_blob_count,omitempty"`
	PlannedDeleteBytes     int64                          `json:"planned_delete_bytes,omitempty"`
	MaxDeleteBytes         int64                          `json:"max_delete_bytes,omitempty"`
	MinManifestAge         string                         `json:"min_manifest_age,omitempty"`
	ProtectionSummary      map[string]int                 `json:"protection_summary,omitempty"`
	CandidateSummary       map[string]int                 `json:"candidate_summary,omitempty"`
	Candidates             []ImageCachePruneCandidate     `json:"candidates,omitempty"`
	ProtectedManifests     []ImageCachePruneCandidate     `json:"protected_manifests,omitempty"`
	SkippedManifests       []ImageCachePruneCandidate     `json:"skipped_manifests,omitempty"`
	UnreferencedBlobs      []ImageCachePruneBlobCandidate `json:"unreferenced_blobs,omitempty"`
	NodePressure           bool                           `json:"node_pressure,omitempty"`
	BudgetExhausted        bool                           `json:"budget_exhausted,omitempty"`
	CreatedAt              time.Time                      `json:"created_at"`
	ExecutedAt             *time.Time                     `json:"executed_at,omitempty"`
	Status                 string                         `json:"status"`
	Error                  string                         `json:"error,omitempty"`
}

type ImageCachePrunePlanFilter struct {
	NodeID          string
	ClusterNodeName string
	RuntimeID       string
	Mode            string
	Status          string
	Limit           int
}

type LocalPVInventory struct {
	ID                      string    `json:"id"`
	NodeID                  string    `json:"node_id,omitempty"`
	ClusterNodeName         string    `json:"cluster_node_name,omitempty"`
	RuntimeID               string    `json:"runtime_id,omitempty"`
	NodeRoles               []string  `json:"node_roles,omitempty"`
	VGName                  string    `json:"vg_name,omitempty"`
	ImagePath               string    `json:"image_path,omitempty"`
	ImageSizeBytes          int64     `json:"image_size_bytes,omitempty"`
	LoopDevice              string    `json:"loop_device,omitempty"`
	LoopBackingFile         string    `json:"loop_backing_file,omitempty"`
	PVSizeBytes             int64     `json:"pv_size_bytes,omitempty"`
	PVFreeBytes             int64     `json:"pv_free_bytes,omitempty"`
	LVCount                 int       `json:"lv_count"`
	LVNames                 []string  `json:"lv_names,omitempty"`
	ActiveLVCount           int       `json:"active_lv_count"`
	BoundPVCount            int       `json:"bound_pv_count"`
	BoundPVCRefs            []string  `json:"bound_pvc_refs,omitempty"`
	SafeToDecommission      bool      `json:"safe_to_decommission"`
	UnsafeReasons           []string  `json:"unsafe_reasons,omitempty"`
	ObservedAt              time.Time `json:"observed_at"`
	ReportedByNodeUpdaterID string    `json:"reported_by_node_updater_id,omitempty"`
	CreatedAt               time.Time `json:"created_at"`
	UpdatedAt               time.Time `json:"updated_at"`
}

type LocalPVInventoryFilter struct {
	NodeID          string
	ClusterNodeName string
	RuntimeID       string
	StaleAfter      time.Time
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

type AppImageTracking struct {
	ID                 string     `json:"id"`
	TenantID           string     `json:"tenant_id"`
	AppID              string     `json:"app_id"`
	ImageRef           string     `json:"image_ref"`
	Enabled            bool       `json:"enabled"`
	LastSeenDigest     string     `json:"last_seen_digest,omitempty"`
	LastQueuedDigest   string     `json:"last_queued_digest,omitempty"`
	LastDeployedDigest string     `json:"last_deployed_digest,omitempty"`
	LastOperationID    string     `json:"last_operation_id,omitempty"`
	LastDeliveryID     string     `json:"last_delivery_id,omitempty"`
	LastEvent          string     `json:"last_event,omitempty"`
	LastError          string     `json:"last_error,omitempty"`
	LastCheckedAt      *time.Time `json:"last_checked_at,omitempty"`
	LastTriggeredAt    *time.Time `json:"last_triggered_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

const (
	AppImageTrackingDecisionQueued          = "queued"
	AppImageTrackingDecisionAlreadyDeployed = "already_deployed"
	AppImageTrackingDecisionNoChange        = "no_change"
	AppImageTrackingDecisionReplicasZero    = "replicas_zero"
	AppImageTrackingDecisionActiveOperation = "active_operation"
	AppImageTrackingDecisionRetrySuppressed = "retry_suppressed"
	AppImageTrackingDecisionResolverError   = "resolver_error"
	AppImageTrackingDecisionQueueConflict   = "queue_conflict"
	AppImageTrackingDecisionQueueError      = "queue_error"
)

type AppImageTrackingFilter struct {
	TenantID      string
	PlatformAdmin bool
	AppID         string
	ImageRef      string
	Enabled       *bool
}

type AppImageTrackingCheck struct {
	ID                       string    `json:"id"`
	TenantID                 string    `json:"tenant_id"`
	AppID                    string    `json:"app_id"`
	TrackingID               string    `json:"tracking_id"`
	ImageRef                 string    `json:"image_ref"`
	ObservedDigest           string    `json:"observed_digest,omitempty"`
	CurrentAppDigest         string    `json:"current_app_digest,omitempty"`
	LastQueuedDigest         string    `json:"last_queued_digest,omitempty"`
	LastDeployedDigest       string    `json:"last_deployed_digest,omitempty"`
	Decision                 string    `json:"decision"`
	SkipReason               string    `json:"skip_reason,omitempty"`
	OperationID              string    `json:"operation_id,omitempty"`
	ActiveOperationID        string    `json:"active_operation_id,omitempty"`
	ResolverError            string    `json:"resolver_error,omitempty"`
	DeliveryID               string    `json:"delivery_id,omitempty"`
	Event                    string    `json:"event,omitempty"`
	DurationMilliseconds     int64     `json:"duration_ms"`
	ControllerPod            string    `json:"controller_pod,omitempty"`
	ControllerLeaderIdentity string    `json:"controller_leader_identity,omitempty"`
	CheckedAt                time.Time `json:"checked_at"`
}

type AppImageTrackingCheckFilter struct {
	TenantID      string
	PlatformAdmin bool
	AppID         string
	TrackingID    string
	Limit         int
}

type Operation struct {
	ID                       string                             `json:"id"`
	TenantID                 string                             `json:"tenant_id"`
	Type                     string                             `json:"type"`
	Status                   string                             `json:"status"`
	ExecutionMode            string                             `json:"execution_mode"`
	RequestedByType          string                             `json:"requested_by_type"`
	RequestedByID            string                             `json:"requested_by_id"`
	AppID                    string                             `json:"app_id"`
	ServiceID                string                             `json:"service_id,omitempty"`
	SourceRuntimeID          string                             `json:"source_runtime_id,omitempty"`
	TargetRuntimeID          string                             `json:"target_runtime_id,omitempty"`
	DesiredReplicas          *int                               `json:"desired_replicas,omitempty"`
	DesiredSpec              *AppSpec                           `json:"desired_spec,omitempty"`
	DesiredSource            *AppSource                         `json:"desired_source,omitempty"`
	DesiredOriginSource      *AppSource                         `json:"desired_origin_source,omitempty"`
	ResultMessage            string                             `json:"result_message,omitempty"`
	ManifestPath             string                             `json:"manifest_path,omitempty"`
	AssignedRuntimeID        string                             `json:"assigned_runtime_id,omitempty"`
	ErrorMessage             string                             `json:"error_message,omitempty"`
	ControllerTimingSegments []OperationControllerTimingSegment `json:"controller_timing_segments,omitempty"`
	CreatedAt                time.Time                          `json:"created_at"`
	UpdatedAt                time.Time                          `json:"updated_at"`
	StartedAt                *time.Time                         `json:"started_at,omitempty"`
	CompletedAt              *time.Time                         `json:"completed_at,omitempty"`
}

type OperationControllerTimingSegment struct {
	Name                 string `json:"name"`
	DurationMilliseconds int64  `json:"duration_ms"`
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

func AppTrackedImageDigest(app App, imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return ""
	}
	source := AppBuildSource(app)
	if source == nil ||
		strings.TrimSpace(source.Type) != AppSourceTypeDockerImage ||
		strings.TrimSpace(source.ImageRef) != imageRef {
		return ""
	}
	if digest := ImageDigestFromReference(source.ResolvedImageRef); digest != "" {
		return digest
	}
	return ImageDigestFromReference(app.Spec.Image)
}

func ImageDigestsMatch(current, expected string) bool {
	current = strings.ToLower(strings.TrimSpace(current))
	expected = strings.ToLower(strings.TrimSpace(expected))
	if current == "" || expected == "" {
		return false
	}
	if current == expected {
		return true
	}
	current = strings.TrimPrefix(current, "sha256:")
	expected = strings.TrimPrefix(expected, "sha256:")
	if current == "" || expected == "" {
		return false
	}
	if len(current) < len(expected) {
		return strings.HasPrefix(expected, current)
	}
	return strings.HasPrefix(current, expected)
}

func ImageDigestFromReference(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return ""
	}
	if strings.HasPrefix(imageRef, "sha256:") {
		return imageRef
	}
	if idx := strings.Index(imageRef, "@sha256:"); idx >= 0 {
		return imageRef[idx+1:]
	}
	if idx := strings.LastIndex(imageRef, ":image-"); idx >= 0 {
		value := strings.ToLower(imageRef[idx+len(":image-"):])
		if isHexDigestPrefix(value) {
			return "sha256:" + value
		}
	}
	return ""
}

func isHexDigestPrefix(value string) bool {
	if len(value) < 12 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
			continue
		}
		return false
	}
	return true
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

type StorePromotion struct {
	ID                           string            `json:"id"`
	SourceKind                   string            `json:"source_kind"`
	SourceFingerprint            string            `json:"source_fingerprint"`
	TargetStore                  string            `json:"target_store"`
	Generation                   string            `json:"generation"`
	OperatorType                 string            `json:"operator_type,omitempty"`
	OperatorID                   string            `json:"operator_id,omitempty"`
	Status                       string            `json:"status"`
	DryRun                       bool              `json:"dry_run"`
	BackupRef                    string            `json:"backup_ref,omitempty"`
	RollbackRef                  string            `json:"rollback_ref,omitempty"`
	RestoreManifestRef           string            `json:"restore_manifest_ref,omitempty"`
	PermissionVerificationStatus string            `json:"permission_verification_status,omitempty"`
	InvariantStatus              string            `json:"invariant_status,omitempty"`
	Message                      string            `json:"message,omitempty"`
	Metadata                     map[string]string `json:"metadata,omitempty"`
	StartedAt                    time.Time         `json:"started_at"`
	CompletedAt                  *time.Time        `json:"completed_at,omitempty"`
	CreatedAt                    time.Time         `json:"created_at"`
	UpdatedAt                    time.Time         `json:"updated_at"`
}

type StoreInvariantCheck struct {
	Name    string `json:"name"`
	Pass    bool   `json:"pass"`
	Count   int    `json:"count,omitempty"`
	Message string `json:"message,omitempty"`
}

type ControlPlaneStoreStatus struct {
	AuthoritativeStore           string                `json:"authoritative_store"`
	StoreGeneration              string                `json:"store_generation"`
	SourceFingerprint            string                `json:"source_fingerprint"`
	SchemaVersion                string                `json:"schema_version,omitempty"`
	LastPromotion                *StorePromotion       `json:"last_promotion,omitempty"`
	LastRestore                  *StorePromotion       `json:"last_restore,omitempty"`
	LastBackupRef                string                `json:"last_backup_ref,omitempty"`
	PermissionVerificationStatus string                `json:"permission_verification_status"`
	RestoreReadiness             string                `json:"restore_readiness"`
	Invariants                   []StoreInvariantCheck `json:"invariants"`
	BlockRollout                 bool                  `json:"block_rollout"`
	GateReason                   string                `json:"gate_reason,omitempty"`
}

type RestoreManifest struct {
	DumpRef        string            `json:"dump_ref"`
	TargetStore    string            `json:"target_store"`
	Owner          string            `json:"owner"`
	ExpectedCounts map[string]int    `json:"expected_counts,omitempty"`
	RequiredGrants []string          `json:"required_grants,omitempty"`
	RestoreOrder   []string          `json:"restore_order,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type RouteExplainResponse struct {
	Hostname          string             `json:"hostname"`
	ServingMode       string             `json:"serving_mode"`
	Route             *EdgeRouteBinding  `json:"route,omitempty"`
	Routes            []EdgeRouteBinding `json:"routes,omitempty"`
	HealthyEdgeGroups map[string]bool    `json:"healthy_edge_groups,omitempty"`
	FallbackChain     []string           `json:"fallback_chain,omitempty"`
	Reasons           []string           `json:"reasons,omitempty"`
	GeneratedAt       time.Time          `json:"generated_at"`
}

type RouteServingMode struct {
	Hostname          string    `json:"hostname"`
	PathPrefix        string    `json:"path_prefix,omitempty"`
	ServingMode       string    `json:"serving_mode"`
	SelectedEdgeGroup string    `json:"selected_edge_group,omitempty"`
	RuntimeEdgeGroup  string    `json:"runtime_edge_group,omitempty"`
	RouteKind         string    `json:"route_kind,omitempty"`
	RoutePolicy       string    `json:"route_policy,omitempty"`
	Reason            string    `json:"reason,omitempty"`
	GeneratedAt       time.Time `json:"generated_at"`
}

type RouteServingModeListResponse struct {
	Routes      []RouteServingMode `json:"routes"`
	GeneratedAt time.Time          `json:"generated_at"`
}

type PlatformAutonomyStatus struct {
	GeneratedAt       time.Time               `json:"generated_at"`
	Pass              bool                    `json:"pass"`
	BlockRollout      bool                    `json:"block_rollout"`
	ControlPlaneStore ControlPlaneStoreStatus `json:"control_plane_store"`
	DiscoveryBundle   string                  `json:"discovery_bundle"`
	NodePolicy        string                  `json:"node_policy"`
	Edge              string                  `json:"edge"`
	DNS               string                  `json:"dns"`
	Registry          string                  `json:"registry"`
	Headscale         string                  `json:"headscale"`
	RouteFallback     string                  `json:"route_fallback"`
	RestoreReadiness  string                  `json:"restore_readiness"`
	Checks            []StoreInvariantCheck   `json:"checks"`
}

type DNSFullZonePreflightResponse struct {
	Zone           string                        `json:"zone"`
	Pass           bool                          `json:"pass"`
	GeneratedAt    time.Time                     `json:"generated_at"`
	DNSSECStatus   string                        `json:"dnssec_status"`
	Checks         []DNSDelegationPreflightCheck `json:"checks"`
	DelegationPlan DNSDelegationPlan             `json:"delegation_plan"`
	RollbackPlan   DNSDelegationPlan             `json:"rollback_plan"`
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

func NormalizeMachineDedicatedMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case MachineDedicatedModeEdge:
		return MachineDedicatedModeEdge
	case MachineDedicatedModeDNS:
		return MachineDedicatedModeDNS
	case MachineDedicatedModeInternal:
		return MachineDedicatedModeInternal
	case "", MachineDedicatedModeNone:
		return MachineDedicatedModeNone
	default:
		return ""
	}
}

func MachinePolicyDedicatedMode(policy MachinePolicy) string {
	if policy.AllowAppRuntime || policy.AllowBuilds || policy.AllowSharedPool {
		return MachineDedicatedModeNone
	}
	switch {
	case policy.AllowEdge:
		return MachineDedicatedModeEdge
	case policy.AllowDNS:
		return MachineDedicatedModeDNS
	case policy.AllowInternalMaintenance:
		return MachineDedicatedModeInternal
	default:
		return MachineDedicatedModeNone
	}
}

type State struct {
	Version                    string                      `json:"version"`
	Tenants                    []Tenant                    `json:"tenants"`
	Projects                   []Project                   `json:"projects"`
	ProjectDeleteRequests      map[string]time.Time        `json:"project_delete_requests,omitempty"`
	ProjectRuntimeReservations []ProjectRuntimeReservation `json:"project_runtime_reservations,omitempty"`
	APIKeys                    []APIKey                    `json:"api_keys"`
	SSHKeys                    []SSHKey                    `json:"ssh_keys,omitempty"`
	AppSSHEndpoints            []AppSSHEndpoint            `json:"app_ssh_endpoints,omitempty"`
	EnrollmentTokens           []EnrollmentToken           `json:"enrollment_tokens"`
	NodeKeys                   []NodeKey                   `json:"node_keys"`
	Machines                   []Machine                   `json:"machines"`
	NodeUpdaters               []NodeUpdater               `json:"node_updaters,omitempty"`
	NodeUpdateTasks            []NodeUpdateTask            `json:"node_update_tasks,omitempty"`
	ImageLocations             []ImageLocation             `json:"image_locations,omitempty"`
	Images                     []Image                     `json:"images,omitempty"`
	ImageAliases               []ImageAlias                `json:"image_aliases,omitempty"`
	ImageReplicas              []ImageReplica              `json:"image_replicas,omitempty"`
	ImagePins                  []ImagePin                  `json:"image_pins,omitempty"`
	ImageReplicationTasks      []ImageReplicationTask      `json:"image_replication_tasks,omitempty"`
	ImageCacheNodes            []ImageCacheNodeInventory   `json:"image_cache_nodes,omitempty"`
	ImageCacheManifests        []ImageCacheManifest        `json:"image_cache_manifests,omitempty"`
	ImageCachePrunePlans       []ImageCachePrunePlan       `json:"image_cache_prune_plans,omitempty"`
	LocalPVInventories         []LocalPVInventory          `json:"localpv_inventories,omitempty"`
	AppImageTrackings          []AppImageTracking          `json:"app_image_trackings,omitempty"`
	AppImageTrackingChecks     []AppImageTrackingCheck     `json:"app_image_tracking_checks,omitempty"`
	AppReleases                []AppRelease                `json:"app_releases,omitempty"`
	ReleaseAttempts            []ReleaseAttempt            `json:"release_attempts,omitempty"`
	ReleaseSteps               []ReleaseStep               `json:"release_steps,omitempty"`
	AppTrafficPolicies         []AppTrafficPolicy          `json:"app_traffic_policies,omitempty"`
	Runtimes                   []Runtime                   `json:"runtimes"`
	RuntimeGrants              []RuntimeAccessGrant        `json:"runtime_grants"`
	AppDatabaseImportJobs      []AppDatabaseImportJob      `json:"app_database_import_jobs,omitempty"`
	AppDatabaseAccessGrants    []AppDatabaseAccessGrant    `json:"app_database_access_grants,omitempty"`
	Apps                       []App                       `json:"apps"`
	ProjectRouteTables         []ProjectRouteTable         `json:"project_route_tables,omitempty"`
	AppDomains                 []AppDomain                 `json:"app_domains"`
	EdgeTLSCertificates        []EdgeTLSCertificate        `json:"edge_tls_certificates,omitempty"`
	EdgeGroups                 []EdgeGroup                 `json:"edge_groups,omitempty"`
	EdgeNodes                  []EdgeNode                  `json:"edge_nodes,omitempty"`
	DNSNodes                   []DNSNode                   `json:"dns_nodes,omitempty"`
	DNSACMEChallenges          []DNSACMEChallenge          `json:"dns_acme_challenges,omitempty"`
	EdgeRoutePolicies          []EdgeRoutePolicy           `json:"edge_route_policies,omitempty"`
	EdgePerformanceSamples     []EdgePerformanceSample     `json:"edge_performance_samples,omitempty"`
	EdgeQualityRollups         []EdgeQualityRollup         `json:"edge_quality_rollups,omitempty"`
	EdgeDNSRoutingDecisions    []EdgeDNSRoutingDecision    `json:"edge_dns_routing_decisions,omitempty"`
	StorePromotions            []StorePromotion            `json:"store_promotions,omitempty"`
	BackingServices            []BackingService            `json:"backing_services"`
	ServiceBindings            []ServiceBinding            `json:"service_bindings"`
	Operations                 []Operation                 `json:"operations"`
	OperationEvidence          []OperationEvidence         `json:"operation_evidence,omitempty"`
	AuditEvents                []AuditEvent                `json:"audit_events"`
	Idempotency                []IdempotencyRecord         `json:"idempotency"`
	TenantBilling              []TenantBilling             `json:"tenant_billing"`
	BillingEvents              []TenantBillingEvent        `json:"billing_events"`
	ResourceUsageSamples       []ResourceUsageSample       `json:"resource_usage_samples,omitempty"`
	DataBackends               []DataBackend               `json:"data_backends,omitempty"`
	DataBackendSecrets         []DataBackendSecret         `json:"data_backend_secrets,omitempty"`
	BackupBackends             []BackupBackend             `json:"backup_backends,omitempty"`
	BackupBackendSecrets       []BackupBackendSecret       `json:"backup_backend_secrets,omitempty"`
	BackupPolicies             []BackupPolicy              `json:"backup_policies,omitempty"`
	BackupRuns                 []BackupRun                 `json:"backup_runs,omitempty"`
	BackupArtifacts            []BackupArtifact            `json:"backup_artifacts,omitempty"`
	BackupRestorePlans         []BackupRestorePlan         `json:"backup_restore_plans,omitempty"`
	BackupRestoreRuns          []BackupRestoreRun          `json:"backup_restore_runs,omitempty"`
	DataWorkspaces             []DataWorkspace             `json:"data_workspaces,omitempty"`
	DataSnapshots              []DataSnapshot              `json:"data_snapshots,omitempty"`
	DataTransfers              []DataTransfer              `json:"data_transfers,omitempty"`
	DataGrants                 []DataGrant                 `json:"data_grants,omitempty"`
	DataWorkspaceAccessGrants  []DataWorkspaceAccessGrant  `json:"data_workspace_access_grants,omitempty"`
	DataRuntimeCaches          []RuntimeDataCacheMetadata  `json:"data_runtime_caches,omitempty"`
}
