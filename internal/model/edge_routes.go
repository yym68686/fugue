package model

import (
	"strings"
	"time"
)

const (
	BundleSchemaVersionV1 = "1.0"
	BundleIssuerFugue     = "fugue-control-plane"
)

const (
	EdgeRouteKindPlatform        = "platform"
	EdgeRouteKindPlatformDomain  = "platform-domain"
	EdgeRouteKindCustomDomain    = "custom-domain"
	EdgeRouteKindPlatformRoute   = "platform-route"
	EdgeRouteKindControlPlaneAPI = "control-plane-api"
)

const (
	EdgeRouteStatusActive         = "active"
	EdgeRouteStatusDisabled       = "disabled"
	EdgeRouteStatusUnavailable    = "unavailable"
	EdgeRouteStatusRuntimeMissing = "runtime-missing"
)

const (
	EdgeRoutePolicyRouteAOnly = "route_a_only"
	EdgeRoutePolicyCanary     = "edge_canary"
	EdgeRoutePolicyEnabled    = "edge_enabled"

	// EdgeRoutePolicyPrimary is a legacy pre-opt-in value kept only so stale
	// cached bundles decode cleanly. New route bundles must not emit it.
	EdgeRoutePolicyPrimary = "primary"
)

const (
	EdgeRouteUpstreamKindKubernetesService = "kubernetes-service"
	EdgeRouteUpstreamKindMesh              = "mesh"
)

const (
	EdgeRouteUpstreamScopeLocalService = "local-service"
	EdgeRouteUpstreamScopeCluster      = "cluster"
	EdgeRouteUpstreamScopeMesh         = "mesh"
)

const (
	EdgeRouteTLSPolicyPlatform     = "platform"
	EdgeRouteTLSPolicyCustomDomain = "custom-domain"
)

const (
	EdgeDNSRecordKindCustomDomainTarget = "custom-domain-target"
	EdgeDNSRecordKindPlatformDomain     = "platform-domain"
	EdgeDNSRecordKindPlatform           = "platform"
	EdgeDNSRecordKindPlatformRoute      = "platform-route"
	EdgeDNSRecordKindProbe              = "probe"
	EdgeDNSRecordKindProtected          = "protected"
	EdgeDNSRecordKindACMEChallenge      = "acme-challenge"
)

const (
	PlatformRouteEdgeGroupModeAllHealthy  = "all_healthy"
	PlatformRouteEdgeGroupModeRegionAware = "region_aware"
	PlatformRouteEdgeGroupModePinned      = "pinned"
)

const (
	EdgeDNSRecordTypeA     = "A"
	EdgeDNSRecordTypeAAAA  = "AAAA"
	EdgeDNSRecordTypeCAA   = "CAA"
	EdgeDNSRecordTypeCNAME = "CNAME"
	EdgeDNSRecordTypeMX    = "MX"
	EdgeDNSRecordTypeNS    = "NS"
	EdgeDNSRecordTypeTXT   = "TXT"
)

const (
	EdgeHealthUnknown   = "unknown"
	EdgeHealthHealthy   = "healthy"
	EdgeHealthDegraded  = "degraded"
	EdgeHealthUnhealthy = "unhealthy"
)

type EdgeRouteBundle struct {
	SchemaVersion      string                  `json:"schema_version,omitempty"`
	Version            string                  `json:"version"`
	Generation         string                  `json:"generation,omitempty"`
	PreviousGeneration string                  `json:"previous_generation,omitempty"`
	GeneratedAt        time.Time               `json:"generated_at"`
	ValidUntil         time.Time               `json:"valid_until,omitempty"`
	Issuer             string                  `json:"issuer,omitempty"`
	KeyID              string                  `json:"key_id,omitempty"`
	Signature          string                  `json:"signature,omitempty"`
	Signatures         []BundleSignature       `json:"signatures,omitempty"`
	EdgeID             string                  `json:"edge_id,omitempty"`
	EdgeGroupID        string                  `json:"edge_group_id,omitempty"`
	Routes             []EdgeRouteBinding      `json:"routes"`
	TLSAllowlist       []EdgeTLSAllowlistEntry `json:"tls_allowlist"`
}

type EdgeRouteBinding struct {
	Hostname            string    `json:"hostname"`
	RouteKind           string    `json:"route_kind"`
	AppID               string    `json:"app_id"`
	TenantID            string    `json:"tenant_id"`
	RuntimeID           string    `json:"runtime_id"`
	RuntimeType         string    `json:"runtime_type,omitempty"`
	RuntimeEdgeGroup    string    `json:"runtime_edge_group,omitempty"`
	RuntimeEdgeGroupID  string    `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode  string    `json:"runtime_cluster_node,omitempty"`
	SelectedEdgeGroup   string    `json:"selected_edge_group,omitempty"`
	EdgeGroupID         string    `json:"edge_group_id"`
	FallbackEdgeGroupID string    `json:"fallback_edge_group_id,omitempty"`
	PolicyEdgeGroupID   string    `json:"policy_edge_group_id,omitempty"`
	RoutePolicy         string    `json:"route_policy"`
	SelectionReason     string    `json:"selection_reason,omitempty"`
	FallbackReason      string    `json:"fallback_reason,omitempty"`
	UpstreamKind        string    `json:"upstream_kind"`
	UpstreamScope       string    `json:"upstream_scope,omitempty"`
	UpstreamURL         string    `json:"upstream_url,omitempty"`
	ServicePort         int       `json:"service_port"`
	TLSPolicy           string    `json:"tls_policy"`
	Streaming           bool      `json:"streaming"`
	Status              string    `json:"status"`
	StatusReason        string    `json:"status_reason,omitempty"`
	RouteGeneration     string    `json:"route_generation"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type EdgeRoutePolicy struct {
	ID          string    `json:"id"`
	Hostname    string    `json:"hostname"`
	AppID       string    `json:"app_id"`
	TenantID    string    `json:"tenant_id"`
	EdgeGroupID string    `json:"edge_group_id,omitempty"`
	RoutePolicy string    `json:"route_policy"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type PlatformRoute struct {
	Hostname      string `json:"hostname"`
	Kind          string `json:"kind,omitempty"`
	UpstreamKind  string `json:"upstream_kind,omitempty"`
	UpstreamScope string `json:"upstream_scope,omitempty"`
	UpstreamURL   string `json:"upstream_url"`
	TLSPolicy     string `json:"tls_policy,omitempty"`
	RoutePolicy   string `json:"route_policy,omitempty"`
	EdgeGroupMode string `json:"edge_group_mode,omitempty"`
	EdgeGroupID   string `json:"edge_group_id,omitempty"`
	Status        string `json:"status,omitempty"`
	StatusReason  string `json:"status_reason,omitempty"`
	TTL           int    `json:"ttl,omitempty"`
}

type BundleSignature struct {
	SchemaVersion      string    `json:"schema_version,omitempty"`
	Issuer             string    `json:"issuer,omitempty"`
	KeyID              string    `json:"key_id,omitempty"`
	Signature          string    `json:"signature,omitempty"`
	GeneratedAt        time.Time `json:"generated_at,omitempty"`
	ValidUntil         time.Time `json:"valid_until,omitempty"`
	PreviousGeneration string    `json:"previous_generation,omitempty"`
}

type DiscoveryEndpoint struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type DiscoveryRegistryEndpoint struct {
	Name     string `json:"name"`
	PushBase string `json:"push_base,omitempty"`
	PullBase string `json:"pull_base,omitempty"`
	Mirror   string `json:"mirror,omitempty"`
}

type DiscoveryKubernetesEndpoint struct {
	Name             string   `json:"name"`
	Server           string   `json:"server,omitempty"`
	FallbackServers  []string `json:"fallback_servers,omitempty"`
	CAHash           string   `json:"ca_hash,omitempty"`
	RegistryEndpoint string   `json:"registry_endpoint,omitempty"`
}

type DiscoveryBundle struct {
	SchemaVersion      string                        `json:"schema_version"`
	Generation         string                        `json:"generation"`
	PreviousGeneration string                        `json:"previous_generation,omitempty"`
	GeneratedAt        time.Time                     `json:"generated_at"`
	ValidUntil         time.Time                     `json:"valid_until"`
	Issuer             string                        `json:"issuer"`
	KeyID              string                        `json:"key_id,omitempty"`
	Signature          string                        `json:"signature,omitempty"`
	Signatures         []BundleSignature             `json:"signatures,omitempty"`
	APIEndpoints       []DiscoveryEndpoint           `json:"api_endpoints"`
	Kubernetes         []DiscoveryKubernetesEndpoint `json:"kubernetes"`
	Registry           []DiscoveryRegistryEndpoint   `json:"registry"`
	EdgeGroups         []EdgeGroup                   `json:"edge_groups"`
	EdgeNodes          []EdgeNode                    `json:"edge_nodes"`
	DNSNodes           []DNSNode                     `json:"dns_nodes"`
	PlatformRoutes     []PlatformRoute               `json:"platform_routes,omitempty"`
	PublicRuntimeEnv   map[string]string             `json:"public_runtime_env,omitempty"`
}

type EdgeSelectionPolicy struct {
	RuntimeLocality bool     `json:"runtime_locality"`
	ClientLocality  bool     `json:"client_locality"`
	EdgeHealth      bool     `json:"edge_health"`
	Capacity        bool     `json:"capacity"`
	Latency         bool     `json:"latency"`
	FallbackOrder   []string `json:"fallback_order,omitempty"`
}

type PlatformDomainBinding struct {
	Hostname    string    `json:"hostname"`
	Zone        string    `json:"zone"`
	AppID       string    `json:"app_id"`
	AppName     string    `json:"app_name,omitempty"`
	ProjectID   string    `json:"project_id,omitempty"`
	TenantID    string    `json:"tenant_id"`
	Status      string    `json:"status"`
	TLSStatus   string    `json:"tls_status,omitempty"`
	RoutePolicy string    `json:"route_policy"`
	EdgeGroupID string    `json:"edge_group_id,omitempty"`
	DNSKind     string    `json:"dns_record_kind"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func NormalizeEdgeRoutePolicy(policy string) string {
	switch policy {
	case EdgeRoutePolicyCanary, EdgeRoutePolicyEnabled:
		return policy
	case EdgeRoutePolicyRouteAOnly, "":
		return EdgeRoutePolicyRouteAOnly
	default:
		return ""
	}
}

func EdgeRoutePolicyAllowsTraffic(policy string) bool {
	switch NormalizeEdgeRoutePolicy(policy) {
	case EdgeRoutePolicyCanary, EdgeRoutePolicyEnabled:
		return true
	default:
		return false
	}
}

type EdgeTLSAllowlistEntry struct {
	Hostname  string `json:"hostname"`
	AppID     string `json:"app_id"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	TLSStatus string `json:"tls_status,omitempty"`
}

type EdgeDNSBundle struct {
	SchemaVersion      string            `json:"schema_version,omitempty"`
	Version            string            `json:"version"`
	Generation         string            `json:"generation,omitempty"`
	PreviousGeneration string            `json:"previous_generation,omitempty"`
	GeneratedAt        time.Time         `json:"generated_at"`
	ValidUntil         time.Time         `json:"valid_until,omitempty"`
	Issuer             string            `json:"issuer,omitempty"`
	KeyID              string            `json:"key_id,omitempty"`
	Signature          string            `json:"signature,omitempty"`
	Signatures         []BundleSignature `json:"signatures,omitempty"`
	DNSNodeID          string            `json:"dns_node_id,omitempty"`
	EdgeGroupID        string            `json:"edge_group_id,omitempty"`
	Zone               string            `json:"zone"`
	Records            []EdgeDNSRecord   `json:"records"`
}

type EdgeDNSRecord struct {
	Name                string   `json:"name"`
	Type                string   `json:"type"`
	Values              []string `json:"values"`
	TTL                 int      `json:"ttl"`
	RecordKind          string   `json:"record_kind"`
	AppID               string   `json:"app_id,omitempty"`
	TenantID            string   `json:"tenant_id,omitempty"`
	EdgeGroupID         string   `json:"edge_group_id,omitempty"`
	FallbackEdgeGroupID string   `json:"fallback_edge_group_id,omitempty"`
	Status              string   `json:"status"`
	StatusReason        string   `json:"status_reason,omitempty"`
	RecordGeneration    string   `json:"record_generation"`
}

type DNSACMEChallenge struct {
	ID        string    `json:"id"`
	Zone      string    `json:"zone"`
	Name      string    `json:"name"`
	Value     string    `json:"value"`
	TTL       int       `json:"ttl"`
	Owner     string    `json:"owner,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type EdgeNode struct {
	ID                  string     `json:"id"`
	EdgeGroupID         string     `json:"edge_group_id"`
	Region              string     `json:"region,omitempty"`
	Country             string     `json:"country,omitempty"`
	PublicHostname      string     `json:"public_hostname,omitempty"`
	PublicIPv4          string     `json:"public_ipv4,omitempty"`
	PublicIPv6          string     `json:"public_ipv6,omitempty"`
	MeshIP              string     `json:"mesh_ip,omitempty"`
	Status              string     `json:"status"`
	Healthy             bool       `json:"healthy"`
	Draining            bool       `json:"draining"`
	RouteBundleVersion  string     `json:"route_bundle_version,omitempty"`
	DNSBundleVersion    string     `json:"dns_bundle_version,omitempty"`
	ServingGeneration   string     `json:"serving_generation,omitempty"`
	LKGGeneration       string     `json:"lkg_generation,omitempty"`
	CaddyRouteCount     int        `json:"caddy_route_count"`
	CaddyAppliedVersion string     `json:"caddy_applied_version,omitempty"`
	CaddyLastError      string     `json:"caddy_last_error,omitempty"`
	CacheStatus         string     `json:"cache_status,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
	TokenPrefix         string     `json:"token_prefix,omitempty"`
	TokenHash           string     `json:"token_hash,omitempty"`
	LastSeenAt          *time.Time `json:"last_seen_at,omitempty"`
	LastHeartbeatAt     *time.Time `json:"last_heartbeat_at,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

type DNSNode struct {
	ID                string            `json:"id"`
	EdgeGroupID       string            `json:"edge_group_id"`
	PublicHostname    string            `json:"public_hostname,omitempty"`
	PublicIPv4        string            `json:"public_ipv4,omitempty"`
	PublicIPv6        string            `json:"public_ipv6,omitempty"`
	MeshIP            string            `json:"mesh_ip,omitempty"`
	Zone              string            `json:"zone"`
	Status            string            `json:"status"`
	Healthy           bool              `json:"healthy"`
	DNSBundleVersion  string            `json:"dns_bundle_version,omitempty"`
	ServingGeneration string            `json:"serving_generation,omitempty"`
	LKGGeneration     string            `json:"lkg_generation,omitempty"`
	RecordCount       int               `json:"record_count"`
	CacheStatus       string            `json:"cache_status,omitempty"`
	CacheWriteErrors  uint64            `json:"cache_write_errors"`
	CacheLoadErrors   uint64            `json:"cache_load_errors"`
	BundleSyncErrors  uint64            `json:"bundle_sync_errors"`
	QueryCount        uint64            `json:"query_count"`
	QueryErrorCount   uint64            `json:"query_error_count"`
	QueryRCodeCounts  map[string]uint64 `json:"query_rcode_counts,omitempty"`
	QueryQTypeCounts  map[string]uint64 `json:"query_qtype_counts,omitempty"`
	ListenAddr        string            `json:"listen_addr,omitempty"`
	UDPAddr           string            `json:"udp_addr,omitempty"`
	TCPAddr           string            `json:"tcp_addr,omitempty"`
	UDPListen         bool              `json:"udp_listen"`
	TCPListen         bool              `json:"tcp_listen"`
	LastError         string            `json:"last_error,omitempty"`
	LastSeenAt        *time.Time        `json:"last_seen_at,omitempty"`
	LastHeartbeatAt   *time.Time        `json:"last_heartbeat_at,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type DNSDelegationRecord struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Values  []string `json:"values"`
	TTL     int      `json:"ttl,omitempty"`
	Comment string   `json:"comment,omitempty"`
}

type DNSDelegationPlan struct {
	CurrentParentNS       []string              `json:"current_parent_ns"`
	PlannedARecords       []DNSDelegationRecord `json:"planned_a_records"`
	PlannedNSRecords      []DNSDelegationRecord `json:"planned_ns_records"`
	RollbackDeleteRecords []DNSDelegationRecord `json:"rollback_delete_records"`
	Notes                 []string              `json:"notes,omitempty"`
}

type DNSDelegationPreflightCheck struct {
	Name    string `json:"name"`
	Pass    bool   `json:"pass"`
	Message string `json:"message,omitempty"`
}

type DNSDelegationNodeCheck struct {
	DNSNodeID           string     `json:"dns_node_id"`
	EdgeGroupID         string     `json:"edge_group_id,omitempty"`
	PublicIP            string     `json:"public_ip,omitempty"`
	Zone                string     `json:"zone,omitempty"`
	Status              string     `json:"status,omitempty"`
	Healthy             bool       `json:"healthy"`
	DNSBundleVersion    string     `json:"dns_bundle_version,omitempty"`
	RecordCount         int        `json:"record_count"`
	CacheStatus         string     `json:"cache_status,omitempty"`
	CacheWriteErrors    uint64     `json:"cache_write_errors"`
	CacheLoadErrors     uint64     `json:"cache_load_errors"`
	BundleSyncErrors    uint64     `json:"bundle_sync_errors"`
	QueryCount          uint64     `json:"query_count"`
	QueryErrorCount     uint64     `json:"query_error_count"`
	UDP53Reachable      bool       `json:"udp_53_reachable"`
	TCP53Reachable      bool       `json:"tcp_53_reachable"`
	ProbePass           bool       `json:"probe_pass"`
	ProbeAnswers        []string   `json:"probe_answers,omitempty"`
	KubernetesNodeKnown bool       `json:"kubernetes_node_known"`
	NodeReady           bool       `json:"node_ready"`
	NodeDiskPressure    bool       `json:"node_disk_pressure"`
	LastSeenAt          *time.Time `json:"last_seen_at,omitempty"`
	Pass                bool       `json:"pass"`
	Message             string     `json:"message,omitempty"`
}

type DNSDelegationPreflightResponse struct {
	Pass             bool                          `json:"pass"`
	Zone             string                        `json:"zone"`
	ProbeName        string                        `json:"probe_name"`
	MinHealthyNodes  int                           `json:"min_healthy_nodes"`
	HealthyNodeCount int                           `json:"healthy_node_count"`
	DNSBundleVersion string                        `json:"dns_bundle_version,omitempty"`
	GeneratedAt      time.Time                     `json:"generated_at"`
	Checks           []DNSDelegationPreflightCheck `json:"checks"`
	Nodes            []DNSDelegationNodeCheck      `json:"nodes"`
	DelegationPlan   DNSDelegationPlan             `json:"delegation_plan"`
}

type EdgeGroup struct {
	ID               string     `json:"id"`
	Region           string     `json:"region,omitempty"`
	Country          string     `json:"country,omitempty"`
	Status           string     `json:"status,omitempty"`
	NodeCount        int        `json:"node_count"`
	HealthyNodeCount int        `json:"healthy_node_count"`
	HasHealthyNodes  bool       `json:"has_healthy_nodes"`
	LastSeenAt       *time.Time `json:"last_seen_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

func NormalizeEdgeHealthStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case EdgeHealthHealthy, EdgeHealthDegraded, EdgeHealthUnhealthy:
		return strings.TrimSpace(strings.ToLower(status))
	case EdgeHealthUnknown, "":
		return EdgeHealthUnknown
	default:
		return ""
	}
}
