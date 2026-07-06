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
	CachePolicyKindStaticAssets  = "static-assets"
	CachePolicyKindHTMLDocuments = "html-documents"
	CachePolicyKindDisabled      = "disabled"

	CachePolicyPurgeModeGeneration = "generation"
	CachePolicyPurgeModeNone       = "none"
)

const (
	DNSAnswerPolicyKindGlobal       = "global"
	DNSAnswerPolicyKindGeo          = "geo"
	DNSAnswerPolicyKindWeighted     = "weighted"
	DNSAnswerPolicyKindLatencyAware = "latency_aware"
	DNSAnswerPolicyKindPinned       = "pinned"
	DNSAnswerPolicyKindDisabled     = "disabled"
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

const (
	EdgeTLSStatusPending = "pending"
	EdgeTLSStatusReady   = "ready"
	EdgeTLSStatusError   = "error"
)

const (
	EdgeWorkloadModeStatic  = "static"
	EdgeWorkloadModeDynamic = "dynamic"
)

const (
	EdgeCanaryStateJoined  = "joined"
	EdgeCanaryStateWarming = "warming"
	EdgeCanaryStateProbing = "probing"
	EdgeCanaryStateCanary  = "canary"
	EdgeCanaryStateActive  = "active"
	EdgeCanaryStateDrained = "drained"
)

const (
	EdgePublicProbeStatusUnknown = "unknown"
	EdgePublicProbeStatusPassing = "passing"
	EdgePublicProbeStatusFailing = "failing"
)

func NormalizeEdgeWorkloadMode(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case EdgeWorkloadModeStatic:
		return EdgeWorkloadModeStatic
	case EdgeWorkloadModeDynamic:
		return EdgeWorkloadModeDynamic
	default:
		return ""
	}
}

func NormalizeEdgeCanaryState(state string) string {
	switch strings.TrimSpace(strings.ToLower(state)) {
	case EdgeCanaryStateJoined:
		return EdgeCanaryStateJoined
	case EdgeCanaryStateWarming:
		return EdgeCanaryStateWarming
	case EdgeCanaryStateProbing:
		return EdgeCanaryStateProbing
	case EdgeCanaryStateCanary:
		return EdgeCanaryStateCanary
	case EdgeCanaryStateActive:
		return EdgeCanaryStateActive
	case EdgeCanaryStateDrained:
		return EdgeCanaryStateDrained
	default:
		return ""
	}
}

func NormalizeEdgePublicProbeStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case EdgePublicProbeStatusUnknown:
		return EdgePublicProbeStatusUnknown
	case EdgePublicProbeStatusPassing:
		return EdgePublicProbeStatusPassing
	case EdgePublicProbeStatusFailing:
		return EdgePublicProbeStatusFailing
	default:
		return ""
	}
}

func NormalizeEdgeTLSStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case EdgeTLSStatusPending:
		return EdgeTLSStatusPending
	case EdgeTLSStatusReady:
		return EdgeTLSStatusReady
	case EdgeTLSStatusError:
		return EdgeTLSStatusError
	default:
		return ""
	}
}

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
	CachePolicies      []CachePolicy           `json:"cache_policies,omitempty"`
}

type EdgeRouteBinding struct {
	Hostname             string              `json:"hostname"`
	PathPrefix           string              `json:"path_prefix,omitempty"`
	RouteKind            string              `json:"route_kind"`
	AppID                string              `json:"app_id"`
	TenantID             string              `json:"tenant_id"`
	RuntimeID            string              `json:"runtime_id"`
	RuntimeType          string              `json:"runtime_type,omitempty"`
	RuntimeEdgeGroup     string              `json:"runtime_edge_group,omitempty"`
	RuntimeEdgeGroupID   string              `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode   string              `json:"runtime_cluster_node,omitempty"`
	SelectedEdgeGroup    string              `json:"selected_edge_group,omitempty"`
	EdgeGroupID          string              `json:"edge_group_id"`
	FallbackEdgeGroupID  string              `json:"fallback_edge_group_id,omitempty"`
	PolicyEdgeGroupID    string              `json:"policy_edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string            `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string            `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string              `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time          `json:"exclusion_expires_at,omitempty"`
	RoutePolicy          string              `json:"route_policy"`
	SelectionReason      string              `json:"selection_reason,omitempty"`
	FallbackReason       string              `json:"fallback_reason,omitempty"`
	UpstreamKind         string              `json:"upstream_kind"`
	UpstreamScope        string              `json:"upstream_scope,omitempty"`
	UpstreamURL          string              `json:"upstream_url,omitempty"`
	Upstreams            []EdgeRouteUpstream `json:"upstreams,omitempty"`
	ServicePort          int                 `json:"service_port"`
	TLSPolicy            string              `json:"tls_policy"`
	CachePolicyID        string              `json:"cache_policy_id,omitempty"`
	CacheNamespace       string              `json:"cache_namespace,omitempty"`
	DeploymentGeneration string              `json:"deployment_generation,omitempty"`
	Streaming            bool                `json:"streaming"`
	Status               string              `json:"status"`
	StatusReason         string              `json:"status_reason,omitempty"`
	RouteGeneration      string              `json:"route_generation"`
	CreatedAt            time.Time           `json:"created_at"`
	UpdatedAt            time.Time           `json:"updated_at"`
}

type EdgeRouteUpstream struct {
	Role                 string `json:"role,omitempty"`
	ReleaseID            string `json:"release_id,omitempty"`
	Weight               int    `json:"weight"`
	UpstreamKind         string `json:"upstream_kind,omitempty"`
	UpstreamScope        string `json:"upstream_scope,omitempty"`
	UpstreamURL          string `json:"upstream_url"`
	ServicePort          int    `json:"service_port,omitempty"`
	RuntimeID            string `json:"runtime_id,omitempty"`
	DeploymentGeneration string `json:"deployment_generation,omitempty"`
	Status               string `json:"status,omitempty"`
	StatusReason         string `json:"status_reason,omitempty"`
}

type EdgeRoutePolicy struct {
	ID                   string     `json:"id"`
	Hostname             string     `json:"hostname"`
	AppID                string     `json:"app_id"`
	TenantID             string     `json:"tenant_id"`
	EdgeGroupID          string     `json:"edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string   `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string   `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string     `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time `json:"exclusion_expires_at,omitempty"`
	RoutePolicy          string     `json:"route_policy"`
	Enabled              bool       `json:"enabled"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
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

type CachePolicy struct {
	ID                          string   `json:"id"`
	Kind                        string   `json:"kind"`
	HostnameScope               string   `json:"hostname_scope,omitempty"`
	PathPatterns                []string `json:"path_patterns,omitempty"`
	MethodAllowlist             []string `json:"method_allowlist,omitempty"`
	StatusAllowlist             []int    `json:"status_allowlist,omitempty"`
	TTLSeconds                  int      `json:"ttl_seconds,omitempty"`
	StaleWhileRevalidateSeconds int      `json:"stale_while_revalidate_seconds,omitempty"`
	BrowserCacheControl         string   `json:"browser_cache_control,omitempty"`
	EdgeCacheControl            string   `json:"edge_cache_control,omitempty"`
	BypassOnAuthorization       bool     `json:"bypass_on_authorization,omitempty"`
	BypassOnCookie              bool     `json:"bypass_on_cookie,omitempty"`
	VaryAllowlist               []string `json:"vary_allowlist,omitempty"`
	PurgeMode                   string   `json:"purge_mode,omitempty"`
}

type DNSAnswerPolicy struct {
	PolicyKind                string   `json:"policy_kind"`
	AllowedEdgeGroups         []string `json:"allowed_edge_groups,omitempty"`
	PreferredEdgeGroups       []string `json:"preferred_edge_groups,omitempty"`
	FallbackEdgeGroups        []string `json:"fallback_edge_groups,omitempty"`
	TTLSeconds                int      `json:"ttl_seconds,omitempty"`
	ECSEnabled                bool     `json:"ecs_enabled,omitempty"`
	HealthRequired            bool     `json:"health_required,omitempty"`
	RouteReadyRequired        bool     `json:"route_ready_required,omitempty"`
	ExplorationPercent        int      `json:"exploration_percent,omitempty"`
	SwitchCooldownSec         int      `json:"switch_cooldown_seconds,omitempty"`
	Region                    string   `json:"region,omitempty"`
	Country                   string   `json:"country,omitempty"`
	Priority                  int      `json:"priority,omitempty"`
	Weight                    int      `json:"weight,omitempty"`
	Reason                    string   `json:"reason,omitempty"`
	SelectedEdgeGroupID       string   `json:"selected_edge_group_id,omitempty"`
	ShadowSelectedEdgeGroupID string   `json:"shadow_selected_edge_group_id,omitempty"`
	ShadowReason              string   `json:"shadow_reason,omitempty"`
}

type EdgeDNSAnswerCandidate struct {
	IP                string             `json:"ip"`
	EdgeID            string             `json:"edge_id,omitempty"`
	EdgeGroupID       string             `json:"edge_group_id,omitempty"`
	Region            string             `json:"region,omitempty"`
	Country           string             `json:"country,omitempty"`
	WorkloadMode      string             `json:"workload_mode,omitempty"`
	CanaryState       string             `json:"canary_state,omitempty"`
	CanaryWeight      int                `json:"canary_weight,omitempty"`
	PublicProbeStatus string             `json:"public_probe_status,omitempty"`
	DNSEligible       bool               `json:"dns_eligible,omitempty"`
	Priority          int                `json:"priority,omitempty"`
	Weight            int                `json:"weight,omitempty"`
	Reason            string             `json:"reason,omitempty"`
	TrafficClass      string             `json:"traffic_class,omitempty"`
	Score             float64            `json:"score,omitempty"`
	ScoreBreakdown    map[string]float64 `json:"score_breakdown,omitempty"`
	Healthy           bool               `json:"healthy,omitempty"`
	RouteReady        bool               `json:"route_ready,omitempty"`
	TLSReady          bool               `json:"tls_ready,omitempty"`
}

type EdgeDNSScopedAnswerCandidates struct {
	ScopeKey            string                   `json:"scope_key"`
	Country             string                   `json:"country,omitempty"`
	Region              string                   `json:"region,omitempty"`
	ASN                 string                   `json:"asn,omitempty"`
	PolicyKind          string                   `json:"policy_kind,omitempty"`
	Reason              string                   `json:"reason,omitempty"`
	SelectedEdgeGroupID string                   `json:"selected_edge_group_id,omitempty"`
	CooldownUntil       time.Time                `json:"cooldown_until,omitempty"`
	Candidates          []EdgeDNSAnswerCandidate `json:"candidates,omitempty"`
}

type EdgePerformanceSample struct {
	ID                        string    `json:"id,omitempty"`
	EdgeID                    string    `json:"edge_id,omitempty"`
	EdgeGroupID               string    `json:"edge_group_id"`
	Hostname                  string    `json:"hostname"`
	PathPrefix                string    `json:"path_prefix,omitempty"`
	Method                    string    `json:"method,omitempty"`
	TrafficClass              string    `json:"traffic_class,omitempty"`
	ClientCountry             string    `json:"client_country,omitempty"`
	ClientRegion              string    `json:"client_region,omitempty"`
	ClientASN                 string    `json:"client_asn,omitempty"`
	RuntimeRegion             string    `json:"runtime_region,omitempty"`
	RouteGeneration           string    `json:"route_generation,omitempty"`
	CacheStatus               string    `json:"cache_status,omitempty"`
	DNSPolicy                 string    `json:"dns_policy,omitempty"`
	TLSHandshakeMS            int64     `json:"tls_handshake_ms,omitempty"`
	TTFBMS                    int64     `json:"ttfb_ms,omitempty"`
	UpstreamMS                int64     `json:"upstream_ms,omitempty"`
	TotalMS                   int64     `json:"total_ms,omitempty"`
	StatusCode                int       `json:"status_code,omitempty"`
	SampleCount               int       `json:"sample_count,omitempty"`
	CacheHitCount             int       `json:"cache_hit_count,omitempty"`
	CacheObservationCount     int       `json:"cache_observation_count,omitempty"`
	ErrorCount                int       `json:"error_count,omitempty"`
	UploadRequestCount        int       `json:"upload_request_count,omitempty"`
	BodyBufferCount           int       `json:"body_buffer_count,omitempty"`
	BodyReadBlockMS           int64     `json:"body_read_block_ms,omitempty"`
	FileWriteMS               int64     `json:"file_write_ms,omitempty"`
	UploadEffectiveBPS        int64     `json:"upload_effective_bps,omitempty"`
	MinWindowBPS              int64     `json:"min_window_bps,omitempty"`
	MaxReadGapMS              int64     `json:"max_read_gap_ms,omitempty"`
	RequestBodyBytes          int64     `json:"request_body_bytes,omitempty"`
	RequestBodyReadBytes      int64     `json:"request_body_read_bytes,omitempty"`
	BodyIncompleteCount       int       `json:"body_incomplete_count,omitempty"`
	BodyReadErrorCount        int       `json:"body_read_error_count,omitempty"`
	ResponseWriteMS           int64     `json:"response_write_ms,omitempty"`
	ResponseBytes             int64     `json:"response_bytes,omitempty"`
	ResponseEgressBPS         int64     `json:"response_egress_bps,omitempty"`
	OriginDNSMS               int64     `json:"origin_dns_ms,omitempty"`
	OriginConnectMS           int64     `json:"origin_connect_ms,omitempty"`
	OriginRequestWriteMS      int64     `json:"origin_request_write_ms,omitempty"`
	OriginResponseWaitMS      int64     `json:"origin_response_wait_ms,omitempty"`
	OriginTTFBMS              int64     `json:"origin_ttfb_ms,omitempty"`
	OriginTotalMS             int64     `json:"origin_total_ms,omitempty"`
	StreamingRequestCount     int       `json:"streaming_request_count,omitempty"`
	WebSocketRequestCount     int       `json:"websocket_request_count,omitempty"`
	SSERequestCount           int       `json:"sse_request_count,omitempty"`
	ClientCancelCount         int       `json:"client_cancel_count,omitempty"`
	ActiveRequests            int       `json:"active_requests,omitempty"`
	ActiveBodyBuffers         int       `json:"active_body_buffers,omitempty"`
	GoroutineCount            int       `json:"goroutine_count,omitempty"`
	MemoryAllocBytes          int64     `json:"memory_alloc_bytes,omitempty"`
	ClientTCPRTTMS            float64   `json:"client_tcp_rtt_ms,omitempty"`
	ClientTCPMinRTTMS         float64   `json:"client_tcp_min_rtt_ms,omitempty"`
	ClientTCPRTTVarMS         float64   `json:"client_tcp_rttvar_ms,omitempty"`
	ClientTCPTotalRetrans     int64     `json:"client_tcp_total_retrans,omitempty"`
	ClientTCPRetransRate      float64   `json:"client_tcp_retrans_rate,omitempty"`
	ClientTCPBytesRetrans     int64     `json:"client_tcp_bytes_retrans,omitempty"`
	ClientTCPBytesRetransRate float64   `json:"client_tcp_bytes_retrans_rate,omitempty"`
	ClientTCPTotalRTO         int64     `json:"client_tcp_total_rto,omitempty"`
	ClientTCPRTORate          float64   `json:"client_tcp_rto_rate,omitempty"`
	ClientTCPDeliveryBPS      int64     `json:"client_tcp_delivery_rate_bps,omitempty"`
	SampledAt                 time.Time `json:"sampled_at"`
}

type EdgeNodeQualityResponse struct {
	Node        EdgeNode               `json:"node"`
	Group       EdgeGroup              `json:"group"`
	Summary     EdgeNodeQualitySummary `json:"summary"`
	Routes      []EdgeNodeQualityRoute `json:"routes,omitempty"`
	GeneratedAt time.Time              `json:"generated_at"`
}

type EdgeNodeQualitySummary struct {
	EdgeID                    string     `json:"edge_id"`
	EdgeGroupID               string     `json:"edge_group_id"`
	Since                     time.Time  `json:"since"`
	SampleRecordCount         int        `json:"sample_record_count"`
	RequestCount              int        `json:"request_count"`
	ErrorCount                int        `json:"error_count"`
	ErrorRate                 float64    `json:"error_rate"`
	AvgTLSHandshakeMS         float64    `json:"avg_tls_handshake_ms"`
	AvgTTFBMS                 float64    `json:"avg_ttfb_ms"`
	AvgUpstreamMS             float64    `json:"avg_upstream_ms"`
	AvgTotalMS                float64    `json:"avg_total_ms"`
	AvgUploadBPS              float64    `json:"avg_upload_bps"`
	MinUploadBPS              int64      `json:"min_upload_bps,omitempty"`
	AvgBodyReadMS             float64    `json:"avg_body_read_ms"`
	AvgMaxReadGapMS           float64    `json:"avg_max_read_gap_ms"`
	BodyIncompleteCount       int        `json:"body_incomplete_count"`
	BodyReadErrorCount        int        `json:"body_read_error_count"`
	AvgResponseEgressBPS      float64    `json:"avg_response_egress_bps"`
	AvgResponseWriteMS        float64    `json:"avg_response_write_ms"`
	AvgOriginDNSMS            float64    `json:"avg_origin_dns_ms"`
	AvgOriginConnectMS        float64    `json:"avg_origin_connect_ms"`
	AvgOriginWriteMS          float64    `json:"avg_origin_write_ms"`
	AvgOriginWaitMS           float64    `json:"avg_origin_wait_ms"`
	AvgOriginTTFBMS           float64    `json:"avg_origin_ttfb_ms"`
	AvgOriginTotalMS          float64    `json:"avg_origin_total_ms"`
	AvgActiveRequests         float64    `json:"avg_active_requests"`
	AvgActiveBodyBuffers      float64    `json:"avg_active_body_buffers"`
	AvgClientTCPRTTMS         float64    `json:"avg_client_tcp_rtt_ms"`
	AvgClientTCPMinRTTMS      float64    `json:"avg_client_tcp_min_rtt_ms"`
	AvgClientTCPRTTVarMS      float64    `json:"avg_client_tcp_rttvar_ms"`
	ClientTCPRetransRate      float64    `json:"client_tcp_retrans_rate"`
	ClientTCPBytesRetransRate float64    `json:"client_tcp_bytes_retrans_rate"`
	ClientTCPRTORate          float64    `json:"client_tcp_rto_rate"`
	AvgClientTCPDeliveryBPS   float64    `json:"avg_client_tcp_delivery_rate_bps"`
	CacheHitCount             int        `json:"cache_hit_count"`
	CacheObservationCount     int        `json:"cache_observation_count"`
	CacheHitRate              float64    `json:"cache_hit_rate"`
	TLSStatus                 string     `json:"tls_status,omitempty"`
	TLSLastMessage            string     `json:"tls_last_message,omitempty"`
	TLSReadyAt                *time.Time `json:"tls_ready_at,omitempty"`
	CacheStatus               string     `json:"cache_status,omitempty"`
	CaddyRouteCount           int        `json:"caddy_route_count"`
	RouteBundleVersion        string     `json:"route_bundle_version,omitempty"`
	DNSBundleVersion          string     `json:"dns_bundle_version,omitempty"`
	LastSampledAt             *time.Time `json:"last_sampled_at,omitempty"`
}

type EdgeNodeQualityRoute struct {
	Hostname                  string     `json:"hostname"`
	PathPrefix                string     `json:"path_prefix,omitempty"`
	Method                    string     `json:"method,omitempty"`
	TrafficClass              string     `json:"traffic_class,omitempty"`
	SampleRecordCount         int        `json:"sample_record_count"`
	RequestCount              int        `json:"request_count"`
	ErrorCount                int        `json:"error_count"`
	ErrorRate                 float64    `json:"error_rate"`
	AvgTLSHandshakeMS         float64    `json:"avg_tls_handshake_ms"`
	AvgTTFBMS                 float64    `json:"avg_ttfb_ms"`
	AvgUpstreamMS             float64    `json:"avg_upstream_ms"`
	AvgTotalMS                float64    `json:"avg_total_ms"`
	AvgUploadBPS              float64    `json:"avg_upload_bps"`
	MinUploadBPS              int64      `json:"min_upload_bps,omitempty"`
	AvgBodyReadMS             float64    `json:"avg_body_read_ms"`
	AvgMaxReadGapMS           float64    `json:"avg_max_read_gap_ms"`
	BodyIncompleteCount       int        `json:"body_incomplete_count"`
	BodyReadErrorCount        int        `json:"body_read_error_count"`
	AvgResponseEgressBPS      float64    `json:"avg_response_egress_bps"`
	AvgResponseWriteMS        float64    `json:"avg_response_write_ms"`
	AvgOriginDNSMS            float64    `json:"avg_origin_dns_ms"`
	AvgOriginConnectMS        float64    `json:"avg_origin_connect_ms"`
	AvgOriginWriteMS          float64    `json:"avg_origin_write_ms"`
	AvgOriginWaitMS           float64    `json:"avg_origin_wait_ms"`
	AvgOriginTTFBMS           float64    `json:"avg_origin_ttfb_ms"`
	AvgOriginTotalMS          float64    `json:"avg_origin_total_ms"`
	AvgActiveRequests         float64    `json:"avg_active_requests"`
	AvgActiveBodyBuffers      float64    `json:"avg_active_body_buffers"`
	AvgClientTCPRTTMS         float64    `json:"avg_client_tcp_rtt_ms"`
	AvgClientTCPMinRTTMS      float64    `json:"avg_client_tcp_min_rtt_ms"`
	AvgClientTCPRTTVarMS      float64    `json:"avg_client_tcp_rttvar_ms"`
	ClientTCPRetransRate      float64    `json:"client_tcp_retrans_rate"`
	ClientTCPBytesRetransRate float64    `json:"client_tcp_bytes_retrans_rate"`
	ClientTCPRTORate          float64    `json:"client_tcp_rto_rate"`
	AvgClientTCPDeliveryBPS   float64    `json:"avg_client_tcp_delivery_rate_bps"`
	CacheHitCount             int        `json:"cache_hit_count"`
	CacheObservationCount     int        `json:"cache_observation_count"`
	CacheHitRate              float64    `json:"cache_hit_rate"`
	LastSampledAt             *time.Time `json:"last_sampled_at,omitempty"`
}

type EdgeQualityRankResponse struct {
	Hostname         string                     `json:"hostname"`
	TrafficClass     string                     `json:"traffic_class,omitempty"`
	Method           string                     `json:"method,omitempty"`
	PathPrefixBucket string                     `json:"path_prefix_bucket,omitempty"`
	RequestedScope   string                     `json:"requested_scope"`
	SelectedScope    string                     `json:"selected_scope"`
	FallbackLevel    int                        `json:"fallback_level"`
	FallbackReason   string                     `json:"fallback_reason,omitempty"`
	Window           string                     `json:"window"`
	Since            time.Time                  `json:"since"`
	GeneratedAt      time.Time                  `json:"generated_at"`
	Candidates       []EdgeQualityRankCandidate `json:"candidates,omitempty"`
	HardGated        []EdgeQualityRankCandidate `json:"hard_gated,omitempty"`
}

type EdgeQualityRankCandidate struct {
	Rank                      int                `json:"rank,omitempty"`
	EdgeID                    string             `json:"edge_id,omitempty"`
	EdgeGroupID               string             `json:"edge_group_id,omitempty"`
	Region                    string             `json:"region,omitempty"`
	Country                   string             `json:"country,omitempty"`
	Healthy                   bool               `json:"healthy,omitempty"`
	Draining                  bool               `json:"draining,omitempty"`
	RouteReady                bool               `json:"route_ready,omitempty"`
	TLSReady                  bool               `json:"tls_ready,omitempty"`
	Excluded                  bool               `json:"excluded,omitempty"`
	ExclusionReason           string             `json:"exclusion_reason,omitempty"`
	Score                     float64            `json:"score,omitempty"`
	ScoreBreakdown            map[string]float64 `json:"score_breakdown,omitempty"`
	Confidence                float64            `json:"confidence,omitempty"`
	ConfidencePenalty         float64            `json:"confidence_penalty,omitempty"`
	SampleRecordCount         int                `json:"sample_record_count,omitempty"`
	RequestCount              int                `json:"request_count,omitempty"`
	ErrorRate                 float64            `json:"error_rate,omitempty"`
	CacheHitRate              float64            `json:"cache_hit_rate,omitempty"`
	AvgTTFBMS                 float64            `json:"avg_ttfb_ms,omitempty"`
	AvgTotalMS                float64            `json:"avg_total_ms,omitempty"`
	AvgUploadBPS              float64            `json:"avg_upload_bps,omitempty"`
	MinUploadBPS              int64              `json:"min_upload_bps,omitempty"`
	AvgResponseEgressBPS      float64            `json:"avg_response_egress_bps,omitempty"`
	ClientTCPRetransRate      float64            `json:"client_tcp_retrans_rate,omitempty"`
	ClientTCPBytesRetransRate float64            `json:"client_tcp_bytes_retrans_rate,omitempty"`
	ClientTCPRTORate          float64            `json:"client_tcp_rto_rate,omitempty"`
	LastSampledAt             *time.Time         `json:"last_sampled_at,omitempty"`
	Reason                    string             `json:"reason,omitempty"`
}

type EdgeQualityRollup struct {
	Window                    string             `json:"window"`
	WindowStartedAt           time.Time          `json:"window_started_at"`
	WindowEndedAt             time.Time          `json:"window_ended_at"`
	Hostname                  string             `json:"hostname"`
	TrafficClass              string             `json:"traffic_class,omitempty"`
	Method                    string             `json:"method,omitempty"`
	PathPrefixBucket          string             `json:"path_prefix_bucket,omitempty"`
	ClientScopeKind           string             `json:"client_scope_kind"`
	ClientScopeValue          string             `json:"client_scope_value"`
	EdgeGroupID               string             `json:"edge_group_id,omitempty"`
	EdgeID                    string             `json:"edge_id,omitempty"`
	SampleCount               int                `json:"sample_count,omitempty"`
	RequestCount              int                `json:"request_count,omitempty"`
	ErrorCount                int                `json:"error_count,omitempty"`
	ErrorRate                 float64            `json:"error_rate,omitempty"`
	CacheHitCount             int                `json:"cache_hit_count,omitempty"`
	CacheObservationCount     int                `json:"cache_observation_count,omitempty"`
	CacheHitRate              float64            `json:"cache_hit_rate,omitempty"`
	P50TTFBMS                 float64            `json:"p50_ttfb_ms,omitempty"`
	P95TTFBMS                 float64            `json:"p95_ttfb_ms,omitempty"`
	P99TTFBMS                 float64            `json:"p99_ttfb_ms,omitempty"`
	AvgUpstreamMS             float64            `json:"avg_upstream_ms,omitempty"`
	AvgTotalMS                float64            `json:"avg_total_ms,omitempty"`
	AvgOriginDNSMS            float64            `json:"avg_origin_dns_ms,omitempty"`
	AvgOriginConnectMS        float64            `json:"avg_origin_connect_ms,omitempty"`
	AvgOriginRequestWriteMS   float64            `json:"avg_origin_request_write_ms,omitempty"`
	AvgOriginResponseWaitMS   float64            `json:"avg_origin_response_wait_ms,omitempty"`
	AvgOriginTTFBMS           float64            `json:"avg_origin_ttfb_ms,omitempty"`
	AvgOriginTotalMS          float64            `json:"avg_origin_total_ms,omitempty"`
	AvgUploadEffectiveBPS     float64            `json:"avg_upload_effective_bps,omitempty"`
	P10UploadEffectiveBPS     float64            `json:"p10_upload_effective_bps,omitempty"`
	AvgMinWindowBPS           float64            `json:"avg_min_window_bps,omitempty"`
	P10MinWindowBPS           float64            `json:"p10_min_window_bps,omitempty"`
	P95MaxReadGapMS           float64            `json:"p95_max_read_gap_ms,omitempty"`
	AvgBodyReadBlockMS        float64            `json:"avg_body_read_block_ms,omitempty"`
	BodyIncompleteRate        float64            `json:"body_incomplete_rate,omitempty"`
	BodyReadErrorRate         float64            `json:"body_read_error_rate,omitempty"`
	AvgResponseEgressBPS      float64            `json:"avg_response_egress_bps,omitempty"`
	P10ResponseEgressBPS      float64            `json:"p10_response_egress_bps,omitempty"`
	P95ResponseWriteMS        float64            `json:"p95_response_write_ms,omitempty"`
	ClientCancelRate          float64            `json:"client_cancel_rate,omitempty"`
	AvgClientTCPRTTMS         float64            `json:"avg_client_tcp_rtt_ms,omitempty"`
	AvgClientTCPMinRTTMS      float64            `json:"avg_client_tcp_min_rtt_ms,omitempty"`
	AvgClientTCPRTTVarMS      float64            `json:"avg_client_tcp_rttvar_ms,omitempty"`
	ClientTCPRetransRate      float64            `json:"client_tcp_retrans_rate,omitempty"`
	ClientTCPBytesRetransRate float64            `json:"client_tcp_bytes_retrans_rate,omitempty"`
	ClientTCPRTORate          float64            `json:"client_tcp_rto_rate,omitempty"`
	AvgClientTCPDeliveryBPS   float64            `json:"avg_client_tcp_delivery_rate_bps,omitempty"`
	AvgActiveRequests         float64            `json:"avg_active_requests,omitempty"`
	AvgActiveBodyBuffers      float64            `json:"avg_active_body_buffers,omitempty"`
	AvgGoroutineCount         float64            `json:"avg_goroutine_count,omitempty"`
	AvgMemoryAllocBytes       float64            `json:"avg_memory_alloc_bytes,omitempty"`
	Confidence                float64            `json:"confidence,omitempty"`
	Score                     float64            `json:"score,omitempty"`
	ScoreBreakdown            map[string]float64 `json:"score_breakdown,omitempty"`
	UpdatedAt                 time.Time          `json:"updated_at"`
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
	Name                string                          `json:"name"`
	Type                string                          `json:"type"`
	Values              []string                        `json:"values"`
	TTL                 int                             `json:"ttl"`
	RecordKind          string                          `json:"record_kind"`
	AppID               string                          `json:"app_id,omitempty"`
	TenantID            string                          `json:"tenant_id,omitempty"`
	EdgeGroupID         string                          `json:"edge_group_id,omitempty"`
	FallbackEdgeGroupID string                          `json:"fallback_edge_group_id,omitempty"`
	Status              string                          `json:"status"`
	StatusReason        string                          `json:"status_reason,omitempty"`
	RecordGeneration    string                          `json:"record_generation"`
	AnswerPolicy        DNSAnswerPolicy                 `json:"answer_policy,omitempty"`
	Candidates          []EdgeDNSAnswerCandidate        `json:"candidates,omitempty"`
	ScopedCandidates    []EdgeDNSScopedAnswerCandidates `json:"scoped_candidates,omitempty"`
}

type EdgeDNSRoutingDecision struct {
	Hostname            string    `json:"hostname"`
	ScopeKey            string    `json:"scope_key"`
	Country             string    `json:"country,omitempty"`
	Region              string    `json:"region,omitempty"`
	ASN                 string    `json:"asn,omitempty"`
	SelectedEdgeGroupID string    `json:"selected_edge_group_id"`
	PreviousEdgeGroupID string    `json:"previous_edge_group_id,omitempty"`
	Reason              string    `json:"reason,omitempty"`
	Score               float64   `json:"score,omitempty"`
	SampleCount         int       `json:"sample_count,omitempty"`
	SwitchedAt          time.Time `json:"switched_at,omitempty"`
	CooldownUntil       time.Time `json:"cooldown_until,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
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
	ID                   string     `json:"id"`
	EdgeGroupID          string     `json:"edge_group_id"`
	WorkloadMode         string     `json:"workload_mode,omitempty"`
	CanaryState          string     `json:"canary_state,omitempty"`
	CanaryWeight         int        `json:"canary_weight,omitempty"`
	PublicProbeStatus    string     `json:"public_probe_status,omitempty"`
	PublicProbeLastError string     `json:"public_probe_last_error,omitempty"`
	PublicProbeLastAt    *time.Time `json:"public_probe_last_at,omitempty"`
	Region               string     `json:"region,omitempty"`
	Country              string     `json:"country,omitempty"`
	PublicHostname       string     `json:"public_hostname,omitempty"`
	PublicIPv4           string     `json:"public_ipv4,omitempty"`
	PublicIPv6           string     `json:"public_ipv6,omitempty"`
	MeshIP               string     `json:"mesh_ip,omitempty"`
	Status               string     `json:"status"`
	Healthy              bool       `json:"healthy"`
	Draining             bool       `json:"draining"`
	RouteBundleVersion   string     `json:"route_bundle_version,omitempty"`
	DNSBundleVersion     string     `json:"dns_bundle_version,omitempty"`
	ServingGeneration    string     `json:"serving_generation,omitempty"`
	LKGGeneration        string     `json:"lkg_generation,omitempty"`
	CaddyRouteCount      int        `json:"caddy_route_count"`
	CaddyAppliedVersion  string     `json:"caddy_applied_version,omitempty"`
	CaddyLastError       string     `json:"caddy_last_error,omitempty"`
	CacheStatus          string     `json:"cache_status,omitempty"`
	TLSStatus            string     `json:"tls_status,omitempty"`
	TLSLastMessage       string     `json:"tls_last_message,omitempty"`
	TLSReadyAt           *time.Time `json:"tls_ready_at,omitempty"`
	LastError            string     `json:"last_error,omitempty"`
	TokenPrefix          string     `json:"token_prefix,omitempty"`
	TokenHash            string     `json:"token_hash,omitempty"`
	LastSeenAt           *time.Time `json:"last_seen_at,omitempty"`
	LastHeartbeatAt      *time.Time `json:"last_heartbeat_at,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
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
