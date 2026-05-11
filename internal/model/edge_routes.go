package model

import "time"

const (
	EdgeRouteKindPlatform     = "platform"
	EdgeRouteKindCustomDomain = "custom-domain"
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
)

const (
	EdgeRouteTLSPolicyPlatform     = "platform"
	EdgeRouteTLSPolicyCustomDomain = "custom-domain"
)

const (
	EdgeDNSRecordKindCustomDomainTarget = "custom-domain-target"
	EdgeDNSRecordKindProbe              = "probe"
)

const (
	EdgeDNSRecordTypeA    = "A"
	EdgeDNSRecordTypeAAAA = "AAAA"
)

type EdgeRouteBundle struct {
	Version      string                  `json:"version"`
	GeneratedAt  time.Time               `json:"generated_at"`
	EdgeID       string                  `json:"edge_id,omitempty"`
	EdgeGroupID  string                  `json:"edge_group_id,omitempty"`
	Routes       []EdgeRouteBinding      `json:"routes"`
	TLSAllowlist []EdgeTLSAllowlistEntry `json:"tls_allowlist"`
}

type EdgeRouteBinding struct {
	Hostname            string    `json:"hostname"`
	RouteKind           string    `json:"route_kind"`
	AppID               string    `json:"app_id"`
	TenantID            string    `json:"tenant_id"`
	RuntimeID           string    `json:"runtime_id"`
	EdgeGroupID         string    `json:"edge_group_id"`
	FallbackEdgeGroupID string    `json:"fallback_edge_group_id,omitempty"`
	RoutePolicy         string    `json:"route_policy"`
	UpstreamKind        string    `json:"upstream_kind"`
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
	Version     string          `json:"version"`
	GeneratedAt time.Time       `json:"generated_at"`
	DNSNodeID   string          `json:"dns_node_id,omitempty"`
	EdgeGroupID string          `json:"edge_group_id,omitempty"`
	Zone        string          `json:"zone"`
	Records     []EdgeDNSRecord `json:"records"`
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
