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
	EdgeRoutePolicyPrimary = "primary"
)

const (
	EdgeRouteUpstreamKindKubernetesService = "kubernetes-service"
)

const (
	EdgeRouteTLSPolicyPlatform     = "platform"
	EdgeRouteTLSPolicyCustomDomain = "custom-domain"
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

type EdgeTLSAllowlistEntry struct {
	Hostname  string `json:"hostname"`
	AppID     string `json:"app_id"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	TLSStatus string `json:"tls_status,omitempty"`
}
