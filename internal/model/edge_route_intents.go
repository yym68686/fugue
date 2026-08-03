package model

import "time"

const (
	EdgeRouteIntentSchemaVersionV1 = "edge-route-intent/v1"

	EdgeRouteIntentGroupModeAllGroups   = "all_groups"
	EdgeRouteIntentGroupModePinnedGroup = "pinned_group"
)

// EdgeRouteIntentSnapshot is Core's inventory-independent desired-route
// projection. Edge health, epoch, slot, selection, redundancy, and serving
// availability are deliberately absent; those belong to Edge Control.
type EdgeRouteIntentSnapshot struct {
	SchemaVersion string                  `json:"schema_version"`
	Generation    string                  `json:"generation"`
	GeneratedAt   time.Time               `json:"generated_at"`
	Routes        []EdgeRouteIntent       `json:"routes"`
	TLSAllowlist  []EdgeTLSAllowlistEntry `json:"tls_allowlist"`
	CachePolicies []CachePolicy           `json:"cache_policies,omitempty"`
}

type EdgeRouteIntent struct {
	Generation           string                  `json:"generation"`
	Hostname             string                  `json:"hostname"`
	PathPrefix           string                  `json:"path_prefix,omitempty"`
	RouteKind            string                  `json:"route_kind"`
	AppID                string                  `json:"app_id"`
	TenantID             string                  `json:"tenant_id"`
	RuntimeID            string                  `json:"runtime_id"`
	RuntimeType          string                  `json:"runtime_type,omitempty"`
	RuntimeEdgeGroupID   string                  `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode   string                  `json:"runtime_cluster_node,omitempty"`
	TargetGroupMode      string                  `json:"target_group_mode"`
	PinnedEdgeGroupID    string                  `json:"pinned_edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string                `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string                `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string                  `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time              `json:"exclusion_expires_at,omitempty"`
	ExclusionLifecycle   string                  `json:"exclusion_lifecycle,omitempty"`
	MinHealthyEdgeNodes  int                     `json:"min_healthy_edge_nodes,omitempty"`
	RoutePolicy          string                  `json:"route_policy"`
	UpstreamKind         string                  `json:"upstream_kind"`
	UpstreamScope        string                  `json:"upstream_scope,omitempty"`
	UpstreamURL          string                  `json:"upstream_url,omitempty"`
	Upstreams            []EdgeRouteUpstream     `json:"upstreams,omitempty"`
	ServicePort          int                     `json:"service_port"`
	TLSPolicy            string                  `json:"tls_policy"`
	CachePolicyID        string                  `json:"cache_policy_id,omitempty"`
	CacheNamespace       string                  `json:"cache_namespace,omitempty"`
	DeploymentGeneration string                  `json:"deployment_generation,omitempty"`
	RequestBodyPolicies  []EdgeRequestBodyPolicy `json:"request_body_policies,omitempty"`
	Streaming            bool                    `json:"streaming"`
	OriginStatus         string                  `json:"origin_status"`
	OriginStatusReason   string                  `json:"origin_status_reason,omitempty"`
	CreatedAt            time.Time               `json:"created_at"`
	UpdatedAt            time.Time               `json:"updated_at"`
}
