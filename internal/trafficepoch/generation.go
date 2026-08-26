package trafficepoch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"fugue/internal/model"
)

func EdgeRouteGeneration(binding model.EdgeRouteBinding) string {
	payload, _ := json.Marshal(edgeRouteVersionMaterialFromBinding(binding))
	sum := sha256.Sum256(payload)
	return "routegen_" + hex.EncodeToString(sum[:])[:16]
}

type edgeRouteVersionMaterial struct {
	Hostname             string                        `json:"hostname"`
	PathPrefix           string                        `json:"path_prefix,omitempty"`
	RouteKind            string                        `json:"route_kind"`
	AppID                string                        `json:"app_id"`
	TenantID             string                        `json:"tenant_id"`
	RuntimeID            string                        `json:"runtime_id"`
	RuntimeType          string                        `json:"runtime_type,omitempty"`
	RuntimeEdgeGroupID   string                        `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode   string                        `json:"runtime_cluster_node,omitempty"`
	RuntimeEdgeGroup     string                        `json:"runtime_edge_group,omitempty"`
	SelectedEdgeGroup    string                        `json:"selected_edge_group,omitempty"`
	EdgeGroupID          string                        `json:"edge_group_id"`
	FallbackEdgeGroupID  string                        `json:"fallback_edge_group_id,omitempty"`
	PolicyEdgeGroupID    string                        `json:"policy_edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string                      `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string                      `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string                        `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time                    `json:"exclusion_expires_at,omitempty"`
	MinHealthyEdgeNodes  int                           `json:"min_healthy_edge_nodes,omitempty"`
	HealthyEdgeNodeCount int                           `json:"healthy_edge_node_count,omitempty"`
	EdgeRedundancyStatus string                        `json:"edge_redundancy_status,omitempty"`
	EdgeRedundancyReason string                        `json:"edge_redundancy_reason,omitempty"`
	RoutePolicy          string                        `json:"route_policy"`
	SelectionReason      string                        `json:"selection_reason,omitempty"`
	FallbackReason       string                        `json:"fallback_reason,omitempty"`
	UpstreamKind         string                        `json:"upstream_kind"`
	UpstreamScope        string                        `json:"upstream_scope,omitempty"`
	UpstreamURL          string                        `json:"upstream_url,omitempty"`
	Upstreams            []model.EdgeRouteUpstream     `json:"upstreams,omitempty"`
	ServicePort          int                           `json:"service_port"`
	TLSPolicy            string                        `json:"tls_policy"`
	CachePolicyID        string                        `json:"cache_policy_id,omitempty"`
	CacheNamespace       string                        `json:"cache_namespace,omitempty"`
	DeploymentGeneration string                        `json:"deployment_generation,omitempty"`
	RequestBodyPolicies  []model.EdgeRequestBodyPolicy `json:"request_body_policies,omitempty"`
	Streaming            bool                          `json:"streaming"`
	Status               string                        `json:"status"`
	StatusReason         string                        `json:"status_reason,omitempty"`
}

func edgeRouteVersionMaterialFromBinding(binding model.EdgeRouteBinding) edgeRouteVersionMaterial {
	return edgeRouteVersionMaterial{
		Hostname: binding.Hostname, PathPrefix: model.NormalizeAppRoutePathPrefix(binding.PathPrefix), RouteKind: binding.RouteKind,
		AppID: binding.AppID, TenantID: binding.TenantID, RuntimeID: binding.RuntimeID, RuntimeType: binding.RuntimeType,
		RuntimeEdgeGroupID: binding.RuntimeEdgeGroupID, RuntimeClusterNode: binding.RuntimeClusterNode, RuntimeEdgeGroup: binding.RuntimeEdgeGroup,
		SelectedEdgeGroup: binding.SelectedEdgeGroup, EdgeGroupID: binding.EdgeGroupID, FallbackEdgeGroupID: binding.FallbackEdgeGroupID,
		PolicyEdgeGroupID: binding.PolicyEdgeGroupID, ExcludedEdgeIDs: append([]string(nil), binding.ExcludedEdgeIDs...),
		ExcludedEdgeGroupIDs: append([]string(nil), binding.ExcludedEdgeGroupIDs...), ExclusionReason: binding.ExclusionReason,
		ExclusionExpiresAt: binding.ExclusionExpiresAt, MinHealthyEdgeNodes: binding.MinHealthyEdgeNodes,
		HealthyEdgeNodeCount: binding.HealthyEdgeNodeCount, EdgeRedundancyStatus: binding.EdgeRedundancyStatus,
		EdgeRedundancyReason: binding.EdgeRedundancyReason, RoutePolicy: binding.RoutePolicy, SelectionReason: binding.SelectionReason,
		FallbackReason: binding.FallbackReason, UpstreamKind: binding.UpstreamKind, UpstreamScope: binding.UpstreamScope,
		UpstreamURL: binding.UpstreamURL, Upstreams: append([]model.EdgeRouteUpstream(nil), binding.Upstreams...), ServicePort: binding.ServicePort,
		TLSPolicy: binding.TLSPolicy, CachePolicyID: binding.CachePolicyID, CacheNamespace: binding.CacheNamespace,
		DeploymentGeneration: binding.DeploymentGeneration, RequestBodyPolicies: model.CloneEdgeRequestBodyPolicies(binding.RequestBodyPolicies),
		Streaming: binding.Streaming, Status: binding.Status, StatusReason: binding.StatusReason,
	}
}
