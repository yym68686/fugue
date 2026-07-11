package rollbackpreflight

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	edgeRouteArtifactSchemaV1 = "fugue.edge-route-bundle.rollback.v1"
	dnsAnswerArtifactSchemaV1 = "fugue.dns-answer-bundle.rollback.v1"
)

// BuildEdgeRouteBundleArtifact converts a signed, expiring route bundle into
// stable rollback material. Signature and validity fields are deliberately
// omitted because they must be freshly issued when the generation is restored.
func BuildEdgeRouteBundleArtifact(bundle model.EdgeRouteBundle) (model.PlatformArtifact, error) {
	edgeID := strings.TrimSpace(bundle.EdgeID)
	edgeGroupID := strings.TrimSpace(bundle.EdgeGroupID)
	if edgeID == "" || edgeGroupID == "" {
		return model.PlatformArtifact{}, fmt.Errorf("edge route rollback artifact requires edge_id and edge_group_id")
	}
	schemaVersion, err := normalizedBundleSchemaVersion(bundle.SchemaVersion)
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("edge route rollback artifact: %w", err)
	}

	routes := make([]edgeRouteVersionMaterial, len(bundle.Routes))
	for index, route := range bundle.Routes {
		routes[index] = edgeRouteVersionMaterialFromBinding(route)
		expectedRouteGeneration := routeGeneration(routes[index])
		if actual := strings.TrimSpace(route.RouteGeneration); actual != "" && actual != expectedRouteGeneration {
			return model.PlatformArtifact{}, fmt.Errorf("edge route rollback artifact route %d generation mismatch: got %q want %q", index, actual, expectedRouteGeneration)
		}
	}
	material := edgeRouteBundleGenerationMaterial{
		Routes:        routes,
		TLSAllowlist:  append([]model.EdgeTLSAllowlistEntry(nil), bundle.TLSAllowlist...),
		CachePolicies: append([]model.CachePolicy(nil), bundle.CachePolicies...),
	}
	expectedGeneration := generationForMaterial("routegen_", material)
	if err := validateBundleGeneration(bundle.Version, bundle.Generation, expectedGeneration); err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("edge route rollback artifact: %w", err)
	}
	content, err := semanticContentMap(edgeRouteArtifactContent{
		ArtifactSchemaVersion: edgeRouteArtifactSchemaV1,
		BundleSchemaVersion:   schemaVersion,
		Generation:            expectedGeneration,
		Routes:                routes,
		TLSAllowlist:          material.TLSAllowlist,
		CachePolicies:         material.CachePolicies,
	})
	if err != nil {
		return model.PlatformArtifact{}, err
	}

	return model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		Scope: model.PlatformArtifactScope{
			ScopeType:   "edge",
			EdgeGroupID: edgeGroupID,
			EdgeID:      edgeID,
		},
		SchemaVersion: model.PlatformArtifactSchemaVersionV1,
		Generation:    expectedGeneration,
		Status:        model.PlatformArtifactStatusDraft,
		Content:       content,
	}, nil
}

// BuildDNSAnswerBundleArtifact converts a signed, expiring DNS bundle into
// stable rollback material scoped to the exact DNS node and zone.
func BuildDNSAnswerBundleArtifact(bundle model.EdgeDNSBundle) (model.PlatformArtifact, error) {
	dnsNodeID := strings.TrimSpace(bundle.DNSNodeID)
	edgeGroupID := strings.TrimSpace(bundle.EdgeGroupID)
	zone := normalizeDomain(bundle.Zone)
	if dnsNodeID == "" || edgeGroupID == "" || zone == "" {
		return model.PlatformArtifact{}, fmt.Errorf("DNS answer rollback artifact requires dns_node_id, edge_group_id, and zone")
	}
	schemaVersion, err := normalizedBundleSchemaVersion(bundle.SchemaVersion)
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("DNS answer rollback artifact: %w", err)
	}

	records := make([]edgeDNSRecordVersionMaterial, len(bundle.Records))
	for index, record := range bundle.Records {
		records[index] = edgeDNSRecordVersionMaterialFromRecord(record)
		expectedRecordGeneration := generationForMaterial("dnsgen_", records[index])
		if actual := strings.TrimSpace(record.RecordGeneration); actual != "" && actual != expectedRecordGeneration {
			return model.PlatformArtifact{}, fmt.Errorf("DNS answer rollback artifact record %d generation mismatch: got %q want %q", index, actual, expectedRecordGeneration)
		}
	}
	material := edgeDNSBundleGenerationMaterial{Zone: zone, Records: records}
	expectedGeneration := generationForMaterial("dnsgen_", material)
	if err := validateBundleGeneration(bundle.Version, bundle.Generation, expectedGeneration); err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("DNS answer rollback artifact: %w", err)
	}
	content, err := semanticContentMap(dnsAnswerArtifactContent{
		ArtifactSchemaVersion: dnsAnswerArtifactSchemaV1,
		BundleSchemaVersion:   schemaVersion,
		Generation:            expectedGeneration,
		Zone:                  zone,
		Records:               records,
	})
	if err != nil {
		return model.PlatformArtifact{}, err
	}

	return model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
		Scope: model.PlatformArtifactScope{
			ScopeType:   "dns_node",
			Hostname:    zone,
			EdgeGroupID: edgeGroupID,
			NodeID:      dnsNodeID,
		},
		SchemaVersion: model.PlatformArtifactSchemaVersionV1,
		Generation:    expectedGeneration,
		Status:        model.PlatformArtifactStatusDraft,
		Content:       content,
	}, nil
}

type edgeRouteArtifactContent struct {
	ArtifactSchemaVersion string                        `json:"artifact_schema_version"`
	BundleSchemaVersion   string                        `json:"bundle_schema_version"`
	Generation            string                        `json:"generation"`
	Routes                []edgeRouteVersionMaterial    `json:"routes"`
	TLSAllowlist          []model.EdgeTLSAllowlistEntry `json:"tls_allowlist"`
	CachePolicies         []model.CachePolicy           `json:"cache_policies,omitempty"`
}

type edgeRouteVersionMaterial struct {
	Hostname             string                    `json:"hostname"`
	PathPrefix           string                    `json:"path_prefix,omitempty"`
	RouteKind            string                    `json:"route_kind"`
	AppID                string                    `json:"app_id"`
	TenantID             string                    `json:"tenant_id"`
	RuntimeID            string                    `json:"runtime_id"`
	RuntimeType          string                    `json:"runtime_type,omitempty"`
	RuntimeEdgeGroupID   string                    `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode   string                    `json:"runtime_cluster_node,omitempty"`
	RuntimeEdgeGroup     string                    `json:"runtime_edge_group,omitempty"`
	SelectedEdgeGroup    string                    `json:"selected_edge_group,omitempty"`
	EdgeGroupID          string                    `json:"edge_group_id"`
	FallbackEdgeGroupID  string                    `json:"fallback_edge_group_id,omitempty"`
	PolicyEdgeGroupID    string                    `json:"policy_edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string                  `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string                  `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string                    `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time                `json:"exclusion_expires_at,omitempty"`
	MinHealthyEdgeNodes  int                       `json:"min_healthy_edge_nodes,omitempty"`
	HealthyEdgeNodeCount int                       `json:"healthy_edge_node_count,omitempty"`
	EdgeRedundancyStatus string                    `json:"edge_redundancy_status,omitempty"`
	EdgeRedundancyReason string                    `json:"edge_redundancy_reason,omitempty"`
	RoutePolicy          string                    `json:"route_policy"`
	SelectionReason      string                    `json:"selection_reason,omitempty"`
	FallbackReason       string                    `json:"fallback_reason,omitempty"`
	UpstreamKind         string                    `json:"upstream_kind"`
	UpstreamScope        string                    `json:"upstream_scope,omitempty"`
	UpstreamURL          string                    `json:"upstream_url,omitempty"`
	Upstreams            []model.EdgeRouteUpstream `json:"upstreams,omitempty"`
	ServicePort          int                       `json:"service_port"`
	TLSPolicy            string                    `json:"tls_policy"`
	CachePolicyID        string                    `json:"cache_policy_id,omitempty"`
	CacheNamespace       string                    `json:"cache_namespace,omitempty"`
	DeploymentGeneration string                    `json:"deployment_generation,omitempty"`
	Streaming            bool                      `json:"streaming"`
	Status               string                    `json:"status"`
	StatusReason         string                    `json:"status_reason,omitempty"`
}

type edgeRouteBundleGenerationMaterial struct {
	Routes        []edgeRouteVersionMaterial    `json:"routes"`
	TLSAllowlist  []model.EdgeTLSAllowlistEntry `json:"tls_allowlist"`
	CachePolicies []model.CachePolicy           `json:"cache_policies,omitempty"`
}

func edgeRouteVersionMaterialFromBinding(binding model.EdgeRouteBinding) edgeRouteVersionMaterial {
	return edgeRouteVersionMaterial{
		Hostname:             binding.Hostname,
		PathPrefix:           model.NormalizeAppRoutePathPrefix(binding.PathPrefix),
		RouteKind:            binding.RouteKind,
		AppID:                binding.AppID,
		TenantID:             binding.TenantID,
		RuntimeID:            binding.RuntimeID,
		RuntimeType:          binding.RuntimeType,
		RuntimeEdgeGroupID:   binding.RuntimeEdgeGroupID,
		RuntimeClusterNode:   binding.RuntimeClusterNode,
		RuntimeEdgeGroup:     binding.RuntimeEdgeGroup,
		SelectedEdgeGroup:    binding.SelectedEdgeGroup,
		EdgeGroupID:          binding.EdgeGroupID,
		FallbackEdgeGroupID:  binding.FallbackEdgeGroupID,
		PolicyEdgeGroupID:    binding.PolicyEdgeGroupID,
		ExcludedEdgeIDs:      append([]string(nil), binding.ExcludedEdgeIDs...),
		ExcludedEdgeGroupIDs: append([]string(nil), binding.ExcludedEdgeGroupIDs...),
		ExclusionReason:      binding.ExclusionReason,
		ExclusionExpiresAt:   binding.ExclusionExpiresAt,
		MinHealthyEdgeNodes:  binding.MinHealthyEdgeNodes,
		HealthyEdgeNodeCount: binding.HealthyEdgeNodeCount,
		EdgeRedundancyStatus: binding.EdgeRedundancyStatus,
		EdgeRedundancyReason: binding.EdgeRedundancyReason,
		RoutePolicy:          binding.RoutePolicy,
		SelectionReason:      binding.SelectionReason,
		FallbackReason:       binding.FallbackReason,
		UpstreamKind:         binding.UpstreamKind,
		UpstreamScope:        binding.UpstreamScope,
		UpstreamURL:          binding.UpstreamURL,
		Upstreams:            append([]model.EdgeRouteUpstream(nil), binding.Upstreams...),
		ServicePort:          binding.ServicePort,
		TLSPolicy:            binding.TLSPolicy,
		CachePolicyID:        binding.CachePolicyID,
		CacheNamespace:       binding.CacheNamespace,
		DeploymentGeneration: binding.DeploymentGeneration,
		Streaming:            binding.Streaming,
		Status:               binding.Status,
		StatusReason:         binding.StatusReason,
	}
}

func routeGeneration(material edgeRouteVersionMaterial) string {
	return generationForMaterial("routegen_", material)
}

type dnsAnswerArtifactContent struct {
	ArtifactSchemaVersion string                         `json:"artifact_schema_version"`
	BundleSchemaVersion   string                         `json:"bundle_schema_version"`
	Generation            string                         `json:"generation"`
	Zone                  string                         `json:"zone"`
	Records               []edgeDNSRecordVersionMaterial `json:"records"`
}

type edgeDNSRecordVersionMaterial struct {
	Name                string                                `json:"name"`
	Type                string                                `json:"type"`
	Values              []string                              `json:"values"`
	TTL                 int                                   `json:"ttl"`
	RecordKind          string                                `json:"record_kind"`
	AppID               string                                `json:"app_id,omitempty"`
	TenantID            string                                `json:"tenant_id,omitempty"`
	EdgeGroupID         string                                `json:"edge_group_id,omitempty"`
	FallbackEdgeGroupID string                                `json:"fallback_edge_group_id,omitempty"`
	Status              string                                `json:"status"`
	StatusReason        string                                `json:"status_reason,omitempty"`
	AnswerPolicy        model.DNSAnswerPolicy                 `json:"answer_policy,omitempty"`
	Candidates          []model.EdgeDNSAnswerCandidate        `json:"candidates,omitempty"`
	ScopedCandidates    []model.EdgeDNSScopedAnswerCandidates `json:"scoped_candidates,omitempty"`
}

type edgeDNSBundleGenerationMaterial struct {
	Zone    string                         `json:"zone"`
	Records []edgeDNSRecordVersionMaterial `json:"records"`
}

func edgeDNSRecordVersionMaterialFromRecord(record model.EdgeDNSRecord) edgeDNSRecordVersionMaterial {
	return edgeDNSRecordVersionMaterial{
		Name:                record.Name,
		Type:                record.Type,
		Values:              append([]string(nil), record.Values...),
		TTL:                 record.TTL,
		RecordKind:          record.RecordKind,
		AppID:               record.AppID,
		TenantID:            record.TenantID,
		EdgeGroupID:         record.EdgeGroupID,
		FallbackEdgeGroupID: record.FallbackEdgeGroupID,
		Status:              record.Status,
		StatusReason:        record.StatusReason,
		AnswerPolicy:        record.AnswerPolicy,
		Candidates:          append([]model.EdgeDNSAnswerCandidate(nil), record.Candidates...),
		ScopedCandidates:    append([]model.EdgeDNSScopedAnswerCandidates(nil), record.ScopedCandidates...),
	}
}

func normalizedBundleSchemaVersion(raw string) (string, error) {
	version := strings.TrimSpace(raw)
	if version == "" {
		version = model.BundleSchemaVersionV1
	}
	if version != model.BundleSchemaVersionV1 {
		return "", fmt.Errorf("unsupported bundle schema version %q", version)
	}
	return version, nil
}

func validateBundleGeneration(version, generation, expected string) error {
	version = strings.TrimSpace(version)
	generation = strings.TrimSpace(generation)
	if version == "" || generation == "" {
		return fmt.Errorf("bundle version and generation are required")
	}
	if version != generation {
		return fmt.Errorf("bundle version %q does not match generation %q", version, generation)
	}
	if generation != expected {
		return fmt.Errorf("bundle generation %q does not match semantic generation %q", generation, expected)
	}
	return nil
}

func generationForMaterial(prefix string, material any) string {
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return prefix + hex.EncodeToString(sum[:])[:16]
}

func semanticContentMap(content any) (map[string]any, error) {
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal rollback artifact content: %w", err)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("canonicalize rollback artifact content: %w", err)
	}
	return result, nil
}

func normalizeDomain(raw string) string {
	return strings.Trim(strings.ToLower(strings.TrimSpace(raw)), ".")
}
