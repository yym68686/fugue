package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListEdgeNodes(edgeGroupID string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	return s.pgListEdgeNodes(edgeGroupID)
}

// EdgeGroupExists validates control-plane group identity without loading the
// complete edge-node inventory.
func (s *Store) EdgeGroupExists(edgeGroupID string) (bool, error) {
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	if edgeGroupID == "" {
		return false, ErrInvalidInput
	}
	return s.pgEdgeGroupExists(edgeGroupID)
}

func (s *Store) GetEdgeNode(edgeID string) (model.EdgeNode, model.EdgeGroup, error) {
	edgeID = normalizeEdgeID(edgeID)
	if edgeID == "" {
		return model.EdgeNode{}, model.EdgeGroup{}, ErrInvalidInput
	}
	return s.pgGetEdgeNode(edgeID)
}

func (s *Store) CreateEdgeNodeToken(node model.EdgeNode) (model.EdgeNode, string, error) {
	node, err := normalizeEdgeNodeForStore(node)
	if err != nil {
		return model.EdgeNode{}, "", err
	}
	return s.pgCreateEdgeNodeToken(node)
}

func (s *Store) AuthenticateEdgeNode(secret string) (model.EdgeNode, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return model.EdgeNode{}, ErrInvalidInput
	}
	return s.pgAuthenticateEdgeNode(secret)
}

// RecordEdgeRouteSync records freshness only after route-bundle validation.
// Authentication is intentionally read-only so failed sync attempts cannot
// keep a stale edge in the serving inventory.
func (s *Store) RecordEdgeRouteSync(edgeID string) error {
	edgeID = normalizeEdgeID(edgeID)
	if edgeID == "" {
		return ErrInvalidInput
	}
	return s.pgRecordEdgeRouteSync(edgeID)
}

func (s *Store) UpdateEdgeHeartbeat(node model.EdgeNode) (model.EdgeNode, model.EdgeGroup, error) {
	node, err := normalizeEdgeNodeForStore(node)
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, err
	}
	return s.pgUpdateEdgeHeartbeat(node)
}

func (s *Store) UpdateEdgeNodeControlState(edgeID string, patch model.EdgeNode) (model.EdgeNode, model.EdgeGroup, error) {
	edgeID = normalizeEdgeID(edgeID)
	if edgeID == "" {
		return model.EdgeNode{}, model.EdgeGroup{}, ErrInvalidInput
	}
	return s.pgUpdateEdgeNodeControlState(edgeID, patch)
}

func applyEdgeNodeControlPatch(node *model.EdgeNode, patch model.EdgeNode) {
	if node == nil {
		return
	}
	if mode := model.NormalizeEdgeWorkloadMode(patch.WorkloadMode); mode != "" {
		node.WorkloadMode = mode
	}
	if state := model.NormalizeEdgeCanaryState(patch.CanaryState); state != "" {
		node.CanaryState = state
	}
	if patch.CanaryWeight > 0 {
		node.CanaryWeight = patch.CanaryWeight
	}
	if status := model.NormalizeEdgePublicProbeStatus(patch.PublicProbeStatus); status != "" {
		node.PublicProbeStatus = status
	}
	if node.PublicProbeStatus == model.EdgePublicProbeStatusPassing && strings.TrimSpace(patch.PublicProbeLastError) == "" {
		node.PublicProbeLastError = ""
	}
	if strings.TrimSpace(patch.PublicProbeLastError) != "" {
		node.PublicProbeLastError = strings.TrimSpace(patch.PublicProbeLastError)
	}
	if patch.PublicProbeLastAt != nil {
		node.PublicProbeLastAt = patch.PublicProbeLastAt
	}
	node.Draining = patch.Draining
}

func normalizeEdgeNodeForStore(node model.EdgeNode) (model.EdgeNode, error) {
	node.ID = normalizeEdgeID(node.ID)
	node.EdgeGroupID = normalizeEdgeGroupID(node.EdgeGroupID)
	node.WorkloadMode = model.NormalizeEdgeWorkloadMode(node.WorkloadMode)
	if node.WorkloadMode == "" {
		node.WorkloadMode = model.EdgeWorkloadModeStatic
	}
	node.CanaryState = model.NormalizeEdgeCanaryState(node.CanaryState)
	node.PublicProbeStatus = model.NormalizeEdgePublicProbeStatus(node.PublicProbeStatus)
	node.Region = normalizeEdgeMetadataValue(node.Region)
	node.Country = normalizeEdgeMetadataValue(node.Country)
	node.PublicHostname = normalizeEdgeHostname(node.PublicHostname)
	node.PublicIPv4 = strings.TrimSpace(node.PublicIPv4)
	node.PublicIPv6 = strings.TrimSpace(node.PublicIPv6)
	node.MeshIP = strings.TrimSpace(node.MeshIP)
	node.Status = model.NormalizeEdgeHealthStatus(strings.TrimSpace(node.Status))
	node.RouteBundleVersion = strings.TrimSpace(node.RouteBundleVersion)
	node.DNSBundleVersion = strings.TrimSpace(node.DNSBundleVersion)
	node.CaddyAppliedVersion = strings.TrimSpace(node.CaddyAppliedVersion)
	node.CaddyLastError = strings.TrimSpace(node.CaddyLastError)
	node.CacheStatus = strings.TrimSpace(node.CacheStatus)
	node.TLSStatus = model.NormalizeEdgeTLSStatus(strings.TrimSpace(node.TLSStatus))
	node.TLSLastMessage = strings.TrimSpace(node.TLSLastMessage)
	node.PublicProbeLastError = strings.TrimSpace(node.PublicProbeLastError)
	node.LastError = strings.TrimSpace(node.LastError)
	node.TokenPrefix = strings.TrimSpace(node.TokenPrefix)
	node.TokenHash = strings.TrimSpace(node.TokenHash)
	if node.CaddyRouteCount < 0 {
		node.CaddyRouteCount = 0
	}
	if node.CanaryWeight < 0 {
		node.CanaryWeight = 0
	}
	if node.ID == "" || node.EdgeGroupID == "" || node.Status == "" {
		return model.EdgeNode{}, ErrInvalidInput
	}
	return node, nil
}

func normalizeEdgeNodeForRead(node *model.EdgeNode) {
	if node == nil {
		return
	}
	node.ID = normalizeEdgeID(node.ID)
	node.EdgeGroupID = normalizeEdgeGroupID(node.EdgeGroupID)
	node.WorkloadMode = model.NormalizeEdgeWorkloadMode(node.WorkloadMode)
	if node.WorkloadMode == "" {
		node.WorkloadMode = model.EdgeWorkloadModeStatic
	}
	node.CanaryState = model.NormalizeEdgeCanaryState(node.CanaryState)
	if node.CanaryState == "" {
		if node.WorkloadMode == model.EdgeWorkloadModeDynamic {
			node.CanaryState = model.EdgeCanaryStateJoined
		} else {
			node.CanaryState = model.EdgeCanaryStateActive
		}
	}
	if node.CanaryWeight <= 0 {
		if node.WorkloadMode == model.EdgeWorkloadModeDynamic && node.CanaryState == model.EdgeCanaryStateCanary {
			node.CanaryWeight = 1
		} else if node.CanaryState == model.EdgeCanaryStateActive {
			node.CanaryWeight = 100
		}
	}
	node.PublicProbeStatus = model.NormalizeEdgePublicProbeStatus(node.PublicProbeStatus)
	if node.PublicProbeStatus == "" {
		node.PublicProbeStatus = model.EdgePublicProbeStatusUnknown
	}
	node.Region = normalizeEdgeMetadataValue(node.Region)
	node.Country = normalizeEdgeMetadataValue(node.Country)
	node.PublicHostname = normalizeEdgeHostname(node.PublicHostname)
	node.Status = model.NormalizeEdgeHealthStatus(strings.TrimSpace(node.Status))
	if node.Status == "" {
		node.Status = model.EdgeHealthUnknown
	}
	if node.CaddyRouteCount < 0 {
		node.CaddyRouteCount = 0
	}
}

func edgeGroupSummaries(groups []model.EdgeGroup, nodes []model.EdgeNode, edgeGroupID string) []model.EdgeGroup {
	groupIDs := make(map[string]struct{}, len(groups)+len(nodes))
	for _, group := range groups {
		id := normalizeEdgeGroupID(group.ID)
		if id == "" || (edgeGroupID != "" && id != edgeGroupID) {
			continue
		}
		groupIDs[id] = struct{}{}
	}
	for _, node := range nodes {
		id := normalizeEdgeGroupID(node.EdgeGroupID)
		if id == "" || (edgeGroupID != "" && id != edgeGroupID) {
			continue
		}
		groupIDs[id] = struct{}{}
	}
	out := make([]model.EdgeGroup, 0, len(groupIDs))
	for id := range groupIDs {
		out = append(out, edgeGroupSummary(id, groups, nodes))
	}
	sortEdgeGroups(out)
	return out
}

func edgeGroupSummary(edgeGroupID string, groups []model.EdgeGroup, nodes []model.EdgeNode) model.EdgeGroup {
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	now := time.Now().UTC()
	group := model.EdgeGroup{
		ID:        edgeGroupID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	for _, existing := range groups {
		if normalizeEdgeGroupID(existing.ID) != edgeGroupID {
			continue
		}
		group = existing
		group.ID = edgeGroupID
		break
	}
	for _, node := range nodes {
		normalizeEdgeNodeForRead(&node)
		if node.EdgeGroupID != edgeGroupID {
			continue
		}
		group.NodeCount++
		if node.Healthy && !node.Draining && node.Status == model.EdgeHealthHealthy {
			group.HealthyNodeCount++
		}
		if node.LastSeenAt != nil && (group.LastSeenAt == nil || node.LastSeenAt.After(*group.LastSeenAt)) {
			t := *node.LastSeenAt
			group.LastSeenAt = &t
		}
		if strings.TrimSpace(group.Region) == "" {
			group.Region = node.Region
		}
		if strings.TrimSpace(group.Country) == "" {
			group.Country = node.Country
		}
	}
	group.HasHealthyNodes = group.HealthyNodeCount > 0
	if group.Status == "" {
		switch {
		case group.HasHealthyNodes:
			group.Status = model.EdgeHealthHealthy
		case group.NodeCount > 0:
			group.Status = model.EdgeHealthUnhealthy
		default:
			group.Status = model.EdgeHealthUnknown
		}
	}
	return group
}

func redactEdgeNode(node model.EdgeNode) model.EdgeNode {
	normalizeEdgeNodeForRead(&node)
	node.TokenHash = ""
	return node
}

func sortEdgeNodes(nodes []model.EdgeNode) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].EdgeGroupID != nodes[j].EdgeGroupID {
			return nodes[i].EdgeGroupID < nodes[j].EdgeGroupID
		}
		return nodes[i].ID < nodes[j].ID
	})
}

func sortEdgeGroups(groups []model.EdgeGroup) {
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].ID < groups[j].ID
	})
}

func normalizeEdgeID(edgeID string) string {
	return strings.TrimSpace(strings.ToLower(edgeID))
}

func normalizeEdgeHostname(hostname string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(hostname)), ".")
}

func normalizeEdgeMetadataValue(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}
