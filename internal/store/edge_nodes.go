package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListEdgeNodes(edgeGroupID string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	if s.usingDatabase() {
		return s.pgListEdgeNodes(edgeGroupID)
	}

	var nodes []model.EdgeNode
	var groups []model.EdgeGroup
	err := s.withLockedState(false, func(state *model.State) error {
		for _, node := range state.EdgeNodes {
			normalizeEdgeNodeForRead(&node)
			if edgeGroupID != "" && !strings.EqualFold(node.EdgeGroupID, edgeGroupID) {
				continue
			}
			nodes = append(nodes, redactEdgeNode(node))
		}
		groups = edgeGroupSummaries(state.EdgeGroups, state.EdgeNodes, edgeGroupID)
		sortEdgeNodes(nodes)
		return nil
	})
	return nodes, groups, err
}

func (s *Store) GetEdgeNode(edgeID string) (model.EdgeNode, model.EdgeGroup, error) {
	edgeID = normalizeEdgeID(edgeID)
	if edgeID == "" {
		return model.EdgeNode{}, model.EdgeGroup{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetEdgeNode(edgeID)
	}

	var node model.EdgeNode
	var group model.EdgeGroup
	err := s.withLockedState(false, func(state *model.State) error {
		index := findEdgeNode(state, edgeID)
		if index < 0 {
			return ErrNotFound
		}
		node = state.EdgeNodes[index]
		normalizeEdgeNodeForRead(&node)
		group = edgeGroupSummary(node.EdgeGroupID, state.EdgeGroups, state.EdgeNodes)
		return nil
	})
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, err
	}
	return redactEdgeNode(node), group, nil
}

func (s *Store) CreateEdgeNodeToken(node model.EdgeNode) (model.EdgeNode, string, error) {
	node, err := normalizeEdgeNodeForStore(node)
	if err != nil {
		return model.EdgeNode{}, "", err
	}
	if s.usingDatabase() {
		return s.pgCreateEdgeNodeToken(node)
	}

	secret := model.NewSecret("fugue_edge")
	node.TokenPrefix = model.SecretPrefix(secret)
	node.TokenHash = model.HashSecret(secret)
	var out model.EdgeNode
	err = s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findEdgeNode(state, node.ID)
		if index >= 0 {
			existing := state.EdgeNodes[index]
			if node.CreatedAt.IsZero() {
				node.CreatedAt = existing.CreatedAt
			}
			if node.LastSeenAt == nil {
				node.LastSeenAt = existing.LastSeenAt
			}
			if node.LastHeartbeatAt == nil {
				node.LastHeartbeatAt = existing.LastHeartbeatAt
			}
		} else if node.CreatedAt.IsZero() {
			node.CreatedAt = now
		}
		node.UpdatedAt = now
		if index >= 0 {
			state.EdgeNodes[index] = node
		} else {
			state.EdgeNodes = append(state.EdgeNodes, node)
		}
		upsertEdgeGroupForNode(state, node, now)
		out = redactEdgeNode(node)
		return nil
	})
	if err != nil {
		return model.EdgeNode{}, "", err
	}
	return out, secret, nil
}

func (s *Store) AuthenticateEdgeNode(secret string) (model.EdgeNode, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return model.EdgeNode{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAuthenticateEdgeNode(secret)
	}

	var node model.EdgeNode
	err := s.withLockedState(true, func(state *model.State) error {
		hash := model.HashSecret(secret)
		now := time.Now().UTC()
		for idx := range state.EdgeNodes {
			if strings.TrimSpace(state.EdgeNodes[idx].TokenHash) == "" || state.EdgeNodes[idx].TokenHash != hash {
				continue
			}
			state.EdgeNodes[idx].LastSeenAt = &now
			state.EdgeNodes[idx].UpdatedAt = now
			node = state.EdgeNodes[idx]
			return nil
		}
		return ErrNotFound
	})
	if err != nil {
		return model.EdgeNode{}, err
	}
	return redactEdgeNode(node), nil
}

func (s *Store) UpdateEdgeHeartbeat(node model.EdgeNode) (model.EdgeNode, model.EdgeGroup, error) {
	node, err := normalizeEdgeNodeForStore(node)
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, err
	}
	if s.usingDatabase() {
		return s.pgUpdateEdgeHeartbeat(node)
	}

	var out model.EdgeNode
	var group model.EdgeGroup
	err = s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findEdgeNode(state, node.ID)
		if index >= 0 {
			existing := state.EdgeNodes[index]
			node.TokenPrefix = existing.TokenPrefix
			node.TokenHash = existing.TokenHash
			if node.CreatedAt.IsZero() {
				node.CreatedAt = existing.CreatedAt
			}
		} else if node.CreatedAt.IsZero() {
			node.CreatedAt = now
		}
		node.LastSeenAt = &now
		node.LastHeartbeatAt = &now
		node.UpdatedAt = now
		if index >= 0 {
			state.EdgeNodes[index] = node
		} else {
			state.EdgeNodes = append(state.EdgeNodes, node)
		}
		upsertEdgeGroupForNode(state, node, now)
		out = redactEdgeNode(node)
		group = edgeGroupSummary(node.EdgeGroupID, state.EdgeGroups, state.EdgeNodes)
		return nil
	})
	if err != nil {
		return model.EdgeNode{}, model.EdgeGroup{}, err
	}
	return out, group, nil
}

func normalizeEdgeNodeForStore(node model.EdgeNode) (model.EdgeNode, error) {
	node.ID = normalizeEdgeID(node.ID)
	node.EdgeGroupID = normalizeEdgeGroupID(node.EdgeGroupID)
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
	node.LastError = strings.TrimSpace(node.LastError)
	node.TokenPrefix = strings.TrimSpace(node.TokenPrefix)
	node.TokenHash = strings.TrimSpace(node.TokenHash)
	if node.CaddyRouteCount < 0 {
		node.CaddyRouteCount = 0
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

func upsertEdgeGroupForNode(state *model.State, node model.EdgeNode, now time.Time) {
	if state == nil || strings.TrimSpace(node.EdgeGroupID) == "" {
		return
	}
	group := model.EdgeGroup{
		ID:        node.EdgeGroupID,
		Region:    node.Region,
		Country:   node.Country,
		Status:    "",
		CreatedAt: now,
		UpdatedAt: now,
	}
	index := findEdgeGroup(state, node.EdgeGroupID)
	if index >= 0 {
		group = state.EdgeGroups[index]
		if strings.TrimSpace(node.Region) != "" {
			group.Region = node.Region
		}
		if strings.TrimSpace(node.Country) != "" {
			group.Country = node.Country
		}
		group.UpdatedAt = now
		state.EdgeGroups[index] = group
		return
	}
	state.EdgeGroups = append(state.EdgeGroups, group)
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

func findEdgeNode(state *model.State, edgeID string) int {
	if state == nil {
		return -1
	}
	edgeID = normalizeEdgeID(edgeID)
	for index, node := range state.EdgeNodes {
		if strings.EqualFold(node.ID, edgeID) {
			return index
		}
	}
	return -1
}

func findEdgeGroup(state *model.State, edgeGroupID string) int {
	if state == nil {
		return -1
	}
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	for index, group := range state.EdgeGroups {
		if strings.EqualFold(group.ID, edgeGroupID) {
			return index
		}
	}
	return -1
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
