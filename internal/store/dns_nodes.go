package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListDNSNodes(edgeGroupID string) ([]model.DNSNode, error) {
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	if s.usingDatabase() {
		return s.pgListDNSNodes(edgeGroupID)
	}

	var nodes []model.DNSNode
	err := s.withLockedState(false, func(state *model.State) error {
		for _, node := range state.DNSNodes {
			normalizeDNSNodeForRead(&node)
			if edgeGroupID != "" && !strings.EqualFold(node.EdgeGroupID, edgeGroupID) {
				continue
			}
			nodes = append(nodes, node)
		}
		sortDNSNodes(nodes)
		return nil
	})
	return nodes, err
}

func (s *Store) GetDNSNode(dnsNodeID string) (model.DNSNode, error) {
	dnsNodeID = normalizeDNSNodeID(dnsNodeID)
	if dnsNodeID == "" {
		return model.DNSNode{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetDNSNode(dnsNodeID)
	}

	var node model.DNSNode
	err := s.withLockedState(false, func(state *model.State) error {
		index := findDNSNode(state, dnsNodeID)
		if index < 0 {
			return ErrNotFound
		}
		node = state.DNSNodes[index]
		normalizeDNSNodeForRead(&node)
		return nil
	})
	if err != nil {
		return model.DNSNode{}, err
	}
	return node, nil
}

func (s *Store) UpdateDNSHeartbeat(node model.DNSNode) (model.DNSNode, error) {
	node, err := normalizeDNSNodeForStore(node)
	if err != nil {
		return model.DNSNode{}, err
	}
	if s.usingDatabase() {
		return s.pgUpdateDNSHeartbeat(node)
	}

	var out model.DNSNode
	err = s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findDNSNode(state, node.ID)
		if index >= 0 {
			existing := state.DNSNodes[index]
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
			state.DNSNodes[index] = node
		} else {
			state.DNSNodes = append(state.DNSNodes, node)
		}
		out = node
		return nil
	})
	if err != nil {
		return model.DNSNode{}, err
	}
	normalizeDNSNodeForRead(&out)
	return out, nil
}

func normalizeDNSNodeForStore(node model.DNSNode) (model.DNSNode, error) {
	node.ID = normalizeDNSNodeID(node.ID)
	node.EdgeGroupID = normalizeEdgeGroupID(node.EdgeGroupID)
	node.PublicHostname = normalizeEdgeHostname(node.PublicHostname)
	node.PublicIPv4 = strings.TrimSpace(node.PublicIPv4)
	node.PublicIPv6 = strings.TrimSpace(node.PublicIPv6)
	node.MeshIP = strings.TrimSpace(node.MeshIP)
	node.Zone = normalizeDNSZone(node.Zone)
	node.Status = model.NormalizeEdgeHealthStatus(node.Status)
	node.DNSBundleVersion = strings.TrimSpace(node.DNSBundleVersion)
	node.CacheStatus = strings.TrimSpace(node.CacheStatus)
	node.ListenAddr = strings.TrimSpace(node.ListenAddr)
	node.UDPAddr = strings.TrimSpace(node.UDPAddr)
	node.TCPAddr = strings.TrimSpace(node.TCPAddr)
	node.LastError = strings.TrimSpace(node.LastError)
	if node.RecordCount < 0 {
		node.RecordCount = 0
	}
	if node.QueryRCodeCounts == nil {
		node.QueryRCodeCounts = map[string]uint64{}
	}
	if node.QueryQTypeCounts == nil {
		node.QueryQTypeCounts = map[string]uint64{}
	}
	if node.ID == "" || node.EdgeGroupID == "" || node.Zone == "" || node.Status == "" {
		return model.DNSNode{}, ErrInvalidInput
	}
	return node, nil
}

func normalizeDNSNodeForRead(node *model.DNSNode) {
	if node == nil {
		return
	}
	node.ID = normalizeDNSNodeID(node.ID)
	node.EdgeGroupID = normalizeEdgeGroupID(node.EdgeGroupID)
	node.PublicHostname = normalizeEdgeHostname(node.PublicHostname)
	node.Zone = normalizeDNSZone(node.Zone)
	node.Status = model.NormalizeEdgeHealthStatus(node.Status)
	if node.Status == "" {
		node.Status = model.EdgeHealthUnknown
	}
	if node.RecordCount < 0 {
		node.RecordCount = 0
	}
	if node.QueryRCodeCounts == nil {
		node.QueryRCodeCounts = map[string]uint64{}
	}
	if node.QueryQTypeCounts == nil {
		node.QueryQTypeCounts = map[string]uint64{}
	}
}

func findDNSNode(state *model.State, dnsNodeID string) int {
	if state == nil {
		return -1
	}
	dnsNodeID = normalizeDNSNodeID(dnsNodeID)
	for index, node := range state.DNSNodes {
		if strings.EqualFold(node.ID, dnsNodeID) {
			return index
		}
	}
	return -1
}

func sortDNSNodes(nodes []model.DNSNode) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].EdgeGroupID != nodes[j].EdgeGroupID {
			return nodes[i].EdgeGroupID < nodes[j].EdgeGroupID
		}
		return nodes[i].ID < nodes[j].ID
	})
}

func normalizeDNSNodeID(dnsNodeID string) string {
	return strings.TrimSpace(strings.ToLower(dnsNodeID))
}

func normalizeDNSZone(zone string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(zone)), ".")
}
