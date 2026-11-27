package store

import (
	"strings"

	"fugue/internal/model"
)

func (s *Store) ListDNSNodes(edgeGroupID string) ([]model.DNSNode, error) {
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	return s.pgListDNSNodes(edgeGroupID)
}

func (s *Store) GetDNSNode(dnsNodeID string) (model.DNSNode, error) {
	dnsNodeID = normalizeDNSNodeID(dnsNodeID)
	if dnsNodeID == "" {
		return model.DNSNode{}, ErrInvalidInput
	}
	return s.pgGetDNSNode(dnsNodeID)
}

func (s *Store) UpdateDNSHeartbeat(node model.DNSNode) (model.DNSNode, error) {
	node, err := normalizeDNSNodeForStore(node)
	if err != nil {
		return model.DNSNode{}, err
	}
	return s.pgUpdateDNSHeartbeat(node)
}

func normalizeDNSNodeForStore(node model.DNSNode) (model.DNSNode, error) {
	node.ID = normalizeDNSNodeID(node.ID)
	node.PhysicalNodeID = normalizeDNSNodeID(node.PhysicalNodeID)
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
	node.PhysicalNodeID = normalizeDNSNodeID(node.PhysicalNodeID)
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

func normalizeDNSNodeID(dnsNodeID string) string {
	return strings.TrimSpace(strings.ToLower(dnsNodeID))
}

func normalizeDNSZone(zone string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(zone)), ".")
}
