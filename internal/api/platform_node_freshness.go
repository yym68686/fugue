package api

import (
	"strings"
	"time"

	"fugue/internal/model"
)

const platformNodeHeartbeatStaleAfter = 90 * time.Second

func edgeNodeHeartbeatFresh(node model.EdgeNode, now time.Time) bool {
	return nodeHeartbeatFresh(node.LastHeartbeatAt, node.LastSeenAt, now)
}

func edgeNodeRouteServingCapable(node model.EdgeNode, now time.Time) bool {
	if node.Draining || !node.Healthy || !edgeNodeHeartbeatFresh(node, now) {
		return false
	}
	status := model.NormalizeEdgeHealthStatus(strings.TrimSpace(node.Status))
	switch status {
	case model.EdgeHealthHealthy:
		return true
	case model.EdgeHealthDegraded:
		return strings.TrimSpace(node.CaddyLastError) == "" && node.CaddyRouteCount > 0
	default:
		return false
	}
}

func edgeNodeRouteBootstrapCapable(node model.EdgeNode, now time.Time) bool {
	if node.Draining || !node.Healthy || !edgeNodeHeartbeatFresh(node, now) {
		return false
	}
	if edgeNodeRouteServingCapable(node, now) {
		return false
	}
	return edgeNodeHasRouteState(node)
}

func dnsNodeHeartbeatFresh(node model.DNSNode, now time.Time) bool {
	return nodeHeartbeatFresh(node.LastHeartbeatAt, node.LastSeenAt, now)
}

func freshEdgeNodes(nodes []model.EdgeNode, now time.Time) []model.EdgeNode {
	if len(nodes) == 0 {
		return nodes
	}
	out := make([]model.EdgeNode, 0, len(nodes))
	for _, node := range nodes {
		if edgeNodeHeartbeatFresh(node, now) {
			out = append(out, node)
		}
	}
	return out
}

func freshDNSNodes(nodes []model.DNSNode, now time.Time) []model.DNSNode {
	if len(nodes) == 0 {
		return nodes
	}
	out := make([]model.DNSNode, 0, len(nodes))
	for _, node := range nodes {
		if dnsNodeHeartbeatFresh(node, now) {
			out = append(out, node)
		}
	}
	return out
}

func nodeHeartbeatFresh(lastHeartbeatAt, lastSeenAt *time.Time, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	seen := lastHeartbeatAt
	if seen == nil || seen.IsZero() {
		seen = lastSeenAt
	}
	if seen == nil || seen.IsZero() {
		return false
	}
	seenAt := seen.UTC()
	now = now.UTC()
	if seenAt.After(now) {
		return true
	}
	return now.Sub(seenAt) <= platformNodeHeartbeatStaleAfter
}
