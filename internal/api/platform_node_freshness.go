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

func dnsNodeHeartbeatFresh(node model.DNSNode, now time.Time) bool {
	return nodeHeartbeatFresh(node.LastHeartbeatAt, node.LastSeenAt, now)
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
