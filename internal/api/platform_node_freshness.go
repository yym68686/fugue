package api

import (
	"time"

	"fugue/internal/model"
)

const platformNodeHeartbeatStaleAfter = 90 * time.Second

func edgeNodeHeartbeatFresh(node model.EdgeNode, now time.Time) bool {
	return nodeHeartbeatFresh(node.LastHeartbeatAt, node.LastSeenAt, now)
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
