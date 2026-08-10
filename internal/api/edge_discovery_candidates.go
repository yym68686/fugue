package api

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

// eligibleDiscoveryEdgeNodes is the policy-filtered candidate set published
// inside the signed discovery bundle. Country and region remain descriptive
// labels; selection is keyed only by the explicit edge and group identities.
func eligibleDiscoveryEdgeNodes(nodes []model.EdgeNode, now time.Time, quarantined map[string]model.NodeDeepHealthResult) []model.EdgeNode {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result := make([]model.EdgeNode, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		edgeID := strings.TrimSpace(node.ID)
		if edgeID == "" || strings.TrimSpace(node.EdgeGroupID) == "" {
			continue
		}
		if _, ok := seen[edgeID]; ok || !edgeNodeRouteServingCapable(node, now) || !edgeNodeHasRouteState(node) || !edgeNodeTLSReadyForDNS(node) || !edgeNodeDNSEligible(node) || edgeNodeQuarantined(node, quarantined) {
			continue
		}
		if firstNonEmpty(strings.TrimSpace(node.PublicHostname), strings.TrimSpace(node.PublicIPv4), strings.TrimSpace(node.PublicIPv6)) == "" {
			continue
		}
		seen[edgeID] = struct{}{}
		result = append(result, node)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].EdgeGroupID != result[j].EdgeGroupID {
			return result[i].EdgeGroupID < result[j].EdgeGroupID
		}
		return result[i].ID < result[j].ID
	})
	return result
}
