package platformcontrol

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
)

// Zone rows are telemetry owned by one DNS process. They cannot create extra
// required process heartbeats or refresh an unhealthy parent's evidence.
func physicalDNSConsumerNodes(nodes []model.DNSNode) ([]model.DNSNode, map[string]string, error) {
	byPhysical := make(map[string]model.DNSNode)
	aliases := make(map[string]string)
	for _, node := range nodes {
		id := strings.TrimSpace(node.ID)
		physical := firstNonEmptyExpected(strings.TrimSpace(node.PhysicalNodeID), id)
		if id == "" || physical == "" {
			return nil, nil, fmt.Errorf("DNS consumer identity is empty")
		}
		if previous, exists := aliases[id]; exists && previous != physical {
			return nil, nil, fmt.Errorf("DNS zone identity has conflicting physical owners")
		}
		aliases[id] = physical
		if prior, exists := byPhysical[physical]; exists && prior.EdgeGroupID != node.EdgeGroupID {
			return nil, nil, fmt.Errorf("physical DNS consumer has conflicting edge groups")
		}
		// Only identity and group feed expected topology; readiness is checked
		// from the physical consumer's own trusted heartbeat, never zone rows.
		byPhysical[physical] = model.DNSNode{ID: physical, PhysicalNodeID: physical, EdgeGroupID: node.EdgeGroupID}
	}
	for physical := range byPhysical {
		if owner, exists := aliases[physical]; exists && owner != physical {
			return nil, nil, fmt.Errorf("physical DNS consumer is also another process alias")
		}
		aliases[physical] = physical
	}
	out := make([]model.DNSNode, 0, len(byPhysical))
	for _, node := range byPhysical {
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, aliases, nil
}
