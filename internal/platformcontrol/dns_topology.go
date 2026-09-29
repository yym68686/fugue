package platformcontrol

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
)

// Zone rows are telemetry owned by one physical DNS process and authority.
// They cannot create extra process heartbeats, refresh an unhealthy parent's
// evidence or lend its identity to another authority on the same node.
func physicalDNSConsumerNodes(nodes []model.DNSNode) ([]model.DNSNode, map[string]string, error) {
	byPhysical := make(map[string]model.DNSNode)
	aliases := make(map[string]string)
	for _, node := range nodes {
		id := strings.TrimSpace(node.ID)
		physical := firstNonEmptyExpected(strings.TrimSpace(node.PhysicalNodeID), id)
		if id == "" || physical == "" {
			return nil, nil, fmt.Errorf("DNS consumer identity is empty")
		}
		authority := ConsumerAuthorityID(node.EdgeGroupID)
		if strings.HasPrefix(node.EdgeGroupID, "cell-") && authority == "" {
			return nil, nil, fmt.Errorf("neutral DNS authority is invalid")
		}
		ownerID, err := PlatformConsumerID(model.PlatformConsumerComponentDNSServer, physical, authority)
		if err != nil {
			return nil, nil, err
		}
		alias := dnsAliasKey(id, authority)
		if previous, exists := aliases[alias]; exists && previous != physical {
			return nil, nil, fmt.Errorf("DNS zone identity has conflicting physical owners")
		}
		aliases[alias] = physical
		if prior, exists := byPhysical[ownerID]; exists && prior.EdgeGroupID != node.EdgeGroupID {
			return nil, nil, fmt.Errorf("physical DNS consumer has conflicting edge groups")
		}
		// Only identity and group feed expected topology; readiness is checked
		// from the physical consumer's own trusted heartbeat, never zone rows.
		byPhysical[ownerID] = model.DNSNode{ID: physical, PhysicalNodeID: physical, EdgeGroupID: node.EdgeGroupID}
	}
	for _, node := range byPhysical {
		physical := node.ID
		alias := dnsAliasKey(physical, ConsumerAuthorityID(node.EdgeGroupID))
		if owner, exists := aliases[alias]; exists && owner != physical {
			return nil, nil, fmt.Errorf("physical DNS consumer is also another process alias")
		}
		aliases[alias] = physical
	}
	out := make([]model.DNSNode, 0, len(byPhysical))
	for _, node := range byPhysical {
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := PlatformConsumerID(model.PlatformConsumerComponentDNSServer, out[i].ID, ConsumerAuthorityID(out[i].EdgeGroupID))
		b, _ := PlatformConsumerID(model.PlatformConsumerComponentDNSServer, out[j].ID, ConsumerAuthorityID(out[j].EdgeGroupID))
		return a < b
	})
	return out, aliases, nil
}

func dnsAliasKey(nodeID, authority string) string { return authority + "\x00" + nodeID }
