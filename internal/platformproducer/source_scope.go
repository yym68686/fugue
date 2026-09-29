package platformproducer

import (
	"fmt"

	"fugue/internal/platformconfig"
)

// ValidatePinnedSources binds immutable input ownership before any business or
// runtime projection. Publication repeats this check inside its transaction.
func ValidatePinnedSources(p Policy, static StaticIntentInput, dns *ProjectionPolicyInput) error {
	if static.PublicationRole != p.PublicationRole || static.Scope != p.TargetScope || static.AuthorityCellID != p.AuthorityCellID ||
		(dns != nil && (dns.PublicationRole != p.PublicationRole || dns.Scope != p.TargetScope || dns.AuthorityCellID != p.AuthorityCellID)) {
		return fmt.Errorf("producer inputs cross declared scope or authority")
	}
	if p.AuthorityCellID == "" {
		if p.TargetScope != "global" || dns != nil && dns.ConsumerTopologyDigest != "" {
			return fmt.Errorf("global producer inputs contain cell authority")
		}
		return nil
	}
	if dns == nil || p.PublicationRole != platformconfig.PublicationRoleCellRoutes && (dns.DNSQueryPolicy == nil || dns.DNSPlacementMode != platformconfig.DNSPlacementConsumerReadiness) {
		return fmt.Errorf("cell producer requires pinned query policy and consumer readiness placement")
	}
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(platformconfig.PlatformIntent{
		PublicationRole: static.PublicationRole, Scope: static.Scope, AuthorityCellID: static.AuthorityCellID, EdgeTopology: static.EdgeTopology, DNSConsumers: static.Consumers,
	})
	if err != nil || topology == nil {
		return fmt.Errorf("cell producer input membership unavailable")
	}
	digest, err := platformconfig.Digest(topology)
	if err != nil || digest != dns.ConsumerTopologyDigest {
		return fmt.Errorf("cell producer input membership differs from pinned policy")
	}
	return nil
}
