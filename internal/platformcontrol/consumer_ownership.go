package platformcontrol

import "fugue/internal/model"

// ProjectExpectedConsumerOwners preserves the immutable expectation identity
// while resolving TLS sidecar ownership to its node's Worker. This projection
// is shared by assignment, authenticated heartbeat binding and convergence.
// It does not transfer evidence from the old component to the new owner.
func ProjectExpectedConsumerOwners(set model.PlatformExpectedConsumerSet) model.PlatformExpectedConsumerSet {
	if set.ArtifactKind != model.PlatformArtifactKindCaddyRouteConfig {
		return set
	}
	out := set
	out.Consumers = append([]model.PlatformExpectedConsumer(nil), set.Consumers...)
	for i := range out.Consumers {
		c := &out.Consumers[i]
		id, err := PlatformConsumerID(c.Component, c.NodeID, c.AuthorityID)
		if err == nil && c.Component == model.PlatformConsumerComponentCaddyEdgeFront && c.NodeID != "" && c.ConsumerID == id && c.ArtifactKind == set.ArtifactKind && c.ScopeKey == set.ScopeKey && (c.AuthorityID == "" || c.Cohort == c.AuthorityID) {
			c.Component = model.PlatformConsumerComponentEdgeWorker
			c.ConsumerID, _ = PlatformConsumerID(c.Component, c.NodeID, c.AuthorityID)
		}
	}
	return out
}
