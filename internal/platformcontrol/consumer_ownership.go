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
		if c.Component == model.PlatformConsumerComponentCaddyEdgeFront && c.NodeID != "" && c.ConsumerID == c.Component+":"+c.NodeID && c.ArtifactKind == set.ArtifactKind && c.ScopeKey == set.ScopeKey {
			c.Component = model.PlatformConsumerComponentEdgeWorker
			c.ConsumerID = c.Component + ":" + c.NodeID
		}
	}
	return out
}
