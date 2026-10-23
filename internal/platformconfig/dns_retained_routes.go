package platformconfig

import (
	"encoding/json"
	"fmt"
)

// Only the immutable previous publication is a routing authority. These local
// projections reuse the DNS compiler's alias handling; they are never signed,
// emitted as replacement Cell publications, or used as proof bindings. Every
// generated target must also match the previous signed DNS plan in
// addPreviousAuthorityRequirements, including every hostname/path dependency.
func retainedDNSRouteProjections(intent PlatformIntent, previous PreviousTrafficPublicationInput) ([]CellRoutePublicationInput, error) {
	aliases, err := RouteAuthorityAliases(intent)
	if err != nil || !DNSUsesRetainedRoutes(intent) {
		return nil, fmt.Errorf("explicit retained routing authority required")
	}
	route := previous.Route
	raw, err := json.Marshal(route.Content)
	if err != nil {
		return nil, err
	}
	route.Content = nil
	if json.Unmarshal(raw, &route.Content) != nil {
		return nil, fmt.Errorf("retained routing projection unavailable")
	}
	normalizeDNSRuntimeAliases(route.Content, aliases)
	r := previous.Reference
	out := make([]CellRoutePublicationInput, 0, len(intent.EdgeTopology.Cells))
	for _, cell := range intent.EdgeTopology.Cells {
		ref := CellRoutePublicationReference{AuthorityCellID: cell.ID, ReleaseSetID: r.ReleaseSetID, ReleaseSetDigest: r.ReleaseSetDigest, ReleaseID: r.ReleaseID, ReleaseChannel: "full", FencingToken: r.FencingToken, RouteArtifactID: r.RouteArtifactID, RouteArtifactDigest: r.RouteArtifactDigest, TLSArtifactID: r.TLSArtifactID, TLSArtifactDigest: r.TLSArtifactDigest}
		out = append(out, CellRoutePublicationInput{Reference: ref, Parent: previous.Parent, Route: route, TLS: previous.TLS})
	}
	return out, nil
}
