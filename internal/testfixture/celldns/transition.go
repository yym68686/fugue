package celldns

import (
	"encoding/json"
	"testing"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

// TransitionRequest supplies a complete previous publication and two neutral
// Cells with identical serving behavior. All identities are synthetic.
func TransitionRequest(t testing.TB) platformconfig.CompileRequest {
	t.Helper()
	req := Request(t)
	routes := []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", UpstreamURL: "http://origin:8080", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled}}
	second := routes[0]
	second.PathPrefix = "/admin"
	routes = append(routes, second)
	tlsIntent := []platformconfig.TLSIntent{{Hostname: "app.example.test", Policy: "auto"}}
	var routePolicy platformconfig.PolicySnapshot
	for i, p := range req.CellRoutePublications {
		raw, _ := json.Marshal(p.Route.Content["policy"])
		if err := json.Unmarshal(raw, &routePolicy); err != nil {
			t.Fatal(err)
		}
		cell := p.Reference.AuthorityCellID
		edge := req.Intent.EdgeTopology.Edges[i]
		intent := platformconfig.PlatformIntent{PublicationRole: platformconfig.PublicationRoleCellRoutes, AuthorityCellID: cell, Scope: platformconfig.AuthorityCellScope(cell), Generation: "equivalent-routes", SchemaVersion: platformconfig.SchemaVersion, EdgeTopology: &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: cell}}, Pools: req.Intent.EdgeTopology.Pools, Edges: []edgetopology.Edge{edge}}, Routes: routes, TLS: tlsIntent}
		c, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: intent, Policy: routePolicy, CreatedAt: req.CreatedAt})
		if err != nil {
			t.Fatal(err)
		}
		p.Route = Sign(t, c.RouteArtifact, p.Route.ID, 1)
		p.TLS = Sign(t, c.TLSArtifact, p.TLS.ID, 1)
		p.Parent = Sign(t, platformconfig.BuildReleaseSetArtifact(c.ReleaseSet, []string{p.Route.ID, p.TLS.ID}, req.CreatedAt), p.Parent.ID, 1)
		p.Reference.ReleaseSetDigest, p.Reference.RouteArtifactDigest, p.Reference.TLSArtifactDigest = p.Parent.ContentHash, p.Route.ContentHash, p.TLS.ContentHash
		req.CellRoutePublications[i] = p
		req.Intent.CellRoutePublications[i] = p.Reference
	}
	previousTopology := req.Intent.EdgeTopology.Clone()
	aliases := map[string]string{}
	for i, c := range previousTopology.Cells {
		old := "edge-group-" + c.ID[len("cell-"):]
		previousTopology.Cells[i].LegacyGroupID = old
		aliases[c.ID] = old
	}
	policy := routePolicy
	policy.PublicationRole, policy.AuthorityCellID, policy.ConsumerTopologyDigest = "", "", ""
	policy.Scope, policy.Generation = "global", "previous-policy"
	policy.DNSPlacementMode, policy.DNSQueryPolicy, policy.DNSReadiness = req.Policy.DNSPlacementMode, req.Policy.DNSQueryPolicy, req.Policy.DNSReadiness
	policy.DNSAuthorities, policy.DNSClientPolicies, policy.DNSAnswerRules = req.Policy.DNSAuthorities, req.Policy.DNSClientPolicies, req.Policy.DNSAnswerRules
	policy.TrafficRolloutCohorts = []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{"edge-group-a", "edge-group-b"}}}
	intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "previous-intent", Routes: routes, TLS: tlsIntent, DNS: req.Intent.DNS, DNSConsumers: append([]platformconfig.DNSConsumerIntent(nil), req.Intent.DNSConsumers...)}
	intent.DNSConsumers[0].EdgeGroupID = "edge-group-a"
	raw, _ := json.Marshal(req.RuntimeSnapshot)
	var snapshot platformconfig.RuntimeSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.IntentGeneration, snapshot.PolicyGeneration = "", ""
	for i := range snapshot.DNSEdgeEndpoints {
		snapshot.DNSEdgeEndpoints[i].EdgeGroupID = aliases[snapshot.DNSEdgeEndpoints[i].EdgeGroupID]
	}
	for i := range snapshot.DNSConsumers {
		snapshot.DNSConsumers[i].EdgeGroupID = "edge-group-a"
	}
	for i := range snapshot.DNSSelections {
		for j := range snapshot.DNSSelections[i].Candidates {
			candidate := &snapshot.DNSSelections[i].Candidates[j]
			candidate.EdgeGroupID = aliases[candidate.EdgeGroupID]
		}
	}
	c, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: intent, Policy: policy, RuntimeSnapshot: snapshot, CreatedAt: req.CreatedAt})
	if err != nil {
		t.Fatal(err)
	}
	route := Sign(t, c.RouteArtifact, "previous-route", 1)
	tls := Sign(t, c.TLSArtifact, "previous-tls", 1)
	dns := Sign(t, c.DNSArtifact, "previous-dns", 1)
	parent := Sign(t, platformconfig.BuildReleaseSetArtifact(c.ReleaseSet, []string{route.ID, dns.ID, tls.ID}, req.CreatedAt), "previous-parent", 1)
	ref := platformconfig.PreviousTrafficPublicationReference{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, ReleaseID: "previous-release", FencingToken: 1, RouteArtifactID: route.ID, RouteArtifactDigest: route.ContentHash, TLSArtifactID: tls.ID, TLSArtifactDigest: tls.ContentHash, DNSArtifactID: dns.ID, DNSArtifactDigest: dns.ContentHash}
	req.PreviousTrafficPublication = &platformconfig.PreviousTrafficPublicationInput{Reference: ref, Parent: parent, Route: route, TLS: tls, DNS: dns}
	req.Intent.RouteAuthorityTransition = &platformconfig.RouteAuthorityTransition{PreviousTopology: previousTopology, PreviousPublication: ref}
	return req
}

func PreviousPublication(p platformconfig.PreviousTrafficPublicationInput) model.PlatformArtifactRelease {
	at := p.Parent.CreatedAt
	return model.PlatformArtifactRelease{ID: p.Reference.ReleaseID, ArtifactID: p.Parent.ID, ArtifactKind: p.Parent.ArtifactKind, Scope: p.Parent.Scope, ScopeKey: p.Parent.ScopeKey, Generation: p.Parent.Generation, ReleaseChannel: "full", FencingToken: p.Reference.FencingToken, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: at, LaneKey: platformsafety.ReleaseLaneKey(p.Parent.ArtifactKind, p.Parent.ScopeKey, "full"), VerificationState: model.PlatformArtifactVerificationStateVerified, VerifiedLKGGeneration: p.Parent.Generation, VerifiedAt: &at}
}
