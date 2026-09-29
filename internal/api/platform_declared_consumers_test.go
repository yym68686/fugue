package api

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestDeclaredCellPreparationIgnoresLegacyInventoryAndRequiresEveryMember(t *testing.T) {
	state, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	// The retained inventory has the same stable physical ID under its old
	// authority. Preparation must neither relabel it nor borrow its evidence.
	if _, _, err := state.CreateEdgeNodeToken(model.EdgeNode{ID: "node-a", EdgeGroupID: "edge-group-old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", EdgeGroupID: "edge-group-old", Zone: "example.test"}); err != nil {
		t.Fatal(err)
	}
	oldEdges, _, _ := state.ListEdgeNodes("")
	oldDNS, _ := state.ListDNSNodes("")
	intent := platformconfig.PlatformIntent{AuthorityCellID: "cell-a", SchemaVersion: platformconfig.SchemaVersion, Generation: "cell-intent", Scope: platformconfig.AuthorityCellScope("cell-a"), EdgeTopology: &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: "cell-a"}}, Pools: []edgetopology.ServingPool{{ID: "pool-public"}}, Edges: []edgetopology.Edge{{ID: "node-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-a"}}, {ID: "node-b", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-b"}}}}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "cell-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: false}}}
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := platformconfig.Digest(topology)
	now := time.Now().UTC()
	req := platformConfigCompileRequest{Intent: intent, Policy: platformconfig.PolicySnapshot{AuthorityCellID: "cell-a", ConsumerTopologyDigest: digest, Scope: intent.Scope, Generation: "cell-policy", TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{"cell-a"}}}}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "cell-a", ObservedAt: now, A: []string{"8.8.8.8"}}}}}
	r := performJSONRequest(t, s, "POST", "/v1/admin/platform-config/compile", admin, req)
	if r.Code != 201 {
		t.Fatal(r.Code, r.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, r, &compiled)
	r = performJSONRequest(t, s, "POST", "/v1/admin/artifacts/"+compiled.ReleaseArtifact.ID+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=complete", Reason: "isolated cell fixture"})
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, r, &released)
	sets, err := s.preparePlatformReleaseSetConsumers(context.Background(), model.Principal{}, compiled.ReleaseArtifact, released.Release)
	if err != nil || len(sets) != 3 {
		t.Fatal(sets, err)
	}
	for _, set := range sets {
		want := 2
		if set.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
			want = 1
		}
		if set.RequiredCardinality != want || set.OptionalCardinality != 0 || len(set.Consumers) != want {
			t.Fatal("declared member missing", set)
		}
		if err := platformcontrol.ValidateDeclaredTrafficConsumerSet(compiled.ReleaseArtifact, set); err != nil {
			t.Fatal(err)
		}
		rounded := set
		rounded.CreatedAt = rounded.CreatedAt.Truncate(time.Microsecond)
		if err := platformcontrol.ValidateDeclaredTrafficConsumerSet(compiled.ReleaseArtifact, rounded); err != nil {
			t.Fatal("database precision changed membership", err)
		}
		for _, member := range set.Consumers {
			if member.AuthorityID != "cell-a" || member.Cohort != "cell-a" || !member.Required {
				t.Fatal(member)
			}
		}
		if got := platformcontrol.EvaluateConsumerConvergence(set, nil, time.Now(), s.platformConvergenceBinding(set)); got.Pass {
			t.Fatal("declaration invented serving health")
		}
	}
	more, err := s.preparePlatformReleaseSetConsumers(context.Background(), model.Principal{}, compiled.ReleaseArtifact, released.Release)
	if err != nil || !reflect.DeepEqual(sets, more) {
		t.Fatal("repeat preparation changed immutable declaration", err)
	}
	newEdges, _, _ := state.ListEdgeNodes("")
	newDNS, _ := state.ListDNSNodes("")
	if !reflect.DeepEqual(oldEdges, newEdges) || !reflect.DeepEqual(oldDNS, newDNS) {
		t.Fatal("preparation rewrote retained inventory")
	}
	response := performJSONRequest(t, s, "GET", "/v1/admin/platform-state/convergence?release_set_id="+compiled.ReleaseArtifact.ID, admin, nil)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var statuses struct {
		Convergence []model.PlatformConsumerConvergenceStatus `json:"convergence"`
	}
	mustDecodeJSON(t, response, &statuses)
	if len(statuses.Convergence) != 3 {
		t.Fatal("cell convergence absent")
	}
	for _, status := range statuses.Convergence {
		if status.Pass || status.RequiredExpected == 0 {
			t.Fatal("legacy projection removed required cell members", status)
		}
	}
	full := performJSONRequest(t, s, "POST", "/v1/admin/artifacts/"+compiled.ReleaseArtifact.ID+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", Reason: "missing consumers must reject"})
	if full.Code != 409 {
		t.Fatal("unobserved cell published", full.Code, full.Body.String())
	}
	// An apparently new immutable revision cannot omit a required silent Edge.
	bad := sets[0]
	bad.Consumers = append([]model.PlatformExpectedConsumer(nil), bad.Consumers...)
	if bad.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
		bad = sets[1]
	}
	bad.ID += "-incomplete"
	bad.Revision += 100
	bad.Consumers = bad.Consumers[:1]
	bad.RequiredCardinality = 1
	if _, err = state.CreatePlatformExpectedConsumerSet(bad); err != nil {
		t.Fatal(err)
	}
	claims := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentEdgeWorker, NodeID: bad.Consumers[0].NodeID, AuthorityID: "cell-a", ScopeKey: intent.Scope, ArtifactKinds: []string{bad.ArtifactKind}}
	if _, err = s.resolvePlatformConsumerAssignments(claims); err == nil {
		b, _ := json.Marshal(bad)
		t.Fatal("tampered expectation authorized assignment", string(b))
	}
}
