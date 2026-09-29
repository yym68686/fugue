package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func cellProducerFixture(t *testing.T, s *Server, cell string) platformproducer.Policy {
	t.Helper()
	scope := platformconfig.AuthorityCellScope(cell)
	intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: scope, AuthorityCellID: cell, Generation: "base-" + cell,
		EdgeTopology:       &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: cell}}, Pools: []edgetopology.ServingPool{{ID: "pool-public"}}, Edges: []edgetopology.Edge{{ID: "edge-a", AuthorityCellID: cell, ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "edge-a"}}}},
		DNSConsumers:       []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: cell, Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}},
		ApplicationDomains: &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", CustomDomainBaseDomain: "dns.example.test", ReservedHostnames: []string{"api.example.test"}, DefaultDNSTTL: 60},
		Routes:             []platformconfig.RouteIntent{{Hostname: "api.example.test", Kind: model.EdgeRouteKindControlPlaneAPI, UpstreamKind: "url", UpstreamURL: "http://api:8080", Enabled: true}}}
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := platformconfig.Digest(topology)
	save := func(kind, scope, generation string, value any) model.PlatformArtifact {
		raw, _ := json.Marshal(value)
		content := map[string]any{}
		json.Unmarshal(raw, &content)
		a, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: generation, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		a, err = s.store.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture-input", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	base := save(model.PlatformArtifactKindPlatformIntent, scope, intent.Generation, intent)
	dns, _, _ := pinnedDNSFixture()
	dns.Scope = scope
	dns.AuthorityCellID = cell
	dns.ConsumerTopologyDigest = digest
	dns.Generation = "dns-" + cell
	dns.Cohorts = []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{cell}}}
	dns.DNSPlacementMode = platformconfig.DNSPlacementConsumerReadiness
	dns.DNSQueryPolicy = &platformconfig.DNSQueryPolicy{RankingMode: "disabled", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}
	minimum, stale := 1, 300
	rules := []platformconfig.RoutePolicyConstraint{}
	states := []platformconfig.DNSRouteStateConstraint{}
	dns.MinimumHealthyEdges, dns.MaxStaleSeconds, dns.RouteConstraints, dns.DNSRouteStateConstraints = &minimum, &stale, &rules, &states
	pa := save(model.PlatformArtifactKindPolicySnapshot, scope, dns.Generation, dns)
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "producer-" + cell, AuthorityCellID: cell, Mode: "shadow", InputSource: "business-static-intent", TargetScope: scope, IntervalSeconds: 30, RefreshSeconds: 120, StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, DNSPolicyArtifactID: pa.ID, DNSPolicyDigest: pa.ContentHash, RequireApplicationDomains: true, RequireRouteDefaults: true, RequireDNSQueryPolicy: true}
	ownerScope, err := platformproducer.PolicyScopeForTarget(scope)
	if err != nil {
		t.Fatal(err)
	}
	owner := save(model.PlatformArtifactKindPolicySnapshot, ownerScope, p.Generation, p)
	if _, err := platformproducer.Decode(owner); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := s.store.ReleasePlatformArtifact(owner.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal()); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCellProducerUsesDeclaredMembershipAndCurrentBusinessWithoutLegacyReceipts(t *testing.T) {
	st, s, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	p := cellProducerFixture(t, s, "cell-a")
	missing := false
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("endpoint capture attempted mutation")
		}
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/")
		if name == "edge-a" || name == "dns-a" {
			if missing && name == "edge-a" {
				w.WriteHeader(404)
				return
			}
			ip := "8.8.8.8"
			if name == "edge-a" {
				ip = "1.1.1.1"
			}
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": name}, "status": map[string]any{"addresses": []map[string]string{{"type": "ExternalIP", "address": ip}}}})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
	}))
	defer kube.Close()
	s.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "reader"}, nil
	}
	// Inventory keeps its old authority and deliberately conflicting address.
	if _, _, err := st.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "edge-group-old", PublicIPv4: "9.9.9.9"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", EdgeGroupID: "edge-group-old", Zone: "example.test", PublicIPv4: "9.9.9.10", Healthy: true, ServingGeneration: "legacy"}); err != nil {
		t.Fatal(err)
	}
	oldEdges, _, _ := st.ListEdgeNodes("")
	oldDNS, _ := st.ListDNSNodes("")
	projection, err := s.capturePlatformIntentForProducer(context.Background(), platformProducerPrincipal(), p)
	if err != nil {
		t.Fatal("cell capture", err)
	}
	if projection.Intent.Scope != p.TargetScope || projection.Intent.AuthorityCellID != p.AuthorityCellID || projection.Policy.Scope != p.TargetScope || projection.BusinessSnapshotRevision == "" {
		t.Fatal("cell projection lost authority/business revision")
	}
	foundApp := false
	for _, r := range projection.Intent.Routes {
		foundApp = foundApp || r.AppID == app.ID
	}
	if !foundApp {
		t.Fatal("current business routes missing")
	}
	if len(projection.RuntimeSnapshot.DNSEdgeEndpoints) != 1 || projection.RuntimeSnapshot.DNSEdgeEndpoints[0].A[0] != "1.1.1.1" || projection.RuntimeSnapshot.DNSConsumers[0].A[0] != "8.8.8.8" {
		t.Fatal("old inventory supplied cell endpoint")
	}
	scope, _ := platformproducer.PolicyScopeForTarget(p.TargetScope)
	if _, err := s.reconcilePlatformConfigurationScope(context.Background(), scope, nil); err != nil {
		t.Fatal("cell publish", err)
	}
	parent, release, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if err != nil || !found || parent.Metadata[platformproducer.PolicyReleaseMetadata] == "" {
		t.Fatal("cell shadow absent", err)
	}
	if _, _, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "shadow"); err != nil || found {
		t.Fatal("cell displaced global")
	}
	if lkg, err := st.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope); err != nil || lkg != nil {
		t.Fatal("shadow minted LKG")
	}
	scopes, err := st.ListPlatformProducerScopes()
	if err != nil || !reflect.DeepEqual(scopes, []string{scope}) {
		t.Fatal(scopes, err)
	}
	// Signed output cannot claim the right static reference while compiling a
	// different consumer membership. The store checks the output against the
	// pinned source policy again at publication.
	var forged platformIntentProjectionResponse
	raw, _ := json.Marshal(projection)
	json.Unmarshal(raw, &forged)
	forged.Intent.EdgeTopology.Edges[0].ID = "edge-other"
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(forged.Intent)
	if err != nil {
		t.Fatal(err)
	}
	forged.Policy.ConsumerTopologyDigest, _ = platformconfig.Digest(topology)
	forged.Intent.Generation, _ = platformconfig.PlatformIntentGeneration(forged.Intent)
	forged.Policy.Generation, _ = platformconfig.PolicySnapshotGeneration(forged.Policy)
	forged.RuntimeSnapshot.IntentGeneration, forged.RuntimeSnapshot.PolicyGeneration = forged.Intent.Generation, forged.Policy.Generation
	forged.RuntimeSnapshot.DNSEdgeEndpoints[0].EdgeID = "edge-other"
	for i := range forged.RuntimeSnapshot.DNSSelections {
		for j := range forged.RuntimeSnapshot.DNSSelections[i].Candidates {
			forged.RuntimeSnapshot.DNSSelections[i].Candidates[j].EdgeID = "edge-other"
		}
	}
	sourceDigest := "sha256:" + strings.Repeat("a", 64)
	policyRelease := parent.Metadata[platformproducer.PolicyReleaseMetadata]
	forged.RuntimeSnapshot.Facts = map[string]any{"configuration_producer": map[string]any{"policy_release_id": policyRelease, "source_digest": sourceDigest, "static_intent_artifact_id": p.StaticIntentArtifactID, "static_intent_digest": p.StaticIntentDigest, "dns_policy_artifact_id": p.DNSPolicyArtifactID, "dns_policy_digest": p.DNSPolicyDigest}}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: forged.Intent, Policy: forged.Policy, RuntimeSnapshot: forged.RuntimeSnapshot})
	if err != nil {
		t.Fatal("forged fixture compile", err)
	}
	bad, err := s.materializePlatformCompilation(context.Background(), compiled, platformProducerPrincipal(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.ReleaseProducedPlatformArtifact(bad.ReleaseArtifact.ID, policyRelease, release.ID, platformProducerPrincipal()); err == nil {
		t.Fatal("changed output membership bypassed pinned sources")
	}
	// A second independent scope cannot authorize the first cell's output,
	// even though the physical fixture nodes and source business are shared.
	other := cellProducerFixture(t, s, "cell-b")
	otherScope, _ := platformproducer.PolicyScopeForTarget(other.TargetScope)
	_, otherAuthority, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, otherScope, "shadow")
	if err != nil || !found {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.ReleaseProducedPlatformArtifact(parent.ID, otherAuthority.ID, release.ID, platformProducerPrincipal()); err == nil {
		t.Fatal("foreign producer authorized this cell")
	}
	if _, err := s.reconcilePlatformConfigurationScope(context.Background(), otherScope, nil); err != nil {
		t.Fatal("independent cell publish", err)
	}
	otherParent, otherRelease, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, other.TargetScope, "shadow")
	if err != nil || !found || otherParent.ID == parent.ID {
		t.Fatal("independent output absent", err)
	}
	// Refresh captures live business input instead of copying the first cell
	// publication or the current global release.
	added, err := st.CreateAppWithRoute(app.TenantID, app.ProjectID, "second", "", model.AppSpec{Image: "registry.example.test/app:next", Ports: []int{8080}, Replicas: 1, RuntimeID: app.Spec.RuntimeID}, model.AppRoute{Hostname: "second.example.test", BaseDomain: "example.test", PublicURL: "https://second.example.test", ServicePort: 8080})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.reconcilePlatformConfigurationScope(context.Background(), scope, nil); err != nil {
		t.Fatal("live source refresh", err)
	}
	refreshed, nextRelease, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if err != nil || !found || refreshed.ID == parent.ID || nextRelease.FencingToken <= release.FencingToken {
		t.Fatal("business update did not publish a new cell revision", err)
	}
	route, err := s.consumerAssignmentChild(refreshed, model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := platformconfig.ProjectRouteArtifact(route)
	if err != nil {
		t.Fatal(err)
	}
	gotAdded := false
	for _, r := range projected.Routes {
		gotAdded = gotAdded || r.AppID == added.ID
	}
	if !gotAdded {
		t.Fatal("refreshed artifact omitted new business route")
	}
	_, retainedOther, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, other.TargetScope, "shadow")
	if err != nil || !found || retainedOther.ID != otherRelease.ID {
		t.Fatal("one cell refresh displaced another")
	}
	parent, release = refreshed, nextRelease
	missing = true
	if _, err := s.reconcilePlatformConfigurationScope(context.Background(), scope, nil); err == nil {
		t.Fatal("missing required node was erased")
	}
	retained, rr, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if err != nil || !found || retained.ID != parent.ID || rr.ID != release.ID {
		t.Fatal("failed capture changed publication")
	}
	newEdges, _, _ := st.ListEdgeNodes("")
	newDNS, _ := st.ListDNSNodes("")
	if !reflect.DeepEqual(oldEdges, newEdges) || !reflect.DeepEqual(oldDNS, newDNS) {
		t.Fatal("cell capture relabeled legacy inventory")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.reconcilePlatformConfigurationScope(ctx, scope, nil); err == nil {
		t.Fatal("canceled producer ran")
	}
}
