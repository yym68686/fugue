package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
)

func cellProducerFixture(t *testing.T, s *Server, cell string) platformproducer.Policy {
	return cellProducerRoleFixture(t, s, cell, "")
}

func cellProducerRoleFixture(t *testing.T, s *Server, cell, role string) platformproducer.Policy {
	t.Helper()
	scope := platformconfig.AuthorityCellScope(cell)
	intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: scope, AuthorityCellID: cell, Generation: "base-" + cell,
		EdgeTopology:       &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: cell}}, Pools: []edgetopology.ServingPool{{ID: "pool-public"}}, Edges: []edgetopology.Edge{{ID: "edge-a", AuthorityCellID: cell, ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "edge-a"}}}},
		DNSConsumers:       []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: cell, Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}},
		ApplicationDomains: &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", CustomDomainBaseDomain: "dns.example.test", ReservedHostnames: []string{"api.example.test"}, DefaultDNSTTL: 60},
		Routes:             []platformconfig.RouteIntent{{Hostname: "api.example.test", Kind: model.EdgeRouteKindControlPlaneAPI, UpstreamKind: "url", UpstreamURL: "http://api:8080", Enabled: true}}}
	intent.PublicationRole = role
	if role == platformconfig.PublicationRoleCellRoutes {
		intent.DNSConsumers = nil
	}
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
	dns.PublicationRole = role
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
	if role == platformconfig.PublicationRoleCellRoutes {
		dns.Authorities, dns.Clients, dns.DNSReadiness, dns.DNSQueryPolicy = nil, nil, nil, nil
		dns.DNSPlacementMode = ""
		rules = []platformconfig.RoutePolicyConstraint{{ID: "cross-cell-availability", Hostname: "api.example.test", EdgeGroupID: "cell-other", MinHealthyEdgeNodes: 2, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true}}
	}
	pa := save(model.PlatformArtifactKindPolicySnapshot, scope, dns.Generation, dns)
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "producer-" + cell, AuthorityCellID: cell, Mode: "shadow", InputSource: "business-static-intent", TargetScope: scope, IntervalSeconds: 30, RefreshSeconds: 120, StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, DNSPolicyArtifactID: pa.ID, DNSPolicyDigest: pa.ContentHash, RequireApplicationDomains: true, RequireRouteDefaults: true, RequireDNSQueryPolicy: true}
	p.PublicationRole = role
	if role == platformconfig.PublicationRoleCellRoutes {
		p.RequireDNSQueryPolicy = false
	}
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

func TestCellRouteProducerPublishesAndPreparesOnlyItsRouteTLSAuthority(t *testing.T) {
	testCellRouteProducer(t, false)
}

func TestCellRouteProducerSingleVerifiedPublication(t *testing.T) {
	testCellRouteProducer(t, true)
}

func testCellRouteProducer(t *testing.T, single bool) {
	st, s, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	verified := time.Now().UTC().Add(-time.Minute)
	_, err := st.PutAppDomain(model.AppDomain{Hostname: "private.example.net", AppID: app.ID, TenantID: app.TenantID, Status: model.AppDomainStatusVerified, TLSStatus: model.AppDomainTLSStatusReady, VerifiedAt: &verified, TLSReadyAt: &verified})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutEdgeTLSCertificate(model.EdgeTLSCertificate{Hostname: "private.example.net", AppID: app.ID, TenantID: app.TenantID, CertificatePEM: "synthetic-certificate", PrivateKeyPEM: "synthetic-private-key"}); err != nil {
		t.Fatal(err)
	}
	p := cellProducerRoleFixture(t, s, "cell-a", platformconfig.PublicationRoleCellRoutes)
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || strings.HasPrefix(r.URL.Path, "/api/v1/nodes/") {
			t.Error("route-only publication read DNS endpoint candidates or mutated inventory")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
	}))
	defer kube.Close()
	s.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "reader"}, nil
	}
	scope, _ := platformproducer.PolicyScopeForTarget(p.TargetScope)
	if _, err := s.reconcilePlatformConfigurationScope(context.Background(), scope, nil); err != nil {
		t.Fatal(err)
	}
	parent, shadow, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if err != nil || !found {
		t.Fatal("cell output absent", err)
	}
	sets, err := st.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID, ArtifactReleaseID: shadow.ID})
	if err != nil || len(sets) != 2 {
		t.Fatal("route/TLS expectations incomplete", sets, err)
	}
	assertCellCertificateAccess(t, s, app, parent, shadow, false)
	route, err := s.consumerAssignmentChild(parent, model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := platformconfig.ProjectRouteArtifact(route)
	if err != nil {
		t.Fatal(err)
	}
	foundApp, foundConstraint := false, false
	for _, r := range projected.Routes {
		foundApp = foundApp || r.AppID == app.ID
		foundConstraint = foundConstraint || r.Hostname == "api.example.test" && r.MinHealthyEdgeNodes == 2
	}
	if !foundApp || !foundConstraint {
		t.Fatal("live business or cross-cell DNS constraint lost")
	}
	var payload struct {
		Routes []platformconfig.CompiledRoute `json:"routes"`
	}
	raw, _ := json.Marshal(route.Content)
	if json.Unmarshal(raw, &payload) != nil {
		t.Fatal("route artifact unreadable")
	}
	foundConstraint = false
	for _, r := range payload.Routes {
		foundConstraint = foundConstraint || r.Hostname == "api.example.test" && r.DNSPlacementEdgeGroupID == "cell-other"
	}
	if !foundConstraint {
		t.Fatal("DNS constraint lost from signed artifact")
	}
	dns, err := st.ListPlatformArtifacts(model.PlatformArtifactFilter{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: p.TargetScope})
	if err != nil || len(dns) != 0 {
		t.Fatal("route producer created DNS authority", err)
	}
	request := model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=complete", Reason: "isolated route publication test"}
	if _, _, _, _, err := st.ReleasePlatformArtifact(parent.ID, request, platformProducerPrincipal()); err == nil {
		t.Fatal("old executor capability admitted new role")
	}
	sequence := int64(0)
	report := func(release model.PlatformArtifactRelease, capabilities []string, serving bool) {
		t.Helper()
		prepared, err := s.preparePlatformReleaseSetConsumers(context.Background(), platformProducerPrincipal(), parent, release)
		if err != nil || len(prepared) != 2 {
			t.Fatal("role preparation", err)
		}
		sequence++
		for _, set := range prepared {
			if set.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle || len(set.Consumers) != 1 {
				t.Fatal("unexpected consumer", set)
			}
			member := set.Consumers[0]
			child, err := s.consumerAssignmentChild(parent, set.ArtifactKind)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "kubernetes:test-system:worker:pod-a", Component: member.Component, NodeID: member.NodeID, AuthorityID: member.AuthorityID, ScopeKey: p.TargetScope, ArtifactKinds: []string{set.ArtifactKind}}
			token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), claims, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			claims, err = platformcontrol.ParsePlatformComponentIdentity(edgeRouteIntentTestKeyring(), token, now)
			if err != nil {
				t.Fatal(err)
			}
			h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: member.ConsumerID, Component: member.Component, NodeID: member.NodeID, ArtifactKind: set.ArtifactKind, ScopeKey: p.TargetScope, ReleaseSetID: parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", CompatibilityCapabilities: capabilities, Sequence: sequence, IssuedAt: now, Nonce: fmt.Sprintf("%032x", now.UnixNano()), GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, CandidateGeneration: child.Generation, ApplyStatus: "staged", ProbeStatus: "shadow_validated"}
			if serving {
				h.ActualGeneration, h.LKGGeneration, h.ApplyStatus, h.ProbeStatus = child.Generation, child.Generation, "applied", "passed"
			}
			h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	report(shadow, []string{platformcontrol.TrafficReleaseCapabilityV1}, false)
	if _, _, _, _, err := st.ReleasePlatformArtifact(parent.ID, request, platformProducerPrincipal()); err == nil {
		t.Fatal("legacy traffic capability admitted route-only role")
	}
	caps := []string{platformcontrol.TrafficReleaseCapabilityV1, platformcontrol.CellRoutesCapabilityV1}
	report(shadow, caps, false)
	_, gray, _, _, err := st.ReleasePlatformArtifact(parent.ID, request, platformProducerPrincipal())
	if err != nil {
		t.Fatal("compatible gray publication", err)
	}
	if _, _, err := s.edgeRouteIntentSnapshotFromTrafficRelease("cell-a"); err == nil {
		t.Fatal("unprepared gray authority became readable")
	}
	report(gray, caps, true)
	assertCellCertificateAccess(t, s, app, parent, gray, true)
	projection, found, err := s.edgeRouteIntentSnapshotFromTrafficRelease("cell-a")
	if err != nil || !found || projection.TrafficRelease == nil || projection.TrafficRelease.ReleaseID != gray.ID {
		t.Fatal("prepared role route source unavailable", err)
	}
	verify := model.PlatformArtifactVerifyLKGRequest{FencingToken: gray.FencingToken, Reason: "verified route and TLS fixture", AllowInitialLKG: true, Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{"fixture:route-tls"}}}
	if _, _, _, _, err := st.VerifyPlatformArtifactReleaseLKG(gray.ID, verify, platformProducerPrincipal()); err != nil {
		t.Fatal("route/TLS verification", err)
	}
	if lkg, err := st.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, p.TargetScope); err != nil || lkg != nil {
		t.Fatal("route LKG created DNS recovery authority", err)
	}
	_, full, _, _, err := st.ReleasePlatformArtifact(parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal("full role publication", err)
	}
	verify.FencingToken, verify.AllowInitialLKG = full.FencingToken, false
	if _, _, _, _, err := st.VerifyPlatformArtifactReleaseLKG(full.ID, verify, platformProducerPrincipal()); err == nil {
		t.Fatal("full reused gray facts")
	}
	report(full, caps, true)
	assertCellCertificateAccess(t, s, app, parent, full, true)
	if _, _, _, _, err := st.VerifyPlatformArtifactReleaseLKG(full.ID, verify, platformProducerPrincipal()); err != nil {
		t.Fatal("fresh full verification", err)
	}
	baseline, err := st.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
	if err != nil || baseline == nil {
		t.Fatal("full LKG absent", err)
	}
	if single {
		p.Generation, p.Mode = "producer-serving-once", "serving"
		p.Serving = &platformproducer.ServingPolicy{SinglePublication: true, CanaryRuleRef: "cohort=complete", GrayMinSeconds: 1, FullMinSeconds: 1, RolloutTimeoutSeconds: 60}
		raw, _ := json.Marshal(p)
		var content map[string]any
		json.Unmarshal(raw, &content)
		a, err := st.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: p.Generation, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		a, err = st.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		_, authority, _, _, err := st.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal())
		if err != nil {
			t.Fatal(err)
		}
		reconcile := func() {
			t.Helper()
			if _, err := s.reconcilePlatformConfigurationScope(context.Background(), scope, nil); err != nil {
				t.Fatal(err)
			}
		}
		reconcile()
		parent, gray, found, err = st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "gray")
		if err != nil || !found || parent.ID == baseline.ArtifactID {
			t.Fatal("new gray absent", err)
		}
		// Exercise actual minimum windows, without changing production clocks.
		time.Sleep(1100 * time.Millisecond)
		reconcile()
		_, still, _, _ := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "full")
		if still.ID != full.ID {
			t.Fatal("stale baseline facts promoted candidate")
		}
		report(gray, caps, true)
		reconcile()
		_, full, found, err = st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "full")
		if err != nil || !found || full.ArtifactID != parent.ID {
			t.Fatal("fresh gray did not promote", err)
		}
		time.Sleep(1100 * time.Millisecond)
		reconcile()
		lkg, err := st.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
		if err != nil || lkg.ArtifactID != baseline.ArtifactID {
			t.Fatal("gray facts advanced full LKG", err)
		}
		report(full, caps, true)
		reconcile()
		lkg, err = st.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
		if err != nil || lkg.ArtifactID != parent.ID {
			t.Fatal("full was not verified", err)
		}
		if done, err := st.HasVerifiedProducerPublication(p.TargetScope, authority.ID); err != nil || !done {
			t.Fatal("verified publication was not recorded", err)
		}
		// The hold is reconstructed from the durable ledger by a fresh server.
		restarted := NewServer(st, s.auth, nil, ServerConfig{BundleSigningKey: "platform-artifact-api-test-signing-key", BundleSigningKeyID: "platform-artifact-api-test"})
		capture := func(context.Context, model.Principal) (platformIntentProjectionResponse, error) {
			t.Fatal("single publication recaptured business after restart")
			return platformIntentProjectionResponse{}, nil
		}
		if _, err := restarted.reconcilePlatformConfigurationScope(context.Background(), scope, capture); err != nil {
			t.Fatal(err)
		}
		_, held, _, _ := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "full")
		if held.ID != full.ID {
			t.Fatal("hold changed full")
		}
		if _, _, _, _, err := st.ReleaseProducedTrafficArtifact(parent.ID, authority.ID, "gray", gray.ID, full.ID, parent.ID, p.Serving.CanaryRuleRef, platformProducerPrincipal()); err == nil {
			t.Fatal("transaction admitted second gray under completed authority")
		}
		return
	}
	_, err = st.CreateAppWithRoute(app.TenantID, app.ProjectID, "next", "", model.AppSpec{Image: "registry.example.test/app:next", Ports: []int{8080}, Replicas: 1, RuntimeID: app.Spec.RuntimeID}, model.AppRoute{Hostname: "next.example.test", BaseDomain: "example.test", PublicURL: "https://next.example.test", ServicePort: 8080})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.reconcilePlatformConfigurationScope(context.Background(), scope, nil); err != nil {
		t.Fatal(err)
	}
	previousParent := parent
	parent, shadow, found, err = st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if err != nil || !found || parent.ID == previousParent.ID {
		t.Fatal("live route update did not create candidate", err)
	}
	report(shadow, caps, false)
	_, candidateGray, _, _, err := st.ReleasePlatformArtifact(parent.ID, request, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	report(candidateGray, caps, false)
	if _, _, _, _, err := st.ReleasePlatformArtifact(parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full"}, platformProducerPrincipal()); err == nil {
		t.Fatal("unprobed next candidate became full")
	}
	retained, err := st.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, p.TargetScope)
	if err != nil || !reflect.DeepEqual(retained, baseline) {
		t.Fatal("failed next rollout changed positive LKG", err)
	}
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
