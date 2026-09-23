package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	runtimepkg "fugue/internal/runtime"
)

func seedVerifiedDNSDelegationFixture(t *testing.T, s *Server, zone string, compiledExtras ...platformconfig.RouteIntent) {
	t.Helper()
	nodes, err := s.store.ListDNSNodes("")
	if err != nil {
		t.Fatal(err)
	}
	consumers := []platformconfig.DNSConsumerIntent{}
	observations := []platformconfig.DNSConsumerObservation{}
	authorities := []platformconfig.DNSAuthorityPolicy{}
	clients := []platformconfig.DNSClientPolicy{}
	templates := []platformproducer.HostedZoneTemplate{}
	inventory := []clusterNodeSnapshot{}
	names := []string{"ns1." + zone, "ns2." + zone}
	groups := []string{}
	seen := map[string]bool{}
	now := time.Now().UTC()
	for _, n := range nodes {
		id := firstNonEmpty(n.PhysicalNodeID, n.ID)
		if seen[id] {
			continue
		}
		seen[id] = true
		groups = append(groups, n.EdgeGroupID)
		consumers = append(consumers, platformconfig.DNSConsumerIntent{NodeID: id, EdgeGroupID: n.EdgeGroupID, Zones: []string{zone}, ProbeLabel: defaultEdgeDNSProbeLabel, ProbeTTL: 60})
		observations = append(observations, platformconfig.DNSConsumerObservation{NodeID: id, EdgeGroupID: n.EdgeGroupID, A: []string{[]string{"8.8.8.8", "8.8.4.4", "9.9.9.9"}[len(observations)]}, ObservedAt: now})
		authorities = append(authorities, platformconfig.DNSAuthorityPolicy{NodeID: id, Zone: zone, Nameservers: names, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600})
		clients = append(clients, platformconfig.DNSClientPolicy{NodeID: id})
		templates = append(templates, platformproducer.HostedZoneTemplate{NodeID: id, TemplateZone: zone})
		inventory = append(inventory, clusterNodeSnapshot{node: model.ClusterNode{Name: id}, labels: map[string]string{runtimepkg.DNSRoleLabelKey: runtimepkg.NodeRoleLabelValue}})
	}
	if len(consumers) == 0 {
		t.Fatal("DNS fixture requires nodes")
	}
	for i, group := range uniqueSortedStrings(groups) {
		id := fmt.Sprintf("delegation-edge-%d", i)
		inventory = append(inventory, clusterNodeSnapshot{node: model.ClusterNode{Name: id}, labels: map[string]string{runtimepkg.EdgeRoleLabelKey: runtimepkg.NodeRoleLabelValue}})
		if _, _, err = s.store.CreateEdgeNodeToken(model.EdgeNode{ID: id, EdgeGroupID: group, PublicIPv4: []string{"1.1.1.1", "1.0.0.1", "9.9.9.10"}[i]}); err != nil {
			t.Fatal(err)
		}
	}
	oldInventory, hadInventory := s.clusterNodeInventoryCache.get(clusterNodeInventoryCacheKey)
	defer func() {
		s.clusterNodeInventoryCache.clear(clusterNodeInventoryCacheKey)
		if hadInventory {
			s.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, oldInventory)
		}
	}()
	s.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, inventory)
	save := func(kind, scope, gen string, value any) model.PlatformArtifact {
		t.Helper()
		b, _ := json.Marshal(value)
		m := map[string]any{}
		json.Unmarshal(b, &m)
		a, e := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: gen, Content: m})
		if e != nil {
			t.Fatal(e)
		}
		a, e = s.store.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "synthetic baseline", Pass: true}})
		if e != nil {
			t.Fatal(e)
		}
		return a
	}
	intent := platformconfig.NormalizePlatformIntent(platformconfig.PlatformIntent{Scope: "global", Generation: "delegation-base", DNSConsumers: consumers, Routes: []platformconfig.RouteIntent{{Hostname: "disabled." + zone, UpstreamURL: "http://origin:8080", Enabled: false}}})
	base := save(model.PlatformArtifactKindPlatformIntent, "global", intent.Generation, intent)
	probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
	policyInput := platformproducer.ProjectionPolicyInput{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "delegation-policy", Authorities: authorities, Clients: clients, DNSReadiness: probe, TLSReadiness: probe, Cohorts: []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: uniqueSortedStrings(groups)}}}
	pa := save(model.PlatformArtifactKindPolicySnapshot, "global", policyInput.Generation, policyInput)
	producer := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "delegation-producer", Mode: "shadow", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 300, StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, DNSPolicyArtifactID: pa.ID, DNSPolicyDigest: pa.ContentHash, HostedZoneTemplates: templates}
	authority := save(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, producer.Generation, producer)
	_, pr, _, _, err := s.store.ReleasePlatformArtifact(authority.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	policy := platformconfig.PolicySnapshot{Scope: "global", Generation: "delegation-compiled-policy", DNSReadiness: probe, DNSAuthorities: authorities, DNSClientPolicies: clients, TrafficRolloutCohorts: policyInput.Cohorts}
	intent.Generation = "delegation-compiled-intent"
	intent.Routes = append(intent.Routes, compiledExtras...)
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: intent, Policy: policy, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSConsumers: observations, Facts: map[string]any{"configuration_producer": map[string]any{"policy_release_id": pr.ID, "source_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "static_intent_artifact_id": base.ID, "static_intent_digest": base.ContentHash, "dns_policy_artifact_id": pa.ID, "dns_policy_digest": pa.ContentHash}}}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.materializePlatformCompilation(context.Background(), compiled, platformProducerPrincipal(), nil, platformCompilationSource{PolicyReleaseID: pr.ID, SourceDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", StaticIntentID: base.ID, StaticIntentDigest: base.ContentHash, DNSPolicyID: pa.ID, DNSPolicyDigest: pa.ContentHash})
	if err != nil {
		t.Fatal(err)
	}
	_, gray, _, _, err := s.store.ReleasePlatformArtifact(c.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=complete"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal("release gray", err)
	}
	reportServingProducerAPI(t, s, c.ReleaseArtifact, gray)
	_, _, _, _, err = s.store.VerifyPlatformArtifactReleaseLKG(gray.ID, model.PlatformArtifactVerifyLKGRequest{FencingToken: gray.FencingToken, AllowInitialLKG: true, Reason: "verified synthetic baseline", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{"synthetic"}}}, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
}

func TestDelegationNeverFallsBackToEnvironmentOrDraft(t *testing.T) {
	_, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.dnsNameservers = []string{"ambient.example.test"}
	s.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "example.test", Type: "NS", Values: s.dnsNameservers, TTL: 60}}
	if _, err := s.verifiedDNSDelegationHints("example.test"); err == nil {
		t.Fatal("ambient delegation accepted")
	}
	response := s.buildDNSDelegationPreflight(context.Background(), model.Principal{}, dnsDelegationPreflightOptions{Zone: "example.test"})
	if response.Pass || len(response.DelegationPlan.PlannedNSRecords) != 0 || response.Checks[0].Name != "dns_delegation_configuration" {
		t.Fatal("missing source produced registrar plan", response)
	}
	old := model.HostedZone{ZoneName: "example.test", ExpectedNameservers: []string{"keep.example.test"}}
	updated := s.applyHostedDNSZonePreflight(old, response)
	if !reflect.DeepEqual(old.ExpectedNameservers, updated.ExpectedNameservers) || updated.DelegationStatus != model.HostedZoneDelegationStatusPending {
		t.Fatal("bad config erased last expectation")
	}
	r := performJSONRequest(t, s, http.MethodPost, "/v1/dns/zones", admin, map[string]any{"zone_name": "new.example.test", "tenant_id": "synthetic-tenant"})
	if r.Code != 503 {
		t.Fatal(r.Code, r.Body.String())
	}
	zones, _ := s.store.ListHostedZones("", true)
	if len(zones) != 0 {
		t.Fatal("missing verified config created zone")
	}
}

func TestVerifiedDelegationBindsHistoricalSourcesAndRejectsInvalidSource(t *testing.T) {
	state, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	now := time.Now().UTC()
	for i, ip := range []string{"192.0.2.10", "192.0.2.11"} {
		if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: fmt.Sprintf("dns-%d", i), EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: ip, Healthy: true, Status: model.EdgeHealthHealthy, LastSeenAt: &now}); err != nil {
			t.Fatal(err)
		}
	}
	seedVerifiedDNSDelegationFixture(t, s, "example.test")
	s.dnsNameservers = []string{"ambient.invalid"}
	s.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "example.test", Type: "NS", Values: s.dnsNameservers, TTL: 60}}
	for _, zone := range []string{"example.test", "new.example.test"} {
		hint, err := s.verifiedDNSDelegationHints(zone)
		if err != nil || !reflect.DeepEqual(hint.Nameservers, []string{"ns1.example.test", "ns2.example.test"}) {
			t.Fatal("verified source/template not used", zone, hint, err)
		}
	}
	parent, found, err := s.verifiedPlatformArtifactForScope(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || !found {
		t.Fatal(err)
	}
	producerRelease, err := state.GetPlatformArtifactRelease(parent.Metadata[platformproducer.PolicyReleaseMetadata])
	if err != nil {
		t.Fatal(err)
	}
	source, err := state.GetPlatformArtifact(producerRelease.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := platformproducer.Decode(source)
	if err != nil {
		t.Fatal(err)
	}
	p.Generation = "new-unverified-authority"
	p.HostedZoneTemplates = nil
	b, _ := json.Marshal(p)
	content := map[string]any{}
	json.Unmarshal(b, &content)
	a, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: p.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	a, err = state.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "new draft", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err = state.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.verifiedDNSDelegationHints("new.example.test"); err != nil {
		t.Fatal("new unverified producer changed verified template", err)
	}
	original, _ := state.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if _, err = state.ValidatePlatformArtifact(parent.Metadata[platformproducer.DNSPolicyIDMetadata], []model.PlatformArtifactValidationResult{{Name: "invalid source", Pass: false}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.verifiedDNSDelegationHints("example.test"); err == nil {
		t.Fatal("invalid pinned policy fell back to environment")
	}
	after, _ := state.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if !reflect.DeepEqual(original, after) {
		t.Fatal("delegation read changed recovery baseline")
	}
}

func TestPinnedDelegationUsesExplicitTemplateAndUnexpiredDeclaredGlue(t *testing.T) {
	p, consumers, _ := pinnedDNSFixture()
	now := time.Now().UTC()
	intent := platformproducer.StaticIntentInput{Consumers: consumers, DNS: []model.EdgeDNSRecord{
		{Name: "ns.example.test", Type: "A", Values: []string{"192.0.2.1", "192.0.2.2"}, ValueExpirations: map[string]time.Time{"192.0.2.1": now.Add(-time.Second)}, TTL: 60},
		{Name: "new.example.test", Type: "NS", Values: []string{"rogue.example.test"}, TTL: 60},
		{Name: "unused.example.test", Type: "A", Values: []string{"192.0.2.3"}, TTL: 60},
	}}
	if _, err := dnsDelegationHintsFromPinned("new.example.test", intent, p, nil, now); err == nil {
		t.Fatal("undeclared zone accepted without template")
	}
	hint, err := dnsDelegationHintsFromPinned("new.example.test", intent, p, []platformproducer.HostedZoneTemplate{{NodeID: "dns-a", TemplateZone: "example.test"}}, now)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hint.Nameservers, []string{"ns.example.test"}) || !reflect.DeepEqual(hint.ARecords["ns.example.test"], []string{"192.0.2.2"}) || len(hint.ARecords) != 1 {
		t.Fatal("authority/glue mixed with ambient records or expiry", hint)
	}
	p.Authorities = nil
	if _, err := dnsDelegationHintsFromPinned("example.test", intent, p, nil, now); err == nil {
		t.Fatal("missing authority accepted")
	}
}

func TestDelegationUsesDeclaredPhysicalMultiZoneFacts(t *testing.T) {
	now := time.Now().UTC()
	nodes := []model.DNSNode{
		{ID: "dns-a--zone-old", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "hosted.example.test", ServingGeneration: "obsolete", Healthy: false},
		{ID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "base.example.test", ServingGeneration: "current", Healthy: true, LastSeenAt: &now},
		{ID: "foreign", EdgeGroupID: "edge-group-a", Zone: "hosted.example.test", Healthy: true},
		{ID: "dns-b", EdgeGroupID: "wrong-group", Zone: "hosted.example.test", Healthy: true},
	}
	original := append([]model.DNSNode(nil), nodes...)
	for _, input := range [][]model.DNSNode{nodes, {nodes[1], nodes[0], nodes[2], nodes[3]}} {
		out := dnsNodesForVerifiedDelegation(input, "hosted.example.test", map[string]string{"dns-a": "edge-group-a", "dns-b": "edge-group-b"})
		if len(out) != 1 || out[0].ID != "dns-a" || out[0].PhysicalNodeID != "dns-a" || out[0].Zone != "hosted.example.test" || out[0].ServingGeneration != "current" || !out[0].Healthy {
			t.Fatal("legacy alias or foreign node replaced physical facts", out)
		}
	}
	if !reflect.DeepEqual(nodes, original) {
		t.Fatal("preflight mutated runtime facts")
	}
}

func TestDelegationPlanCannotReplaceDeclaredGlueWithInventoryAddress(t *testing.T) {
	plan := buildDNSDelegationPlan("example.test", []model.DNSDelegationNodeCheck{{DNSNodeID: "dns-a", PublicIP: "192.0.2.1", Pass: true}}, nil, dnsDelegationPlanHint{Nameservers: []string{"ns.example.test"}, ARecords: map[string][]string{"ns.example.test": {"192.0.2.2"}}})
	if len(plan.PlannedARecords) != 1 || !reflect.DeepEqual(plan.PlannedARecords[0].Values, []string{"192.0.2.2"}) || !reflect.DeepEqual(plan.RollbackDeleteRecords[0].Values, []string{"192.0.2.2"}) {
		t.Fatal("inventory replaced declared glue", plan)
	}
	p, c, _ := pinnedDNSFixture()
	now := time.Now().UTC()
	in := platformproducer.StaticIntentInput{Consumers: c, DNS: []model.EdgeDNSRecord{{Name: "ns.example.test", Type: "A", Values: []string{"192.0.2.1"}, ValueExpirations: map[string]time.Time{"192.0.2.1": now.Add(-time.Second)}}}}
	if _, err := dnsDelegationHintsFromPinned("example.test", in, p, nil, now); err == nil {
		t.Fatal("expired glue allowed inventory fallback")
	}
}

func TestHostedZoneCreationUsesVerifiedTemplateWithoutAmbientNS(t *testing.T) {
	state, s, _, admin, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	now := time.Now().UTC()
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "192.0.2.8", Healthy: true, Status: model.EdgeHealthHealthy, LastSeenAt: &now}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, s, "example.test")
	s.dnsNameservers = []string{"ambient.invalid"}
	s.newClusterNodeClient = func() (*clusterNodeClient, error) { return nil, fmt.Errorf("synthetic unavailable topology") }
	s.dnsDelegationProbe = func(context.Context, model.DNSNode, string, string) dnsDelegationProbeResult {
		return dnsDelegationProbeResult{}
	}
	s.dnsParentNSLookup = func(context.Context, string) ([]string, error) { return nil, nil }
	response := performJSONRequest(t, s, http.MethodPost, "/v1/dns/zones", admin, map[string]any{"zone_name": "hosted.example.test", "tenant_id": app.TenantID})
	if response.Code != 201 {
		t.Fatal(response.Code, response.Body.String())
	}
	var result struct {
		Zone model.HostedZone `json:"zone"`
	}
	mustDecodeJSON(t, response, &result)
	if !reflect.DeepEqual(result.Zone.ExpectedNameservers, []string{"ns1.example.test", "ns2.example.test"}) || result.Zone.DelegationStatus != model.HostedZoneDelegationStatusPending {
		t.Fatal("new zone did not preserve declared template", result.Zone)
	}
	stored, err := state.GetHostedZoneByName(result.Zone.ZoneName)
	if err != nil || !reflect.DeepEqual(stored.ExpectedNameservers, result.Zone.ExpectedNameservers) {
		t.Fatal("declaration not persisted", err)
	}
}
