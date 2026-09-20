package api

import (
	"context"
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"reflect"
	"strings"
	"testing"
	"time"
)

func directQueryFixture() (platformIntentProjectionResponse, []model.EdgeNode, platformconfig.DNSQueryPolicy, time.Time) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Country: "aa"}, {ID: "edge-b", EdgeGroupID: "edge-group-b", PublicIPv4: "9.9.9.9", Country: "bb"}}
	p := platformconfig.DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", ECSEnabled: true, ExplorationPercent: 5, SwitchCooldownSeconds: 1800, MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}
	r := platformIntentProjectionResponse{CapturedAt: now, Intent: platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Generation: "intent", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", Kind: model.EdgeRouteKindPlatform, AppID: "app", TenantID: "tenant", RuntimeID: "runtime", OriginRef: "origin", UpstreamURL: "http://app:80", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled}}, DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", Type: "FUGUE_ROUTE", Values: []string{}, TTL: 60, AppID: "app", TenantID: "tenant", RecordKind: model.EdgeDNSRecordKindPlatform, Route: &platformconfig.DNSRouteIntent{Hostnames: []string{"app.example.test"}, DNSApplicationIntent: platformconfig.DNSApplicationIntent{IPv4Policy: "ipv4_only", IPv6Policy: "ipv4_only", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}}, Policy: platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{Generation: "policy", Scope: "global"}), RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, Origins: []platformconfig.OriginObservation{{Ref: "origin", RuntimeID: "runtime", RuntimeEdgeGroupID: "edge-group-b", ObservedAt: now, Status: model.EdgeRouteStatusActive}}, DNSEdgeEndpoints: []platformconfig.DNSEdgeEndpoint{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", ObservedAt: now, A: []string{"8.8.8.8"}}, {EdgeID: "edge-b", EdgeGroupID: "edge-group-b", ObservedAt: now, A: []string{"9.9.9.9"}}}}}
	return r, nodes, p, now
}
func TestDirectDNSQueriesMatchLegacyFixedObservationsWithoutBundle(t *testing.T) {
	_, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	result, nodes, p, now := directQueryFixture()
	candidateByIP := map[string]model.EdgeDNSAnswerCandidate{}
	for _, n := range nodes {
		candidateByIP[n.PublicIPv4] = edgeDNSAnswerCandidateForNode(n.PublicIPv4, n, "")
	}
	ips := []string{"8.8.8.8", "9.9.9.9"}
	profiles := edgeDNSLatencyProfileCatalog{Global: map[string]*edgeDNSLatencyProfile{"app.example.test": {Hostname: "app.example.test", Enabled: true, BestEdgeGroupID: "edge-group-a", Reason: "observed_rank", Weight: 150, Candidates: map[string]edgeDNSLatencyCandidateProfile{"edge-group-a": {Weight: 150, Score: 100}, "edge-group-b": {Weight: 80, Score: 300}}}}}
	sourcePolicy := edgeDNSAnswerPolicy(edgeDNSBundleOptions{EdgeGroupID: "edge-group-a"}, "edge-group-b", "", ips, candidateByIP, profiles.globalProfile("app.example.test"), 60, true)
	sourceCandidates := edgeDNSCandidatesForAnswerIPs(ips, candidateByIP, nil, "edge-group-b", "", profiles.globalProfile("app.example.test"), true)
	before, _ := json.Marshal(result.Intent)
	if err := projectDirectDNSQueries(&result, p, nodes, profiles, now); err != nil {
		t.Fatal(err)
	}
	if len(result.Policy.DNSAnswerRules) != 1 || len(result.RuntimeSnapshot.DNSSelections) != 1 {
		t.Fatal("missing direct query")
	}
	rule, fact := result.Policy.DNSAnswerRules[0], result.RuntimeSnapshot.DNSSelections[0]
	if rule.SelectionMode != sourcePolicy.PolicyKind || !reflect.DeepEqual(rule.PreferredEdgeGroups, sourcePolicy.PreferredEdgeGroups) || rule.TTLSeconds != 60 || !reflect.DeepEqual(fact.Candidates, dnsSelectionCandidates(sourceCandidates)) || fact.SelectedEdgeGroupID != sourcePolicy.SelectedEdgeGroupID {
		t.Fatal("fixed observation projection differs", rule, fact)
	}
	after, _ := json.Marshal(result.Intent)
	if string(before) != string(after) {
		t.Fatal("runtime ranking changed intent")
	}
	raw, _ := json.Marshal(fact)
	for _, f := range []string{"healthy", "route_ready", "tls_ready", "serving_generation", "dns_eligible"} {
		if strings.Contains(string(raw), f) {
			t.Fatal("readiness leaked into selection", f)
		}
	}
	if !strings.HasPrefix(fact.SourceGeneration, "dns-observation_") || !fact.ObservedAt.Equal(now) {
		t.Fatal("direct source identity lost")
	}
	// Missing signed DNS publications is irrelevant to the direct path. An empty
	// runtime inventory cannot authorize candidates, so downstream compilation fails.
	empty, _, disabled, _ := directQueryFixture()
	disabled.RankingMode = "disabled"
	if err := server.captureDirectDNSQueries(context.Background(), &empty, disabled); err != nil {
		t.Fatal("collector consulted missing legacy DNS source", err)
	}
	if len(empty.RuntimeSnapshot.DNSSelections) != 0 {
		t.Fatal("missing inventory created candidates")
	}
}
func TestDirectDNSQueriesPolicyChangesAndNewNames(t *testing.T) {
	r, nodes, p, now := directQueryFixture()
	second := r.Intent.DNS[0]
	second.Hostname = "alias.example.test"
	second.RecordKind = model.EdgeDNSRecordKindCustomDomainTarget
	r.Intent.DNS = append(r.Intent.DNS, second)
	r.Intent.DNSConsumers[0].Zones = append(r.Intent.DNSConsumers[0].Zones, "alias.example.test")
	p.RankingMode = "disabled"
	p.ECSEnabled = false
	p.ExplorationPercent = 0
	p.MinimumTTLSeconds = 180
	p.MaximumTTLSeconds = 300
	if err := projectDirectDNSQueries(&r, p, nodes, edgeDNSLatencyProfileCatalog{}, now); err != nil {
		t.Fatal(err)
	}
	if len(r.Policy.DNSAnswerRules) != 2 {
		t.Fatal("new name or nested zone duplicated or lost")
	}
	for _, rule := range r.Policy.DNSAnswerRules {
		if rule.SelectionMode != "geo" || rule.ECSEnabled || rule.ExplorationPercent != 0 || rule.TTLSeconds != 180 {
			t.Fatal("explicit query policy ignored", rule)
		}
	}
	// Removing a desired name removes both rules and facts, regardless of past publications.
	r.Intent.DNS = r.Intent.DNS[:1]
	if err := projectDirectDNSQueries(&r, p, nodes, edgeDNSLatencyProfileCatalog{}, now); err != nil || len(r.Policy.DNSAnswerRules) != 1 || len(r.RuntimeSnapshot.DNSSelections) != 1 {
		t.Fatal("deleted name persisted", err)
	}
}
func TestDirectDNSQueriesRejectOwnershipWithoutPartialMutation(t *testing.T) {
	for _, scenario := range []string{"group", "address", "invalid policy"} {
		t.Run(scenario, func(t *testing.T) {
			r, nodes, p, now := directQueryFixture()
			before, _ := json.Marshal(r)
			switch scenario {
			case "group":
				nodes[0].EdgeGroupID = "foreign"
			case "address":
				nodes[0].PublicIPv4 = "1.1.1.1"
			case "invalid policy":
				p.MaximumTTLSeconds = 0
			}
			if err := projectDirectDNSQueries(&r, p, nodes, edgeDNSLatencyProfileCatalog{}, now); err == nil {
				t.Fatal("invalid direct observation accepted")
			}
			after, _ := json.Marshal(r)
			if string(before) != string(after) {
				t.Fatal("invalid capture changed draft")
			}
		})
	}
}
func TestDirectDNSExplicitCooldownUsesOriginalSwitchTime(t *testing.T) {
	now := time.Now().UTC()
	existing := model.EdgeDNSRoutingDecision{SelectedEdgeGroupID: "edge-group-a", SwitchedAt: now.Add(-120 * time.Second), CooldownUntil: now.Add(time.Hour)}
	makeProfile := func() *edgeDNSLatencyProfile {
		return &edgeDNSLatencyProfile{BestEdgeGroupID: "edge-group-b", Candidates: map[string]edgeDNSLatencyCandidateProfile{"edge-group-a": {Weight: 100}, "edge-group-b": {Weight: 150}}}
	}
	held, _ := applyEdgeDNSRoutingDecisionWithCooldown(makeProfile(), existing, now, 180*time.Second, true)
	if held.BestEdgeGroupID != "edge-group-a" || !held.CooldownUntil.Equal(existing.SwitchedAt.Add(180*time.Second)) {
		t.Fatal("explicit cooldown ignored original switch time")
	}
	changed, _ := applyEdgeDNSRoutingDecisionWithCooldown(makeProfile(), existing, now, 60*time.Second, true)
	if changed.BestEdgeGroupID != "edge-group-b" {
		t.Fatal("shorter policy kept legacy cooldown")
	}
}

func TestDirectDNSQueriesCompileReplayAndRequireIndependentReadiness(t *testing.T) {
	r, nodes, p, now := directQueryFixture()
	r.Policy.DNSReadiness = &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
	r.RuntimeSnapshot.DNSConsumers = []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", ObservedAt: now, A: []string{"1.1.1.1"}}}
	if err := projectDirectDNSQueries(&r, p, nodes, edgeDNSLatencyProfileCatalog{}, now); err != nil {
		t.Fatal(err)
	}
	request := platformconfig.CompileRequest{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot}
	if _, err := platformconfig.Compile(request); err == nil {
		t.Fatal("selection observation became readiness")
	}
	owners, _, err := placementRecordRoutes(r, r.Intent.DNS[0])
	if err != nil {
		t.Fatal(err)
	}
	digest, err := platformconfig.DNSPlacementInputDigest(r.Intent.DNS[0], owners, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	fact := platformconfig.DNSPlacementObservation{InputDigest: digest, CheckedAt: now, Status: "resolved", TargetTTL: 60}
	for _, n := range nodes {
		fact.Candidates = append(fact.Candidates, platformconfig.DNSPlacementCandidate{EdgeID: n.ID, EdgeGroupID: n.EdgeGroupID, ServingGeneration: "verified-route", ObservedAt: now, ValidUntil: now.Add(time.Minute), Healthy: true, RouteReady: true, TLSReady: true, A: []string{n.PublicIPv4}})
	}
	request.RuntimeSnapshot.DNSPlacements = []platformconfig.DNSPlacementObservation{fact}
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	var views []platformconfig.DNSQueryView
	raw, _ := json.Marshal(compiled.DNSArtifact.Content["query_views"])
	if err = json.Unmarshal(raw, &views); err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || len(views[0].Records) != 2 {
		t.Fatal("compiled consumer query missing", views)
	}
	for _, record := range views[0].Records {
		for _, c := range record.Candidates {
			if c.Healthy || c.RouteReady || c.TLSReady {
				t.Fatal("compiler fabricated readiness")
			}
		}
	}
	request.CreatedAt = time.Now().Add(time.Hour)
	replay, err := platformconfig.Compile(request)
	if err != nil || !reflect.DeepEqual(compiled.DNSArtifact.Content, replay.DNSArtifact.Content) {
		t.Fatal("fixed input not replayable", err)
	}
	request.RuntimeSnapshot.DNSPlacements[0].Candidates[0].RouteReady = false
	request.RuntimeSnapshot.DNSPlacements[0].Candidates[1].RouteReady = false
	if _, err = platformconfig.Compile(request); err == nil {
		t.Fatal("failed route proofs accepted")
	}
}

func TestDirectDNSQueriesSingleTargetPreservesPreferredGroupsAndOwnerRanking(t *testing.T) {
	r, nodes, p, now := directQueryFixture()
	r.Policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{{ID: "pin", Hostname: "app.example.test", AppID: "app", TenantID: "tenant", EdgeGroupID: "edge-group-b", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true}}
	profiles := edgeDNSLatencyProfileCatalog{Global: map[string]*edgeDNSLatencyProfile{"app.example.test": {Hostname: "app.example.test", Enabled: true, BestEdgeGroupID: "edge-group-b", Candidates: map[string]edgeDNSLatencyCandidateProfile{"edge-group-a": {Weight: 20, Score: 200}, "edge-group-b": {Weight: 200, Score: 100}}}}}
	r.Intent.DNS[0].Hostname = "target.example.test"
	r.Intent.DNS[0].RecordKind = model.EdgeDNSRecordKindCustomDomainTarget
	if err := projectDirectDNSQueries(&r, p, nodes, profiles, now); err != nil {
		t.Fatal(err)
	}
	rule := r.Policy.DNSAnswerRules[0]
	fact := r.RuntimeSnapshot.DNSSelections[0]
	if !reflect.DeepEqual(rule.PreferredEdgeGroups, []string{"edge-group-a", "edge-group-b"}) || len(fact.Candidates) != 1 || fact.Candidates[0].EdgeGroupID != "edge-group-b" {
		t.Fatal("single target prematurely filtered policy or escaped DNS pin", rule, fact)
	}
	if rule.SelectionMode != "latency_aware" || fact.Candidates[0].Score != 100 {
		t.Fatal("query ranking must follow declared route hostname, not target or former app alias")
	}
}
