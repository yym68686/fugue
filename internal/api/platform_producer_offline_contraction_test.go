package api

import (
	"errors"
	"slices"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/routeprobe"
)

func TestOfflineContractionDoesNotChangeUnrelatedConsumers(t *testing.T) {
	offline := map[string]string{"offline": "edge-group-fixture"}
	assessment := model.PlatformConsumerEvidenceAssessment{Required: true, State: model.InvariantEvidenceStateUnknown,
		Expected: model.PlatformExpectedConsumer{NodeID: "offline", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Component: model.PlatformConsumerComponentEdgeWorker, Cohort: "edge-group-fixture", AuthorityID: platformcontrol.ConsumerAuthorityID("edge-group-fixture")}}
	survivor := model.PlatformConsumerEvidenceAssessment{Required: true, State: model.InvariantEvidenceStatePass,
		Expected: model.PlatformExpectedConsumer{NodeID: "healthy", Component: model.PlatformConsumerComponentEdgeWorker}}
	st := model.PlatformConsumerConvergenceStatus{RequiredExpected: 2, Assessments: []model.PlatformConsumerEvidenceAssessment{assessment, survivor}}
	if checkOfflineContractionSurvivors(st, offline, nil) != nil {
		t.Fatal("stale receipts from the withdrawn Worker blocked its own withdrawal")
	}
	st.Assessments[0].Expected.Component = model.PlatformConsumerComponentDNSServer
	if checkOfflineContractionSurvivors(st, offline, nil) == nil {
		t.Fatal("removed a DNS dependency at the same node")
	}
	st.Assessments[0] = assessment
	st.Assessments[0].Expected.Cohort = "unrelated"
	if checkOfflineContractionSurvivors(st, offline, nil) == nil {
		t.Fatal("omitted a Worker from another cohort")
	}
	st.Assessments[0] = assessment
	st.Assessments[1].State = model.InvariantEvidenceStateUnknown
	if checkOfflineContractionSurvivors(st, offline, nil) == nil {
		t.Fatal("admitted a stale surviving Worker")
	}
	st.Assessments = st.Assessments[:1]
	st.RequiredExpected = 1
	if checkOfflineContractionSurvivors(st, offline, nil) == nil {
		t.Fatal("removed the last required member")
	}
}

func TestOfflineContractionOnlyProjectsDNSProbeFailure(t *testing.T) {
	const node = "resolver"
	status := model.PlatformConsumerConvergenceStatus{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: platformconfig.GlobalScopeKey, RequiredExpected: 1, Assessments: []model.PlatformConsumerEvidenceAssessment{{
		Required: true, State: model.InvariantEvidenceStateFail, Reasons: []string{probeNotPassedReason},
		Expected: model.PlatformExpectedConsumer{NodeID: node, Component: model.PlatformConsumerComponentDNSServer, ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: platformconfig.GlobalScopeKey, ExpectedGeneration: "verified"},
		Observed: &model.PlatformConsumerInstance{IdentityVerified: true, CredentialID: "kubernetes:namespace:dns:pod", ProbeStatus: "failed", LastError: retainedDNSProbeError, ServingLKG: true, LKGGeneration: "verified"},
	}}}
	for _, tt := range []struct {
		name     string
		mutate   func(*model.PlatformConsumerEvidenceAssessment)
		proofErr error
		calls    int
		pass     bool
	}{
		{name: "positive projection", calls: 1, pass: true},
		{name: "missing projection", calls: 1, proofErr: errors.New("missing proof")},
		{name: "stale heartbeat", mutate: func(a *model.PlatformConsumerEvidenceAssessment) {
			a.Reasons = append(a.Reasons, "consumer heartbeat is stale")
		}},
		{name: "unverified identity", mutate: func(a *model.PlatformConsumerEvidenceAssessment) { a.Observed.IdentityVerified = false }},
		{name: "unrelated listener error", mutate: func(a *model.PlatformConsumerEvidenceAssessment) { a.Observed.LastError = "different failure" }},
		{name: "wrong LKG", mutate: func(a *model.PlatformConsumerEvidenceAssessment) { a.Observed.LKGGeneration = "different" }},
		{name: "pending candidate", mutate: func(a *model.PlatformConsumerEvidenceAssessment) { a.Observed.CandidateGeneration = "successor" }},
		{name: "other DNS authority", mutate: func(a *model.PlatformConsumerEvidenceAssessment) { a.Expected.ScopeKey = "authority-cell:example" }},
		{name: "worker probe", mutate: func(a *model.PlatformConsumerEvidenceAssessment) {
			a.Expected.Component = model.PlatformConsumerComponentEdgeWorker
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			test := status
			test.Assessments = slices.Clone(status.Assessments)
			observed := *test.Assessments[0].Observed
			test.Assessments[0].Observed = &observed
			if tt.mutate != nil {
				tt.mutate(&test.Assessments[0])
			}
			calls := 0
			err := checkOfflineContractionSurvivors(test, map[string]string{"offline": "pool"}, func(a model.PlatformConsumerEvidenceAssessment) error {
				calls++
				if a.Expected.NodeID != node {
					t.Fatal("resolved the wrong DNS listener")
				}
				return tt.proofErr
			})
			if (err == nil) != tt.pass || calls != tt.calls {
				t.Fatalf("projection calls = %d, err = %v", calls, err)
			}
		})
	}
}

func TestOfflineContractionDNSProjectionBindsLiveTransportAndSurvivingQuorum(t *testing.T) {
	const resolver = "resolver"
	for _, tt := range []struct {
		name   string
		mutate func(*platformDNSRuntimeFactsResponse, *platformconfig.HostedAddressSource, *model.PlatformConsumerEvidenceAssessment)
	}{
		{name: "only removed target failed"},
		{name: "wrong full fence", mutate: func(f *platformDNSRuntimeFactsResponse, _ *platformconfig.HostedAddressSource, _ *model.PlatformConsumerEvidenceAssessment) {
			f.Snapshot.Assignment.FencingToken++
		}},
		{name: "different DNS Pod", mutate: func(f *platformDNSRuntimeFactsResponse, _ *platformconfig.HostedAddressSource, _ *model.PlatformConsumerEvidenceAssessment) {
			f.Backend.PodUID = "replacement"
		}},
		{name: "unready DNS transport", mutate: func(f *platformDNSRuntimeFactsResponse, _ *platformconfig.HostedAddressSource, _ *model.PlatformConsumerEvidenceAssessment) {
			f.transportReady = false
		}},
		{name: "retained route failed", mutate: func(f *platformDNSRuntimeFactsResponse, _ *platformconfig.HostedAddressSource, _ *model.PlatformConsumerEvidenceAssessment) {
			f.ReadyProbeIDs = nil
		}},
		{name: "quorum requires both cells", mutate: func(f *platformDNSRuntimeFactsResponse, _ *platformconfig.HostedAddressSource, _ *model.PlatformConsumerEvidenceAssessment) {
			f.readinessPlan.Records[0].MinDistinctCells = 2
		}},
		{name: "stale selected heartbeat", mutate: func(f *platformDNSRuntimeFactsResponse, _ *platformconfig.HostedAddressSource, _ *model.PlatformConsumerEvidenceAssessment) {
			f.heartbeatValidUntil = time.Time{}
		}},
		{name: "already healthy routes cannot excuse listener failure", mutate: func(f *platformDNSRuntimeFactsResponse, _ *platformconfig.HostedAddressSource, _ *model.PlatformConsumerEvidenceAssessment) {
			f.ReadyProbeIDs = append(f.ReadyProbeIDs, "offline")
			f.Snapshot.Facts = append(f.Snapshot.Facts, dnsfacts.Probe{ProbeID: "offline", Ready: true, Proof: f.Snapshot.Facts[0].Proof})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now().UTC()
			fresh := now.Add(time.Minute)
			plan := &platformconfig.DNSReadinessPlan{
				Probes: []platformconfig.DNSReadinessProbe{{ID: "offline", EdgeID: "offline", EdgeGroupID: "pool", Address: "192.0.2.1:443"}, {ID: "healthy", EdgeID: "healthy", EdgeGroupID: "pool", Address: "192.0.2.2:443"}},
				Records: []platformconfig.DNSReadinessRecord{{Hostname: "app.example.test", MinimumHealthyEdges: 1, Targets: []platformconfig.DNSReadinessTarget{
					{EdgeID: "offline", EdgeGroupID: "pool", Address: "192.0.2.1", ProbeIDs: []string{"offline"}},
					{EdgeID: "healthy", EdgeGroupID: "pool", Address: "192.0.2.2", ProbeIDs: []string{"healthy"}},
				}}},
			}
			source := platformconfig.HostedAddressSource{Prior: platformconfig.DNSArtifactPayload{ReadinessPlan: plan}, Publication: platformconfig.PreviousTrafficPublicationReference{
				ReleaseSetID: "parent", ReleaseSetDigest: "pd", ReleaseID: "full", FencingToken: 7, RouteArtifactID: "route", DNSArtifactID: "dns", DNSArtifactDigest: "dd",
			}}
			facts := platformDNSRuntimeFactsResponse{readinessPlan: plan, transportReady: true, heartbeatValidUntil: fresh, ReadyProbeIDs: []string{"healthy"}, Backend: dnsRuntimeBackend{PodUID: "dns-pod"}, Snapshot: dnsfacts.Snapshot{
				NodeID: resolver, ParentDigest: "pd", RouteArtifactID: "route", CheckpointValidUntil: fresh,
				Assignment: model.PlatformConsumerAssignment{ExpectedConsumerSetID: "set", ReleaseSetID: "parent", ArtifactReleaseID: "full", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: platformconfig.GlobalScopeKey,
					ArtifactID: "dns", ContentHash: "dd", ReleaseChannel: model.PlatformArtifactReleaseChannelFull, FencingToken: 7},
				Facts: []dnsfacts.Probe{{ProbeID: "healthy", Ready: true, Proof: routeprobe.Proof{GroupID: "pool", ValidUntil: fresh, TrafficRelease: &model.TrafficReleaseBinding{ReleaseID: "full"}}}},
			}}
			a := model.PlatformConsumerEvidenceAssessment{FreshUntil: &fresh, Expected: model.PlatformExpectedConsumer{NodeID: resolver}, Observed: &model.PlatformConsumerInstance{CredentialID: "kubernetes:test:dns:dns-pod", ExpectedConsumerSetID: "set"}}
			if tt.mutate != nil {
				tt.mutate(&facts, &source, &a)
			}
			held := map[string]platformconfig.HostedAddressHold{"offline": {EdgeID: "offline", EdgeGroupID: "pool", Address: "192.0.2.1"}}
			if err := verifyOfflineDNSProjection(facts, a, source, held, now); (err == nil) != (tt.mutate == nil) {
				t.Fatalf("DNS projection: %v", err)
			}
		})
	}
}

func TestOfflineContractionCanOnlyAddEffectiveHolds(t *testing.T) {
	a := platformconfig.HostedAddressHold{EdgeID: "a", EdgeGroupID: "pool", Address: "8.8.8.8"}
	b := platformconfig.HostedAddressHold{EdgeID: "b", EdgeGroupID: "pool", Address: "9.9.9.9"}
	changed := a
	changed.Address = "1.1.1.1"
	prior := []platformconfig.HostedAddressHold{a}
	if got := newlyHeldEdges(prior, []platformconfig.HostedAddressHold{b, a}); !slices.Equal(got, []platformconfig.HostedAddressHold{b}) {
		t.Fatal("did not select the one newly held endpoint", got)
	}
	for _, next := range [][]platformconfig.HostedAddressHold{nil, {a}, {b}, {changed, b}, {a, b, b}} {
		if got := newlyHeldEdges(prior, next); len(got) != 0 {
			t.Fatalf("non-contraction admitted: %+v", next)
		}
	}
}

func TestOfflineContractionDefersConcurrentReadmission(t *testing.T) {
	old := platformproducer.OfflineEdge{EdgeID: "prior-hold"}
	lost := platformproducer.OfflineEdge{EdgeID: "new-hold"}
	ready := []platformproducer.OfflineRecoveryObservation{{EdgeID: old.EdgeID}, {EdgeID: "already-serving"}}
	holds, evidence := offlineContractionHolds([]platformproducer.OfflineEdge{old}, []platformproducer.OfflineEdge{lost}, ready)
	if len(holds) != 2 || holds[0].EdgeID != old.EdgeID || holds[1].EdgeID != lost.EdgeID ||
		len(evidence) != 1 || evidence[0].EdgeID != "already-serving" || len(ready) != 2 || ready[0].EdgeID != old.EdgeID {
		t.Fatalf("contraction must preserve existing holds without changing observation history: %v, %v", holds, evidence)
	}
}

func TestOfflineContractionPreservesDNSDeclarationsExceptProvenAddressWithdrawal(t *testing.T) {
	rr := platformconfig.DNSIntent{Hostname: "app.example.test", Type: "A", TTL: 60, Values: []string{"8.8.8.8", "9.9.9.9"}}
	prior := platformconfig.DNSArtifactPayload{
		Records: []platformconfig.DNSIntent{rr},
		ReadinessPlan: &platformconfig.DNSReadinessPlan{Records: []platformconfig.DNSReadinessRecord{{Hostname: rr.Hostname,
			Targets: []platformconfig.DNSReadinessTarget{{EdgeID: "offline", Family: "A", Address: "8.8.8.8"}, {EdgeID: "healthy", Family: "A", Address: "9.9.9.9"}}}}},
	}
	for _, tc := range []struct {
		name string
		edit func(*platformconfig.DNSIntent)
		pass bool
	}{
		{"retained reservation", func(*platformconfig.DNSIntent) {}, true},
		{"offline address removed", func(r *platformconfig.DNSIntent) { r.Values = []string{"9.9.9.9"} }, true},
		{"healthy address removed", func(r *platformconfig.DNSIntent) { r.Values = []string{"8.8.8.8"} }, false},
		{"address substituted", func(r *platformconfig.DNSIntent) { r.Values = []string{"9.9.9.9", "1.1.1.1"} }, false},
		{"TTL changed", func(r *platformconfig.DNSIntent) { r.TTL = 30 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := prior
			next.Records = []platformconfig.DNSIntent{rr}
			tc.edit(&next.Records[0])
			if got := onlyOfflineDNSRecordsChanged(prior, next, map[string]bool{"offline": true}); got != tc.pass {
				t.Fatalf("want %t got %t", tc.pass, got)
			}
		})
	}
}

func TestOfflineContractionReadsSameProgramAcrossCompilerGenerations(t *testing.T) {
	art := func(generation, target string) model.PlatformArtifact {
		return model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
			Content: map[string]any{"generation": generation, "lineage": generation, "routes": target,
				"policy": map[string]any{"generation": generation, "scope": "global"}}}
	}
	prior := art("old", "unchanged")
	if !sameTrafficProgram(&prior, art("new", "unchanged")) {
		t.Fatal("compiler lineage must not change the comparison of serving programs")
	}
	if sameTrafficProgram(&prior, art("new", "different")) {
		t.Fatal("membership-only publication changed the route")
	}
}

func TestOfflineContractionPreservesDNSReadinessAndQuorum(t *testing.T) {
	target := func(name, addr string) platformconfig.DNSReadinessTarget {
		return platformconfig.DNSReadinessTarget{EdgeID: name, EdgeGroupID: "edge-group-fixture", Address: addr, Family: "A", ProbeIDs: []string{name}}
	}
	probe := func(t platformconfig.DNSReadinessTarget) platformconfig.DNSReadinessProbe {
		return platformconfig.DNSReadinessProbe{ID: t.EdgeID, EdgeID: t.EdgeID, EdgeGroupID: t.EdgeGroupID, Address: t.Address}
	}
	a, b := target("active", "8.8.8.8"), target("offline", "9.9.9.9")
	held := map[string]platformconfig.HostedAddressHold{b.EdgeID: {EdgeID: b.EdgeID, EdgeGroupID: b.EdgeGroupID, Address: b.Address}}
	record := platformconfig.DNSReadinessRecord{Hostname: "app.example.test", MinimumHealthyEdges: 1, Targets: []platformconfig.DNSReadinessTarget{a, b}}
	before := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{probe(a), probe(b)}, Records: []platformconfig.DNSReadinessRecord{record}}
	record.Targets = []platformconfig.DNSReadinessTarget{a}
	after := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{probe(a)}, Records: []platformconfig.DNSReadinessRecord{record}}
	if !offlineDNSPlanContracts(before, after, held) {
		t.Fatal("verified withdrawal with sufficient survivors blocked")
	}
	before.Records[0].MinimumHealthyEdges = 2
	if offlineDNSPlanContracts(before, after, held) {
		t.Fatal("withdrawal silently lowered quorum")
	}
	before.Records[0].MinimumHealthyEdges = 1
	after.Probes[0].Path = "/other"
	if offlineDNSPlanContracts(before, after, held) {
		t.Fatal("withdrawal changed unrelated route proof")
	}
}

func TestOfflineContractionCompilesOnlyWithdrawalFromVerifiedInputs(t *testing.T) {
	projection, nodes, strategy, now := directQueryFixture()
	strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
	strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{nodes[0].ID, nodes[1].ID}}, Overrides: []platformconfig.DNSOrderOverride{}}
	p := &projection.Policy
	p.SchemaVersion, p.DNSPlacementMode, p.MinimumHealthyEdges = platformconfig.SchemaVersion, platformconfig.DNSPlacementConsumerReadiness, 1
	p.DNSQueryPolicy = &strategy
	p.DNSReadiness = &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 32}
	p.TLSReadiness = p.DNSReadiness
	p.DNSAuthorities = []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}}
	p.DNSClientPolicies = []platformconfig.DNSClientPolicy{{NodeID: "dns-a"}}
	p.TrafficRolloutCohorts = []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{nodes[0].EdgeGroupID, nodes[1].EdgeGroupID}}}
	projection.RuntimeSnapshot.DNSConsumers = []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: nodes[0].EdgeGroupID, ObservedAt: now, A: []string{nodes[0].PublicIPv4}}}
	if err := projectDirectDNSQueries(&projection, strategy, nodes, now); err != nil {
		t.Fatal(err)
	}
	// The prior measurement knew only the now-offline Edge. Keep the existing
	// signed deterministic fallback; do not invent a new quality measurement.
	p.DNSAnswerRules[0].SelectionMode, p.DNSAnswerRules[0].PhysicalOrder = model.DNSAnswerPolicyKindPhysicalQuality, nil
	f := &projection.RuntimeSnapshot.DNSSelections[0]
	f.RankingVersion, f.RankingScope = model.DNSPhysicalSelectionVersion, "global"
	f.PhysicalSelection = &model.DNSPhysicalSelection{Version: f.RankingVersion, Scope: f.RankingScope, QualityState: "measured", CapturedAt: f.ObservedAt,
		PrimaryEdgeID: nodes[0].ID, OrderedEdgeIDs: []string{nodes[0].ID}, EvidenceDigest: f.SourceDigest, LoadedDigest: f.SourceDigest, PolicyDigest: f.SourceDigest, DNSReceiptID: "observed"}
	baseline, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: projection.Intent, Policy: *p, RuntimeSnapshot: projection.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	baseline.DNSArtifact.ScopeKey = platformconfig.GlobalScopeKey
	prior, err := platformconfig.DecodeDNSArtifact(baseline.DNSArtifact)
	if err != nil {
		t.Fatal(err)
	}
	h := platformconfig.HostedAddressHold{EdgeID: nodes[0].ID, EdgeGroupID: nodes[0].EdgeGroupID, Address: nodes[0].PublicIPv4}
	source := platformconfig.HostedAddressSource{Prior: prior, PriorIntent: projection.Intent, PriorRuntime: baseline.InputSnapshot, PriorRoute: &baseline.RouteArtifact, PriorTLS: &baseline.TLSArtifact}
	compiled, err := compileOfflineContractionCandidate(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "unrelated-new-input"}}, source,
		map[string]platformconfig.HostedAddressHold{h.EdgeID: h}, map[string]bool{h.EdgeID: true}, []platformproducer.OfflineEdge{{EdgeID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, PublicIPv4: h.Address, Reason: "network_unavailable"}})
	if err != nil {
		t.Fatal(err)
	}
	compiled.DNSArtifact.ScopeKey = platformconfig.GlobalScopeKey
	next, err := platformconfig.DecodeDNSArtifact(compiled.DNSArtifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, rr := range next.QueryViews[0].Records {
		if rr.Name == p.DNSAnswerRules[0].Hostname && rr.Type == "A" {
			a := rr.AnswerPolicy
			if a.PhysicalSelection != nil || a.PhysicalOrder == nil || a.PhysicalOrder.QualityObservedAt != nil || !slices.Equal(a.PhysicalOrder.OrderedEdgeIDs, []string{nodes[1].ID}) {
				t.Fatalf("invalid survivor fallback: %+v", a)
			}
			return
		}
	}
	t.Fatal("candidate lost the dynamic answer")
}
