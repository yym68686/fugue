package dnsserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
)

func TestDNSRouteRecoveryDoesNotRequireRoomForPublicationAliases(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, false)
	artifact := celldns.Compile(t, req).DNSArtifact
	now := time.Now().UTC()
	plans, err := dnsroutesource.Build(artifact, celldns.SourceSnapshot(t, req, artifact, policies, activations, now), celldns.Keys(), now)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := dnsRouteRecoveryKeys(artifact, plans)
	if err != nil || len(keys) == 0 {
		t.Fatalf("test source recovery keys unavailable: %v", err)
	}
	var probeID, key string
	for probeID, key = range keys {
		break
	}
	s := NewService(config.DNSConfig{}, nil)
	history := s.probeHistoryStore()
	history.values = make(map[string]dnsProbeHistory, 16384)
	for i := 1; i < 16384; i++ {
		history.values[fmt.Sprint(i)] = dnsProbeHistory{observedAt: now}
	}
	history.values[key] = dnsProbeHistory{observedAt: now, successes: []time.Time{now.Add(-time.Second)}}
	facts := []dnsReadinessFact{{ProbeID: probeID, Reason: "probe_unavailable"}}
	if err := s.observeDNSRouteRecovery(artifact, plans, facts, &platformconfig.DNSReadinessPolicy{FactFreshnessSeconds: 60}, now); err != nil {
		t.Fatal("a new publication alias prevented processing existing history", err)
	}
	if entry := history.values[key]; len(entry.successes) != 0 || !entry.failedAt.Equal(now) {
		t.Fatalf("real failure did not reset exact continuity: %+v", entry)
	}
	if len(history.values) != 16384 {
		t.Fatal("bounded history unexpectedly grew or dropped unrelated live records")
	}
}

func TestApprovedSourceAdvancePreservesContinuousRecovery(t *testing.T) {
	testApprovedSourceAdvance(t, false)
}

func TestUnrelatedSourcePolicyChangePreservesContinuousRecovery(t *testing.T) {
	testApprovedSourceAdvance(t, true)
}

func TestUnrelatedQualitySourcePolicyPreservesContinuousRecovery(t *testing.T) {
	testApprovedSourceAdvance(t, true, true)
}

func TestShadowRecoveryCarriesOnlyExactSourcesIntoServing(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, false)
	compiled := celldns.Compile(t, req)
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
	plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, celldns.Keys(), now)
	if err != nil {
		t.Fatal(err)
	}
	var probe platformconfig.DNSReadinessProbe
	for _, requirement := range plans.Probes {
		probe = requirement
		break
	}
	if probe.ID == "" {
		t.Fatal("missing source probe fixture")
	}
	parent := NewService(config.DNSConfig{
		PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), EdgeGroupID: "cell-dns",
		CachePath: filepath.Join(t.TempDir(), "dns"),
	}, nil)
	parent.PlatformTokenFile = filepath.Join(t.TempDir(), "token")
	shadow := parent.newIsolatedDNSShadowValidator()
	if shadow == nil || shadow.probeHistoryStore() == parent.probeHistoryStore() {
		t.Fatal("shadow must keep its ordinary probe state separate")
	}
	policy := &platformconfig.DNSReadinessPolicy{FactFreshnessSeconds: 120}
	recovery := &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{probe.EdgeID}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	at := now.Add(-90 * time.Second)
	makeFact := func() dnsReadinessFact {
		return dnsReadinessFact{ProbeID: probe.ID, Ready: true, Proof: routeprobe.Proof{
			EdgeID: probe.EdgeID, GroupID: probe.EdgeGroupID, Digest: probe.RouteDigest, State: probe.State,
			Version: "observed", TrafficRelease: plans.Bindings[probe.CellPublicationDigest],
			CheckedAt: at, ValidUntil: at.Add(2 * time.Minute),
		}}
	}
	observe := func(worker *Service, artifact model.PlatformArtifact, f dnsReadinessFact) dnsReadinessFact {
		t.Helper()
		facts := []dnsReadinessFact{f}
		if err := worker.observeDNSRouteRecovery(artifact, plans, facts, policy, at); err != nil {
			t.Fatal(err)
		}
		return facts[0]
	}
	for i := 0; i < 2; i++ {
		if f := observe(shadow, compiled.DNSArtifact, makeFact()); !f.Ready {
			t.Fatal("fixture did not supply a fresh exact shadow proof", f.Reason)
		}
		at = at.Add(30 * time.Second)
	}
	other := compiled.DNSArtifact
	other.ID += "-different"
	if f := observe(parent, other, makeFact()); dnsAvailabilityRecovered(recovery, f, 2*time.Minute, at) {
		t.Fatal("another DNS artifact inherited shadow recovery")
	}
	if f := observe(parent, compiled.DNSArtifact, makeFact()); !dnsAvailabilityRecovered(recovery, f, 2*time.Minute, at) {
		t.Fatal("serving the same DNS artifact lost actual shadow observations", f.ProbeHistory)
	}
	at = at.Add(time.Second)
	negative := makeFact()
	negative.Ready, negative.Reason = false, "probe_unavailable"
	observe(shadow, compiled.DNSArtifact, negative)
	at = at.Add(time.Second)
	if f := observe(parent, compiled.DNSArtifact, makeFact()); dnsAvailabilityRecovered(recovery, f, 2*time.Minute, at) || len(f.ProbeHistory) != 1 {
		t.Fatal("a genuine negative observation must reset exact-source recovery", f.ProbeHistory)
	}
}

func testApprovedSourceAdvance(t *testing.T, changePolicy bool, retained ...bool) {
	global := len(retained) > 0 && retained[0]
	req, policies, activations := celldns.SourceAuthorizedRequest(t, global)
	if global {
		celldns.RetainGlobalOnly(&req)
	}
	compiled := celldns.Compile(t, req)
	keys := celldns.Keys()
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "dns"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	release := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now}
	assignment := model.PlatformConsumerAssignment{ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: "dns-expected", Revision: 1, ScopeKey: compiled.DNSArtifact.ScopeKey, ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence, ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "full"}
	candidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
	payload, _, err := s.verifyDNSServingRelease(compiled.ReleaseArtifact, candidate)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise authenticated observations with distinct earlier probe times,
	// then advance only the approved publication identity.
	checkedAt := now
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(model.PlatformConsumerDNSRouteSourcesResponse{Assignment: assignment, Release: release, Snapshot: snapshot})
	}))
	defer server.Close()
	client := platformconsumer.Client{BaseURL: server.URL}
	probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
		for _, p := range plans.Probes {
			if p.Hostname == host && p.Path == path && p.Address == address {
				group, digest := p.EdgeGroupID, p.RouteDigest
				if p.PreviousAuthority != nil {
					group, digest = p.PreviousAuthority.EdgeGroupID, p.PreviousAuthority.RouteDigest
				}
				return routeprobe.Proof{EdgeID: p.EdgeID, GroupID: group, Digest: digest, Version: "observed", State: state, TrafficRelease: plans.Bindings[p.CellPublicationDigest], CheckedAt: checkedAt, ValidUntil: checkedAt.Add(time.Duration(payload.Policy.DNSReadiness.FactFreshnessSeconds) * time.Second)}, nil
			}
		}
		t.Fatal("unexpected physical probe")
		return routeprobe.Proof{}, nil
	}

	availability := &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 10}
	for _, delta := range []time.Duration{-12 * time.Second, -6 * time.Second, -time.Second} {
		checkedAt = now.Add(delta)
		// Advance the mocked probe clock without a real-time sleep. The scheduler
		// runs each sweep; authenticated recovery state is never seeded by the test.
		s.servingDNSProbes.digest = ""
		_, facts, err := s.observeDNSRouteSources(context.Background(), client, platformconsumer.Identity{Token: "test"}, candidate, payload, nil, probe)
		if err != nil {
			t.Fatal(err)
		}
		if delta == -time.Second {
			for _, f := range facts {
				if !dnsAvailabilityRecovered(availability, f, time.Minute, time.Now()) {
					t.Fatal("baseline did not recover", f.ProbeHistory)
				}
			}
		}
	}
	for i := range snapshot.Scopes {
		scope := &snapshot.Scopes[i]
		pub := &scope.Publications[0]
		pub.Release.ID += "-next"
		pub.Release.FencingToken++
		scope.Lanes[0].ActiveReleaseID = pub.Release.ID
		scope.Lanes[0].FencingToken = pub.Release.FencingToken
		scope.Lanes[0].Version++
	}
	if changePolicy {
		snapshot = celldns.AdvanceSourcePolicy(t, snapshot, func(policy *platformconfig.PolicySnapshot) {
			if global {
				policy.DNSAnswerRules = append(policy.DNSAnswerRules, platformconfig.DNSAnswerRule{NodeID: "dns-a", Hostname: "other.example.test", Type: "A", SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "edge-a"}}, TTLSeconds: 60})
			}
			policy.RouteConstraints = append(policy.RouteConstraints, platformconfig.RoutePolicyConstraint{ID: "unrelated-policy", Hostname: "other.example.test", TenantID: "tenant-other", AppID: "app-other", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true})
		})
	}
	snapshot.SelectionDigest, _ = dnsroutesource.SelectionDigest(snapshot)
	plans, err = dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	checkedAt = time.Now().UTC()
	_, facts, err := s.observeDNSRouteSources(context.Background(), client, platformconsumer.Identity{Token: "test"}, candidate, payload, nil, probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) == 0 {
		t.Fatal("no current authorized facts")
	}
	for _, f := range facts {
		if !dnsAvailabilityRecovered(availability, f, time.Minute, time.Now()) {
			t.Fatalf("unchanged authorized route lost continuous recovery on publication advance: history=%v", f.ProbeHistory)
		}
	}
}

func TestRetainedSourceCannotResetRecoveryForAuthenticatedSuccessor(t *testing.T) {
	request, policies, activations := celldns.SourceAuthorizedRequest(t, false)
	artifact := celldns.Compile(t, request).DNSArtifact
	decoded, err := platformconfig.DecodeDNSArtifact(artifact)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	makePlans := func(advance bool) (*dnsroutesource.Plans, dnsServingPayload) {
		t.Helper()
		snapshot := celldns.SourceSnapshot(t, request, artifact, policies, activations, now)
		if advance {
			for i := range snapshot.Scopes {
				scope := &snapshot.Scopes[i]
				release := &scope.Publications[0].Release
				release.ID += "-next"
				release.FencingToken++
				lane := &scope.Lanes[0]
				lane.ActiveReleaseID, lane.FencingToken = release.ID, release.FencingToken
				lane.Version++
			}
			snapshot.SelectionDigest, _ = dnsroutesource.SelectionDigest(snapshot)
		}
		plans, err := dnsroutesource.Build(artifact, snapshot, celldns.Keys(), now)
		if err != nil {
			t.Fatal(err)
		}
		sources := plans.Choose(func(string) bool { return true })
		plan, err := plans.Replay(sources)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := dnsRouteRecoveryKeys(artifact, plans)
		if err != nil {
			t.Fatal(err)
		}
		p := dnsServingPayload{Plan: plan, Queries: decoded.QueryViews, routeSources: sources, sourceBindings: plans.Bindings, sourceRecoveryKeys: keys, Policy: decoded.Policy}
		p.indexSourceRequirements()
		return plans, p
	}
	_, previous := makePlans(false)
	plans, next := makePlans(true)
	oldProbe := previous.Plan.Probes[0]
	var newProbe platformconfig.DNSReadinessProbe
	for _, probe := range next.Plan.Probes {
		if next.sourceRecoveryKeys[probe.ID] == previous.sourceRecoveryKeys[oldProbe.ID] {
			newProbe = probe
			break
		}
	}
	if newProbe.ID == "" || newProbe.ID == oldProbe.ID {
		t.Fatal("fixture must change binding while preserving exact behavior")
	}
	for _, scenario := range []string{"verified successor", "unknown binding", "different DNS artifact", "unverified source", "physical failure"} {
		t.Run(scenario, func(t *testing.T) {
			s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns"}, nil)
			candidate := dnsPlatformCandidate{Artifact: artifact}
			old := &dnsServingState{record: dnsServingCheckpoint{Candidate: candidate}, payload: previous}
			bridge := &dnsReleaseBridge{payload: next, candidate: candidate}
			at := now.Add(-40 * time.Second)
			fact := func() dnsReadinessFact {
				return dnsReadinessFact{ProbeID: newProbe.ID, Ready: true, Proof: routeprobe.Proof{EdgeID: newProbe.EdgeID, GroupID: newProbe.EdgeGroupID, Digest: newProbe.RouteDigest, Version: "observed", State: newProbe.State, TrafficRelease: plans.Bindings[newProbe.CellPublicationDigest], CheckedAt: at, ValidUntil: at.Add(time.Minute)}}
			}
			observe := func() []dnsReadinessFact {
				t.Helper()
				facts := []dnsReadinessFact{fact()}
				if err := s.observeDNSRouteRecovery(artifact, plans, facts, next.Policy.DNSReadiness, at); err != nil {
					t.Fatal(err)
				}
				return facts
			}
			observe()
			at = at.Add(10 * time.Second)
			observe()
			at = at.Add(10 * time.Second)
			retained := fact()
			retained.ProbeID = oldProbe.ID
			switch scenario {
			case "unknown binding":
				binding := *retained.Proof.TrafficRelease
				binding.FencingToken++
				retained.Proof.TrafficRelease = &binding
			case "different DNS artifact":
				bridge.candidate.Artifact.ID += "-unrelated"
			case "unverified source":
				bridge = nil
			case "physical failure":
				retained.Ready, retained.Reason = false, "probe_unavailable"
			}
			var fallback dnsReadinessFact
			if scenario == "verified successor" {
				// Exercise the local fallback used by a rejected update of the
				// current assignment's source context. There is no separate
				// retained grant, and the physical target now presents the
				// successfully observed successor proof.
				observations := observe()
				refreshed, err := s.refreshedDNSServingFacts(context.Background(), old, func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
					if host == oldProbe.Hostname && path == oldProbe.Path && address == oldProbe.Address {
						return retained.Proof, nil
					}
					return routeprobe.Proof{}, routeprobe.ErrUnavailable
				}, "readiness_incomplete", bridge, observations)
				if err != nil {
					t.Fatal(err)
				}
				for _, f := range refreshed.facts {
					if f.ProbeID == oldProbe.ID {
						fallback = f
					}
				}
				if fallback.ProbeID == "" || fallback.Ready {
					t.Fatal("fallback accepted the successor under retained authority")
				}
			} else {
				facts := []dnsReadinessFact{retained}
				s.observeRetainedDNSRouteRecovery(old, facts, at, bridge)
				fallback = facts[0]
			}
			if len(fallback.ProbeHistory) != 0 || dnsFactMatchesRelease(fallback, old.record.Parent, old.record.Candidate, old.routeID, previous) {
				t.Fatal("fallback borrowed readiness from the successor")
			}
			at = at.Add(time.Second)
			got := len(observe()[0].ProbeHistory)
			want := 1
			if scenario == "verified successor" {
				want = 4
			}
			if got != want {
				t.Fatalf("the successor's valid current sample got %d successes, want %d", got, want)
			}
		})
	}
}

func TestDNSRouteRecoverySemanticBoundaries(t *testing.T) {
	for _, scenario := range []string{"failed", "retained failure", "unobserved", "cached", "wrong binding", "restart", "changed DNS", "changed producer", "unrelated source policy", "changed constraints", "removed eligibility", "changed endpoint", "changed route", "overlap"} {
		t.Run(scenario, func(t *testing.T) {
			req, policies, activations := celldns.SourceAuthorizedRequest(t, false)
			compiled := celldns.Compile(t, req)
			now := time.Now().UTC()
			snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
			plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, celldns.Keys(), now)
			if err != nil {
				t.Fatal(err)
			}
			var requirement platformconfig.DNSReadinessProbe
			for _, p := range plans.Probes {
				requirement = p
				break
			}
			binding := *plans.Bindings[requirement.CellPublicationDigest]
			at := now.Add(-90 * time.Second)
			makeFact := func() dnsReadinessFact {
				return dnsReadinessFact{ProbeID: requirement.ID, Ready: true, Proof: routeprobe.Proof{EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, Digest: requirement.RouteDigest, Version: "observed", State: requirement.State, TrafficRelease: &binding, CheckedAt: at, ValidUntil: at.Add(time.Minute)}}
			}
			policy := &platformconfig.DNSReadinessPolicy{FactFreshnessSeconds: 60}
			recovery := &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{requirement.EdgeID}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
			s := NewService(config.DNSConfig{}, nil)
			observe := func(facts []dnsReadinessFact) {
				t.Helper()
				if err := s.observeDNSRouteRecovery(compiled.DNSArtifact, plans, facts, policy, at); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 3; i++ {
				facts := []dnsReadinessFact{makeFact()}
				observe(facts)
				at = at.Add(30 * time.Second)
			}
			facts := []dnsReadinessFact{makeFact()}
			observe(facts)
			if !dnsAvailabilityRecovered(recovery, facts[0], time.Minute, at) {
				t.Fatal("baseline recovery missing")
			}
			previous := facts[0]
			at = at.Add(time.Second)
			switch scenario {
			case "failed":
				f := makeFact()
				f.Ready = false
				f.Reason = "probe_unavailable"
				observe([]dnsReadinessFact{f})
			case "retained failure":
				f := makeFact()
				f.Ready = false
				f.Reason = "probe_unavailable"
				keys, err := dnsRouteRecoveryKeys(compiled.DNSArtifact, plans)
				if err != nil {
					t.Fatal(err)
				}
				// A retained failure resolves through the original authenticated
				// plan, even if a shadow artifact observed this same probe ID.
				other := compiled.DNSArtifact
				other.ID += "-shadow"
				if err := s.observeDNSRouteRecovery(other, plans, []dnsReadinessFact{makeFact()}, policy, at); err != nil {
					t.Fatal(err)
				}
				old := &dnsServingState{payload: dnsServingPayload{
					routeSources: &dnsroutesource.Context{Snapshot: snapshot}, sourceRecoveryKeys: keys,
					Policy: platformconfig.PolicySnapshot{DNSReadiness: policy},
				}}
				s.observeRetainedDNSRouteRecovery(old, []dnsReadinessFact{f}, at, nil)
			case "unobserved":
				f := makeFact()
				f.Ready = false
				f.Reason = "not_observed"
				observe([]dnsReadinessFact{f})
			case "cached":
				facts = []dnsReadinessFact{previous}
				observe(facts)
				if len(facts[0].ProbeHistory) != len(previous.ProbeHistory) {
					t.Fatal("cached proof counted twice")
				}
				return
			case "wrong binding":
				f := makeFact()
				wrong := binding
				wrong.FencingToken++
				f.Proof.TrafficRelease = &wrong
				rows := []dnsReadinessFact{f}
				observe(rows)
				if rows[0].Ready || len(rows[0].ProbeHistory) != 0 {
					t.Fatal("unapproved publication contributed recovery")
				}
			case "restart":
				s = NewService(config.DNSConfig{}, nil)
			case "changed DNS":
				compiled.DNSArtifact.ID += "-new"
			case "changed producer":
				for i := range plans.Snapshot.Scopes {
					for j := range plans.Snapshot.Scopes[i].Publications {
						plans.Snapshot.Scopes[i].Publications[j].ProducerPolicy.ID += "-new"
					}
				}
			case "unrelated source policy":
				binding.PolicyDigest, _ = platformconfig.Digest("changed source policy")
				plans.Bindings[requirement.CellPublicationDigest] = &binding
			case "changed constraints":
				plans.RecoveryConstraints[requirement.ID], _ = platformconfig.Digest("stricter current availability")
			case "removed eligibility":
				previous := plans.Probes
				plans.Probes = map[string]platformconfig.DNSReadinessProbe{}
				observe(nil)
				plans.Probes = previous
			case "changed endpoint":
				requirement.Address = "93.184.216.99"
				plans.Probes[requirement.ID] = requirement
			case "changed route":
				requirement.RouteDigest, _ = platformconfig.Digest("changed route")
				plans.Probes[requirement.ID] = requirement
			case "overlap":
				alternate := requirement
				alternate.ID += "-other"
				alternate.CellPublicationDigest += "other"
				plans.Probes[alternate.ID] = alternate
				plans.RecoveryConstraints[alternate.ID] = plans.RecoveryConstraints[requirement.ID]
				alternateBinding := binding
				alternateBinding.ReleaseID += "-other"
				plans.Bindings[alternate.CellPublicationDigest] = &alternateBinding
				for i := range plans.Snapshot.Scopes {
					for _, pub := range plans.Snapshot.Scopes[i].Publications {
						if pub.Release.ID == binding.ReleaseID {
							pub.Release.ID = alternateBinding.ReleaseID
							plans.Snapshot.Scopes[i].Publications = append(plans.Snapshot.Scopes[i].Publications, pub)
							break
						}
					}
				}
				wrongVariant := makeFact()
				wrongVariant.ProbeID = alternate.ID
				facts = []dnsReadinessFact{wrongVariant, makeFact()}
				observe(facts)
				if facts[0].Ready || !dnsAvailabilityRecovered(recovery, facts[1], time.Minute, at) {
					t.Fatal("unmatched release variant erased a matched physical success")
				}
				return
			}
			at = at.Add(time.Second)
			facts = []dnsReadinessFact{makeFact()}
			observe(facts)
			if scenario == "unobserved" || scenario == "unrelated source policy" {
				if !dnsAvailabilityRecovered(recovery, facts[0], time.Minute, at) {
					t.Fatal("unobserved sweep erased continuity")
				}
			} else if len(facts[0].ProbeHistory) != 1 || dnsAvailabilityRecovered(recovery, facts[0], time.Minute, at) {
				t.Fatal("changed or failed context reused recovery", scenario, facts[0].ProbeHistory)
			}
		})
	}
}
