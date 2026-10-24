package dnsserver

import (
	"context"
	"encoding/json"
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

func TestApprovedSourceAdvancePreservesContinuousRecovery(t *testing.T) {
	testApprovedSourceAdvance(t, false)
}

func TestUnrelatedSourcePolicyChangePreservesContinuousRecovery(t *testing.T) {
	testApprovedSourceAdvance(t, true)
}

func TestUnrelatedQualitySourcePolicyPreservesContinuousRecovery(t *testing.T) {
	testApprovedSourceAdvance(t, true, true)
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
				retained := (&dnsReadinessScheduler{}).shareHistory(&s.dnsProbeHistories)
				retained.observeProbeHistory(&f, policy, at)
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
