package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestDNSRecordTransitionPreservesAuthorizationAndCompleteQuorum(t *testing.T) {
	view, plan, readiness, facts, now := queryExecutionFixture()
	view.Records[0].AppID, view.Records[0].TenantID = "application", "tenant"
	payload := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{
		MaxStaleSeconds: 3600, DNSReadiness: &readiness,
		DNSAuthorities:    []platformconfig.DNSAuthorityPolicy{{NodeID: view.NodeID, Zone: view.Zone, Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}},
		DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: view.NodeID}},
	}}
	raw := mustDNSJSON(t, payload)
	for _, scenario := range []string{"deployment", "quorum", "fault domain", "path", "state", "edge", "address", "tenant", "app", "client policy", "freshness", "cell publication", "traffic constraint", "same release", "older release"} {
		t.Run(scenario, func(t *testing.T) {
			var before, after dnsServingPayload
			json.Unmarshal(raw, &before)
			json.Unmarshal(raw, &after)
			remap := map[string]string{}
			for i := range after.Plan.Probes {
				p := &after.Plan.Probes[i]
				id := p.ID
				p.RouteDigest = "sha256:" + strings.Repeat("f", 64)
				p.ID, _ = platformconfig.DNSReadinessProbeID(*p)
				remap[id] = p.ID
			}
			for i := range after.Plan.Records {
				for j := range after.Plan.Records[i].Targets {
					for k, id := range after.Plan.Records[i].Targets[j].ProbeIDs {
						after.Plan.Records[i].Targets[j].ProbeIDs[k] = remap[id]
					}
				}
			}
			old := &dnsServingState{payload: before, record: dnsServingCheckpoint{AppliedAt: now.Add(-time.Minute), Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "previous", ReleasedAt: now.Add(-time.Minute)}}}}
			bridge := &dnsReleaseBridge{payload: after, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "next", ReleasedAt: now}}}
			switch scenario {
			case "quorum":
				bridge.payload.Plan.Records[0].MinimumHealthyEdges--
			case "fault domain":
				bridge.payload.Plan.Records[0].MinDistinctDomains = map[string]int{"host": 2}
			case "path":
				bridge.payload.Plan.Probes[0].Path = "/other"
			case "state":
				bridge.payload.Plan.Probes[0].State = "disabled"
			case "edge":
				bridge.payload.Queries[0].Records[0].Candidates[0].EdgeID = "foreign"
			case "address":
				bridge.payload.Queries[0].Records[0].Candidates[0].IP = "1.1.1.1"
			case "tenant":
				bridge.payload.Queries[0].Records[0].TenantID = "foreign"
			case "app":
				bridge.payload.Queries[0].Records[0].AppID = "foreign"
			case "client policy":
				bridge.payload.Policy.DNSClientPolicies = nil
			case "freshness":
				bridge.payload.Policy.DNSReadiness.FactFreshnessSeconds++
			case "cell publication":
				bridge.payload.Plan.Probes[0].CellPublicationDigest = "sha256:" + strings.Repeat("e", 64)
			case "traffic constraint":
				bridge.payload.Policy.TrafficConstraints = []platformconfig.TrafficPolicyConstraint{{AppID: bridge.payload.Queries[0].Records[0].AppID, TenantID: "changed"}}
			case "same release":
				bridge.candidate.Release.ID = old.record.Candidate.Release.ID
			case "older release":
				bridge.candidate.Release.ReleasedAt = old.record.Candidate.Release.ReleasedAt
			}
			allowed := dnsTransitionRecords(old, bridge)
			if allowed[plan.Records[0].Hostname] != (scenario == "deployment") {
				t.Fatal("transition changed authorization", scenario, allowed)
			}
			if string(mustDNSJSON(t, before)) != string(raw) {
				t.Fatal("comparison mutated signed plan")
			}
			if scenario != "deployment" {
				return
			}
			old, err := buildDNSServingState(old.record, before, "route", view.NodeID, view.EdgeGroupID, nil, now)
			if err != nil {
				t.Fatal(err)
			}
			nextFacts := append([]dnsReadinessFact(nil), facts...)
			for i := range nextFacts {
				nextFacts[i].ProbeID = remap[nextFacts[i].ProbeID]
				nextFacts[i].Proof.Digest = "sha256:" + strings.Repeat("f", 64)
			}
			makeNext := func(f []dnsReadinessFact) *dnsServingState {
				next, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: old.record.AppliedAt}, after, "route", view.NodeID, view.EdgeGroupID, f, now)
				if err != nil {
					t.Fatal(err)
				}
				return next
			}
			q := new(dns.Msg)
			q.SetQuestion(dns.Fqdn(plan.Records[0].Hostname), dns.TypeA)
			old.transition = &dnsRecordTransition{state: makeNext(nextFacts), hosts: allowed}
			if got := old.answer(q, "", now); got.Rcode != dns.RcodeSuccess || len(got.Answer) != 1 {
				t.Fatal("complete successor quorum rejected", got)
			}
			for i := range nextFacts {
				if nextFacts[i].Proof.EdgeID == "edge-b" {
					nextFacts[i].Ready = false
				}
			}
			old.transition.state = makeNext(nextFacts)
			if got := old.answer(q, "", now); got.Rcode != dns.RcodeServerFailure {
				t.Fatal("partial successor quorum lowered minimum", got)
			}
			// Facts from opposite publications may not be added together to meet
			// the two-physical-Edge minimum for this hostname.
			for i := range facts {
				if facts[i].Proof.EdgeID != "edge-b" {
					facts[i].Ready = false
				}
			}
			mixed, err := buildDNSServingState(old.record, before, "route", view.NodeID, view.EdgeGroupID, facts, now)
			if err != nil {
				t.Fatal(err)
			}
			mixed.transition = old.transition
			if got := mixed.answer(q, "", now); got.Rcode != dns.RcodeServerFailure {
				t.Fatal("mixed publications fabricated quorum", got)
			}
		})
	}
}

func TestDNSRecordTransitionIsolatesChangedAliasSharingRouteProofs(t *testing.T) {
	view, plan, readiness, facts, now := queryExecutionFixture()
	hostname := plan.Records[0].Hostname
	alias := view.Records[0]
	alias.Name = "alias.example.test"
	view.Records = append(view.Records, alias)
	aliasPlan := plan.Records[0]
	aliasPlan.Hostname = alias.Name
	plan.Records = append(plan.Records, aliasPlan)
	previous := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{
		MaxStaleSeconds: 3600, DNSReadiness: &readiness,
		DNSAuthorities:    []platformconfig.DNSAuthorityPolicy{{NodeID: view.NodeID, Zone: view.Zone, Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}},
		DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: view.NodeID}},
		DNSAnswerRules: []platformconfig.DNSAnswerRule{
			{NodeID: view.NodeID, Hostname: hostname, Type: "A", SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: model.CloneDNSPhysicalOrder(alias.AnswerPolicy.PhysicalOrder)},
			{NodeID: view.NodeID, Hostname: alias.Name, Type: "A", SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: model.CloneDNSPhysicalOrder(alias.AnswerPolicy.PhysicalOrder)},
		},
	}}
	raw := mustDNSJSON(t, previous)
	var next dnsServingPayload
	if err := json.Unmarshal(raw, &next); err != nil {
		t.Fatal(err)
	}
	next.Policy.DNSAnswerRules[1].PhysicalOrder.OrderedEdgeIDs = []string{"edge-b", "edge-a"}
	next.Queries[0].Records[len(view.Records)-1].AnswerPolicy.PhysicalOrder.OrderedEdgeIDs = []string{"edge-b", "edge-a"}
	record := dnsServingCheckpoint{AppliedAt: now.Add(-time.Minute), Positive: true, Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "previous", ReleasedAt: now.Add(-time.Minute)}}}
	old := &dnsServingState{payload: previous, record: record}
	bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "next", ReleasedAt: now}}}
	for _, allowed := range compatibleDNSReleaseProbes(old, bridge) {
		if allowed {
			t.Fatal("shared proof must not bypass the changed alias policy")
		}
	}
	hosts := dnsTransitionRecords(old, bridge)
	if !hosts[hostname] || hosts[alias.Name] {
		t.Fatal("changed alias incorrectly disabled its unchanged shared-route sibling", hosts)
	}
	old, err := buildDNSServingState(record, previous, "route", view.NodeID, view.EdgeGroupID, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: record.AppliedAt}, next, "route", view.NodeID, view.EdgeGroupID, facts, now)
	if err != nil {
		t.Fatal(err)
	}
	old.transition = &dnsRecordTransition{state: transition, hosts: hosts}
	for _, candidate := range []string{hostname, alias.Name} {
		question := new(dns.Msg)
		question.SetQuestion(dns.Fqdn(candidate), dns.TypeA)
		answer := old.answer(question, "", now)
		if (answer.Rcode == dns.RcodeSuccess) != (candidate == hostname) {
			t.Fatal("per-record authorization was changed", candidate, answer)
		}
		if expired := old.answer(question, "", now.Add(time.Minute)); expired.Rcode != dns.RcodeServerFailure {
			t.Fatal("transition extended original proof deadline")
		}
	}
	if !reflect.DeepEqual(old.record, record) || transition.record.Positive || string(mustDNSJSON(t, previous)) != string(raw) {
		t.Fatal("record transition modified original positive identity or promoted the candidate")
	}
}

// Two hostnames move independently. The changed deployment is already serving
// on its Edge while another hostname still reports the previous publication.
// Neither whole DNS artifact is ready, although each hostname has a complete,
// fresh quorum authorized by one exact signed publication.
func TestDNSDeploymentTransitionKeepsPerRecordAnswersWithoutPromotingLKG(t *testing.T) {
	testDNSDeploymentTransition(t, "deployment")
}

func TestDNSRolloutLifecyclePreservesWireAnswersWithExactSuccessorProofs(t *testing.T) {
	for _, phase := range []string{"abort", "promote", "replace"} {
		t.Run(phase, func(t *testing.T) { testDNSDeploymentTransition(t, phase) })
	}
}

func TestDNSInitialCanaryTransitionKeepsPerRecordAnswersWithoutPromotingLKG(t *testing.T) {
	testDNSDeploymentTransition(t, "start")
}

func TestDNSWireRejectionKeepsHealthyRecordSuccessorProofs(t *testing.T) {
	testDNSDeploymentTransition(t, "wire-rejection")
}

func TestDNSStableReplacementKeepsPerRecordAnswersWithoutPromotingLKG(t *testing.T) {
	testDNSDeploymentTransition(t, "replace-stable")
}

func testDNSDeploymentTransition(t *testing.T, phase string) {
	t.Helper()
	stamp := time.Now().UTC().Truncate(time.Second)
	fixture := func(generation string) (model.PlatformArtifact, dnsPlatformCandidate) {
		return dnsServingFixtureWithIntent(t, "edge-group-a", "global", func(r *platformconfig.CompileRequest) {
			r.Intent.Generation = generation
			r.Intent.Routes[0].DeploymentGeneration = generation
			r.Intent.Routes[0].CacheNamespace = generation
			if phase == "start" || phase == "replace-stable" {
				constraint := platformconfig.TrafficPolicyConstraint{ID: "policy-a", AppID: r.Intent.Routes[0].AppID,
					TenantID: r.Intent.Routes[0].TenantID, Mode: model.AppTrafficModeSingle, StableReleaseID: "stable-a",
					StableWeight: 100, UnavailableCandidate: "stable", StickyCookie: "Fugue-Release-Stickiness"}
				if generation == "deploy-after" && phase == "start" {
					constraint.Mode, constraint.CandidateReleaseID = model.AppTrafficModeCanary, "candidate-a"
					constraint.StableWeight, constraint.CandidateWeight = 99, 1
				}
				if generation == "deploy-after" && phase == "replace-stable" {
					constraint.StableReleaseID = "candidate-a"
				}
				r.Policy.TrafficConstraints = []platformconfig.TrafficPolicyConstraint{constraint}
				r.RuntimeSnapshot.Releases = []platformconfig.ReleaseObservation{
					{ID: "stable-a", AppID: constraint.AppID, TenantID: constraint.TenantID, ObservedAt: stamp,
						Status: model.EdgeRouteStatusActive, RuntimeID: "runtime-a", DeploymentGeneration: "stable-code", UpstreamURL: "http://stable:8080"},
					{ID: "candidate-a", AppID: constraint.AppID, TenantID: constraint.TenantID, ObservedAt: stamp,
						Status: model.EdgeRouteStatusActive, RuntimeID: "runtime-a", DeploymentGeneration: "candidate-code", UpstreamURL: "http://candidate:8080"},
				}
			}
			other := r.Intent.Routes[0]
			other.Hostname, other.AppID, other.DeploymentGeneration, other.CacheNamespace = "other.example.test", "other-app", "fixed", "fixed"
			r.Intent.Routes = append(r.Intent.Routes, other)
			r.Intent.DNS = r.Intent.DNS[:1]
			d := r.Intent.DNS[0]
			d.Hostname, d.AppID, d.Values = other.Hostname, other.AppID, []string{other.AppID}
			r.Intent.DNS = append(r.Intent.DNS, d)
			rule := r.Policy.DNSAnswerRules[0]
			rule.Hostname = other.Hostname
			r.Policy.DNSAnswerRules = append(r.Policy.DNSAnswerRules, rule)
			if phase == "abort" || phase == "promote" || phase == "replace" {
				traffic := platformconfig.TrafficPolicyConstraint{ID: "app-traffic", AppID: r.Intent.Routes[0].AppID, TenantID: r.Intent.Routes[0].TenantID,
					Mode: model.AppTrafficModeCanary, StableReleaseID: "stable-code", CandidateReleaseID: "candidate-code", StableWeight: 75, CandidateWeight: 25, UnavailableCandidate: "stable"}
				if phase == "replace" && generation == "deploy-after" {
					traffic.CandidateReleaseID, traffic.StableWeight, traffic.CandidateWeight = "successor-code", 99, 1
				}
				if phase == "start" && generation == "deploy-before" || (phase == "abort" || phase == "promote") && generation == "deploy-after" {
					traffic.Mode, traffic.CandidateReleaseID, traffic.StableWeight, traffic.CandidateWeight = model.AppTrafficModeSingle, "", 100, 0
					if phase == "promote" {
						traffic.StableReleaseID = "candidate-code"
					}
				}
				r.Policy.TrafficConstraints = []platformconfig.TrafficPolicyConstraint{traffic}
				for _, id := range []string{"stable-code", "candidate-code", "successor-code"} {
					r.RuntimeSnapshot.Releases = append(r.RuntimeSnapshot.Releases, platformconfig.ReleaseObservation{ID: id, AppID: traffic.AppID, TenantID: traffic.TenantID,
						ObservedAt: stamp, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://" + id + ":8080", DeploymentGeneration: id})
				}
			}
			r.RuntimeSnapshot.CapturedAt = &stamp
			r.RuntimeSnapshot.DNSConsumers[0].ObservedAt = stamp
			r.RuntimeSnapshot.DNSEdgeEndpoints[0].ObservedAt = stamp
			r.RuntimeSnapshot.DNSSelections[0].ObservedAt = stamp
			selection := r.RuntimeSnapshot.DNSSelections[0]
			selection.Hostname = other.Hostname
			r.RuntimeSnapshot.DNSSelections = append(r.RuntimeSnapshot.DNSSelections, selection)
			if phase == "wire-rejection" {
				// Both hosts have a one-edge quorum. The second host's signed
				// selector permits only edge-b, which changes publication later.
				r.RuntimeSnapshot.DNSEdgeEndpoints = append(r.RuntimeSnapshot.DNSEdgeEndpoints,
					platformconfig.DNSEdgeEndpoint{EdgeID: "edge-b", EdgeGroupID: "edge-group-a", ObservedAt: stamp, A: []string{"9.9.9.9"}})
				for i := range r.RuntimeSnapshot.DNSSelections {
					r.RuntimeSnapshot.DNSSelections[i].Candidates = append(r.RuntimeSnapshot.DNSSelections[i].Candidates,
						platformconfig.DNSSelectionCandidate{IP: "9.9.9.9", EdgeID: "edge-b", EdgeGroupID: "edge-group-a"})
				}
				r.Policy.DNSAnswerRules[0].PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}
				r.Policy.DNSAnswerRules[1].PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b"}}
			}
		}, true)
	}
	previousParent, previous := fixture("deploy-before")
	parent, candidate := fixture("deploy-after")
	candidate.Release.ID, candidate.Assignment.ArtifactReleaseID = "successor", "successor"
	candidate.Release.FencingToken++
	candidate.Assignment.FencingToken++
	candidate.Release.ReleasedAt = previous.Release.ReleasedAt.Add(time.Millisecond)
	var reports atomic.Int32
	var offline atomic.Bool
	var assignmentChecks atomic.Int32
	var raceAssignment atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if offline.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/v1/platform-state/consumers/identity":
			authority := platformcontrol.ConsumerAuthorityID("edge-group-a")
			id, _ := platformcontrol.PlatformConsumerID(model.PlatformConsumerComponentDNSServer, "dns-a", authority)
			json.NewEncoder(w).Encode(map[string]any{"token": "token", "expires_at": time.Now().Add(time.Minute), "component": "dns-server", "node_id": "dns-a", "authority_id": authority, "consumer_id": id, "scope_key": "global", "artifact_kinds": []string{candidate.Artifact.ArtifactKind}})
		case "/v1/platform-state/consumers/assignment":
			a := candidate.Assignment
			if assignmentChecks.Add(1) >= 3 && raceAssignment.Load() {
				a.FencingToken++
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{a}})
		case "/v1/platform-state/consumers/artifacts/dns":
			json.NewEncoder(w).Encode(candidate)
		case "/v1/platform-state/consumers/artifacts/parent":
			json.NewEncoder(w).Encode(map[string]any{"artifact": parent, "assignment": candidate.Assignment, "release": candidate.Release})
		case "/v1/platform-state/consumers/trusted-heartbeat":
			var h platformcontrol.PlatformConsumerHeartbeatEnvelope
			json.NewDecoder(r.Body).Decode(&h)
			if h.ApplyStatus == "applied" && h.ProbeStatus == "passed" {
				reports.Add(1) // Only a positive receipt can acknowledge the candidate.
			} else if h.ApplyStatus != "failed" || h.ProbeStatus != "failed" || !h.ServingLKG || h.ExpectedConsumerSetID != candidate.Assignment.ExpectedConsumerSetID {
				t.Error("invalid rejected-candidate observation", h)
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, EvidenceHash: h.EvidenceHash, ExpectedConsumerSetID: h.ExpectedConsumerSetID}})
		default:
			t.Error("unexpected endpoint", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	cfg := config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "cache"), BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}
	s := NewService(cfg, nil)
	s.PlatformTokenFile = cfg.CachePath + ".token"
	if err := os.WriteFile(s.PlatformTokenFile, []byte("pod-token"), 0600); err != nil {
		t.Fatal(err)
	}
	oldPayload, oldRoute, err := s.verifyDNSServingRelease(previousParent, previous)
	if err != nil {
		t.Fatal(err)
	}
	nextPayload, nextRoute, err := s.verifyDNSServingRelease(parent, candidate)
	if err != nil {
		t.Fatal(err)
	}
	proof := func(p dnsServingPayload, parent model.PlatformArtifact, c dnsPlatformCandidate, route string, host, address string) (routeprobe.Proof, error) {
		for _, requirement := range p.Plan.Probes {
			if requirement.Hostname == host && requirement.Address == address {
				return routeprobe.Proof{Digest: requirement.RouteDigest, EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, State: requirement.State, Version: "observed", CheckedAt: stamp, ValidUntil: stamp.Add(time.Minute), TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: route, PolicyDigest: p.Lineage.PolicyDigest, IntentDigest: p.Lineage.IntentDigest, InputSnapshotDigest: p.Lineage.InputSnapshotDigest, ReleaseID: c.Release.ID, ReleaseChannel: c.Release.ReleaseChannel, FencingToken: c.Release.FencingToken, ScopeKey: "global"}}, nil
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected probe")
	}
	var next, complete atomic.Bool
	var badProof atomic.Int32
	var lateProof atomic.Bool
	var lateCalls atomic.Int32
	probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
		if next.Load() && (host == "app.example.test" || complete.Load() || phase == "wire-rejection" && address == "8.8.8.8") {
			if lateProof.Load() && host == "app.example.test" && lateCalls.Add(1) == 1 {
				return proof(oldPayload, previousParent, previous, oldRoute, host, address)
			}
			p, err := proof(nextPayload, parent, candidate, nextRoute, host, address)
			if lateProof.Load() {
				p.CheckedAt = time.Now().UTC()
			}
			switch badProof.Load() {
			case 1:
				p.TrafficRelease.FencingToken++
			case 2:
				p.Digest = "foreign"
			case 3:
				p.CheckedAt, p.ValidUntil = stamp.Add(-time.Minute), stamp.Add(-time.Second)
			case 4:
				return routeprobe.Proof{}, errors.New("route is unavailable")
			}
			return p, err
		}
		return proof(oldPayload, previousParent, previous, oldRoute, host, address)
	}
	checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: cfg.DNSNodeID, GroupID: cfg.EdgeGroupID, Parent: previousParent, Candidate: previous, AppliedAt: stamp.Add(-time.Minute), Positive: true}
	if err := s.signDNSCheckpoint(&checkpoint); err != nil {
		t.Fatal(err)
	}
	saved := mustDNSJSON(t, checkpoint)
	if err := os.WriteFile(cfg.CachePath+".platform-serving.json", saved, 0600); err != nil {
		t.Fatal(err)
	}
	facts := collectDNSReadinessFacts(context.Background(), oldPayload.Plan, oldPayload.Policy.DNSReadiness, probe)
	old, err := buildDNSServingState(checkpoint, oldPayload, oldRoute, cfg.DNSNodeID, cfg.EdgeGroupID, facts, stamp)
	if err != nil || !dnsServingReady(old, stamp) {
		t.Fatal("baseline not ready", err)
	}
	s.platformServing.Store(old)
	s.platformServingBound.Store(true)
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.Config.UDPAddr, s.Config.TCPAddr = udp.LocalAddr().String(), tcp.Addr().String()
	u, v := &dns.Server{PacketConn: udp, Handler: s}, &dns.Server{Listener: tcp, Handler: s}
	go u.ActivateAndServe()
	go v.ActivateAndServe()
	defer u.Shutdown()
	defer v.Shutdown()
	next.Store(true)
	if phase == "wire-rejection" {
		candidateFacts := collectDNSReadinessFacts(context.Background(), nextPayload.Plan, nextPayload.Policy.DNSReadiness, probe)
		for i := range candidateFacts {
			if !dnsFactMatchesRelease(candidateFacts[i], parent, candidate, nextRoute, nextPayload) {
				candidateFacts[i].Ready = false
				candidateFacts[i].Reason = "traffic_release_mismatch"
			}
		}
		candidateState, err := buildDNSServingState(dnsServingCheckpoint{Positive: true, AppliedAt: stamp, Candidate: candidate}, nextPayload, nextRoute, cfg.DNSNodeID, cfg.EdgeGroupID, candidateFacts, stamp)
		if err != nil || !dnsServingReady(candidateState, stamp) || probeDNSServingSnapshot(candidateState) == nil {
			t.Fatal("fixture must pass route quorum but fail signed wire selection", err)
		}
		raceAssignment.Store(true)
		if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err == nil || s.platformServing.Load() != old || reports.Load() != 0 {
			t.Fatal("superseded wire-failed candidate changed serving authority", err)
		}
		raceAssignment.Store(false)
		if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err == nil {
			t.Fatal("wire-incomplete candidate promoted")
		}
		active := s.platformServing.Load()
		if !reflect.DeepEqual(active.record, checkpoint) || reports.Load() != 0 {
			t.Fatal("wire rejection changed positive checkpoint or emitted success")
		}
		for _, network := range []string{"udp", "tcp"} {
			address := s.Config.UDPAddr
			if network == "tcp" {
				address = s.Config.TCPAddr
			}
			for _, host := range []string{"app.example.test", "other.example.test"} {
				q := new(dns.Msg)
				q.SetQuestion(dns.Fqdn(host), dns.TypeA)
				answer, _, err := (&dns.Client{Net: network, Timeout: time.Second}).Exchange(q, address)
				if err != nil || answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 {
					t.Fatalf("wire rejection discarded a usable per-record publication (%s %s): %v %v", network, host, answer, err)
				}
				receipt := captureDecision(t, active, q, "", time.Now())
				if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched {
					t.Fatal("transition did not preserve exact replay evidence", replay, err)
				}
			}
		}
		after, err := os.ReadFile(cfg.CachePath + ".platform-serving.json")
		if err != nil || string(after) != string(saved) {
			t.Fatal("wire rejection overwrote positive LKG", err)
		}
		q := new(dns.Msg)
		q.SetQuestion("app.example.test.", dns.TypeA)
		if got := active.answer(q, "", stamp.Add(2*time.Minute)); got.Rcode != dns.RcodeServerFailure {
			t.Fatal("wire fallback extended successor proof lifetime")
		}
		complete.Store(true)
		if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err != nil || reports.Load() != 1 {
			t.Fatal("complete successor did not activate normally", err)
		}
		return
	}
	raceAssignment.Store(true)
	if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err == nil || s.platformServing.Load() != old || reports.Load() != 0 {
		t.Fatal("authority changed during retained scan but pending records activated", err)
	}
	raceAssignment.Store(false)
	for _, invalid := range []int32{1, 2, 3, 4} {
		badProof.Store(invalid)
		s.platformServing.Store(old)
		if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err == nil {
			t.Fatal("invalid candidate activated", invalid)
		}
		q := new(dns.Msg)
		q.SetQuestion("app.example.test.", dns.TypeA)
		if got := s.platformServing.Load().answer(q, "", time.Now()); got.Rcode != dns.RcodeServerFailure {
			t.Fatal("invalid successor proof supplied answer", invalid, got)
		}
	}
	badProof.Store(0)
	// The worker can activate after the candidate scan and before the retained
	// release's additional probe. That newly authenticated successor proof must
	// not be discarded until another whole scan, or the record loses both paths.
	lateProof.Store(true)
	for _, race := range []bool{true, false} {
		lateCalls.Store(0)
		assignmentChecks.Store(0)
		raceAssignment.Store(race)
		s.platformServing.Store(old)
		if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err == nil {
			t.Fatal("partial candidate reported whole-artifact success")
		}
		if race {
			if s.platformServing.Load() != old || reports.Load() != 0 {
				t.Fatal("late successor evidence bypassed the final assignment check")
			}
			continue
		}
		q := new(dns.Msg)
		q.SetQuestion("app.example.test.", dns.TypeA)
		active := s.platformServing.Load()
		if got := active.answer(q, "", time.Now()); got.Rcode != dns.RcodeSuccess || len(got.Answer) != 1 {
			t.Fatal("new successor proof from the retained scan was lost", got)
		}
		receipt := captureDecision(t, active, q, "", time.Now())
		replayed, err := ReplayDNSDecision(receipt)
		if err != nil || !replayed.Matched || !reflect.DeepEqual(active.record, checkpoint) || reports.Load() != 0 {
			t.Fatal("late proof changed publication authority or replay", replayed, err)
		}
	}
	lateProof.Store(false)
	s.platformServing.Store(old)
	if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err == nil {
		t.Fatal("partial candidate reported whole-artifact success")
	}
	for _, network := range []string{"udp", "tcp"} {
		address := s.Config.UDPAddr
		if network == "tcp" {
			address = s.Config.TCPAddr
		}
		for _, host := range []string{"app.example.test", "other.example.test"} {
			q := new(dns.Msg)
			q.SetQuestion(dns.Fqdn(host), dns.TypeA)
			answer, _, err := (&dns.Client{Net: network, Timeout: time.Second}).Exchange(q, address)
			if err != nil || answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 {
				t.Fatalf("healthy record lost during deployment transition (%s %s): %v %v", network, host, answer, err)
			}
		}
	}
	actual := s.platformServing.Load()
	if !reflect.DeepEqual(actual.record, checkpoint) || reports.Load() != 0 || dnsServingReady(actual, time.Now()) {
		t.Fatal("partial serving advanced checkpoint or whole-release readiness")
	}
	runtime, err := dnsRuntimeFacts(actual, time.Now())
	if err != nil || runtime.Ready || !reflect.DeepEqual(runtime.Assignment, previous.Assignment) {
		t.Fatal("partial serving fabricated runtime readiness", err)
	}
	for _, fact := range runtime.Facts {
		if fact.Ready && fact.Proof.TrafficRelease.ReleaseID != previous.Release.ID {
			t.Fatal("successor proof relabeled as predecessor")
		}
	}
	after, err := os.ReadFile(cfg.CachePath + ".platform-serving.json")
	if err != nil || string(after) != string(saved) {
		t.Fatal("partial serving overwrote positive LKG", err)
	}
	q := new(dns.Msg)
	q.SetQuestion("app.example.test.", dns.TypeA)
	if got := actual.answer(q, "", stamp.Add(2*time.Minute)); got.Rcode != dns.RcodeServerFailure {
		t.Fatal("successor proof renewed at query time")
	}
	// API loss keeps only already checked, volatile proof deadlines. Restart
	// has no pending evidence and must recollect it from the signed authority.
	offline.Store(true)
	if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err == nil {
		t.Fatal("offline control plane reported success")
	}
	if got := s.platformServing.Load().answer(q, "", time.Now()); got.Rcode != dns.RcodeSuccess {
		t.Fatal("control-plane outage discarded fresh transition evidence")
	}
	restarted := NewService(cfg, nil)
	restarted.PlatformTokenFile = s.PlatformTokenFile
	if _, err := restarted.loadDNSServingCache(); err != nil {
		t.Fatal(err)
	}
	if got := restarted.platformServing.Load().answer(q, "", time.Now()); got.Rcode != dns.RcodeServerFailure {
		t.Fatal("restart recovered volatile transition facts")
	}
	offline.Store(false)
	complete.Store(true)
	if err := s.syncPlatformDNSServingOnce(context.Background(), probe, s.probeDNSServingListener); err != nil {
		t.Fatal(err)
	}
	if reports.Load() != 1 || !dnsServingReady(s.platformServing.Load(), time.Now()) || !reflect.DeepEqual(s.platformServing.Load().record.Candidate.Assignment, candidate.Assignment) {
		t.Fatal("complete candidate failed normal positive activation")
	}
}
