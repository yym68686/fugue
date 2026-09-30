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

// Two hostnames move independently. The changed deployment is already serving
// on its Edge while another hostname still reports the previous publication.
// Neither whole DNS artifact is ready, although each hostname has a complete,
// fresh quorum authorized by one exact signed publication.
func TestDNSDeploymentTransitionKeepsPerRecordAnswersWithoutPromotingLKG(t *testing.T) {
	stamp := time.Now().UTC().Truncate(time.Second)
	fixture := func(generation string) (model.PlatformArtifact, dnsPlatformCandidate) {
		return dnsServingFixtureWithIntent(t, "edge-group-a", "global", func(r *platformconfig.CompileRequest) {
			r.Intent.Generation = generation
			r.Intent.Routes[0].DeploymentGeneration = generation
			r.Intent.Routes[0].CacheNamespace = generation
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
			r.RuntimeSnapshot.CapturedAt = &stamp
			r.RuntimeSnapshot.DNSConsumers[0].ObservedAt = stamp
			r.RuntimeSnapshot.DNSEdgeEndpoints[0].ObservedAt = stamp
			r.RuntimeSnapshot.DNSSelections[0].ObservedAt = stamp
			selection := r.RuntimeSnapshot.DNSSelections[0]
			selection.Hostname = other.Hostname
			r.RuntimeSnapshot.DNSSelections = append(r.RuntimeSnapshot.DNSSelections, selection)
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
			reports.Add(1)
			var h platformcontrol.PlatformConsumerHeartbeatEnvelope
			json.NewDecoder(r.Body).Decode(&h)
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
	probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
		if next.Load() && (host == "app.example.test" || complete.Load()) {
			p, err := proof(nextPayload, parent, candidate, nextRoute, host, address)
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
