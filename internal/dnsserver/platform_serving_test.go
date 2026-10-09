package dnsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/edgetopology"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestDNSArtifactAnswersRecheckQuorumExpiryAndZoneOwnership(t *testing.T) {
	view, plan, policy, facts, now := queryExecutionFixture()
	view.Records = append(view.Records, model.EdgeDNSRecord{Name: "alias.example.test", Type: "CNAME", Values: []string{"target.example.test"}, TTL: 60}, model.EdgeDNSRecord{Name: "static.example.test", Type: "A", Values: []string{"8.8.4.4"}, TTL: 60}, model.EdgeDNSRecord{Name: "*.wild.example.test", Type: "TXT", Values: []string{"wild"}, TTL: 60})
	views := []platformconfig.DNSQueryView{view, {NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "child.example.test", Records: []model.EdgeDNSRecord{{Name: "child.example.test", Type: "TXT", Values: []string{"child"}, TTL: 30}}}}
	for i := range facts {
		if facts[i].Proof.EdgeID == "edge-b" {
			facts[i].Proof.ValidUntil = now.Add(5 * time.Second)
		}
	}
	authorities := []platformconfig.DNSAuthorityPolicy{}
	for _, v := range views {
		authorities = append(authorities, platformconfig.DNSAuthorityPolicy{NodeID: v.NodeID, Zone: v.Zone, Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600})
	}
	p := dnsServingPayload{Plan: &plan, Queries: views, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &policy, DNSAuthorities: authorities, DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: "dns-a"}}}}
	before, _ := json.Marshal([]any{views, plan, facts})
	st, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: now, Candidate: dnsPlatformCandidate{Artifact: model.PlatformArtifact{GenerationSequence: 12}}}, p, "route", "dns-a", "edge-group-a", facts, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name           string
		kind           uint16
		offset         time.Duration
		rcode, answers int
	}{
		{"target.example.test", dns.TypeA, 0, dns.RcodeSuccess, 1}, {"target.example.test", dns.TypeA, 6 * time.Second, dns.RcodeServerFailure, 0},
		{"_acme-challenge.example.test", dns.TypeTXT, 0, dns.RcodeSuccess, 0}, {"absent.example.test", dns.TypeA, 0, dns.RcodeNameError, 0},
		{"alias.example.test", dns.TypeA, 0, dns.RcodeSuccess, 1}, {"a.wild.example.test", dns.TypeTXT, 0, dns.RcodeSuccess, 1},
		{"child.example.test", dns.TypeTXT, 0, dns.RcodeSuccess, 1}, {"absent.child.example.test", dns.TypeA, 0, dns.RcodeNameError, 0},
		{"outside.test", dns.TypeA, 0, dns.RcodeRefused, 0}, {"static.example.test", dns.TypeA, 6 * time.Second, dns.RcodeSuccess, 1},
		{"example.test", dns.TypeNS, 0, dns.RcodeSuccess, 1}, {"example.test", dns.TypeSOA, 0, dns.RcodeSuccess, 1},
		{"static.example.test", dns.TypeA, 2 * time.Hour, dns.RcodeServerFailure, 0},
	} {
		req := new(dns.Msg)
		req.SetQuestion(dns.Fqdn(tc.name), tc.kind)
		r := st.answer(req, "", now.Add(tc.offset))
		if r.Rcode != tc.rcode || len(r.Answer) != tc.answers {
			t.Fatalf("%s/%s %v: %s", tc.name, dns.TypeToString[tc.kind], tc.offset, r)
		}
		if tc.name == "target.example.test" && len(r.Answer) > 0 && r.Answer[0].Header().Ttl > 5 {
			t.Fatal("TTL outlives quorum")
		}
		if tc.name == "absent.child.example.test" && r.Ns[0].Header().Name != "child.example.test." {
			t.Fatal("longest zone lost")
		}
	}
	after, _ := json.Marshal([]any{views, plan, facts})
	if string(before) != string(after) {
		t.Fatal("answering mutated signed input or renewed facts")
	}
}

func TestDNSServingTemporaryReadinessLossDoesNotPublishEmptyPositiveAnswer(t *testing.T) {
	view, plan, policy, facts, now := queryExecutionFixture()
	p := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{
		MaxStaleSeconds: 3600, DNSReadiness: &policy,
		DNSAuthorities:    []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}},
		DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: "dns-a"}},
	}}
	st, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: now}, p, "route", "dns-a", "edge-group-a", facts, now)
	if err != nil {
		t.Fatal(err)
	}
	query := func(name string) *dns.Msg {
		req := new(dns.Msg)
		req.SetQuestion(name, dns.TypeA)
		return st.answer(req, "", now)
	}
	if got := query("target.example.test."); got.Rcode != dns.RcodeSuccess || len(got.Answer) == 0 {
		t.Fatal("fresh dynamic record lost its answer")
	}
	st, err = buildDNSServingState(dnsServingCheckpoint{AppliedAt: now}, p, "route", "dns-a", "edge-group-a", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := query("target.example.test."); got.Rcode != dns.RcodeServerFailure || len(got.Answer) != 0 {
		t.Fatal("temporary proof loss became an empty positive answer", got)
	}
	if got := query("absent.example.test."); got.Rcode != dns.RcodeNameError {
		t.Fatal("unknown hostname no longer returns NXDOMAIN")
	}
	unconfigured := new(dns.Msg)
	unconfigured.SetQuestion("target.example.test.", dns.TypeAAAA)
	if got := st.answer(unconfigured, "", now); got.Rcode != dns.RcodeSuccess || len(got.Answer) != 0 {
		t.Fatal("unconfigured address family no longer returns NODATA", got)
	}
}

func TestCompatibleDNSReleaseProofRequiresEquivalentPublication(t *testing.T) {
	parent, previous := dnsServingFixture(t, true)
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}, nil)
	payload, routeID, err := s.verifyDNSServingRelease(parent, previous)
	if err != nil {
		t.Fatal(err)
	}
	old := &dnsServingState{record: dnsServingCheckpoint{Parent: parent, Candidate: previous, AppliedAt: time.Now()}, payload: payload, routeID: routeID}
	next := previous
	next.Release.ID = "next-release"
	next.Release.FencingToken++
	next.Release.ReleasedAt = time.Now().UTC()
	bridge := &dnsReleaseBridge{parent: parent, candidate: next, payload: payload, routeID: routeID}
	fact := dnsReadinessFact{Ready: true, ProbeID: payload.Plan.Probes[0].ID, Proof: routeprobe.Proof{TrafficRelease: &model.TrafficReleaseBinding{
		ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID,
		PolicyDigest: next.Artifact.Metadata["policy_digest"], IntentDigest: next.Artifact.Metadata["intent_digest"],
		InputSnapshotDigest: next.Artifact.Metadata["input_snapshot_digest"], ReleaseID: next.Release.ID,
		ReleaseChannel: next.Release.ReleaseChannel, FencingToken: next.Release.FencingToken, ScopeKey: next.Assignment.ScopeKey,
	}}}
	if !compatibleDNSReleaseProbes(old, bridge)[fact.ProbeID] || !dnsProofMatchesRelease(fact.Proof, bridge.parent, bridge.candidate, bridge.routeID) {
		t.Fatal("equivalent successor proof rejected")
	}
	for name, change := range map[string]func(*dnsServingState, *dnsReadinessFact, *dnsReleaseBridge){
		"plan": func(_ *dnsServingState, _ *dnsReadinessFact, b *dnsReleaseBridge) {
			p := b.payload
			plan := *p.Plan
			plan.Probes = append([]platformconfig.DNSReadinessProbe(nil), plan.Probes...)
			plan.Probes[0].RouteDigest = "changed"
			p.Plan = &plan
			b.payload = p
		},
		"answer": func(_ *dnsServingState, _ *dnsReadinessFact, b *dnsReleaseBridge) {
			p := b.payload
			p.Queries = append([]platformconfig.DNSQueryView(nil), p.Queries...)
			p.Queries[0].Zone = "other.test"
			b.payload = p
		},
		"policy": func(_ *dnsServingState, _ *dnsReadinessFact, b *dnsReleaseBridge) {
			b.payload.Policy.MaxStaleSeconds++
		},
		"binding": func(_ *dnsServingState, f *dnsReadinessFact, _ *dnsReleaseBridge) {
			f.Proof.TrafficRelease.ReleaseID = "foreign"
		},
		"replay": func(_ *dnsServingState, _ *dnsReadinessFact, b *dnsReleaseBridge) {
			b.candidate.Release.ReleasedAt = previous.Release.ReleasedAt
		},
		"unready": func(_ *dnsServingState, f *dnsReadinessFact, _ *dnsReleaseBridge) {
			f.Ready = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			copyBridge, copyFact := *bridge, fact
			binding := *fact.Proof.TrafficRelease
			copyFact.Proof.TrafficRelease = &binding
			change(old, &copyFact, &copyBridge)
			if compatibleDNSReleaseProbes(old, &copyBridge)[copyFact.ProbeID] && copyFact.Ready && dnsProofMatchesRelease(copyFact.Proof, copyBridge.parent, copyBridge.candidate, copyBridge.routeID) {
				t.Fatal("changed publication or invalid proof granted readiness")
			}
		})
	}
}

func dnsServingFixture(t *testing.T, consumerMode ...bool) (model.PlatformArtifact, dnsPlatformCandidate) {
	return dnsServingFixtureForGroup(t, "edge-group-a", consumerMode...)
}

func dnsServingFixtureForGroup(t *testing.T, group string, consumerMode ...bool) (model.PlatformArtifact, dnsPlatformCandidate) {
	return dnsServingFixtureForScope(t, group, "global", consumerMode...)
}

func dnsServingFixtureForScope(t *testing.T, group, scope string, consumerMode ...bool) (model.PlatformArtifact, dnsPlatformCandidate) {
	return dnsServingFixtureWithIntent(t, group, scope, nil, consumerMode...)
}

func dnsServingFixtureWithIntent(t *testing.T, group, scope string, change func(*platformconfig.CompileRequest), consumerMode ...bool) (model.PlatformArtifact, dnsPlatformCandidate) {
	t.Helper()
	now := time.Now().UTC()
	zone := "example.test"
	node := "dns-a"
	r := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: scope, Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: "app", TenantID: "tenant", RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamURL: "http://origin:8080", Enabled: true}}, DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", AppID: "app", TenantID: "tenant", Type: "FUGUE_APP", Values: []string{"app"}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "ipv4_only", IPv6Policy: "ipv4_only", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}, {Hostname: "_acme-challenge.example.test", Type: "TXT", Values: []string{"expires"}, TTL: 60, ValueExpirations: map[string]time.Time{"expires": now.Add(30 * time.Second)}}}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: node, EdgeGroupID: group, Zones: []string{zone}, ProbeLabel: "probe", ProbeTTL: 60}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: scope, MinimumHealthyEdges: 1, MaxStaleSeconds: 3600, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{group}}}, DNSReadiness: &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 1, FactFreshnessSeconds: 60, MaxConcurrency: 2, MaxProbes: 10}, DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: node}}, DNSAuthorities: []platformconfig.DNSAuthorityPolicy{{NodeID: node, Zone: zone, Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}}, DNSAnswerRules: []platformconfig.DNSAnswerRule{{NodeID: node, Hostname: "app.example.test", Type: "A", SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a"}}, TTLSeconds: 60}}}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []platformconfig.DNSConsumerObservation{{NodeID: node, EdgeGroupID: group, ObservedAt: now, A: []string{"8.8.8.8"}}}, DNSEdgeEndpoints: []platformconfig.DNSEdgeEndpoint{{EdgeID: "edge-a", EdgeGroupID: group, ObservedAt: now, A: []string{"8.8.8.8"}}}, DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: node, Hostname: "app.example.test", Type: "A", SourceGeneration: "source", SourceDigest: "sha256:" + strings.Repeat("a", 64), ObservedAt: now, Candidates: []platformconfig.DNSSelectionCandidate{{IP: "8.8.8.8", EdgeID: "edge-a", EdgeGroupID: group}}}}}}
	if scope != "global" {
		r.Intent.AuthorityCellID, r.Policy.AuthorityCellID = group, group
		r.Intent.EdgeTopology = &edgetopology.Intent{
			SchemaVersion: edgetopology.SchemaVersion,
			Cells:         []edgetopology.AuthorityCell{{ID: group}},
			Pools:         []edgetopology.ServingPool{{ID: "pool-public"}},
			Edges:         []edgetopology.Edge{{ID: "edge-a", AuthorityCellID: group, ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-a"}}},
		}
		topology, err := platformconfig.TrafficConsumerTopologyFromIntent(r.Intent)
		if err != nil {
			t.Fatal(err)
		}
		r.Policy.ConsumerTopologyDigest, err = platformconfig.Digest(topology)
		if err != nil {
			t.Fatal(err)
		}
	}
	if change != nil {
		change(&r)
	}
	routes, err := platformconfig.ResolveRouteOrigins(r.Intent.Routes, r.RuntimeSnapshot, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := platformconfig.DNSPlacementInputDigest(r.Intent.DNS[0], routes, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	r.RuntimeSnapshot.DNSPlacements = []platformconfig.DNSPlacementObservation{{InputDigest: digest, CheckedAt: now, Status: "resolved", TargetTTL: 60, Candidates: []platformconfig.DNSPlacementCandidate{{EdgeID: "edge-a", EdgeGroupID: group, ServingGeneration: "serving", ObservedAt: now, ValidUntil: now.Add(time.Minute), Healthy: true, RouteReady: true, TLSReady: true, A: []string{"8.8.8.8"}}}}}
	if len(consumerMode) > 0 && consumerMode[0] {
		r.Policy.DNSPlacementMode = platformconfig.DNSPlacementConsumerReadiness
		r.Policy.DNSQueryPolicy = &platformconfig.DNSQueryPolicy{RankingMode: "disabled", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}
		r.Policy.TLSReadiness = r.Policy.DNSReadiness
		r.RuntimeSnapshot.DNSPlacements = nil
	}
	compiled, err := platformconfig.Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(a model.PlatformArtifact, id string) model.PlatformArtifact {
		a.ID, a.ScopeKey, a.Status, a.GenerationSequence = id, scope, model.PlatformArtifactStatusValidated, 7
		a.ContentHash, _ = platformconfig.Digest(a.Content)
		a, err = platformsafety.SignPlatformArtifact(a, bundleauth.NewKeyring("synthetic-dns-serving-secret", "key", "", "", nil))
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	child := sign(compiled.DNSArtifact, "dns")
	parent := sign(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{"route", "dns", "tls"}, now), "parent")
	a := model.PlatformConsumerAssignment{ArtifactID: child.ID, ArtifactKind: child.ArtifactKind, ScopeKey: scope, ReleaseSetID: parent.ID, ArtifactReleaseID: "release", ExpectedConsumerSetID: "expected", ExpectedGeneration: child.Generation, ContentHash: child.ContentHash, GenerationSequence: 7, FencingToken: 2, Revision: 1, ReleaseChannel: "gray"}
	release := model.PlatformArtifactRelease{ID: "release", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, ScopeKey: scope, Generation: parent.Generation, ReleaseChannel: "gray", CanaryRuleRef: "cohort=first", FencingToken: 2, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now}
	return parent, dnsPlatformCandidate{Artifact: child, Assignment: a, Release: release}
}

func TestDNSArtifactApplyProbeCheckpointRestartAndFailedCandidate(t *testing.T) {
	for _, consumerMode := range []bool{false, true} {
		name := "captured_readiness"
		if consumerMode {
			name = "consumer_readiness"
		}
		for _, tc := range []struct{ group, scope string }{{"edge-group-a", "global"}, {"cell-a", "global"}, {"cell-a", platformconfig.AuthorityCellScope("cell-a")}} {
			t.Run(name+"/"+tc.group+"/"+tc.scope, func(t *testing.T) {
				testDNSArtifactApplyProbeCheckpointRestartAndFailedCandidate(t, consumerMode, tc.group, tc.scope)
			})
		}
	}
}

func testDNSArtifactApplyProbeCheckpointRestartAndFailedCandidate(t *testing.T, consumerMode bool, group, scope string) {
	parent, candidate := dnsServingFixtureForScope(t, group, scope, consumerMode)
	authority := platformcontrol.ConsumerAuthorityID(group)
	consumerID, _ := platformcontrol.PlatformConsumerID(model.PlatformConsumerComponentDNSServer, "dns-a", authority)
	var reports int
	offline, changed := false, false
	allowFailed := false
	lastProbeStatus := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if offline {
			w.WriteHeader(503)
			return
		}
		switch r.URL.Path {
		case "/v1/platform-state/consumers/identity":
			json.NewEncoder(w).Encode(map[string]any{"token": "token", "expires_at": time.Now().Add(time.Minute), "component": "dns-server", "node_id": "dns-a", "authority_id": authority, "consumer_id": consumerID, "scope_key": scope, "artifact_kinds": []string{candidate.Artifact.ArtifactKind}})
		case "/v1/platform-state/consumers/assignment":
			if r.URL.Query().Get("serving_only") != "true" {
				t.Error("serving selection omitted")
			}
			a := candidate.Assignment
			if changed {
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
			if h.ConsumerID != consumerID || h.NodeID != "dns-a" {
				t.Error("DNS receipt lost authority or physical node")
			}
			if !slices.Contains(h.CompatibilityCapabilities, platformcontrol.TrafficReleaseCapabilityV1) {
				t.Error("executor capability missing")
			}
			if h.ApplyStatus != "applied" || (h.ProbeStatus != "passed" && !(allowFailed && h.ProbeStatus == "failed")) {
				t.Error("invalid receipt")
			}
			lastProbeStatus = h.ProbeStatus
			reports++
			json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, EvidenceHash: h.EvidenceHash, ExpectedConsumerSetID: h.ExpectedConsumerSetID}})
		default:
			t.Error("unexpected", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	cfg := config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: group, PlatformScopeKey: scope, Zone: "example.test", CachePath: filepath.Join(dir, "cache"), BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}
	token := filepath.Join(dir, "token")
	os.WriteFile(token, []byte("pod-token"), 0600)
	s := NewService(cfg, nil)
	s.PlatformTokenFile = token
	if err := s.LoadCache(); err == nil || !s.platformServingBound.Load() {
		t.Fatal("enrolled fresh disk must wait for an artifact")
	}
	p, routeID, err := s.verifyDNSServingRelease(parent, candidate)
	if err != nil {
		t.Fatal(err)
	}
	probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
		for _, req := range p.Plan.Probes {
			if req.Address == address && req.Hostname == host && req.Path == path {
				now := time.Now().UTC()
				return routeprobe.Proof{Digest: req.RouteDigest, Version: "serving", EdgeID: req.EdgeID, GroupID: req.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(time.Minute), TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID, PolicyDigest: p.Lineage.PolicyDigest, IntentDigest: p.Lineage.IntentDigest, InputSnapshotDigest: p.Lineage.InputSnapshotDigest, ReleaseID: candidate.Release.ID, ReleaseChannel: candidate.Release.ReleaseChannel, FencingToken: candidate.Release.FencingToken, ScopeKey: scope}}, nil
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected")
	}
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.Config.UDPAddr, s.Config.TCPAddr = udp.LocalAddr().String(), tcp.Addr().String()
	udpServer := &dns.Server{PacketConn: udp, Handler: s}
	tcpServer := &dns.Server{Listener: tcp, Handler: s}
	go udpServer.ActivateAndServe()
	go tcpServer.ActivateAndServe()
	defer udpServer.Shutdown()
	defer tcpServer.Shutdown()
	ctx := context.Background()
	if err = s.syncPlatformDNSServingOnce(ctx, func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		p, e := probe(ctx, host, path, address, state, timeout)
		if p.TrafficRelease != nil {
			p.TrafficRelease.FencingToken++
		}
		return p, e
	}, s.probeDNSServingListener); err == nil || s.platformServing.Load() != nil || reports != 0 {
		t.Fatal("foreign release proof activated DNS", err)
	}
	signature := parent.Provenance.Signature
	parent.Provenance.Signature = "invalid"
	if err = s.syncPlatformDNSServingOnce(ctx, probe, s.probeDNSServingListener); err == nil || s.platformServing.Load() != nil {
		t.Fatal("invalid parent signature activated DNS", err)
	}
	parent.Provenance.Signature = signature
	// The production listener convention uses an empty host (:53).
	// Keep the real UDP/TCP servers and prove the activated artifact locally.
	udpAddress, tcpAddress := s.Config.UDPAddr, s.Config.TCPAddr
	_, udpPort, _ := net.SplitHostPort(udpAddress)
	_, tcpPort, _ := net.SplitHostPort(tcpAddress)
	s.Config.UDPAddr, s.Config.TCPAddr = ":"+udpPort, ":"+tcpPort
	if err = s.syncPlatformDNSServingOnce(ctx, probe, s.probeDNSServingListener); err != nil {
		t.Fatal(err)
	}
	s.Config.UDPAddr, s.Config.TCPAddr = udpAddress, tcpAddress
	if reports != 1 || !s.Status().Healthy || s.Status().PlatformServing.State != "serving" {
		t.Fatal("serving not verified", s.Status())
	}
	metrics := s.metricSnapshot()
	if !metrics.Status.Healthy || metrics.Status.ServingGeneration != candidate.Artifact.Generation || metrics.Status.LastGoodGeneration != candidate.Artifact.Generation || metrics.Status.RecordCount == 0 {
		t.Fatal("inventory heartbeat retained dormant legacy state", metrics.Status)
	}
	query := new(dns.Msg)
	query.SetQuestion("app.example.test.", dns.TypeA)
	resp, _, err := (&dns.Client{Timeout: time.Second}).Exchange(query, s.Config.UDPAddr)
	if err != nil || len(resp.Answer) != 1 {
		t.Fatal("real UDP answer missing", resp, err)
	}
	old := s.platformServing.Load()
	saved, err := os.ReadFile(cfg.CachePath + ".platform-serving.json")
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := s.decodeDNSCheckpoint(saved)
	if err != nil || !checkpoint.Positive {
		t.Fatal(err)
	}
	previousParent, previousCandidate := parent, candidate
	parent, candidate = dnsServingFixtureWithIntent(t, group, scope, func(request *platformconfig.CompileRequest) {
		for index := range request.Policy.DNSAnswerRules {
			request.Policy.DNSAnswerRules[index].SelectionMode = "global"
			request.Policy.DNSAnswerRules[index].PhysicalOrder = nil
		}
	})
	retainedProbe := func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
		for _, requirement := range old.payload.Plan.Probes {
			if requirement.Hostname != host || requirement.Path != path || requirement.Address != address {
				continue
			}
			for _, fact := range old.facts {
				if fact.ProbeID == requirement.ID {
					return fact.Proof, nil
				}
			}
		}
		return routeprobe.Proof{}, routeprobe.ErrUnavailable
	}
	if err = s.syncPlatformDNSServingOnce(ctx, retainedProbe, s.probeDNSServingListener); err == nil || !strings.Contains(err.Error(), "legacy DNS selector retired") || s.platformServing.Load() == nil || !reflect.DeepEqual(s.platformServing.Load().record, old.record) {
		t.Fatal("retired signed candidate replaced positive serving state", err)
	}
	if answer := s.platformServing.Load().answer(query, "", time.Now()); answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 {
		t.Fatal("rejected selector interrupted the retained physical answer", answer)
	}
	retainedCheckpoint, err := os.ReadFile(cfg.CachePath + ".platform-serving.json")
	if err != nil || !bytes.Equal(retainedCheckpoint, saved) {
		t.Fatal("retired candidate overwrote positive checkpoint", err)
	}
	parent, candidate = previousParent, previousCandidate
	for _, foreign := range []string{"global", "authority-cell:cell-b"} {
		if foreign == scope {
			continue
		}
		foreignCfg := cfg
		foreignCfg.PlatformScopeKey = foreign
		if _, err := NewService(foreignCfg, nil).decodeDNSCheckpoint(saved); err == nil {
			t.Fatal("foreign scope positive checkpoint accepted")
		}
	}
	stale := *old
	stale.checkedAt = time.Now().Add(-time.Hour)
	s.platformServing.Store(&stale)
	if err = s.syncPlatformDNSServingOnce(ctx, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		return routeprobe.Proof{}, routeprobe.ErrUnavailable
	}, s.probeDNSServingListener); err != nil || lastProbeStatus != "passed" {
		t.Fatal("transport timeout discarded unexpired serving proof", err, lastProbeStatus)
	}
	retained := s.platformServing.Load()
	if !reflect.DeepEqual(retained.record, old.record) {
		t.Fatal("cached proof renewed checkpoint authority")
	}
	for i := range old.facts {
		if !reflect.DeepEqual(retained.facts[i].Proof, old.facts[i].Proof) {
			t.Fatal("cached proof was renewed or rebound")
		}
	}
	if got := retained.answer(query, "", time.Now()); got.Rcode != dns.RcodeSuccess || len(got.Answer) != 1 {
		t.Fatal("retained proof lost wire answer", got)
	}
	if got := retained.answer(query, "", time.Now().Add(2*time.Minute)); got.Rcode != dns.RcodeServerFailure {
		t.Fatal("retained proof served after expiry", got)
	}
	retainedBytes, readErr := os.ReadFile(cfg.CachePath + ".platform-serving.json")
	if readErr != nil || string(retainedBytes) != string(saved) {
		t.Fatal("transport timeout changed persisted checkpoint", readErr)
	}
	s.platformServing.Store(&stale)
	allowFailed = true
	if err = s.syncPlatformDNSServingOnce(ctx, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		return routeprobe.Proof{}, errors.New("readiness lost")
	}, s.probeDNSServingListener); err == nil || lastProbeStatus != "failed" {
		t.Fatal("failed serving retained positive runtime fact", err, lastProbeStatus)
	}
	allowFailed = false
	s.platformServing.Store(old)
	// A new fenced candidate that fails readiness or wire probes cannot change
	// either the currently served snapshot or the persisted positive checkpoint.
	candidate.Assignment.FencingToken++
	candidate.Release.FencingToken++
	candidate.Release.ID = "next"
	candidate.Assignment.ArtifactReleaseID = "next"
	oldProbe := func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
		for _, p := range old.payload.Plan.Probes {
			if p.Hostname == host && p.Path == path && p.Address == address {
				for _, f := range old.facts {
					if f.ProbeID == p.ID {
						return f.Proof, nil
					}
				}
			}
		}
		return routeprobe.Proof{}, errors.New("unknown old probe")
	}
	candidate.Assignment.ConvergenceDeadline = time.Now().Add(time.Minute)
	if err = s.syncPlatformDNSServingOnce(ctx, oldProbe, s.probeDNSServingListener); !errors.Is(err, errDNSReleaseConverging) {
		t.Fatal("fresh exact previous release not classified as bounded convergence", err)
	}
	if !reflect.DeepEqual(s.platformServing.Load().record, old.record) {
		t.Fatal("pending release replaced positive checkpoint")
	}
	candidate.Assignment.ConvergenceDeadline = time.Now().Add(-time.Second)
	if err = s.syncPlatformDNSServingOnce(ctx, oldProbe, s.probeDNSServingListener); err == nil || errors.Is(err, errDNSReleaseConverging) {
		t.Fatal("past-deadline mismatch was not a failure", err)
	}
	candidate.Assignment.ConvergenceDeadline = time.Now().Add(time.Minute)
	if err = s.syncPlatformDNSServingOnce(ctx, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		return routeprobe.Proof{}, errors.New("offline")
	}, s.probeDNSServingListener); err == nil || errors.Is(err, errDNSReleaseConverging) {
		t.Fatal("bad candidate applied")
	}
	if !reflect.DeepEqual(s.platformServing.Load().record, old.record) {
		t.Fatal("failed candidate replaced serving")
	}
	if err = s.syncPlatformDNSServingOnce(ctx, probe, func(*dnsServingState) error { return errors.New("wire failure") }); err == nil {
		t.Fatal("bad listener applied")
	}
	if !reflect.DeepEqual(s.platformServing.Load().record, old.record) {
		t.Fatal("wire failure replaced serving")
	}
	after, _ := os.ReadFile(cfg.CachePath + ".platform-serving.json")
	if string(saved) != string(after) {
		t.Fatal("failed candidate overwrote checkpoint")
	}
	// Restore selected release for recovery. A fresh process starts with only
	// verified configuration; transient readiness must be recollected.
	candidate = checkpoint.Candidate
	restarted := NewService(cfg, nil)
	restarted.PlatformTokenFile = token
	if err = restarted.LoadCache(); err != nil {
		t.Fatal(err)
	}
	if !restarted.platformServingBound.Load() || dnsServingReady(restarted.platformServing.Load(), time.Now()) {
		t.Fatal("restart reused positive transient readiness")
	}
	w := &captureDNSResponseWriter{}
	restarted.ServeDNS(w, query)
	if len(w.msg.Answer) != 0 || w.msg.Rcode != dns.RcodeServerFailure {
		t.Fatal("restart expired facts served dynamic answer")
	}
	offline = true
	if err = restarted.syncPlatformDNSServingOnce(ctx, probe, func(*dnsServingState) error { return nil }); err == nil {
		t.Fatal("API outage hidden")
	}
	restarted.ServeDNS(w, query)
	if len(w.msg.Answer) != 1 || restarted.Status().PlatformServing.FallbackReason != "control_plane_unavailable" {
		t.Fatal("verified config could not refresh independently")
	}
	offline = false
	if err = restarted.syncPlatformDNSServingOnce(ctx, probe, func(*dnsServingState) error { return nil }); err != nil || restarted.Status().StaleCache || restarted.Status().PlatformServing.FallbackReason != "" {
		t.Fatal("reconnected serving kept stale fallback", err, restarted.Status())
	}
	bad := checkpoint
	bad.Positive = false
	if _, err = s.decodeDNSCheckpoint(mustDNSJSON(t, bad)); err == nil {
		t.Fatal("unsigned positive bit changed")
	}
	bad = checkpoint
	bad.Candidate.Artifact.ContentHash = "tampered"
	if _, err = s.decodeDNSCheckpoint(mustDNSJSON(t, bad)); err == nil {
		t.Fatal("tampered local record accepted")
	}
	if !reflect.DeepEqual(checkpoint.Parent, parent) {
		t.Fatal("parent lineage changed")
	}
	// Missing/corrupt current data recovers only from a separately validated
	// positive predecessor, never from the old business-derived cache.
	path := cfg.CachePath + ".platform-serving.json"
	// Use the package's canonical predecessor path.
	if err = os.WriteFile(lkgcache.PreviousPath(path), saved, 0600); err != nil {
		t.Fatal(err)
	}
	for _, missing := range []bool{false, true} {
		if missing {
			os.Remove(path)
		} else {
			os.WriteFile(path, []byte("corrupt"), 0600)
		}
		recovery := NewService(cfg, nil)
		recovery.PlatformTokenFile = token
		if err = recovery.LoadCache(); err != nil || recovery.platformServing.Load() == nil {
			t.Fatal("verified predecessor recovery failed", err)
		}
	}
	os.Remove(lkgcache.PreviousPath(path))
	os.WriteFile(path, []byte("corrupt"), 0600)
	recovery := NewService(cfg, nil)
	recovery.PlatformTokenFile = token
	if err = recovery.LoadCache(); err == nil || !recovery.platformServingBound.Load() {
		t.Fatal("corrupt positive state downgraded")
	}
	writer := &captureDNSResponseWriter{}
	recovery.ServeDNS(writer, query)
	if writer.msg.Rcode != dns.RcodeServerFailure {
		t.Fatal("corrupt recovery served ambient data")
	}
}

func TestEnrolledDNSMissingAllCheckpointsCannotServeLegacyOrFetchBusiness(t *testing.T) {
	var legacyRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		legacyRequests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	cfg := config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "dns-cache.json"), BundleSigningKey: "legacy-key", BundleSigningKeyID: "legacy", AnswerIPs: []string{"192.0.2.9"}}
	s := NewService(cfg, nil)
	legacy := bundleauth.SignEdgeDNSBundle(model.EdgeDNSBundle{Version: "legacy", Generation: "legacy", Zone: cfg.Zone, GeneratedAt: time.Now(), Records: []model.EdgeDNSRecord{{Name: "app.example.test", Type: "A", Values: []string{"192.0.2.9"}, TTL: 60}}}, cfg.BundleSigningKey, cfg.BundleSigningKeyID, time.Hour)
	if err := s.writeCache(cacheFile{Version: cacheFileVersion, Bundle: legacy, CachedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	s.setBundle(legacy, "", false, "")
	s.PlatformTokenFile = filepath.Join(t.TempDir(), "identity")
	for _, beforeLoad := range []bool{true, false} {
		if !beforeLoad {
			if err := s.LoadCache(); err == nil || !s.platformServingBound.Load() {
				t.Fatal("missing traffic checkpoint loaded legacy cache")
			}
		}
		if err := s.SyncOnce(context.Background()); err != nil || legacyRequests.Load() != 0 {
			t.Fatal("enrolled consumer queried legacy serving API", err)
		}
		if err := s.LoadPreviousCache(); err == nil {
			t.Fatal("enrolled consumer accepted previous legacy cache")
		}
		answer := dnsQuery(t, s, "app.example.test", dns.TypeA)
		if answer.Rcode != dns.RcodeServerFailure || len(answer.Answer) != 0 || s.Status().Healthy {
			t.Fatal("ambient configuration served after enrollment", answer)
		}
		if s.metricSnapshot().Status.Healthy {
			t.Fatal("legacy heartbeat claimed readiness without artifact")
		}
	}
}

func TestEnrolledDNSStartupDoesNotRequireAmbientServingConfiguration(t *testing.T) {
	cfg := config.DNSConfig{APIURL: "https://control.example.test", DNSNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "cache"), PublicIPv4: "192.0.2.8", ListenAddr: ":7834", UDPAddr: ":5353", TCPAddr: ":5353"}
	s := NewService(cfg, nil)
	s.PlatformTokenFile = "/var/run/identity/token"
	if err := s.validateConfig(); err != nil {
		t.Fatal("artifact-only bootstrap required answer, TTL or nameserver configuration", err)
	}
	for _, field := range []string{"node", "group", "cache", "public", "invalid_public"} {
		broken := cfg
		switch field {
		case "node":
			broken.DNSNodeID = ""
		case "group":
			broken.EdgeGroupID = ""
		case "cache":
			broken.CachePath = ""
		case "public":
			broken.PublicIPv4 = ""
		case "invalid_public":
			broken.PublicIPv4 = "invalid"
		}
		other := NewService(broken, nil)
		other.PlatformTokenFile = s.PlatformTokenFile
		if err := other.validateConfig(); err == nil {
			t.Fatal("missing execution identity accepted", field)
		}
	}
	s.PlatformTokenFile = ""
	if err := s.validateConfig(); err == nil {
		t.Fatal("bootstrap accepted missing consumer credentials")
	}
	s.platformServingBound.Store(true)
	if err := s.validateConfig(); err == nil {
		t.Fatal("a retained snapshot bypassed explicit credential enrollment")
	}
	s.PlatformTokenFile = " "
	if err := s.validateConfig(); err == nil {
		t.Fatal("blank Pod credential path bypassed authentication")
	}
	s.PlatformTokenFile = ""
	s.platformServingBound.Store(false)
	s.Config.EdgeToken = "inventory-token"
	if err := s.validateConfig(); err == nil {
		t.Fatal("legacy bootstrap accepted missing answer configuration")
	}
}

func TestNeutralDNSCannotWriteLegacyInventoryOrRenamePhysicalNode(t *testing.T) {
	cfg := config.DNSConfig{APIURL: "https://control.example.test", DNSNodeID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "cell-a", Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "cache"), PublicIPv4: "192.0.2.8", ListenAddr: ":7834", UDPAddr: ":5353", TCPAddr: ":5353"}
	for _, scenario := range []string{"valid", "legacy token", "missing Pod credential", "invalid cell", "noncanonical cell", "zone alias", "invalid node"} {
		t.Run(scenario, func(t *testing.T) {
			s := NewService(cfg, nil)
			s.PlatformTokenFile = "/var/run/pod/token"
			switch scenario {
			case "legacy token":
				s.Config.EdgeToken = "legacy-inventory-token"
			case "missing Pod credential":
				s.PlatformTokenFile = ""
			case "invalid cell":
				s.Config.EdgeGroupID = "cell-A"
			case "noncanonical cell":
				s.Config.EdgeGroupID = " cell-a"
			case "zone alias":
				s.Config.DNSNodeID = "zone-alias"
			case "invalid node":
				s.Config.DNSNodeID, s.Config.PhysicalNodeID = "other:node", "other:node"
			}
			if err := s.validateConfig(); (err == nil) != (scenario == "valid") {
				t.Fatal("unexpected neutral startup result", err)
			}
			if s.heartbeatEnabled() {
				t.Fatal("neutral DNS may overwrite legacy inventory")
			}
		})
	}
}

func TestNeutralDNSRejectsForeignIdentityBeforeFetchingOrChangingState(t *testing.T) {
	for _, authority := range []string{"", "cell-b"} {
		for _, serving := range []bool{false, true} {
			t.Run(authority+fmt.Sprint(serving), func(t *testing.T) {
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					if r.URL.Path != "/v1/platform-state/consumers/identity" {
						t.Error("foreign identity used to fetch configuration")
						w.WriteHeader(403)
						return
					}
					id, _ := platformcontrol.PlatformConsumerID(model.PlatformConsumerComponentDNSServer, "dns-a", authority)
					json.NewEncoder(w).Encode(map[string]any{"token": "foreign-token", "expires_at": time.Now().Add(time.Minute), "component": "dns-server", "node_id": "dns-a", "authority_id": authority, "consumer_id": id, "scope_key": "global", "artifact_kinds": []string{model.PlatformArtifactKindDNSAnswerBundle}})
				}))
				defer server.Close()
				dir := t.TempDir()
				tokenPath := filepath.Join(dir, "token")
				if err := os.WriteFile(tokenPath, []byte("pod-credential"), 0600); err != nil {
					t.Fatal(err)
				}
				s := NewService(config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "cell-a", CachePath: filepath.Join(dir, "cache")}, nil)
				s.PlatformTokenFile = tokenPath
				var err error
				if serving {
					err = s.SyncPlatformDNSServingOnce(context.Background())
				} else {
					err = s.SyncPlatformShadowOnce(context.Background())
				}
				if err == nil || requests != 1 || s.platformServing.Load() != nil {
					t.Fatal("foreign exchange reached serving state", err, requests)
				}
				files, err := os.ReadDir(dir)
				if err != nil || len(files) != 1 || files[0].Name() != "token" {
					t.Fatal("foreign exchange rewrote checkpoint or cursor", files, err)
				}
			})
		}
	}
}
func mustDNSJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
