package dnsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	dns "github.com/miekg/dns"
)

func TestDNSPlatformShadowPreservesServingAndDurableCursor(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		name := "legacy policy"
		if versioned {
			name = "versioned exclusion policy"
		}
		t.Run(name, func(t *testing.T) { testDNSPlatformShadowPreservesServingAndDurableCursor(t, versioned) })
	}
}

func testDNSPlatformShadowPreservesServingAndDurableCursor(t *testing.T, versioned bool) {
	const key = "synthetic-dns-platform-signing-key"
	request := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent-1", Scope: "global", DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", Type: "A", Values: []string{"192.0.2.99"}, TTL: 60, Status: "active"}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy-1", Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 86400}}
	if versioned {
		request.Policy.DNSReadiness = &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
		request.Policy.DNSRouteStateConstraints = []platformconfig.DNSRouteStateConstraint{{RecordKind: model.EdgeDNSRecordKindCustomDomainTarget, InactiveBehavior: "serve_error_page"}}
		captured := time.Now().UTC()
		expires := captured.Add(-time.Hour)
		request.Intent.Routes = []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: "app", TenantID: "tenant", Enabled: true, UpstreamURL: "http://origin"}}
		request.Policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{{ID: "route-policy", Hostname: "app.example.test", TenantID: "tenant", AppID: "app", MatchScope: "tenant_hostname", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled,
			ExcludedEdgeGroupIDs: []string{"edge-group-excluded"}, ExclusionOwnerDigest: "sha256:" + strings.Repeat("a", 64), ExclusionGeneration: 2, ExclusionFence: "fence-2", ExclusionExpiresAt: &expires}}
		request.RuntimeSnapshot.CapturedAt = &captured
	}
	if versioned {
		request.Intent.DNSConsumers = []platformconfig.DNSConsumerIntent{
			{NodeID: "physical-dns-node", EdgeGroupID: "group-a", Zones: []string{"example.test", "second.test"}, ProbeLabel: "probe", ProbeTTL: 60},
			{NodeID: "other-dns-node", EdgeGroupID: "group-b", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60},
		}
		request.RuntimeSnapshot.DNSConsumers = []platformconfig.DNSConsumerObservation{
			{NodeID: "physical-dns-node", EdgeGroupID: "group-a", ObservedAt: *request.RuntimeSnapshot.CapturedAt, A: []string{"8.8.8.8"}},
			{NodeID: "other-dns-node", EdgeGroupID: "group-b", ObservedAt: *request.RuntimeSnapshot.CapturedAt, A: []string{"9.9.9.9"}},
		}
	}
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	a := compiled.DNSArtifact
	a.ID = "dns-candidate"
	a.ScopeKey = "global"
	a.GenerationSequence = 1
	a.Status = model.PlatformArtifactStatusValidated
	a.ContentHash, err = platformconfig.Digest(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	a, err = platformsafety.SignPlatformArtifact(a, bundleauth.NewKeyring(key, "signer", "", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	assignment := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "dns-set", ArtifactReleaseID: "release-1", ReleaseSetID: "release-set-1", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, ExpectedGeneration: a.Generation, ContentHash: a.ContentHash, GenerationSequence: 1, FencingToken: 1, ReleaseChannel: "shadow"}
	candidate := dnsPlatformCandidate{Artifact: a, Assignment: assignment, Release: model.PlatformArtifactRelease{ID: "release-1", ArtifactID: "release-set-1", ArtifactKind: model.PlatformArtifactKindReleaseSet, Generation: a.Metadata["release_set_generation"], ReleaseChannel: "shadow", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1}}
	lastSequence := int64(0)
	reports := 0
	loseReceipt := false
	changeAssignmentDuringObservation, artifactFetched := false, false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/platform-state/consumers/identity" {
			if r.Header.Get("Authorization") != "Bearer pod-credential" {
				t.Error("identity did not use Pod token")
			}
			json.NewEncoder(w).Encode(map[string]any{"token": "short-component-token", "expires_at": time.Now().Add(time.Minute), "component": "dns-server", "node_id": "physical-dns-node", "scope_key": "global", "artifact_kinds": []string{a.ArtifactKind}})
			return
		}
		if r.Header.Get("Authorization") != "Bearer short-component-token" {
			t.Error("component API did not use exchanged credential")
		}
		switch r.URL.Path {
		case "/v1/platform-state/consumers/assignment":
			current := assignment
			if changeAssignmentDuringObservation && artifactFetched {
				current.FencingToken++
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{current}})
		case "/v1/platform-state/consumers/artifacts/dns-candidate":
			artifactFetched = true
			if r.URL.Query().Get("expected_consumer_set_id") != "dns-set" {
				t.Error("missing exact expected set binding")
			}
			json.NewEncoder(w).Encode(candidate)
		case "/v1/platform-state/consumers/trusted-heartbeat":
			var h platformcontrol.PlatformConsumerHeartbeatEnvelope
			if json.NewDecoder(r.Body).Decode(&h) != nil {
				t.Fatal("heartbeat decode")
			}
			digest, _ := platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if h.Sequence <= lastSequence || h.EvidenceHash != digest || h.ApplyStatus != "staged" || h.ProbeStatus == "passed" || h.ActualGeneration != "legacy-generation" || h.DesiredGeneration != assignment.ExpectedGeneration {
				t.Errorf("shadow produced false serving evidence: %+v", h)
			}
			lastSequence = h.Sequence
			reports++
			if loseReceipt {
				w.WriteHeader(503)
				return
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, ExpectedConsumerSetID: h.ExpectedConsumerSetID, EvidenceHash: h.EvidenceHash}, Drift: true})
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("pod-credential"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := config.DNSConfig{APIURL: server.URL, DNSNodeID: "physical-dns-node", EdgeGroupID: "group-a", Zone: "example.test", AnswerIPs: []string{"192.0.2.1"}, TTL: 60, CachePath: filepath.Join(dir, "serving.json"), BundleSigningKey: key, BundleSigningKeyID: "signer", MaxStale: time.Hour}
	create := func() *Service {
		s := NewService(cfg, log.New(io.Discard, "", 0))
		s.PlatformTokenFile = tokenPath
		return s
	}
	service := create()
	legacy := bundleauth.SignEdgeDNSBundle(model.EdgeDNSBundle{Version: "legacy-generation", Generation: "legacy-generation", GeneratedAt: time.Now(), Zone: cfg.Zone, Records: []model.EdgeDNSRecord{{Name: "app.example.test", Type: "A", Values: []string{"192.0.2.1"}, TTL: 60, Status: "active"}}}, key, "signer", time.Hour)
	if err := service.writeCache(cacheFile{Version: cacheFileVersion, Bundle: legacy, CachedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := service.LoadCache(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(cfg.CachePath)
	if err := service.SyncPlatformShadowOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkServing := func(s *Service) {
		t.Helper()
		after, _ := os.ReadFile(cfg.CachePath)
		answer := dnsQuery(t, s, "app.example.test", dns.TypeA)
		if !bytes.Equal(before, after) || s.Status().ServingGeneration != "legacy-generation" || len(answer.Answer) != 1 || answer.Answer[0].(*dns.A).A.String() != "192.0.2.1" {
			t.Fatal("shadow mutated serving/LKG")
		}
	}
	checkServing(service)
	if service.Status().PlatformCandidate.State != "shadow_verified" || service.Status().PlatformCandidate.ReportedAt.IsZero() {
		t.Fatal("shadow facts were not exposed")
	}
	if versioned {
		status := service.Status().PlatformCandidate
		if status.Readiness == nil || status.Readiness.Serving || status.Readiness.PlanDigest == "" {
			t.Fatal("readiness status missing or serving")
		}
		if status.RecordCount != 1 || status.ConsumerViewCount != 2 || status.ProbeRecordCount != 2 {
			t.Fatalf("wrong verified counts: %+v", status)
		}
		for _, identity := range [][3]string{{"unknown", "group-a", "example.test"}, {"physical-dns-node", "group-b", "example.test"}, {"physical-dns-node", "group-a", "absent.test"}} {
			other := create()
			other.Config.DNSNodeID = identity[0]
			other.Config.EdgeGroupID = identity[1]
			other.Config.Zone = identity[2]
			if _, err := other.verifyPlatformDNSCandidate(candidate, assignment); err == nil {
				t.Fatal("candidate authorized an unassigned consumer")
			}
		}
	}
	stagedBefore, _ := os.ReadFile(cfg.CachePath + ".platform-shadow.json")
	verifiedStatus := *service.Status().PlatformCandidate
	candidate.Artifact.Provenance.Signature = "tampered"
	if err := service.SyncPlatformShadowOnce(context.Background()); err == nil {
		t.Fatal("tampered signature accepted")
	}
	stagedAfter, _ := os.ReadFile(cfg.CachePath + ".platform-shadow.json")
	if !bytes.Equal(stagedBefore, stagedAfter) || reports != 1 {
		t.Fatal("bad candidate overwrote verified cache or reported success")
	}
	if *service.Status().PlatformCandidate != verifiedStatus {
		t.Fatal("invalid candidate changed verified statistics")
	}
	checkServing(service)
	candidate.Artifact = a
	if versioned {
		changeAssignmentDuringObservation, artifactFetched = true, false
		if err := service.SyncPlatformShadowOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "assignment changed") {
			t.Fatal("changed assignment accepted", err)
		}
		unchanged, _ := os.ReadFile(cfg.CachePath + ".platform-shadow.json")
		if !bytes.Equal(stagedBefore, unchanged) || reports != 1 {
			t.Fatal("changed assignment wrote candidate or heartbeat")
		}
		changeAssignmentDuringObservation = false
		checkServing(service)
	}
	restarted := create()
	if err := restarted.LoadCache(); err != nil {
		t.Fatal(err)
	}
	first := lastSequence
	loseReceipt = true
	if err := restarted.SyncPlatformShadowOnce(context.Background()); err == nil {
		t.Fatal("lost receipt ignored")
	}
	loseReceipt = false
	again := create()
	if err := again.LoadCache(); err != nil {
		t.Fatal(err)
	}
	if err := again.SyncPlatformShadowOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reports != 3 || lastSequence <= first {
		t.Fatal("cursor replay after restart")
	}
	checkServing(again)
	if err := os.WriteFile(cfg.CachePath+".platform-shadow.json", []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := again.SyncPlatformShadowOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "cursor is corrupt") {
		t.Fatalf("corrupt cursor was reset: %v", err)
	}
	checkServing(again)
}
