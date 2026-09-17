package edge

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
	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func TestEdgePlatformShadowPreservesServingAndChecksBindings(t *testing.T) {
	for _, scenario := range []string{"legacy", "compiled placement", "compiled placement and cache"} {
		t.Run(scenario, func(t *testing.T) {
			testEdgePlatformShadowPreservesServingAndChecksBindings(t, scenario)
		})
	}
}

func TestValidatePlatformCandidateIndexIsDeterministicAndRejectsEmpty(t *testing.T) {
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "candidate", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", PathPrefix: "/api", UpstreamURL: "http://origin.example.test:8080", Enabled: true}}},
		Policy: platformconfig.PolicySnapshot{Generation: "policy"},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := validatePlatformCandidateIndex(compiled.RouteArtifact, "edge-group-test")
	if err != nil {
		t.Fatal(err)
	}
	second, err := validatePlatformCandidateIndex(compiled.RouteArtifact, "edge-group-test")
	if err != nil || first != second {
		t.Fatalf("non-deterministic index: %s %s %v", first, second, err)
	}
	compiled.RouteArtifact.Content["routes"] = []platformconfig.CompiledRoute{}
	if _, err := validatePlatformCandidateIndex(compiled.RouteArtifact, "edge-group-test"); err == nil {
		t.Fatal("empty candidate index accepted")
	}
}

func testEdgePlatformShadowPreservesServingAndChecksBindings(t *testing.T, scenario string) {
	keyring := bundleauth.NewKeyring("synthetic-platform-key", "signer", "", "", nil)
	request := platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent-1", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin.example.test:8080", Enabled: true}}},
		Policy: platformconfig.PolicySnapshot{Generation: "policy-1", Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 86400},
	}
	if scenario != "legacy" {
		captured := time.Now().UTC()
		expires := captured.Add(time.Hour)
		request.Intent.Routes[0].RuntimeID = "runtime-a"
		request.Intent.Routes[0].OriginRef = "origin-a"
		request.RuntimeSnapshot = platformconfig.RuntimeSnapshot{CapturedAt: &captured, Origins: []platformconfig.OriginObservation{{
			Ref: "origin-a", ObservedAt: captured, Status: model.EdgeRouteStatusActive, RuntimeID: "runtime-a", RuntimeType: "managed", RuntimeEdgeGroupID: "group-a", RuntimeClusterNode: "node-a",
		}}}
		request.Policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{{
			ID: "route-policy-a", Hostname: "app.example.test", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true,
			MinHealthyEdgeNodes: 2, ExcludedEdgeIDs: []string{"edge-excluded"}, ExcludedEdgeGroupIDs: []string{"group-excluded"}, ExclusionReason: "maintenance", ExclusionExpiresAt: &expires,
		}}
	}
	if scenario == "compiled placement and cache" {
		request.Intent.CachePolicies = []model.CachePolicy{{ID: "assets", Kind: model.CachePolicyKindStaticAssets, HostnameScope: "app.example.test", TTLSeconds: 60, MethodAllowlist: []string{"GET", "HEAD"}}}
		request.Intent.Routes[0].CachePolicyID = "assets"
		request.Intent.Routes[0].CacheNamespace = "app_gen1"
	}
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	a := compiled.RouteArtifact
	a.ID, a.ScopeKey, a.Status, a.GenerationSequence = "route-candidate", "global", model.PlatformArtifactStatusValidated, 1
	a.ContentHash, err = platformconfig.Digest(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	a, err = platformsafety.SignPlatformArtifact(a, keyring)
	if err != nil {
		t.Fatal(err)
	}
	assignment := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "route-set", ArtifactReleaseID: "release-1", ReleaseSetID: "release-set-1", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: "global", ExpectedGeneration: a.Generation, ContentHash: a.ContentHash, GenerationSequence: 1, FencingToken: 1, ReleaseChannel: "shadow"}
	original := edgePlatformCandidate{Artifact: a, Assignment: assignment, Release: model.PlatformArtifactRelease{ID: "release-1", ArtifactID: "release-set-1", ArtifactKind: model.PlatformArtifactKindReleaseSet, Generation: a.Metadata["release_set_generation"], ReleaseChannel: "shadow", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1}}
	var candidate edgePlatformCandidate
	reset := func() {
		raw, _ := json.Marshal(original)
		candidate = edgePlatformCandidate{}
		if err := json.Unmarshal(raw, &candidate); err != nil {
			t.Fatal(err)
		}
	}
	reset()
	reports := 0
	lastSequence := int64(0)
	loseReceipt := false
	var onDownload func()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/platform-state/consumers/identity":
			if r.Header.Get("Authorization") != "Bearer pod-token" {
				t.Error("identity credential mismatch")
			}
			json.NewEncoder(w).Encode(map[string]any{"token": "component-token", "expires_at": time.Now().Add(time.Minute), "component": "edge-worker", "node_id": "edge-node", "scope_key": "global", "artifact_kinds": []string{a.ArtifactKind}})
		case "/v1/platform-state/consumers/assignment":
			if r.Header.Get("Authorization") != "Bearer component-token" {
				t.Error("assignment credential mismatch")
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{assignment}})
		case "/v1/platform-state/consumers/artifacts/route-candidate":
			if onDownload != nil {
				onDownload()
			}
			if r.URL.Query().Get("expected_consumer_set_id") != "route-set" {
				t.Error("expected set missing")
			}
			json.NewEncoder(w).Encode(candidate)
		case "/v1/platform-state/consumers/trusted-heartbeat":
			if r.Header.Get("Authorization") != "Bearer component-token" {
				t.Error("heartbeat credential mismatch")
			}
			var h platformcontrol.PlatformConsumerHeartbeatEnvelope
			if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			hash, _ := platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if h.Sequence <= lastSequence || hash != h.EvidenceHash || h.ApplyStatus != "staged" || h.ProbeStatus != "shadow_validated" || h.ActualGeneration == h.DesiredGeneration {
				t.Errorf("false shadow evidence: %+v", h)
			}
			lastSequence = h.Sequence
			reports++
			if loseReceipt {
				w.WriteHeader(503)
				return
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, ExpectedConsumerSetID: h.ExpectedConsumerSetID, EvidenceHash: h.EvidenceHash}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	cache := filepath.Join(dir, "routes.json")
	if err := os.WriteFile(tokenFile, []byte("pod-token"), 0600); err != nil {
		t.Fatal(err)
	}
	servingBytes := []byte("existing-serving-cache")
	if err := os.WriteFile(cache, servingBytes, 0600); err != nil {
		t.Fatal(err)
	}
	create := func() *Service {
		s := NewService(config.EdgeConfig{APIURL: server.URL, EdgeID: "edge-node", EdgeGroupID: "edge-group-test", CachePath: cache, BundleSigningKey: "synthetic-platform-key", BundleSigningKeyID: "signer"}, log.New(io.Discard, "", 0))
		s.PlatformTokenFile = tokenFile
		s.recordSyncSuccess(testBundle("legacy-serving"), "etag", time.Now(), false)
		return s
	}
	s := create()
	servingIndex := s.currentRouteIndex()
	ctx := context.Background()
	if err := s.SyncPlatformShadowOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Status().PlatformCandidate.State != "shadow_verified" {
		t.Fatal(s.Status().PlatformCandidate)
	}
	if s.Status().PlatformCandidate.RouteIndexDigest == "" {
		t.Fatal("candidate route index digest was not recorded")
	}
	staged, _ := os.ReadFile(cache + ".platform-shadow.json")
	var persisted edgePlatformCandidate
	if err := json.Unmarshal(staged, &persisted); err != nil {
		t.Fatal(err)
	}
	gotContent, _ := json.Marshal(persisted.Artifact.Content)
	wantContent, _ := json.Marshal(candidate.Artifact.Content)
	if !bytes.Equal(gotContent, wantContent) {
		t.Fatal("candidate persistence lost compiled fields or cache policy")
	}
	if persisted.RouteIndexDigest != s.Status().PlatformCandidate.RouteIndexDigest {
		t.Fatalf("candidate route index digest was not persisted: file=%q status=%q", persisted.RouteIndexDigest, s.Status().PlatformCandidate.RouteIndexDigest)
	}
	resignCandidate := func() {
		t.Helper()
		var err error
		candidate.Artifact.ContentHash, err = platformconfig.Digest(candidate.Artifact.Content)
		if err != nil {
			t.Fatal(err)
		}
		candidate.Artifact, err = platformsafety.SignPlatformArtifact(candidate.Artifact, keyring)
		if err != nil {
			t.Fatal(err)
		}
		assignment.ContentHash = candidate.Artifact.ContentHash
		candidate.Assignment = assignment
	}
	checkServing := func() {
		t.Helper()
		raw, _ := os.ReadFile(cache)
		b, _ := s.Bundle()
		if !bytes.Equal(raw, servingBytes) || b.Version != "legacy-serving" || s.currentRouteIndex() != servingIndex {
			t.Fatal("shadow changed serving cache or routes")
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func()
	}{
		{"signature", func() { candidate.Artifact.Provenance.Signature = "tampered" }},
		{"kind", func() { candidate.Artifact.ArtifactKind = model.PlatformArtifactKindDNSAnswerBundle }},
		{"scope", func() { candidate.Artifact.ScopeKey = "other" }},
		{"sequence", func() { candidate.Artifact.GenerationSequence++ }},
		{"release", func() { candidate.Release.ArtifactID = "other" }},
		{"assignment", func() { candidate.Assignment.ExpectedConsumerSetID = "other" }},
		{"unknown compiled route field", func() {
			candidate.Artifact.Content["routes"].([]any)[0].(map[string]any)["runtime_unknown"] = true
			resignCandidate()
		}},
		{"missing explicit enabled", func() {
			delete(candidate.Artifact.Content["routes"].([]any)[0].(map[string]any), "enabled")
			resignCandidate()
		}},
		{"invalid compiled placement type", func() {
			candidate.Artifact.Content["routes"].([]any)[0].(map[string]any)["runtime_cluster_node"] = true
			resignCandidate()
		}},
		{"unknown cache reference", func() {
			candidate.Artifact.Content["routes"].([]any)[0].(map[string]any)["cache_policy_id"] = "missing-policy"
			resignCandidate()
		}},
		{"cache scope mismatch", func() {
			route := candidate.Artifact.Content["routes"].([]any)[0].(map[string]any)
			route["cache_policy_id"], route["cache_namespace"] = "assets", "app_gen1"
			candidate.Artifact.Content["cache_policies"] = []model.CachePolicy{{ID: "assets", Kind: model.CachePolicyKindStaticAssets, HostnameScope: "other.example.test"}}
			resignCandidate()
		}},
		{"unknown payload", func() {
			candidate.Artifact.Content["unknown"] = true
			candidate.Artifact.ContentHash, _ = platformconfig.Digest(candidate.Artifact.Content)
			candidate.Artifact, _ = platformsafety.SignPlatformArtifact(candidate.Artifact, keyring)
			assignment.ContentHash = candidate.Artifact.ContentHash
			candidate.Assignment = assignment
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset()
			tc.mutate()
			if err := s.SyncPlatformShadowOnce(ctx); err == nil {
				t.Fatal("invalid candidate accepted")
			}
			after, _ := os.ReadFile(cache + ".platform-shadow.json")
			if reports != 1 || !bytes.Equal(staged, after) {
				t.Fatal("invalid candidate overwrote staged state or reported")
			}
			checkServing()
			assignment = original.Assignment
		})
	}
	reset()
	loseReceipt = true
	if err := s.SyncPlatformShadowOnce(ctx); err == nil {
		t.Fatal("lost receipt ignored")
	}
	loseReceipt = false
	first := lastSequence
	s = create()
	servingIndex = s.currentRouteIndex()
	if err := s.SyncPlatformShadowOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if lastSequence <= first || reports != 3 {
		t.Fatal("restart replayed cursor")
	}
	checkServing()
	// Unknown activation must stop before any request; inactive slots must not
	// report node-wide facts. Recheck the CAS generation after downloading too.
	s.Config.EdgeGroupID = "edge-group-test"
	s.Config.EdgeSlot = "a"
	activation := filepath.Join(dir, "activation.json")
	s.RouteBundleSource = RouteBundleSourceConfig{URL: "http://authority.example.test/v1/edge/routes", CandidateURL: "http://authority.example.test/v1/edge/candidate-routes", TokenFile: tokenFile, VerifierKeyringFile: filepath.Join(dir, "verifier.json"), ActivationStateFile: activation}
	if err := s.SyncPlatformShadowOnce(ctx); err == nil {
		t.Fatal("unknown activation authorized reporting")
	}
	_, err = edgegroupfront.ApplyActivationCAS(activation, edgegroupfront.ActivationCASRequest{Operation: edgegroupfront.ActivationOperationInit, GroupID: "edge-group-test", ExpectedSlot: "b", TargetSlot: "b", Reason: "platform shadow activation test", BundleGeneration: "legacy-serving", WorkerSourceCommit: strings.Repeat("1", 40), WorkerImageDigest: "sha256:" + strings.Repeat("a", 64)}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SyncPlatformShadowOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if reports != 3 {
		t.Fatal("inactive worker reported")
	}
	s.Config.EdgeSlot = "b"
	onDownload = func() {
		if err := os.WriteFile(activation, []byte("corrupt"), 0600); err != nil {
			t.Error(err)
		}
	}
	if err := s.SyncPlatformShadowOnce(ctx); err == nil {
		t.Fatal("activation changed during download accepted")
	}
	if reports != 3 {
		t.Fatal("displaced worker reported")
	}
	onDownload = nil
	s.RouteBundleSource = RouteBundleSourceConfig{}
	if err := os.WriteFile(cache+".platform-shadow.json", []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncPlatformShadowOnce(ctx); err == nil {
		t.Fatal("corrupt cursor reset")
	}
	checkServing()
}
