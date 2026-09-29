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
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/edgegroupfront"
	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func tlsShadowFixtures(t *testing.T, roles ...string) (edgePlatformCandidate, edgePlatformCandidate) {
	t.Helper()
	now := time.Now().UTC()
	old := now.Add(-24 * time.Hour)
	request := platformconfig.CompileRequest{
		Intent:          platformconfig.PlatformIntent{Generation: "intent", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: "app", TenantID: "tenant", UpstreamURL: "http://origin:8080", TLSPolicy: "custom-domain", Enabled: true}}, TLS: []platformconfig.TLSIntent{{Hostname: "app.example.test", Policy: "custom-domain", DomainRef: "domain", AppID: "app", TenantID: "tenant"}}},
		Policy:          platformconfig.PolicySnapshot{Generation: "policy", Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 86400},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, TLSDomains: []platformconfig.TLSDomainObservation{{Ref: "domain", Hostname: "app.example.test", AppID: "app", TenantID: "tenant", Status: "verified", TLSStatus: "ready", VerifiedAt: &old, TLSReadyAt: &old}}}, CreatedAt: now,
	}
	scope := "global"
	if len(roles) > 0 && roles[0] == platformconfig.PublicationRoleCellRoutes {
		scope = platformconfig.AuthorityCellScope("cell-a")
		request.Intent.PublicationRole, request.Policy.PublicationRole = roles[0], roles[0]
		request.Intent.Scope, request.Policy.Scope = scope, scope
		request.Intent.AuthorityCellID, request.Policy.AuthorityCellID = "cell-a", "cell-a"
		request.Intent.EdgeTopology = &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: "cell-a"}}, Pools: []edgetopology.ServingPool{{ID: "pool-a"}}, Edges: []edgetopology.Edge{{ID: "node-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-a"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-a"}}}}
		topology, err := platformconfig.TrafficConsumerTopologyFromIntent(request.Intent)
		if err != nil {
			t.Fatal(err)
		}
		request.Policy.ConsumerTopologyDigest, _ = platformconfig.Digest(topology)
		request.Policy.TLSReadiness = &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxProbes: 100, MaxConcurrency: 4}
		request.Policy.TrafficRolloutCohorts = []platformconfig.TrafficRolloutCohort{{ID: "initial", EdgeGroupIDs: []string{"cell-a"}}}
	}
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	build := func(a model.PlatformArtifact) edgePlatformCandidate {
		a.ID, a.ScopeKey, a.Status, a.GenerationSequence = a.ArtifactKind, scope, model.PlatformArtifactStatusValidated, 1
		c := edgePlatformCandidate{Artifact: a, Assignment: model.PlatformConsumerAssignment{Revision: 1, ExpectedConsumerSetID: a.ArtifactKind + "-set", ArtifactReleaseID: "release", ReleaseSetID: "release-set", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: scope, ExpectedGeneration: a.Generation, GenerationSequence: 1, FencingToken: 1, ReleaseChannel: "shadow"}, Release: model.PlatformArtifactRelease{ID: "release", ArtifactID: "release-set", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: scope, Generation: a.Metadata["release_set_generation"], ReleaseChannel: "shadow", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1}}
		resignTLSFixture(t, &c)
		raw, _ := json.Marshal(c)
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	tls, route := build(compiled.TLSArtifact), build(compiled.RouteArtifact)
	if request.Intent.PublicationRole != "" {
		parent := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{route.Artifact.ID, tls.Artifact.ID}, now)
		parent.ID, parent.ScopeKey, parent.Status, parent.GenerationSequence = "release-set", scope, model.PlatformArtifactStatusValidated, 1
		parent.ContentHash, _ = platformconfig.Digest(parent.Content)
		parent, err = platformsafety.SignPlatformArtifact(parent, bundleauth.NewKeyring("synthetic-tls-key", "signer", "", "", nil))
		if err != nil {
			t.Fatal(err)
		}
		tls.ReleaseSet, route.ReleaseSet = &parent, &parent
	}
	return tls, route
}

func resignTLSFixture(t *testing.T, c *edgePlatformCandidate) {
	t.Helper()
	var err error
	c.Artifact.ContentHash, err = platformconfig.Digest(c.Artifact.Content)
	if err != nil {
		t.Fatal(err)
	}
	c.Artifact, err = platformsafety.SignPlatformArtifact(c.Artifact, bundleauth.NewKeyring("synthetic-tls-key", "signer", "", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	c.Assignment.ContentHash = c.Artifact.ContentHash
}

func TestTLSShadowRejectsMixedOrInvalidArtifacts(t *testing.T) {
	s := NewService(config.EdgeConfig{BundleSigningKey: "synthetic-tls-key", BundleSigningKeyID: "signer"}, log.New(io.Discard, "", 0))
	a, r := tlsShadowFixtures(t)
	if _, err := s.verifyPlatformTLSCandidate(a, r); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*edgePlatformCandidate, *edgePlatformCandidate)
		sign   bool
	}{
		{"TLS signature", func(a, r *edgePlatformCandidate) { a.Artifact.Provenance.Signature = "invalid" }, false},
		{"route signature", func(a, r *edgePlatformCandidate) { r.Artifact.Provenance.Signature = "invalid" }, false},
		{"mixed release", func(a, r *edgePlatformCandidate) { r.Assignment.ArtifactReleaseID = "other"; r.Release.ID = "other" }, false},
		{"mixed fence", func(a, r *edgePlatformCandidate) { r.Assignment.FencingToken++; r.Release.FencingToken++ }, false},
		{"scope", func(a, r *edgePlatformCandidate) { a.Artifact.ScopeKey = "other" }, false},
		{"lineage", func(a, r *edgePlatformCandidate) {
			a.Artifact.Content["lineage"].(map[string]any)["compiler_version"] = "other"
		}, true},
		{"unknown schema field", func(a, r *edgePlatformCandidate) { a.Artifact.Content["unknown"] = true }, true},
		{"wrong policy", func(a, r *edgePlatformCandidate) {
			a.Artifact.Content["policy"].(map[string]any)["minimum_healthy_edges"] = 3
		}, true},
		{"foreign tenant", func(a, r *edgePlatformCandidate) {
			a.Artifact.Content["certificates"].([]any)[0].(map[string]any)["tenant_id"] = "foreign"
		}, true},
		{"missing reference", func(a, r *edgePlatformCandidate) { a.Artifact.Content["certificates"] = []any{} }, true},
		{"wrong hostname", func(a, r *edgePlatformCandidate) {
			a.Artifact.Content["certificates"].([]any)[0].(map[string]any)["hostname"] = "other.example.test"
		}, true},
		{"wrong TLS policy", func(a, r *edgePlatformCandidate) {
			a.Artifact.Content["certificates"].([]any)[0].(map[string]any)["policy"] = "platform"
		}, true},
		{"future domain event", func(a, r *edgePlatformCandidate) {
			a.Artifact.Content["domain_states"].([]any)[0].(map[string]any)["tls_ready_at"] = time.Now().Add(time.Hour)
		}, true},
		{"missing domain fact", func(a, r *edgePlatformCandidate) { a.Artifact.Content["domain_states"] = []any{} }, true},
		{"TLS allowlist mismatch", func(a, r *edgePlatformCandidate) { a.Artifact.Content["tls_allowlist"] = []any{} }, true},
		{"route allowlist mismatch", func(a, r *edgePlatformCandidate) { r.Artifact.Content["tls_allowlist"] = []any{} }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, r := tlsShadowFixtures(t)
			tc.mutate(&a, &r)
			if tc.sign {
				resignTLSFixture(t, &a)
				resignTLSFixture(t, &r)
			}
			if _, err := s.verifyPlatformTLSCandidate(a, r); err == nil {
				t.Fatal("invalid pair accepted")
			}
		})
	}
}

func TestTLSShadowAllowsSharedHostnamePathPolicies(t *testing.T) {
	s := NewService(config.EdgeConfig{BundleSigningKey: "synthetic-tls-key", BundleSigningKeyID: "signer"}, log.New(io.Discard, "", 0))
	a, r := tlsShadowFixtures(t)
	routes := r.Artifact.Content["routes"].([]any)
	raw, _ := json.Marshal(routes[0])
	var path map[string]any
	json.Unmarshal(raw, &path)
	path["path_prefix"], path["tls_policy"] = "/api", "platform"
	r.Artifact.Content["routes"] = append(routes, path)
	resignTLSFixture(t, &r)
	if _, err := s.verifyPlatformTLSCandidate(a, r); err != nil {
		t.Fatal("SNI hostname reference must coexist with shared path policies", err)
	}
}

func TestTLSShadowConsumerPreservesServingAndDurableCursor(t *testing.T) {
	for _, role := range []string{"", platformconfig.PublicationRoleCellRoutes} {
		t.Run("role="+role, func(t *testing.T) { testTLSShadowConsumer(t, role) })
	}
}
func testTLSShadowConsumer(t *testing.T, role string) {
	tls, route := tlsShadowFixtures(t, role)
	scope, group := route.Assignment.ScopeKey, "edge-group-test"
	if role != "" {
		group = "cell-a"
	}
	lastSequence := int64(0)
	reports := 0
	loseReceipt := false
	var onDownload func()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/platform-state/consumers/identity" {
			if r.Header.Get("Authorization") != "Bearer pod-token" {
				t.Error("missing Pod credential")
			}
			identity := map[string]any{"token": "component-token", "expires_at": time.Now().Add(time.Minute), "component": "edge-worker", "node_id": "node-a", "scope_key": scope, "artifact_kinds": []string{tls.Artifact.ArtifactKind, route.Artifact.ArtifactKind}}
			if role != "" {
				identity["authority_id"], identity["consumer_id"] = group, "edge-worker:"+group+":node-a"
			}
			json.NewEncoder(w).Encode(identity)
			return
		}
		if r.Header.Get("Authorization") != "Bearer component-token" {
			t.Error("missing component credential")
		}
		switch r.URL.Path {
		case "/v1/platform-state/consumers/assignment":
			json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{tls.Assignment, route.Assignment}})
		case "/v1/platform-state/consumers/artifacts/" + model.PlatformArtifactKindCaddyRouteConfig:
			if onDownload != nil {
				onDownload()
			}
			json.NewEncoder(w).Encode(tls)
		case "/v1/platform-state/consumers/artifacts/" + model.PlatformArtifactKindEdgeRouteBundle:
			json.NewEncoder(w).Encode(route)
		case "/v1/platform-state/consumers/artifacts/release-set":
			json.NewEncoder(w).Encode(map[string]any{"artifact": route.ReleaseSet, "assignment": route.Assignment, "release": route.Release})
		case "/v1/platform-state/consumers/trusted-heartbeat":
			var h platformcontrol.PlatformConsumerHeartbeatEnvelope
			if json.NewDecoder(r.Body).Decode(&h) != nil {
				t.Fatal("invalid heartbeat")
			}
			hash, _ := platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if h.Sequence <= lastSequence || hash != h.EvidenceHash || h.ArtifactKind != tls.Artifact.ArtifactKind || h.ExpectedConsumerSetID != tls.Assignment.ExpectedConsumerSetID || h.CandidateGeneration != tls.Artifact.Generation || h.ApplyStatus != "staged" || h.ProbeStatus != "shadow_validated" || h.ActualGeneration != "" || h.LKGGeneration != "" || h.ServingLKG || (!slices.Contains(h.CompatibilityCapabilities, platformcontrol.TrafficReleaseCapabilityV1) || !slices.Contains(h.CompatibilityCapabilities, platformcontrol.CellRoutesCapabilityV1)) {
				t.Errorf("false TLS shadow evidence: %+v", h)
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
	cache := filepath.Join(dir, "cache.json")
	for p, b := range map[string][]byte{tokenFile: []byte("pod-token"), cache: []byte("serving-cache"), cache + ".platform-shadow.json": []byte("independent-route-cursor")} {
		if err := os.WriteFile(p, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	create := func() *Service {
		s := NewService(config.EdgeConfig{APIURL: server.URL, EdgeID: "node-a", EdgeGroupID: group, PlatformScopeKey: scope, CachePath: cache, BundleSigningKey: "synthetic-tls-key", BundleSigningKeyID: "signer"}, log.New(io.Discard, "", 0))
		s.PlatformTokenFile = tokenFile
		if role == "" {
			s.recordSyncSuccess(testBundle("legacy-serving"), "etag", time.Now(), false)
		}
		return s
	}
	s := create()
	index := s.currentRouteIndex()
	ctx := context.Background()
	check := func() {
		t.Helper()
		b, _ := os.ReadFile(cache)
		routeCursor, _ := os.ReadFile(cache + ".platform-shadow.json")
		bundle, _ := s.Bundle()
		if string(b) != "serving-cache" || string(routeCursor) != "independent-route-cursor" || (role == "" && bundle.Version != "legacy-serving" || role != "" && bundle.Version != "") || index != s.currentRouteIndex() {
			t.Fatal("TLS shadow altered serving or route cursor")
		}
	}
	if err := s.SyncPlatformTLSShadowOnce(ctx); err != nil {
		t.Fatal(err)
	}
	status := s.Status().PlatformTLSCandidate
	if status.State != "shadow_verified" || status.TLSVerified || status.Serving || status.CertificateCount != 1 || status.AllowlistCount != 1 || status.RouteDigest != route.Artifact.ContentHash {
		t.Fatal(status)
	}
	path := cache + ".platform-tls-shadow.json"
	staged, _ := os.ReadFile(path)
	if role != "" {
		var saved edgePlatformCandidate
		if json.Unmarshal(staged, &saved) != nil || saved.ReleaseSet == nil || saved.TrafficRelease == nil || saved.TLSReadiness != nil || saved.TrafficRelease.ReleaseChannel != "shadow" {
			t.Fatal("capability bootstrap fabricated readiness or omitted signed parent")
		}
		parent := *route.ReleaseSet
		for _, scenario := range []string{"untrusted parent", "foreign TLS member", "serving without bundle"} {
			original, _ := json.Marshal(parent)
			var changed model.PlatformArtifact
			json.Unmarshal(original, &changed)
			switch scenario {
			case "untrusted parent":
				changed.Provenance.Signature = "invalid"
			case "foreign TLS member":
				changed.Content["artifact_ids"].([]any)[1] = "another-tls-artifact"
				changed.ContentHash, _ = platformconfig.Digest(changed.Content)
				changed, _ = platformsafety.SignPlatformArtifact(changed, bundleauth.NewKeyring("synthetic-tls-key", "signer", "", "", nil))
			case "serving without bundle":
				tls.Assignment.ReleaseChannel, tls.Release.ReleaseChannel = "full", "full"
				route.Assignment.ReleaseChannel, route.Release.ReleaseChannel = "full", "full"
			}
			route.ReleaseSet = &changed
			if err := s.SyncPlatformTLSShadowOnce(ctx); err == nil {
				t.Fatal("invalid bootstrap accepted", scenario)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(staged, after) || reports != 1 {
				t.Fatal("failed validation advanced evidence", scenario)
			}
			tls.Assignment.ReleaseChannel, tls.Release.ReleaseChannel = "shadow", "shadow"
			route.Assignment.ReleaseChannel, route.Release.ReleaseChannel = "shadow", "shadow"
			route.ReleaseSet = &parent
		}
	}
	originalSignature := tls.Artifact.Provenance.Signature
	tls.Artifact.Provenance.Signature = "invalid"
	if err := s.SyncPlatformTLSShadowOnce(ctx); err == nil {
		t.Fatal("invalid signature accepted")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(staged, after) || reports != 1 {
		t.Fatal("invalid candidate persisted or reported")
	}
	check()
	tls.Artifact.Provenance.Signature = originalSignature
	loseReceipt = true
	if err := s.SyncPlatformTLSShadowOnce(ctx); err == nil {
		t.Fatal("missing receipt ignored")
	}
	prior := lastSequence
	loseReceipt = false
	s = create()
	index = s.currentRouteIndex()
	if err := s.SyncPlatformTLSShadowOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if lastSequence <= prior || reports != 3 {
		t.Fatal("restart replayed cursor")
	}
	check()
	var cursor edgePlatformCandidate
	raw, _ := os.ReadFile(path)
	json.Unmarshal(raw, &cursor)
	cursor.Assignment.FencingToken++
	raw, _ = json.Marshal(cursor)
	os.WriteFile(path, raw, 0600)
	if err := s.SyncPlatformTLSShadowOnce(ctx); err == nil {
		t.Fatal("fencing rollback accepted")
	}
	os.WriteFile(path, staged, 0600)
	activation := filepath.Join(dir, "activation.json")
	s.Config.EdgeSlot = "a"
	s.RouteBundleSource = RouteBundleSourceConfig{URL: "http://authority.example.test/v1/edge/routes", CandidateURL: "http://authority.example.test/v1/edge/candidate-routes", TokenFile: tokenFile, VerifierKeyringFile: filepath.Join(dir, "verifier.json"), ActivationStateFile: activation}
	if err := s.SyncPlatformTLSShadowOnce(ctx); err == nil {
		t.Fatal("missing activation accepted")
	}
	_, err := edgegroupfront.ApplyActivationCAS(activation, edgegroupfront.ActivationCASRequest{Operation: edgegroupfront.ActivationOperationInit, GroupID: group, ExpectedSlot: "b", TargetSlot: "b", Reason: "TLS shadow test", BundleGeneration: "legacy-serving", WorkerSourceCommit: strings.Repeat("1", 40), WorkerImageDigest: "sha256:" + strings.Repeat("a", 64)}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SyncPlatformTLSShadowOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if reports != 3 {
		t.Fatal("inactive worker reported")
	}
	s.Config.EdgeSlot = "b"
	onDownload = func() { os.WriteFile(activation, []byte("corrupt"), 0600) }
	if err := s.SyncPlatformTLSShadowOnce(ctx); err == nil {
		t.Fatal("changed activation accepted")
	}
	if reports != 3 {
		t.Fatal("displaced worker reported")
	}
	onDownload = nil
	s.RouteBundleSource = RouteBundleSourceConfig{}
	os.WriteFile(path, []byte("corrupt"), 0600)
	if err := s.SyncPlatformTLSShadowOnce(ctx); err == nil {
		t.Fatal("corrupt cursor reset")
	}
	check()
}
