package api

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
	"k8s.io/utils/ptr"
)

func TestLiveDNSConvergenceRejectsHistoricalBackendWithoutChangingFacts(t *testing.T) {
	path := t.TempDir() + "/state.json"
	state := store.New(path)
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(state, auth.New(state, "backend-admin"), nil, ServerConfig{BundleSigningKey: "synthetic-convergence", BundleSigningKeyID: "key"})
	f := newDNSBackendFixture(t)
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: f.claims.NodeID, EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8"}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, s, "example.test", platformconfig.RouteIntent{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true})
	s.clusterNodeInventoryCache = newExpiringResponseCache[[]clusterNodeSnapshot](time.Hour)
	s.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{
		{node: model.ClusterNode{Name: f.claims.NodeID}, labels: map[string]string{runtimepkg.DNSRoleLabelKey: runtimepkg.NodeRoleLabelValue}},
		{node: model.ClusterNode{Name: "delegation-edge-0"}, labels: map[string]string{runtimepkg.EdgeRoleLabelKey: runtimepkg.NodeRoleLabelValue}},
	})
	parent, release, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "gray")
	if err != nil || !found {
		t.Fatal(err)
	}
	sets, err := s.currentReleaseSetExpectations(parent)
	if err != nil {
		t.Fatal(err)
	}
	var set model.PlatformExpectedConsumerSet
	for _, item := range sets {
		if item.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
			set = item
		}
	}
	child, err := state.GetPlatformArtifactByIdentity(set.ArtifactKind, set.ScopeKey, set.ExpectedGeneration)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "test", Keys: map[string]string{"test": "synthetic-identity"}}
	token, err := platformcontrol.IssuePlatformComponentIdentity(ring, f.claims, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(ring, token, now)
	if err != nil {
		t.Fatal(err)
	}
	h := trustedPlatformHeartbeatRequest(t, claims, set, now, 20, child.GenerationSequence, release.FencingToken, "current-pod-receipt-nonce")
	h.LKGGeneration = child.Generation
	h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1}
	h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if _, err = state.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.install(t, s)
	consumers, err := state.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	if err != nil {
		t.Fatal(err)
	}
	binding := s.platformConvergenceBinding(set)
	if got := s.evaluateLiveConsumerConvergence(context.Background(), set, consumers, binding); !got.Pass {
		t.Fatal(got)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	baseline := *f.slices.DeepCopy()
	for _, scenario := range []string{"replaced", "unready", "unavailable", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			f.slices = *baseline.DeepCopy()
			f.failPath = ""
			ctx := context.Background()
			reason, wantState := "dns_public_backend_mismatch", model.InvariantEvidenceStateFail
			switch scenario {
			case "replaced":
				f.slices.Items[0].Endpoints[0].TargetRef.UID = "replacement-pod"
			case "unready":
				f.slices.Items[0].Endpoints[0].Conditions.Ready = ptr.To(false)
			case "unavailable":
				f.failPath = "/endpointslices"
				reason, wantState = "dns_public_backend_unavailable", model.InvariantEvidenceStateUnknown
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				reason, wantState = "dns_public_backend_unavailable", model.InvariantEvidenceStateUnknown
			}
			// Same exact stored receipt is still fresh and valid for the release.
			if got := platformcontrol.EvaluateConsumerConvergence(set, consumers, time.Now(), binding); !got.Pass {
				t.Fatal("fixture no longer has a fresh positive receipt", got)
			}
			got := s.evaluateLiveConsumerConvergence(ctx, set, consumers, binding)
			if got.Pass || got.State != wantState || got.RequiredObserved != 1 || got.RequiredPassing != 0 || len(got.Assessments) != 1 || !strings.Contains(strings.Join(got.Assessments[0].Reasons, ","), reason) {
				t.Fatal(got)
			}
			if !reflect.DeepEqual(got.Assessments[0].Observed, &consumers[0]) {
				t.Fatal("live assessment rewrote original evidence")
			}
			if scenario != "canceled" {
				response := performJSONRequest(t, s, "GET", "/v1/admin/platform-state/convergence?release_set_id="+parent.ID+"&artifact_kind=dns_answer_bundle", "backend-admin", nil)
				if response.Code != 200 || !strings.Contains(response.Body.String(), reason) {
					t.Fatal(response.Code, response.Body.String())
				}
				_, healthy, err := s.publishedRouteDiagnostics(ctx, "edge-group-a")
				if err != nil || healthy["edge-group-a"] {
					t.Fatal("route diagnostics accepted historical backend", healthy, err)
				}
				if _, err := s.inspectPublishedTrafficArtifacts(ctx, "example.test", map[string]string{f.claims.NodeID: "edge-group-a"}); err == nil {
					t.Fatal("traffic diagnostics accepted historical backend")
				}
				full := performJSONRequest(t, s, "POST", "/v1/admin/artifacts/"+parent.ID+"/release", "backend-admin", model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", Reason: "must retain baseline"})
				if full.Code != http.StatusConflict {
					t.Fatal("full promotion accepted historical backend", full.Code, full.Body.String())
				}
				verify := performJSONRequest(t, s, "POST", "/v1/admin/artifact-releases/"+release.ID+"/verify-lkg", "backend-admin", platformArtifactVerifyLKGHTTPRequest{FencingToken: release.FencingToken, Reason: "must retain LKG", AllowInitialLKG: true, Evidence: passingPlatformVerificationEvidenceHTTPRequest()})
				if verify.Code != http.StatusConflict {
					t.Fatal("LKG verification accepted historical backend", verify.Code, verify.Body.String())
				}
			}
			after, _ := os.ReadFile(path)
			assertConvergenceStateUnchanged(t, raw, after)
		})
	}
	f.slices = baseline
	f.failPath = ""
	if got := s.evaluateLiveConsumerConvergence(context.Background(), set, consumers, binding); !got.Pass {
		t.Fatal("recovered observation remained cached as failed", got)
	}
	full := performJSONRequest(t, s, "POST", "/v1/admin/artifacts/"+parent.ID+"/release", "backend-admin", model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", Reason: "current backend passed"})
	if full.Code != http.StatusOK {
		t.Fatal("valid backend could not promote", full.Code, full.Body.String())
	}
	var promoted model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, full, &promoted)
	reportServingProducerAPI(t, s, parent, promoted.Release)
	currentSets, err := s.currentReleaseSetExpectations(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range currentSets {
		if current.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
			continue
		}
		at := time.Now().UTC()
		h := trustedPlatformHeartbeatRequest(t, claims, current, at, 40, child.GenerationSequence, promoted.Release.FencingToken, "current-full-backend-nonce")
		h.LKGGeneration = child.Generation
		h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1}
		h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
		if _, err := state.AcceptTrustedPlatformConsumerHeartbeat(claims, current.ID, h, at, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	verifyPlatformArtifactReleaseAPI(t, s, "backend-admin", promoted.Release, false)
	// A matching backend cannot renew an old heartbeat while metadata is read.
	set.Consumers[0].HeartbeatFreshnessSeconds = 1
	consumers[0].LastHeartbeatAt = time.Now().Add(-800 * time.Millisecond)
	f.podListDelay = 400 * time.Millisecond
	if got := s.evaluateLiveConsumerConvergence(context.Background(), set, consumers, binding); got.Pass || got.State != model.InvariantEvidenceStateStale {
		t.Fatal("lookup extended heartbeat freshness", got)
	}
}

func assertConvergenceStateUnchanged(t *testing.T, before, after []byte) {
	t.Helper()
	var left, right map[string]any
	if err := json.Unmarshal(before, &left); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(after, &right); err != nil {
		t.Fatal(err)
	}
	// Topology lookup may initialize unrelated machine/backup defaults in the
	// file-store fixture. All configuration, publication, consumer and audit
	// records must remain byte-equivalent after decoding.
	for key, value := range left {
		if strings.HasPrefix(key, "platform_") || strings.Contains(key, "consumer") || strings.Contains(key, "audit") || key == "dns_nodes" {
			if !reflect.DeepEqual(value, right[key]) {
				t.Fatal("convergence changed persistent field", key)
			}
		}
	}
	for key := range right {
		if _, found := left[key]; !found && (strings.HasPrefix(key, "platform_") || strings.Contains(key, "consumer") || strings.Contains(key, "audit")) {
			t.Fatal("convergence created persistent field", key)
		}
	}
}

// Used in the actual producer state-machine regression, at gray->full and at
// full->verified. Only a disposable fixture's credential is changed; original
// artifact lineage, fresh apply/probe evidence and producer policy remain valid.
func assertProducerRejectsRetiredDNSBackend(t *testing.T, s *Server, path string, policyAuthority model.PlatformArtifactRelease) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var before model.State
	if err = json.Unmarshal(raw, &before); err != nil {
		t.Fatal(err)
	}
	f := newDNSBackendFixture(t)
	f.claims.NodeID = "dns-a"
	f.pod.Spec.NodeName = "dns-a"
	f.node.Name = "dns-a"
	f.slices.Items[0].Endpoints = nil
	factory, namespace := s.newClusterNodeClient, s.controlPlaneNamespace
	f.install(t, s)
	defer func() {
		s.newClusterNodeClient = factory
		s.controlPlaneNamespace = namespace
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}()
	for i := range before.PlatformConsumerInstances {
		c := &before.PlatformConsumerInstances[i]
		if c.Component == model.PlatformConsumerComponentDNSServer {
			c.CredentialID = f.claims.CredentialID
		}
	}
	edited, _ := json.Marshal(before)
	if err = os.WriteFile(path, edited, 0600); err != nil {
		t.Fatal(err)
	}
	policyArtifact, err := s.store.GetPlatformArtifact(policyAuthority.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := platformproducer.Decode(policyArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := s.reconcilePendingProducedTraffic(context.Background(), policy, policyAuthority); err != nil || !handled {
		t.Fatal(handled, err)
	}
	afterRaw, _ := os.ReadFile(path)
	var after model.State
	if err = json.Unmarshal(afterRaw, &after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.PlatformArtifactReleases, after.PlatformArtifactReleases) || !reflect.DeepEqual(before.PlatformLKGSnapshots, after.PlatformLKGSnapshots) {
		t.Fatal("producer advanced with retired DNS backend")
	}
}
