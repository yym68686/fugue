package api

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

func createTestStaticIntent(t *testing.T, s *Server, gen, upstream string) model.PlatformArtifact {
	t.Helper()
	intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: gen, Routes: []platformconfig.RouteIntent{{Hostname: "static.example", UpstreamURL: upstream, Enabled: true}}, DNS: []platformconfig.DNSIntent{{Hostname: "text.example", Type: "TXT", Values: []string{"stable"}, TTL: 60, ValueExpirations: map[string]time.Time{"stable": time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}}}}
	a, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: gen, Content: mustPlatformIntentContent(intent)})
	if err != nil {
		t.Fatal(err)
	}
	a, err = s.store.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "static", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestStaticIntentCaptureIgnoresAmbientRoutesAndDNS(t *testing.T) {
	_, s, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	base := createTestStaticIntent(t, s, "base", "http://static:8080")
	input, err := s.loadStaticPlatformIntent(base.ID, base.ContentHash)
	if err != nil {
		t.Fatal(err)
	}
	principal := platformProducerPrincipal()
	first, err := s.capturePlatformIntentWithStatic(context.Background(), principal, input)
	if err != nil {
		t.Fatal(err)
	}
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example", UpstreamURL: "http://wrong:9999"}}
	s.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "text.example", Type: "TXT", Values: []string{"wrong"}, TTL: 1}}
	second, err := s.capturePlatformIntentWithStatic(context.Background(), principal, input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Intent, second.Intent) || !reflect.DeepEqual(first.Policy, second.Policy) {
		t.Fatal("ambient configuration changed pinned input")
	}
	for _, r := range second.Intent.Routes {
		if r.Hostname == "ambient.example" {
			t.Fatal("ambient route imported")
		}
	}
	for _, r := range second.Intent.DNS {
		if r.Hostname == "text.example" && (len(r.Values) != 1 || r.Values[0] != "stable" || len(r.ValueExpirations) != 1) {
			t.Fatal("static value or expiration lost")
		}
	}
	policy := createTestProjectionPolicy(t, s, base, "preview-static", "shadow")
	path := "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + policy.ID
	r := performJSONRequest(t, s, http.MethodGet, path, admin, nil)
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	var projection platformIntentProjectionResponse
	mustDecodeJSON(t, r, &projection)
	if !reflect.DeepEqual(projection.Intent, second.Intent) {
		t.Fatal("HTTP projection differs from pinned capture")
	}
	if r = performJSONRequest(t, s, http.MethodGet, path, tenant, nil); r.Code != 403 {
		t.Fatal("tenant read platform input")
	}
	for _, ref := range []string{"missing", policy.Generation, policy.ID + "&producer_policy_artifact_id=" + policy.ID} {
		r = performJSONRequest(t, s, http.MethodGet, "/v1/admin/platform-config/routes/project?producer_policy_artifact_id="+ref, admin, nil)
		if r.Code == 200 {
			t.Fatal("invalid exact ref fell back to environment", ref)
		}
	}
	if _, err = s.loadStaticPlatformIntent(base.ID, "sha256:wrong"); err == nil {
		t.Fatal("wrong static digest accepted")
	}
	s.bundleRevokedKeyIDs = append(s.bundleRevokedKeyIDs, base.Provenance.KeyID)
	if _, err = s.loadStaticPlatformIntent(base.ID, base.ContentHash); err == nil {
		t.Fatal("revoked static intent accepted")
	}
}

func TestProducerUsesPinnedStaticIntentAndSwitchesVersions(t *testing.T) {
	state := store.New(t.TempDir() + "/state.json")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(state, auth.New(state, ""), nil, ServerConfig{BundleSigningKey: "static-intent-test-key", BundleSigningKeyID: "static-key"})
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example", UpstreamURL: "http://ambient:8080"}}
	first := createTestStaticIntent(t, s, "base-one", "http://first:8080")
	second := createTestStaticIntent(t, s, "base-two", "http://second:8080")
	var previous model.PlatformArtifactRelease
	for i, base := range []model.PlatformArtifact{first, second} {
		p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: base.Generation + "-control", Mode: "shadow", InputSource: "business-static-intent", StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 300}
		raw, _ := json.Marshal(p)
		var content map[string]any
		json.Unmarshal(raw, &content)
		a, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: p.Generation, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		a, err = state.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "policy", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, err = state.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal()); err != nil {
			t.Fatal(err)
		}
		if _, err = s.reconcilePlatformConfiguration(context.Background()); err != nil {
			t.Fatal(err)
		}
		parent, release, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "shadow")
		if err != nil || !found || parent.Metadata[platformproducer.StaticIntentIDMetadata] != base.ID || parent.Metadata[platformproducer.StaticIntentDigestMetadata] != base.ContentHash {
			t.Fatal("pinned source not bound", err)
		}
		if i > 0 && (release.ID == previous.ID || release.FencingToken != previous.FencingToken+1) {
			t.Fatal("new base did not publish")
		}
		previous = release
		intent, err := state.GetPlatformArtifactByIdentity(model.PlatformArtifactKindPlatformIntent, "global", parent.Metadata["intent_generation"])
		if err != nil {
			t.Fatal(err)
		}
		var desired platformconfig.PlatformIntent
		if decodeCompilerArtifactContent(intent, &desired) != nil || len(desired.Routes) != 1 || desired.Routes[0].Hostname != "static.example" {
			t.Fatal("producer used ambient route")
		}
		frozen, err := state.GetPlatformArtifactContent(parent.Metadata["input_snapshot_digest"])
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := platformconfig.DecodeRuntimeSnapshotContent(frozen.Content)
		if err != nil {
			t.Fatal(err)
		}
		binding := snapshot.Facts["configuration_producer"].(map[string]any)
		if binding["static_intent_artifact_id"] != base.ID || binding["static_intent_digest"] != base.ContentHash {
			t.Fatal("replay lost source reference")
		}
		policy, err := state.GetPlatformArtifactByIdentity(model.PlatformArtifactKindPolicySnapshot, "global", parent.Metadata["policy_generation"])
		if err != nil {
			t.Fatal(err)
		}
		var typedPolicy platformconfig.PolicySnapshot
		decodeCompilerArtifactContent(policy, &typedPolicy)
		compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: desired, Policy: typedPolicy, RuntimeSnapshot: snapshot})
		if err != nil {
			t.Fatal(err)
		}
		replay, err := s.materializePlatformCompilation(context.Background(), compiled, platformProducerPrincipal(), &platformConfigStoredInputs{Intent: intent, Policy: policy})
		if err != nil || replay.ReleaseArtifact.ID != parent.ID {
			t.Fatal("static binding not replayable", err)
		}
	}
	if lkg, err := state.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global"); err != nil || lkg != nil {
		t.Fatal("static source changed LKG")
	}
}
