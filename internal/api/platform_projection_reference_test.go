package api

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func TestProjectionRequiresExactPolicyAndPreservesConfiguration(t *testing.T) {
	state, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example.test", UpstreamURL: "http://unapproved:8080"}}
	s.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "ambient.example.test", Type: "A", Values: []string{"8.8.8.8"}, TTL: 60}}
	base := createTestStaticIntent(t, s, "preview-base", "http://static:8080")
	policy := createTestProjectionPolicy(t, s, base, "preview-pinned", "shadow")
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"", 400},
		{"?producer_policy_artifact_id=", 400},
		{"?producer_policy_artifact_id=" + policy.ID + "&producer_policy_artifact_id=" + policy.ID, 400},
		{"?static_intent_artifact_id=" + base.ID, 410},
		{"?static_intent_artifact_id=" + base.ID + "&producer_policy_artifact_id=" + policy.ID, 410},
		{"?producer_policy_artifact_id=missing", 503},
		{"?producer_policy_artifact_id=" + policy.Generation, 503},
		{"?producer_policy_artifact_id=" + base.ID, 503},
	} {
		r := performJSONRequest(t, s, http.MethodGet, "/v1/admin/platform-config/routes/project"+tc.query, admin, nil)
		if r.Code != tc.status || strings.Contains(r.Body.String(), "unapproved:8080") {
			t.Fatal(tc.query, r.Code, r.Body.String())
		}
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rejected preview wrote configuration", err)
	}
	for _, channel := range []string{"shadow", "gray", "full"} {
		_, _, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", channel)
		if err != nil || found {
			t.Fatal("preview published serving", channel, err)
		}
	}
	// A retired selector must fail even when called internally with ambient
	// inputs available; an unsupported source cannot choose a legacy capture.
	if _, err := s.capturePlatformIntentForProducer(context.Background(), platformProducerPrincipal(), platformproducer.Policy{InputSource: "business-migration"}); err == nil {
		t.Fatal("retired producer read environment")
	}
}

func TestRetiredProducerPolicyCannotActivateButRemainsReadable(t *testing.T) {
	state, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	for _, mode := range []string{"shadow", "paused", "serving"} {
		a, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: "retired-" + mode, Content: map[string]any{"schema_version": platformproducer.Schema, "generation": "retired-" + mode, "mode": mode, "input_source": "business-migration", "target_scope": "global", "interval_seconds": 30, "refresh_seconds": 300}})
		if err != nil {
			t.Fatal(err)
		}
		r := performJSONRequest(t, s, http.MethodPost, "/v1/admin/artifacts/"+a.ID+"/validate", admin, model.PlatformArtifactValidateRequest{})
		if r.Code != 409 {
			t.Fatal("retired source validated", r.Code, r.Body.String())
		}
		// A pre-existing signed, validated historical artifact also cannot
		// bypass typed admission at publication. Its immutable content remains.
		a, err = state.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "historical validation", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, _, err = state.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal())
		if err == nil {
			t.Fatal("historical retired policy activated")
		}
		r = performJSONRequest(t, s, http.MethodGet, "/v1/admin/artifacts/"+a.ID, admin, nil)
		if r.Code != 200 || !strings.Contains(r.Body.String(), "business-migration") {
			t.Fatal("history no longer readable", r.Code)
		}
		if _, _, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow"); err != nil || found {
			t.Fatal("rejected policy advanced authority", err)
		}
	}
}
