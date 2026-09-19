package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestCompilerInputIsFrozenReplayableAndAdminOnly(t *testing.T) {
	state, server, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	now := time.Now().UTC()
	request := platformConfigCompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global"}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, Facts: map[string]any{"source_revision": "original"}}}
	r := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, request)
	if r.Code != 201 {
		t.Fatal(r.Body.String())
	}
	var first platformConfigCompileResponse
	mustDecodeJSON(t, r, &first)
	path := "/v1/admin/artifacts/" + first.ReleaseArtifact.ID + "/compiler-input"
	if r := performJSONRequest(t, server, http.MethodGet, path, tenant, nil); r.Code != 403 {
		t.Fatal("tenant read compiler input", r.Code)
	}
	r = performJSONRequest(t, server, http.MethodGet, path, admin, nil)
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	var input struct {
		Digest   string                         `json:"input_snapshot_digest"`
		Snapshot platformconfig.RuntimeSnapshot `json:"runtime_snapshot"`
	}
	mustDecodeJSON(t, r, &input)
	if input.Digest != first.Lineage.InputSnapshotDigest || input.Snapshot.CapturedAt == nil || !input.Snapshot.CapturedAt.Equal(now) || input.Snapshot.Facts["source_revision"] != "original" {
		t.Fatal("frozen input differs")
	}
	r = performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile-from-artifacts", admin, platformConfigCompileArtifactsRequest{IntentArtifactID: first.IntentArtifact.ID, PolicyArtifactID: first.PolicyArtifact.ID, RuntimeSnapshot: input.Snapshot})
	if r.Code != 201 {
		t.Fatal("retained input cannot be replayed", r.Body.String())
	}
	var replay platformConfigCompileResponse
	mustDecodeJSON(t, r, &replay)
	if replay.ReleaseArtifact.ID != first.ReleaseArtifact.ID || replay.RouteArtifact.ContentHash != first.RouteArtifact.ContentHash {
		t.Fatal("retained input produced different output")
	}
	// A malformed normalized snapshot must not be persisted under another digest.
	bad := input.Snapshot
	bad.PolicyGeneration = "different"
	if err := state.EnsurePlatformCompilerInput(bad, input.Digest); err == nil {
		t.Fatal("mismatched input digest stored")
	}
	if _, err := server.reconcilePlatformConfigurationWithCapture(context.Background(), nil); err != nil {
		t.Fatal("missing producer policy should remain inert", err)
	}
	for _, kind := range []string{model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindPolicySnapshot} {
		if lkg, err := state.GetPlatformLKG(kind, "global"); err != nil || lkg != nil {
			t.Fatal("input retention activated LKG")
		}
	}
}
