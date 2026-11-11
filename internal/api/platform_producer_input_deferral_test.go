package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func TestSignedInputDeferralHTTPValidationAndPublication(t *testing.T) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	r := activateTestProducer(t, server, "producer-alpha", "shadow")
	a, _, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Second)
	request := platformproducer.CaptureDeferralRequest{SchemaVersion: platformproducer.CaptureDeferralSchema, OperationID: "http-alpha", RequestedAt: at, ExpiresAt: at.Add(20 * time.Minute), Authorities: []platformproducer.CaptureDeferralAuthority{{Scope: a.ScopeKey, Policy: model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}}}}
	digest, _ := request.Digest()
	p := platformproducer.InputDeferral{SchemaVersion: platformproducer.InputDeferralSchema, Generation: request.OperationID, Action: "defer", Request: request, RequestDigest: digest}
	for _, scenario := range []string{"valid", "string", "fraction", "overflow", "extra token", "foreign scope", "foreign kind"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(p)
			var content map[string]any
			if err := json.Unmarshal(raw, &content); err != nil {
				t.Fatal(err)
			}
			binding := content["request"].(map[string]any)["authorities"].([]any)[0].(map[string]any)["policy"].(map[string]any)
			scope, kind := platformproducer.InputDeferralScope, model.PlatformArtifactKindPolicySnapshot
			switch scenario {
			case "string":
				binding["fencing_token"] = "secret-value"
			case "fraction":
				binding["fencing_token"] = 1.5
			case "overflow":
				binding["fencing_token"] = float64(9007199254740992)
			case "extra token":
				binding["access_token"] = "private"
			case "foreign scope":
				scope = "global"
			case "foreign kind":
				kind = model.PlatformArtifactKindPlatformIntent
			}
			created := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", admin, model.PlatformArtifactCreateRequest{ArtifactKind: kind, Scope: model.PlatformArtifactScope{Key: scope}, Generation: p.Generation, Content: content})
			if created.Code != http.StatusCreated {
				t.Fatal("draft create", created.Code, created.Body.String())
			}
			var draft model.PlatformArtifactResponse
			mustDecodeJSON(t, created, &draft)
			validated := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+draft.Artifact.ID+"/validate", admin, model.PlatformArtifactValidateRequest{DryRun: false})
			var result model.PlatformArtifactValidationResponse
			if validated.Code == http.StatusOK {
				mustDecodeJSON(t, validated, &result)
			}
			if result.Pass != (scenario == "valid") {
				t.Fatal("validation", validated.Code, validated.Body.String())
			}
			stored, err := state.GetPlatformArtifact(draft.Artifact.ID)
			before, _ := json.Marshal(draft.Artifact.Content)
			after, _ := json.Marshal(stored.Content)
			if err != nil || string(before) != string(after) {
				t.Fatal("validation mutated signed content", err)
			}
			if scenario != "valid" {
				return
			}
			_, adminOnly, err := state.CreateAPIKey("", "synthetic-admin", []string{"platform.admin"})
			if err != nil {
				t.Fatal(err)
			}
			_, explicit, err := state.CreateAPIKey("", "synthetic-control", []string{"platform.admin", "producer_capture.control"})
			if err != nil {
				t.Fatal(err)
			}
			req := model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: p.IdempotencyKey()}
			denied := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+draft.Artifact.ID+"/release", adminOnly, req)
			if denied.Code < 400 || strings.Contains(denied.Body.String(), `"release":`) {
				t.Fatal("admin-only caller selected restriction", denied.Code)
			}
			selected := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+draft.Artifact.ID+"/release", explicit, req)
			if selected.Code != http.StatusOK {
				t.Fatal("explicit signed selection", selected.Code, selected.Body.String())
			}
			b := p.Request.Authorities[0]
			if deferred, err := state.ProducerInputDeferred(t.Context(), b.Scope, b.Policy); err != nil || !deferred {
				t.Fatal("HTTP selection not effective", deferred, err)
			}
		})
	}
}

func TestSignedInputDeferralDoesNotCancelAlreadyAdmittedWork(t *testing.T) {
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	r := activateTestProducer(t, server, "producer-alpha", "shadow")
	a, _, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Second)
	request := platformproducer.CaptureDeferralRequest{SchemaVersion: platformproducer.CaptureDeferralSchema, OperationID: "finite-alpha", RequestedAt: at, ExpiresAt: at.Add(20 * time.Minute), Authorities: []platformproducer.CaptureDeferralAuthority{{Scope: a.ScopeKey, Policy: model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}}}}
	digest, _ := request.Digest()
	p := platformproducer.InputDeferral{SchemaVersion: platformproducer.InputDeferralSchema, Generation: request.OperationID, Action: "defer", Request: request, RequestDigest: digest}
	raw, _ := json.Marshal(p)
	var content map[string]any
	if err := json.Unmarshal(raw, &content); err != nil {
		t.Fatal(err)
	}
	control, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{Key: platformproducer.InputDeferralScope}, Generation: p.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePlatformPolicyArtifact(control); err != nil {
		t.Fatal(err)
	}
	control, err = state.ValidatePlatformArtifact(control.ID, []model.PlatformArtifactValidationResult{{Name: "signed finite control", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	returned := errors.New("synthetic capture returned without publication")
	called := 0
	_, err = server.reconcilePlatformConfigurationWithCapture(t.Context(), func(ctx context.Context, _ model.Principal) (platformIntentProjectionResponse, error) {
		called++
		if _, _, _, _, err := state.ReleasePlatformArtifact(control.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: p.IdempotencyKey()}, platformProducerPrincipal()); err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal("selecting finite restriction canceled earlier work", ctx.Err())
		}
		return platformIntentProjectionResponse{}, returned
	})
	if !errors.Is(err, returned) || called != 1 {
		t.Fatal("prior capture did not really return", called, err)
	}
	server = NewServer(state, auth.New(state, ""), nil, ServerConfig{BundleSigningKey: server.bundleSigningKey, BundleSigningKeyID: server.bundleSigningKeyID})
	if _, err := server.reconcilePlatformConfigurationWithCapture(t.Context(), func(context.Context, model.Principal) (platformIntentProjectionResponse, error) {
		t.Fatal("restarted process bypassed persisted restriction")
		return platformIntentProjectionResponse{}, returned
	}); err != nil {
		t.Fatal(err)
	}
}
