package api

import (
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestStaticEdgeRegistrationLifecycleIsTenantScoped(t *testing.T) {
	t.Parallel()
	stateStore, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "example.com")
	digest := "sha256:" + strings.Repeat("a", 64)
	certificate := strings.Repeat("b", 64)

	created := performJSONRequest(t, server, http.MethodPost, "/v1/static-edges", platformAdminKey, map[string]any{
		"tenant_id":               app.TenantID,
		"project_id":              app.ProjectID,
		"name":                    "west-static",
		"edge_id":                 "zerozero-uswest",
		"transport":               "mtls",
		"manager_url":             "https://192.0.2.10:9444",
		"certificate_fingerprint": certificate,
		"signing_key_id":          "edge-signing-v1",
		"possession_proof_digest": digest,
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create static edge: status=%d body=%s", created.Code, created.Body.String())
	}
	var createdResponse model.StaticEdgeRegistrationResponse
	mustDecodeJSON(t, created, &createdResponse)
	registration := createdResponse.Registration
	if registration.Status != model.StaticEdgeStatusPending || registration.ProjectID != app.ProjectID || registration.PossessionProofDigest != digest {
		t.Fatalf("unexpected registration: %+v", registration)
	}

	listed := performJSONRequest(t, server, http.MethodGet, "/v1/static-edges?project_id="+app.ProjectID, platformAdminKey, nil)
	if listed.Code != http.StatusOK {
		t.Fatalf("list static edges: status=%d body=%s", listed.Code, listed.Body.String())
	}
	var listResponse model.StaticEdgeRegistrationListResponse
	mustDecodeJSON(t, listed, &listResponse)
	if len(listResponse.Registrations) != 1 || listResponse.Registrations[0].ID != registration.ID {
		t.Fatalf("unexpected list response: %+v", listResponse)
	}

	proof := performJSONRequest(t, server, http.MethodPost, "/v1/static-edges/"+registration.ID+"/proof", platformAdminKey, map[string]any{
		"possession_proof_digest": "sha256:" + strings.Repeat("c", 64),
		"signing_key_id":          "edge-signing-v2",
		"ready":                   true,
	})
	if proof.Code != http.StatusOK {
		t.Fatalf("update static edge proof: status=%d body=%s", proof.Code, proof.Body.String())
	}
	mustDecodeJSON(t, proof, &createdResponse)
	if createdResponse.Registration.Status != model.StaticEdgeStatusReady || createdResponse.Registration.LastProofAt == nil {
		t.Fatalf("expected ready proof: %+v", createdResponse.Registration)
	}

	revoked := performJSONRequest(t, server, http.MethodDelete, "/v1/static-edges/"+registration.ID, platformAdminKey, nil)
	if revoked.Code != http.StatusOK {
		t.Fatalf("revoke static edge: status=%d body=%s", revoked.Code, revoked.Body.String())
	}
	mustDecodeJSON(t, revoked, &createdResponse)
	if createdResponse.Registration.Status != model.StaticEdgeStatusRevoked {
		t.Fatalf("expected revoked registration: %+v", createdResponse.Registration)
	}
	proofAfterRevoke := performJSONRequest(t, server, http.MethodPost, "/v1/static-edges/"+registration.ID+"/proof", platformAdminKey, map[string]any{
		"possession_proof_digest": "sha256:" + strings.Repeat("d", 64),
		"signing_key_id":          "edge-signing-v3",
		"ready":                   true,
	})
	if proofAfterRevoke.Code != http.StatusConflict {
		t.Fatalf("expected revoked registration proof update to be rejected, got %d body=%s", proofAfterRevoke.Code, proofAfterRevoke.Body.String())
	}

	persisted, err := stateStore.GetStaticEdgeRegistration(registration.ID, app.TenantID, false)
	if err != nil || persisted.Status != model.StaticEdgeStatusRevoked {
		t.Fatalf("expected revoked state to persist, registration=%+v err=%v", persisted, err)
	}
}

func TestStaticEdgeRegistrationRequiresDedicatedScope(t *testing.T) {
	t.Parallel()
	_, server, tenantKey, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.com")
	response := performJSONRequest(t, server, http.MethodGet, "/v1/static-edges", tenantKey, nil)
	if response.Code != http.StatusForbidden {
		t.Fatalf("expected static edge read to require an explicit scope, got %d body=%s", response.Code, response.Body.String())
	}
}
