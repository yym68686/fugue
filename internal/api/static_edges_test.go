package api

import (
	"encoding/json"
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

func TestStaticEdgeRegistrationEmptyListIsArray(t *testing.T) {
	t.Parallel()
	_, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "example.com")
	response := performJSONRequest(t, server, http.MethodGet, "/v1/static-edges?tenant_id="+app.TenantID, platformAdminKey, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("empty list: status=%d body=%s", response.Code, response.Body.String())
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if string(body["registrations"]) != "[]" {
		t.Fatalf("empty list must be an array, got %s", body["registrations"])
	}
}

func TestStaticEdgeRegistrationTenantCredentialCannotCrossProjects(t *testing.T) {
	t.Parallel()
	stateStore, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "example.com")
	_, ownerKey, err := stateStore.CreateAPIKey(app.TenantID, "edge-owner", []string{"static_edge.read", "static_edge.write"})
	if err != nil {
		t.Fatal(err)
	}
	otherTenant, err := stateStore.CreateTenant("other-edge-owner")
	if err != nil {
		t.Fatal(err)
	}
	otherProject, err := stateStore.CreateProject(otherTenant.ID, "other", "")
	if err != nil {
		t.Fatal(err)
	}
	_, otherKey, err := stateStore.CreateAPIKey(otherTenant.ID, "edge-owner", []string{"static_edge.read", "static_edge.write"})
	if err != nil {
		t.Fatal(err)
	}
	createBody := map[string]any{
		"project_id": app.ProjectID, "name": "edge", "edge_id": "edge-one", "transport": "mtls",
		"signing_key_id": "key-one", "possession_proof_digest": "sha256:" + strings.Repeat("a", 64),
	}
	if response := performJSONRequest(t, server, http.MethodPost, "/v1/static-edges", otherKey, createBody); response.Code != http.StatusForbidden {
		t.Fatalf("foreign project create: status=%d body=%s", response.Code, response.Body.String())
	}
	createBody["project_id"] = otherProject.ID
	if response := performJSONRequest(t, server, http.MethodPost, "/v1/static-edges", ownerKey, createBody); response.Code != http.StatusForbidden {
		t.Fatalf("foreign project create: status=%d body=%s", response.Code, response.Body.String())
	}
	createBody["project_id"] = app.ProjectID
	created := performJSONRequest(t, server, http.MethodPost, "/v1/static-edges", ownerKey, createBody)
	if created.Code != http.StatusCreated {
		t.Fatalf("owner create: status=%d body=%s", created.Code, created.Body.String())
	}
	var body model.StaticEdgeRegistrationResponse
	mustDecodeJSON(t, created, &body)
	if response := performJSONRequest(t, server, http.MethodGet, "/v1/static-edges/"+body.Registration.ID, otherKey, nil); response.Code != http.StatusNotFound {
		t.Fatalf("foreign registration read: status=%d body=%s", response.Code, response.Body.String())
	}
	listed := performJSONRequest(t, server, http.MethodGet, "/v1/static-edges?tenant_id="+otherTenant.ID, ownerKey, nil)
	var ownerList model.StaticEdgeRegistrationListResponse
	mustDecodeJSON(t, listed, &ownerList)
	if listed.Code != http.StatusOK || len(ownerList.Registrations) != 1 || ownerList.Registrations[0].TenantID != app.TenantID {
		t.Fatalf("tenant filter crossed credential boundary: status=%d body=%s", listed.Code, listed.Body.String())
	}
	adminList := performJSONRequest(t, server, http.MethodGet, "/v1/static-edges?tenant_id="+otherTenant.ID, platformAdminKey, nil)
	var filtered model.StaticEdgeRegistrationListResponse
	mustDecodeJSON(t, adminList, &filtered)
	if adminList.Code != http.StatusOK || filtered.Registrations == nil || len(filtered.Registrations) != 0 {
		t.Fatalf("admin tenant filter returned other tenant: status=%d body=%s", adminList.Code, adminList.Body.String())
	}
}
