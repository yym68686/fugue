package api

import (
	"bytes"
	"encoding/json"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAppActionReceiptReplaysAfterCASChangesAndIsActorScoped(t *testing.T) {
	state, server, key, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	tag := `"` + model.AppSpecSHA256(app.Spec) + `"`
	submit := func(path, match string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{}`))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("If-Match", match)
		req.Header.Set("Idempotency-Key", "request-a")
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)
		return rec
	}
	path := "/v1/apps/" + app.ID + "/restart"
	first := submit(path, tag)
	if first.Code != 202 {
		t.Fatalf("first %d: %s", first.Code, first.Body)
	}
	second := submit(path, tag)
	if second.Code != 202 {
		t.Fatalf("replay %d: %s", second.Code, second.Body)
	}
	var a, b struct {
		Operation model.Operation `json:"operation"`
	}
	json.Unmarshal(first.Body.Bytes(), &a)
	json.Unmarshal(second.Body.Bytes(), &b)
	if a.Operation.ID == "" || a.Operation.ID != b.Operation.ID {
		t.Fatal("replay created another operation")
	}
	if rec := submit(path, `"`+strings.Repeat("0", 64)+`"`); rec.Code != 409 {
		t.Fatalf("different request accepted: %d", rec.Code)
	}
	receiptPath := "/v1/apps/" + app.ID + "/action-requests/request-a"
	rec := performJSONRequest(t, server, http.MethodGet, receiptPath, key, nil)
	if rec.Code != 200 {
		t.Fatalf("recover receipt %d: %s", rec.Code, rec.Body)
	}
	_, other, err := state.CreateAPIKey(app.TenantID, "other-actor", []string{"app.deploy"})
	if err != nil {
		t.Fatal(err)
	}
	if rec := performJSONRequest(t, server, http.MethodGet, receiptPath, other, nil); rec.Code != 404 {
		t.Fatalf("other actor read receipt: %d", rec.Code)
	}
}

func TestImageActionRejectsChangedDigest(t *testing.T) {
	_, server, key, _, _, app, _, _, imageRef, _ := setupAppImagesTestServer(t)
	response := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/images/redeploy", key, map[string]any{"image_ref": imageRef, "expected_digest": "sha256:" + strings.Repeat("0", 64)})
	if response.Code != http.StatusPreconditionFailed {
		t.Fatalf("changed digest status=%d body=%s", response.Code, response.Body)
	}
}
