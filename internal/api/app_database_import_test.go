package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImportAppDatabaseCreatesPendingJob(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})
	body, contentType := newAppDatabaseImportMultipartBody(t, model.AppDatabaseImportRequest{
		Label:  "legacy-vps",
		Format: model.AppDatabaseImportFormatSQL,
		Clean:  true,
	}, "dump.sql", []byte("select 1;"))

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/"+app.ID+"/database/import", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var response model.AppDatabaseImportResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Job == nil || response.Job.Status != model.OperationStatusPending || response.Job.Format != model.AppDatabaseImportFormatSQL {
		t.Fatalf("unexpected import response: %+v", response)
	}
	if !response.Job.Clean || response.Job.Label != "legacy-vps" {
		t.Fatalf("expected clean import label, got %+v", response.Job)
	}
	upload, dumpBytes, err := stateStore.GetSourceUploadArchive(response.Job.SourceUploadID)
	if err != nil {
		t.Fatalf("get source upload archive: %v", err)
	}
	if upload.Filename != "dump.sql" || string(dumpBytes) != "select 1;" {
		t.Fatalf("unexpected stored dump upload=%+v data=%q", upload, string(dumpBytes))
	}
}

func TestAppDatabaseAccessGrantCanBeCreatedAndRevoked(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})

	create := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/database/access", apiKey, map[string]any{
		"label":              "legacy-vps",
		"expires_in_minutes": 30,
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var createResponse model.AppDatabaseAccessGrantCreateResponse
	mustDecodeJSON(t, create, &createResponse)
	if createResponse.Secret == "" || createResponse.Grant.ID == "" || createResponse.Grant.TokenHash != "" {
		t.Fatalf("unexpected create response: %+v", createResponse)
	}
	if _, err := stateStore.AuthenticateAppDatabaseAccessGrant(app.ID, createResponse.Grant.ID, createResponse.Secret); err != nil {
		t.Fatalf("authenticate access grant: %v", err)
	}

	revoke := performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID+"/database/access/"+createResponse.Grant.ID, apiKey, nil)
	if revoke.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, revoke.Code, revoke.Body.String())
	}
	if _, err := stateStore.AuthenticateAppDatabaseAccessGrant(app.ID, createResponse.Grant.ID, createResponse.Secret); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected revoked grant conflict, got %v", err)
	}
}

func newAppDatabaseImportMultipartBody(t *testing.T, req model.AppDatabaseImportRequest, dumpName string, dumpBytes []byte) (*bytes.Buffer, string) {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	requestJSON, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if err := writer.WriteField("request", string(requestJSON)); err != nil {
		t.Fatalf("write request field: %v", err)
	}
	part, err := writer.CreateFormFile("dump", dumpName)
	if err != nil {
		t.Fatalf("create dump part: %v", err)
	}
	if _, err := part.Write(dumpBytes); err != nil {
		t.Fatalf("write dump part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, writer.FormDataContentType()
}
