package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestGetSourceUploadReturnsMetadataAndReferences(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "demo", "demo")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "demo", model.AppSpec{
		Image:     "registry.example.com/demo:current",
		RuntimeID: "runtime_managed_shared",
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	upload, err := stateStore.CreateSourceUpload(tenant.ID, "demo.tgz", "application/gzip", []byte("test-archive"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		Status:          model.OperationStatusCompleted,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "key_123",
		AppID:           app.ID,
		DesiredSpec: &model.AppSpec{
			Image:     app.Spec.Image,
			RuntimeID: app.Spec.RuntimeID,
			Replicas:  app.Spec.Replicas,
		},
		DesiredSource: &model.AppSource{
			Type:             model.AppSourceTypeUpload,
			UploadID:         upload.ID,
			UploadFilename:   upload.Filename,
			ArchiveSHA256:    upload.SHA256,
			ArchiveSizeBytes: upload.SizeBytes,
			BuildStrategy:    "dockerfile",
			SourceDir:        "services/runtime",
			ResolvedImageRef: "registry.example.com/fugue-apps/demo:git-abc123",
		},
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	_, apiSecret, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	req := httptest.NewRequest(http.MethodGet, "/v1/source-uploads/"+upload.ID, nil)
	req.Header.Set("Authorization", "Bearer "+apiSecret)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		SourceUpload model.SourceUploadInspection `json:"source_upload"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.SourceUpload.Upload.ID != upload.ID {
		t.Fatalf("expected upload id %q, got %q", upload.ID, response.SourceUpload.Upload.ID)
	}
	if len(response.SourceUpload.References) != 1 {
		t.Fatalf("expected 1 upload reference, got %+v", response.SourceUpload.References)
	}
	if response.SourceUpload.References[0].OperationID != op.ID {
		t.Fatalf("expected operation id %q, got %q", op.ID, response.SourceUpload.References[0].OperationID)
	}
	if response.SourceUpload.References[0].AppName != app.Name {
		t.Fatalf("expected app name %q, got %q", app.Name, response.SourceUpload.References[0].AppName)
	}
}
