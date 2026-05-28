package api

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestDataGrantScopeAndCredentialBoundary(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, readOnlyKey, err := stateStore.CreateAPIKey(tenant.ID, "reader", []string{"data.read"})
	if err != nil {
		t.Fatalf("create reader key: %v", err)
	}
	_, grantKey, err := stateStore.CreateAPIKey(tenant.ID, "grant", []string{"data.read", "data.grant"})
	if err != nil {
		t.Fatalf("create grant key: %v", err)
	}
	backend, err := stateStore.CreateDataBackend(model.DataBackend{
		TenantID:    tenant.ID,
		Name:        "r2",
		Provider:    model.DataBackendProviderCloudflareR2,
		Bucket:      "bucket",
		Endpoint:    "https://example.r2.cloudflarestorage.com",
		Credentials: model.DataBackendCredentials{AccessKeyID: "provider-access", SecretAccessKey: "provider-secret"},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "workspace", StorageBackendID: backend.ID})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})

	forbidden := performJSONRequest(t, server, http.MethodPost, "/v1/data/workspaces/"+workspace.ID+"/grants", readOnlyKey, map[string]any{"mode": "read-write"})
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("expected grant creation without data.grant to be forbidden, got %d body=%s", forbidden.Code, forbidden.Body.String())
	}
	created := performJSONRequest(t, server, http.MethodPost, "/v1/data/workspaces/"+workspace.ID+"/grants", grantKey, map[string]any{"mode": "read-write"})
	if created.Code != http.StatusCreated {
		t.Fatalf("expected grant creation, got %d body=%s", created.Code, created.Body.String())
	}
	body := created.Body.String()
	if strings.Contains(body, "provider-secret") || strings.Contains(body, "secret_access_key") || strings.Contains(body, "provider-access") {
		t.Fatalf("grant response exposed provider credential: %s", body)
	}
	if !strings.Contains(body, "fugue_data_grant") {
		t.Fatalf("grant response did not include grant secret: %s", body)
	}
}

func TestDataTransferCheckpointPersistsServerSideState(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "workspace"})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	transfer, err := stateStore.CreateDataTransfer(model.DataTransfer{
		WorkspaceID: workspace.ID,
		Direction:   model.DataTransferDirectionUpload,
		Status:      model.DataTransferStatusPlanned,
		BytesTotal:  10,
		FilesTotal:  1,
		PlanBlobs: []model.DataTransferPlanBlob{{
			SHA256:     strings.Repeat("a", 64),
			Size:       10,
			ObjectKey:  "blobs/sha256/aa/aa/blob",
			UploadURL:  "https://example.invalid/upload",
			UploadMode: model.DataBlobUploadModeMultipart,
			UploadID:   "upload-1",
			Parts: []model.DataTransferPart{{
				PartNumber: 1,
				UploadURL:  "https://example.invalid/part",
				Size:       10,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("create transfer: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/data/transfers/"+transfer.ID+"/checkpoint", secret, map[string]any{
		"bytes_done": 10,
		"files_done": 1,
		"blobs": []map[string]any{{
			"sha256":      strings.Repeat("a", 64),
			"size":        10,
			"object_key":  "blobs/sha256/aa/aa/blob",
			"upload_mode": model.DataBlobUploadModeMultipart,
			"upload_id":   "upload-1",
			"exists":      true,
			"parts": []map[string]any{{
				"part_number": 1,
				"etag":        "etag-1",
				"completed":   true,
				"upload_url":  "https://example.invalid/should-not-persist",
			}},
		}},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("checkpoint status %d body=%s", recorder.Code, recorder.Body.String())
	}
	updated, err := stateStore.GetDataTransfer(transfer.ID)
	if err != nil {
		t.Fatalf("get transfer: %v", err)
	}
	if updated.Status != model.DataTransferStatusRunning || updated.BytesDone != 10 || updated.FilesDone != 1 {
		t.Fatalf("unexpected checkpointed transfer: %+v", updated)
	}
	if !updated.PlanBlobs[0].Exists || !updated.PlanBlobs[0].Parts[0].Completed || updated.PlanBlobs[0].Parts[0].ETag != "etag-1" {
		t.Fatalf("checkpoint did not persist part state: %+v", updated.PlanBlobs)
	}
	if updated.PlanBlobs[0].UploadURL != "https://example.invalid/upload" || updated.PlanBlobs[0].Parts[0].UploadURL != "https://example.invalid/part" {
		t.Fatalf("checkpoint overwrote live presigned URLs: %+v", updated.PlanBlobs)
	}
}
