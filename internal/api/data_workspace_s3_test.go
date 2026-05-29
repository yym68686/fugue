package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestDataWorkspaceS3MultipartPlanRefreshAndComplete(t *testing.T) {
	var headCalls, createMultipartCalls, listPartsCalls, completeCalls int
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls++
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && hasQueryKey(r, "uploads"):
			createMultipartCalls++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<CreateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-1</UploadId></CreateMultipartUploadResult>`))
		case r.Method == http.MethodGet && r.URL.Query().Get("uploadId") == "upload-1":
			listPartsCalls++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<ListPartsResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-1</UploadId><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><ETag>&quot;etag-1&quot;</ETag><Size>67108864</Size></Part></ListPartsResult>`))
		case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") == "upload-1":
			completeCalls++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><ETag>&quot;etag-final&quot;</ETag></CompleteMultipartUploadResult>`))
		default:
			t.Fatalf("unexpected fake s3 request %s %s", r.Method, r.URL.String())
		}
	}))
	defer s3Server.Close()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	backend, err := stateStore.CreateDataBackend(model.DataBackend{
		TenantID: tenant.ID,
		Name:     "fake-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3Server.URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:         tenant.ID,
		Name:             "workspace",
		StorageBackendID: backend.ID,
		Assets:           []model.DataAsset{{Name: "data", Path: "./data"}},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	apiServer := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	httpServer := httptest.NewServer(apiServer.Handler())
	defer httpServer.Close()

	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{
		AssetName:    "data",
		RelativePath: "big.bin",
		Kind:         model.DataManifestEntryKindFile,
		Size:         dataMultipartPartSize + 1,
		SHA256:       strings.Repeat("a", 64),
	}}})
	planBody, _ := json.Marshal(map[string]any{"version": "v1", "manifest": manifest})
	req, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/data/workspaces/"+workspace.ID+"/transfers/plan-upload", bytes.NewReader(planBody))
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("plan upload: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("plan upload status %d", resp.StatusCode)
	}
	planRaw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	if bytes.Contains(planRaw, []byte(`"manifest"`)) || bytes.Contains(planRaw, []byte(`"plan_blobs"`)) {
		t.Fatalf("upload plan response should not echo manifest or plan blobs, got %s", string(planRaw))
	}
	var plan dataUploadPlanResponse
	if err := json.Unmarshal(planRaw, &plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	if createMultipartCalls != 1 || len(plan.Blobs) != 1 || plan.Blobs[0].UploadMode != model.DataBlobUploadModeMultipart || len(plan.Blobs[0].Parts) != 2 {
		t.Fatalf("unexpected multipart plan calls=%d blobs=%+v", createMultipartCalls, plan.Blobs)
	}
	if headCalls != 0 {
		t.Fatalf("upload plan should not issue object HEAD requests, got %d", headCalls)
	}

	refreshReq, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/data/transfers/"+plan.Transfer.ID+"/refresh", nil)
	refreshReq.Header.Set("Authorization", "Bearer "+secret)
	refreshResp, err := http.DefaultClient.Do(refreshReq)
	if err != nil {
		t.Fatalf("refresh transfer: %v", err)
	}
	defer refreshResp.Body.Close()
	if refreshResp.StatusCode != http.StatusOK {
		t.Fatalf("refresh status %d", refreshResp.StatusCode)
	}
	refreshRaw, err := io.ReadAll(refreshResp.Body)
	if err != nil {
		t.Fatalf("read refresh: %v", err)
	}
	if bytes.Contains(refreshRaw, []byte(`"manifest"`)) || bytes.Contains(refreshRaw, []byte(`"plan_blobs"`)) {
		t.Fatalf("upload refresh response should not echo manifest or plan blobs, got %s", string(refreshRaw))
	}
	var refresh struct {
		Workspace model.DataWorkspace    `json:"workspace"`
		Transfer  model.DataTransfer     `json:"transfer"`
		Blobs     []dataTransferPlanBlob `json:"blobs"`
	}
	if err := json.Unmarshal(refreshRaw, &refresh); err != nil {
		t.Fatalf("decode refresh: %v", err)
	}
	if listPartsCalls != 1 || !refresh.Blobs[0].Parts[0].Completed || refresh.Blobs[0].Parts[0].ETag != "etag-1" {
		t.Fatalf("unexpected refreshed parts calls=%d blobs=%+v", listPartsCalls, refresh.Blobs)
	}

	completeBody, _ := json.Marshal(map[string]any{"sha256": manifest.Entries[0].SHA256, "upload_id": "upload-1", "parts": refresh.Blobs[0].Parts[:1]})
	completeReq, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/data/transfers/"+plan.Transfer.ID+"/multipart/complete", bytes.NewReader(completeBody))
	completeReq.Header.Set("Authorization", "Bearer "+secret)
	completeReq.Header.Set("Content-Type", "application/json")
	completeResp, err := http.DefaultClient.Do(completeReq)
	if err != nil {
		t.Fatalf("complete multipart: %v", err)
	}
	defer completeResp.Body.Close()
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf("complete status %d", completeResp.StatusCode)
	}
	if completeCalls != 1 {
		t.Fatalf("expected complete multipart call, got %d", completeCalls)
	}
}

func TestDataWorkspaceS3UploadPlanUsesSnapshotIndexWithoutHEAD(t *testing.T) {
	var headCalls, createMultipartCalls int
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls++
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && hasQueryKey(r, "uploads"):
			createMultipartCalls++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<CreateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-1</UploadId></CreateMultipartUploadResult>`))
		default:
			t.Fatalf("unexpected fake s3 request %s %s", r.Method, r.URL.String())
		}
	}))
	defer s3Server.Close()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	backend, err := stateStore.CreateDataBackend(model.DataBackend{
		TenantID: tenant.ID,
		Name:     "fake-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3Server.URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:         tenant.ID,
		Name:             "workspace",
		StorageBackendID: backend.ID,
		Assets:           []model.DataAsset{{Name: "data", Path: "./data"}},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	digest := strings.Repeat("c", 64)
	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{
		AssetName:    "data",
		RelativePath: "cached.bin",
		Kind:         model.DataManifestEntryKindFile,
		Size:         dataMultipartPartSize + 1,
		SHA256:       digest,
	}}})
	if _, err := stateStore.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: workspace.ID, Version: "v1", Manifest: manifest}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	apiServer := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	httpServer := httptest.NewServer(apiServer.Handler())
	defer httpServer.Close()

	planBody, _ := json.Marshal(map[string]any{"version": "v2", "manifest": manifest})
	req, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/data/workspaces/"+workspace.ID+"/transfers/plan-upload", bytes.NewReader(planBody))
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("plan upload: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("plan upload status %d", resp.StatusCode)
	}
	var plan dataUploadPlanResponse
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	if len(plan.Blobs) != 1 || !plan.Blobs[0].Exists || plan.Blobs[0].UploadURL != "" || plan.Blobs[0].UploadID != "" {
		t.Fatalf("expected snapshot-backed blob to be skipped, got %+v", plan.Blobs)
	}
	if headCalls != 0 || createMultipartCalls != 0 {
		t.Fatalf("expected no s3 calls for snapshot-backed blob, head=%d multipart=%d", headCalls, createMultipartCalls)
	}
}

func TestDataWorkspaceS3UploadPlanPagesBlobs(t *testing.T) {
	var s3Calls int
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s3Calls++
		t.Fatalf("upload plan pagination should not require fake s3 request %s %s", r.Method, r.URL.String())
	}))
	defer s3Server.Close()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	backend, err := stateStore.CreateDataBackend(model.DataBackend{
		TenantID: tenant.ID,
		Name:     "fake-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3Server.URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:         tenant.ID,
		Name:             "workspace",
		StorageBackendID: backend.ID,
		Assets:           []model.DataAsset{{Name: "data", Path: "./data"}},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{
		{AssetName: "data", RelativePath: "a.bin", Kind: model.DataManifestEntryKindFile, Size: 1, SHA256: strings.Repeat("1", 64)},
		{AssetName: "data", RelativePath: "b.bin", Kind: model.DataManifestEntryKindFile, Size: 1, SHA256: strings.Repeat("2", 64)},
		{AssetName: "data", RelativePath: "c.bin", Kind: model.DataManifestEntryKindFile, Size: 1, SHA256: strings.Repeat("3", 64)},
	}})
	apiServer := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	httpServer := httptest.NewServer(apiServer.Handler())
	defer httpServer.Close()

	planBody, _ := json.Marshal(map[string]any{"version": "v1", "manifest": manifest})
	req, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/data/workspaces/"+workspace.ID+"/transfers/plan-upload?blob_limit=2", bytes.NewReader(planBody))
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("plan upload: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("plan upload status %d", resp.StatusCode)
	}
	var plan dataUploadPlanResponse
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	if len(plan.Blobs) != 2 || plan.BlobsTotal != 3 || plan.BlobsLimit != 2 || plan.BlobsNextOffset == nil || *plan.BlobsNextOffset != 2 {
		t.Fatalf("unexpected first page: %+v", plan)
	}
	refreshReq, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/data/transfers/"+plan.Transfer.ID+"/refresh?blob_offset=2&blob_limit=2", nil)
	refreshReq.Header.Set("Authorization", "Bearer "+secret)
	refreshResp, err := http.DefaultClient.Do(refreshReq)
	if err != nil {
		t.Fatalf("refresh page: %v", err)
	}
	defer refreshResp.Body.Close()
	if refreshResp.StatusCode != http.StatusOK {
		t.Fatalf("refresh status %d", refreshResp.StatusCode)
	}
	var refresh struct {
		Blobs           []dataTransferPlanBlob `json:"blobs"`
		BlobsTotal      int                    `json:"blobs_total"`
		BlobsOffset     int                    `json:"blobs_offset"`
		BlobsNextOffset *int                   `json:"blobs_next_offset"`
	}
	if err := json.NewDecoder(refreshResp.Body).Decode(&refresh); err != nil {
		t.Fatalf("decode refresh: %v", err)
	}
	if len(refresh.Blobs) != 1 || refresh.BlobsTotal != 3 || refresh.BlobsOffset != 2 || refresh.BlobsNextOffset != nil {
		t.Fatalf("unexpected second page: %+v", refresh)
	}
	if s3Calls != 0 {
		t.Fatalf("expected no fake s3 requests, got %d", s3Calls)
	}
}

func TestDataWorkspaceS3MultipartAbort(t *testing.T) {
	var headCalls, createMultipartCalls, abortCalls int
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls++
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && hasQueryKey(r, "uploads"):
			createMultipartCalls++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<CreateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-1</UploadId></CreateMultipartUploadResult>`))
		case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") == "upload-1":
			abortCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected fake s3 request %s %s", r.Method, r.URL.String())
		}
	}))
	defer s3Server.Close()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	backend, err := stateStore.CreateDataBackend(model.DataBackend{
		TenantID: tenant.ID,
		Name:     "fake-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3Server.URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:         tenant.ID,
		Name:             "workspace",
		StorageBackendID: backend.ID,
		Assets:           []model.DataAsset{{Name: "data", Path: "./data"}},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	apiServer := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	httpServer := httptest.NewServer(apiServer.Handler())
	defer httpServer.Close()

	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{
		AssetName:    "data",
		RelativePath: "big.bin",
		Kind:         model.DataManifestEntryKindFile,
		Size:         dataMultipartPartSize + 1,
		SHA256:       strings.Repeat("b", 64),
	}}})
	planBody, _ := json.Marshal(map[string]any{"version": "v1", "manifest": manifest})
	req, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/data/workspaces/"+workspace.ID+"/transfers/plan-upload", bytes.NewReader(planBody))
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("plan upload: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("plan upload status %d", resp.StatusCode)
	}
	var plan dataUploadPlanResponse
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	if createMultipartCalls != 1 || len(plan.Blobs) != 1 || plan.Blobs[0].UploadID != "upload-1" {
		t.Fatalf("unexpected multipart plan calls=%d blobs=%+v", createMultipartCalls, plan.Blobs)
	}
	if headCalls != 0 {
		t.Fatalf("upload plan should not issue object HEAD requests, got %d", headCalls)
	}
	abortBody, _ := json.Marshal(map[string]any{"sha256": manifest.Entries[0].SHA256, "upload_id": "upload-1"})
	abortReq, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/data/transfers/"+plan.Transfer.ID+"/multipart/abort", bytes.NewReader(abortBody))
	abortReq.Header.Set("Authorization", "Bearer "+secret)
	abortReq.Header.Set("Content-Type", "application/json")
	abortResp, err := http.DefaultClient.Do(abortReq)
	if err != nil {
		t.Fatalf("abort multipart: %v", err)
	}
	defer abortResp.Body.Close()
	if abortResp.StatusCode != http.StatusOK {
		t.Fatalf("abort status %d", abortResp.StatusCode)
	}
	if abortCalls != 1 {
		t.Fatalf("expected one abort call, got %d", abortCalls)
	}
}

func TestMigrateDataWorkspaceBackendCopiesAndCutsOver(t *testing.T) {
	content := []byte("training-data")
	digest := testDataDigestString(string(content))
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected source request %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Length", "13")
		_, _ = w.Write(content)
	}))
	defer sourceServer.Close()
	var putObjectCalls int
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if putObjectCalls > 0 {
				w.Header().Set("Content-Length", "13")
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			putObjectCalls++
			var body bytes.Buffer
			_, _ = body.ReadFrom(r.Body)
			if body.String() != string(content) {
				t.Fatalf("unexpected migrated body %q", body.String())
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected target request %s %s", r.Method, r.URL.String())
		}
	}))
	defer targetServer.Close()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	sourceBackend, err := stateStore.CreateDataBackend(model.DataBackend{TenantID: tenant.ID, Name: "source-s3", Provider: model.DataBackendProviderS3, Bucket: "bucket", Endpoint: sourceServer.URL, Region: "us-east-1", Credentials: model.DataBackendCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}})
	if err != nil {
		t.Fatalf("create source backend: %v", err)
	}
	targetBackend, err := stateStore.CreateDataBackend(model.DataBackend{TenantID: tenant.ID, Name: "target-s3", Provider: model.DataBackendProviderS3, Bucket: "bucket", Endpoint: targetServer.URL, Region: "us-east-1", Credentials: model.DataBackendCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}})
	if err != nil {
		t.Fatalf("create target backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "workspace", StorageBackendID: sourceBackend.ID})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	_, err = stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "v1",
		Manifest: model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{
			AssetName:    "data",
			RelativePath: "sample.txt",
			Kind:         model.DataManifestEntryKindFile,
			Size:         int64(len(content)),
			SHA256:       digest,
		}}}),
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	server := NewServer(stateStore, nil, nil, ServerConfig{})
	transfer, err := server.migrateDataWorkspaceBackend(nil, workspace, targetBackend.ID, false, true)
	if err != nil {
		t.Fatalf("migrate backend: %v", err)
	}
	if transfer.Status != model.DataTransferStatusCompleted || transfer.FilesDone != 1 || transfer.BytesDone != int64(len(content)) {
		t.Fatalf("unexpected migration transfer: %+v", transfer)
	}
	if putObjectCalls != 1 {
		t.Fatalf("expected one target put, got %d", putObjectCalls)
	}
	updated, err := stateStore.GetDataWorkspace(workspace.ID, tenant.ID, false)
	if err != nil {
		t.Fatalf("get updated workspace: %v", err)
	}
	if updated.StorageBackendID != targetBackend.ID {
		t.Fatalf("expected cutover to target backend, got %s", updated.StorageBackendID)
	}
	rolledBack, rollbackTransfer, err := server.rollbackDataWorkspaceBackendMigration(nil, updated, transfer.ID)
	if err != nil {
		t.Fatalf("rollback backend migration: %v", err)
	}
	if rolledBack.StorageBackendID != sourceBackend.ID {
		t.Fatalf("expected rollback to source backend, got %s", rolledBack.StorageBackendID)
	}
	if rollbackTransfer.Status != model.DataTransferStatusCompleted || rollbackTransfer.Source != targetBackend.ID || rollbackTransfer.Target != sourceBackend.ID {
		t.Fatalf("unexpected rollback transfer: %+v", rollbackTransfer)
	}
}

func hasQueryKey(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}

func TestDataBlobURLAddsSchemeToPublicDomain(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "http://internal.example", nil)
	server := &Server{apiPublicDomain: "api.fugue.pro"}
	got := server.dataBlobURL(req, "transfer-1", strings.Repeat("a", 64))
	if !strings.HasPrefix(got, "https://api.fugue.pro/v1/data/blobs/") {
		t.Fatalf("expected https public data blob URL, got %q", got)
	}
}

func testDataDigestString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
