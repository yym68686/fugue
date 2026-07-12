package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestDataBlobTransferRejectsTraversalNonPlanAndCrossTenantAccess(t *testing.T) {
	t.Parallel()

	stateStore, server, ownerKey, otherTenantKey, workspace := setupDataBlobSecurityServer(t)
	content := "planned-content"
	digest := testDataDigestString(content)
	transfer := createDataBlobSecurityTransfer(t, stateStore, workspace, model.DataTransferDirectionUpload, digest, int64(len(content)))

	traversal := "..%2F..%2F..%2Fcontrol-plane-secret"
	traversalResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, traversal, transfer.ID, ownerKey, "application/octet-stream", strings.NewReader(content), nil)
	if traversalResponse.Code != http.StatusBadRequest {
		t.Fatalf("encoded traversal status = %d body=%s, want 400", traversalResponse.Code, traversalResponse.Body.String())
	}

	uppercaseResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, strings.ToUpper(digest), transfer.ID, ownerKey, "application/octet-stream", strings.NewReader(content), nil)
	if uppercaseResponse.Code != http.StatusBadRequest {
		t.Fatalf("uppercase digest status = %d body=%s, want 400", uppercaseResponse.Code, uppercaseResponse.Body.String())
	}

	nonPlanDigest := testDataDigestString("not-in-plan")
	nonPlanResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, nonPlanDigest, transfer.ID, ownerKey, "application/octet-stream", strings.NewReader("not-in-plan"), nil)
	if nonPlanResponse.Code != http.StatusNotFound {
		t.Fatalf("non-plan digest status = %d body=%s, want 404", nonPlanResponse.Code, nonPlanResponse.Body.String())
	}

	crossTenantResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, digest, transfer.ID, otherTenantKey, "application/octet-stream", strings.NewReader(content), nil)
	if crossTenantResponse.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant status = %d body=%s, want 403", crossTenantResponse.Code, crossTenantResponse.Body.String())
	}

	if stateStore.DataBlobExists(digest) || stateStore.DataBlobExists(nonPlanDigest) {
		t.Fatal("rejected data blob request committed content")
	}
}

func TestPutDataBlobEnforcesMediaTypeAndExactPlannedSize(t *testing.T) {
	t.Parallel()

	stateStore, server, ownerKey, _, workspace := setupDataBlobSecurityServer(t)

	validContent := "hello"
	validDigest := testDataDigestString(validContent)
	validTransfer := createDataBlobSecurityTransfer(t, stateStore, workspace, model.DataTransferDirectionUpload, validDigest, int64(len(validContent)))
	validResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, validDigest, validTransfer.ID, ownerKey, "application/octet-stream", strings.NewReader(validContent), nil)
	if validResponse.Code != http.StatusOK {
		t.Fatalf("valid upload status = %d body=%s", validResponse.Code, validResponse.Body.String())
	}
	if !stateStore.DataBlobExists(validDigest) {
		t.Fatal("valid upload did not commit the blob")
	}

	wrongMediaContent := "media"
	wrongMediaDigest := testDataDigestString(wrongMediaContent)
	wrongMediaTransfer := createDataBlobSecurityTransfer(t, stateStore, workspace, model.DataTransferDirectionUpload, wrongMediaDigest, int64(len(wrongMediaContent)))
	wrongMediaResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, wrongMediaDigest, wrongMediaTransfer.ID, ownerKey, "text/plain", strings.NewReader(wrongMediaContent), nil)
	if wrongMediaResponse.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong media status = %d body=%s, want 415", wrongMediaResponse.Code, wrongMediaResponse.Body.String())
	}

	overflowContent := "abcdef"
	overflowDigest := testDataDigestString(overflowContent)
	overflowTransfer := createDataBlobSecurityTransfer(t, stateStore, workspace, model.DataTransferDirectionUpload, overflowDigest, 5)
	overflowResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, overflowDigest, overflowTransfer.ID, ownerKey, "application/octet-stream", strings.NewReader(overflowContent), nil)
	if overflowResponse.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("known-length overflow status = %d body=%s, want 413", overflowResponse.Code, overflowResponse.Body.String())
	}

	chunkedDigest := testDataDigestString("ghijkl")
	chunkedTransfer := createDataBlobSecurityTransfer(t, stateStore, workspace, model.DataTransferDirectionUpload, chunkedDigest, 5)
	chunkedResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, chunkedDigest, chunkedTransfer.ID, ownerKey, "application/octet-stream", strings.NewReader("ghijkl"), func(request *http.Request) {
		request.ContentLength = -1
		request.TransferEncoding = []string{"chunked"}
	})
	if chunkedResponse.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked overflow status = %d body=%s, want 413", chunkedResponse.Code, chunkedResponse.Body.String())
	}

	shortContent := "tiny"
	shortDigest := testDataDigestString(shortContent)
	shortTransfer := createDataBlobSecurityTransfer(t, stateStore, workspace, model.DataTransferDirectionUpload, shortDigest, 5)
	shortResponse := performDataBlobSecurityRequest(t, server, http.MethodPut, shortDigest, shortTransfer.ID, ownerKey, "application/octet-stream", strings.NewReader(shortContent), nil)
	if shortResponse.Code != http.StatusBadRequest {
		t.Fatalf("short upload status = %d body=%s, want 400", shortResponse.Code, shortResponse.Body.String())
	}

	for _, rejectedDigest := range []string{wrongMediaDigest, overflowDigest, chunkedDigest, shortDigest} {
		if stateStore.DataBlobExists(rejectedDigest) {
			t.Fatalf("rejected blob %s was committed", rejectedDigest)
		}
	}
}

func TestDataTransferCheckpointCannotExpandBlobAuthorization(t *testing.T) {
	t.Parallel()

	stateStore, server, ownerKey, _, workspace := setupDataBlobSecurityServer(t)
	content := "planned-content"
	digest := testDataDigestString(content)
	transfer := createDataBlobSecurityTransfer(t, stateStore, workspace, model.DataTransferDirectionUpload, digest, int64(len(content)))
	injectedContent := "checkpoint-injected"
	injectedDigest := testDataDigestString(injectedContent)

	injected := performJSONRequest(t, server, http.MethodPost, "/v1/data/transfers/"+transfer.ID+"/checkpoint", ownerKey, map[string]any{
		"bytes_done": int64(len(injectedContent)),
		"blobs": []map[string]any{{
			"sha256":     injectedDigest,
			"size":       len(injectedContent),
			"object_key": model.DataObjectKey(injectedDigest),
			"exists":     true,
		}},
	})
	if injected.Code != http.StatusBadRequest {
		t.Fatalf("injected checkpoint status = %d body=%s, want 400", injected.Code, injected.Body.String())
	}

	resized := performJSONRequest(t, server, http.MethodPost, "/v1/data/transfers/"+transfer.ID+"/checkpoint", ownerKey, map[string]any{
		"blobs": []map[string]any{{
			"sha256":     digest,
			"size":       len(content) + 1,
			"object_key": model.DataObjectKey(digest),
		}},
	})
	if resized.Code != http.StatusBadRequest {
		t.Fatalf("resized checkpoint status = %d body=%s, want 400", resized.Code, resized.Body.String())
	}

	storedTransfer, err := stateStore.GetDataTransfer(transfer.ID)
	if err != nil {
		t.Fatalf("get transfer after rejected checkpoints: %v", err)
	}
	if storedTransfer.Status != model.DataTransferStatusPlanned || storedTransfer.BytesDone != 0 || len(storedTransfer.PlanBlobs) != 1 || storedTransfer.PlanBlobs[0].SHA256 != digest || storedTransfer.PlanBlobs[0].Size != int64(len(content)) {
		t.Fatalf("rejected checkpoint changed transfer plan: %+v", storedTransfer)
	}

	unauthorizedUpload := performDataBlobSecurityRequest(t, server, http.MethodPut, injectedDigest, transfer.ID, ownerKey, "application/octet-stream", strings.NewReader(injectedContent), nil)
	if unauthorizedUpload.Code != http.StatusNotFound {
		t.Fatalf("checkpoint-injected blob upload status = %d body=%s, want 404", unauthorizedUpload.Code, unauthorizedUpload.Body.String())
	}
	if stateStore.DataBlobExists(injectedDigest) {
		t.Fatal("checkpoint-injected blob was committed")
	}
}

func TestDataTransferCheckpointCanonicalizesCompactManifestPlan(t *testing.T) {
	t.Parallel()

	stateStore, server, ownerKey, _, workspace := setupDataBlobSecurityServer(t)
	content := "compact-plan-content"
	digest := testDataDigestString(content)
	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{
		AssetName:    "data",
		RelativePath: "blob.bin",
		Kind:         model.DataManifestEntryKindFile,
		Size:         int64(len(content)),
		SHA256:       digest,
	}}})
	transfer, err := stateStore.CreateDataTransfer(model.DataTransfer{
		TenantID:    workspace.TenantID,
		WorkspaceID: workspace.ID,
		Direction:   model.DataTransferDirectionUpload,
		Status:      model.DataTransferStatusPlanned,
		BytesTotal:  int64(len(content)),
		FilesTotal:  1,
		Manifest:    manifest,
	})
	if err != nil {
		t.Fatalf("create compact transfer: %v", err)
	}

	resized := performJSONRequest(t, server, http.MethodPost, "/v1/data/transfers/"+transfer.ID+"/checkpoint", ownerKey, map[string]any{
		"blobs": []map[string]any{{
			"sha256":      digest,
			"size":        len(content) + 1,
			"object_key":  model.DataObjectKey(digest),
			"upload_mode": model.DataBlobUploadModeSingle,
			"exists":      true,
		}},
	})
	if resized.Code != http.StatusBadRequest {
		t.Fatalf("compact resized checkpoint status = %d body=%s, want 400", resized.Code, resized.Body.String())
	}
	unchanged, err := stateStore.GetDataTransfer(transfer.ID)
	if err != nil {
		t.Fatalf("get compact transfer after rejection: %v", err)
	}
	if unchanged.Status != model.DataTransferStatusPlanned || len(unchanged.PlanBlobs) != 0 {
		t.Fatalf("rejected compact checkpoint changed transfer: %+v", unchanged)
	}

	accepted := performJSONRequest(t, server, http.MethodPost, "/v1/data/transfers/"+transfer.ID+"/checkpoint", ownerKey, map[string]any{
		"blobs": []map[string]any{{
			"sha256":      digest,
			"size":        len(content),
			"object_key":  model.DataObjectKey(digest),
			"upload_mode": model.DataBlobUploadModeSingle,
			"upload_url":  "https://example.invalid/must-not-persist",
			"exists":      true,
		}},
	})
	if accepted.Code != http.StatusOK {
		t.Fatalf("canonical compact checkpoint status = %d body=%s, want 200", accepted.Code, accepted.Body.String())
	}
	storedTransfer, err := stateStore.GetDataTransfer(transfer.ID)
	if err != nil {
		t.Fatalf("get canonicalized compact transfer: %v", err)
	}
	if len(storedTransfer.PlanBlobs) != 1 {
		t.Fatalf("canonical compact checkpoint blobs = %+v", storedTransfer.PlanBlobs)
	}
	storedBlob := storedTransfer.PlanBlobs[0]
	if storedBlob.SHA256 != digest || storedBlob.Size != int64(len(content)) || storedBlob.ObjectKey != model.DataObjectKey(digest) || storedBlob.UploadMode != model.DataBlobUploadModeSingle || !storedBlob.Exists || storedBlob.UploadURL != "" {
		t.Fatalf("compact checkpoint was not canonicalized: %+v", storedBlob)
	}
}

func TestGetDataBlobImplementsConditionalAndSingleRangeContract(t *testing.T) {
	t.Parallel()

	stateStore, server, ownerKey, _, workspace := setupDataBlobSecurityServer(t)
	content := "0123456789"
	digest := testDataDigestString(content)
	if _, err := stateStore.WriteDataBlob(digest, strings.NewReader(content)); err != nil {
		t.Fatalf("write fixture blob: %v", err)
	}
	transfer := createDataBlobSecurityTransfer(t, stateStore, workspace, model.DataTransferDirectionDownload, digest, int64(len(content)))

	full := performDataBlobSecurityRequest(t, server, http.MethodGet, digest, transfer.ID, ownerKey, "", nil, nil)
	if full.Code != http.StatusOK || full.Body.String() != content {
		t.Fatalf("full response status=%d body=%q", full.Code, full.Body.String())
	}
	if full.Header().Get("Accept-Ranges") != "bytes" || full.Header().Get("ETag") != `"`+digest+`"` || full.Header().Get("X-Fugue-Data-SHA256") != digest {
		t.Fatalf("full response headers = %+v", full.Header())
	}

	partial := performDataBlobSecurityRequest(t, server, http.MethodGet, digest, transfer.ID, ownerKey, "", nil, func(request *http.Request) {
		request.Header.Set("Range", "bytes=2-5")
	})
	if partial.Code != http.StatusPartialContent || partial.Body.String() != "2345" {
		t.Fatalf("partial response status=%d body=%q", partial.Code, partial.Body.String())
	}
	if partial.Header().Get("Content-Range") != "bytes 2-5/10" || partial.Header().Get("Content-Length") != "4" {
		t.Fatalf("partial response headers = %+v", partial.Header())
	}

	notModified := performDataBlobSecurityRequest(t, server, http.MethodGet, digest, transfer.ID, ownerKey, "", nil, func(request *http.Request) {
		request.Header.Set("If-None-Match", `"`+digest+`"`)
	})
	if notModified.Code != http.StatusNotModified || notModified.Body.Len() != 0 {
		t.Fatalf("ETag conditional response status=%d body=%q", notModified.Code, notModified.Body.String())
	}

	modifiedSince := performDataBlobSecurityRequest(t, server, http.MethodGet, digest, transfer.ID, ownerKey, "", nil, func(request *http.Request) {
		request.Header.Set("If-Modified-Since", full.Header().Get("Last-Modified"))
	})
	if modifiedSince.Code != http.StatusNotModified {
		t.Fatalf("If-Modified-Since status=%d body=%q", modifiedSince.Code, modifiedSince.Body.String())
	}

	ifRangeMiss := performDataBlobSecurityRequest(t, server, http.MethodGet, digest, transfer.ID, ownerKey, "", nil, func(request *http.Request) {
		request.Header.Set("Range", "bytes=2-5")
		request.Header.Set("If-Range", `"different"`)
	})
	if ifRangeMiss.Code != http.StatusOK || ifRangeMiss.Body.String() != content {
		t.Fatalf("If-Range miss status=%d body=%q", ifRangeMiss.Code, ifRangeMiss.Body.String())
	}

	invalidRange := performDataBlobSecurityRequest(t, server, http.MethodGet, digest, transfer.ID, ownerKey, "", nil, func(request *http.Request) {
		request.Header.Set("Range", "bytes=0-1,4-5")
	})
	if invalidRange.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("invalid Range status=%d body=%s, want 416", invalidRange.Code, invalidRange.Body.String())
	}
	if invalidRange.Header().Get("Content-Type") != "application/json" || invalidRange.Header().Get("Content-Range") != "bytes */10" {
		t.Fatalf("invalid Range headers = %+v", invalidRange.Header())
	}
	var errorResponse map[string]any
	if err := json.Unmarshal(invalidRange.Body.Bytes(), &errorResponse); err != nil || errorResponse["error"] == "" {
		t.Fatalf("invalid Range did not return ErrorResponse: body=%s err=%v", invalidRange.Body.String(), err)
	}
}

func setupDataBlobSecurityServer(t *testing.T) (*store.Store, *Server, string, string, model.DataWorkspace) {
	t.Helper()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	ownerTenant, err := stateStore.CreateTenant("Blob Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	otherTenant, err := stateStore.CreateTenant("Other Blob Tenant")
	if err != nil {
		t.Fatalf("create other tenant: %v", err)
	}
	_, ownerKey, err := stateStore.CreateAPIKey(ownerTenant.ID, "blob-owner", []string{"data.read", "data.write"})
	if err != nil {
		t.Fatalf("create owner key: %v", err)
	}
	_, otherTenantKey, err := stateStore.CreateAPIKey(otherTenant.ID, "blob-other", []string{"data.read", "data.write"})
	if err != nil {
		t.Fatalf("create other key: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{TenantID: ownerTenant.ID, Name: "Blob Workspace"})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	return stateStore, server, ownerKey, otherTenantKey, workspace
}

func createDataBlobSecurityTransfer(t *testing.T, stateStore *store.Store, workspace model.DataWorkspace, direction, digest string, size int64) model.DataTransfer {
	t.Helper()

	transfer, err := stateStore.CreateDataTransfer(model.DataTransfer{
		TenantID:    workspace.TenantID,
		WorkspaceID: workspace.ID,
		Direction:   direction,
		Status:      model.DataTransferStatusPlanned,
		BytesTotal:  size,
		FilesTotal:  1,
		PlanBlobs: []model.DataTransferPlanBlob{{
			SHA256:    digest,
			Size:      size,
			ObjectKey: model.DataObjectKey(digest),
		}},
	})
	if err != nil {
		t.Fatalf("create transfer: %v", err)
	}
	return transfer
}

func performDataBlobSecurityRequest(t *testing.T, server *Server, method, digestPath, transferID, apiKey, contentType string, body io.Reader, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()

	target := "/v1/data/blobs/" + digestPath + "?transfer_id=" + url.QueryEscape(transferID)
	request := httptest.NewRequest(method, target, body)
	request.Header.Set("Authorization", "Bearer "+apiKey)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if mutate != nil {
		mutate(request)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}
