package api

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImportUploadAppQueuesPendingImport(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Upload Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "uploader", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	archiveBytes := mustTarGz(t, map[string]string{
		"index.html": "<h1>demo</h1>\n",
	})
	body, contentType := newImportUploadMultipartBody(t, importUploadRequest{
		Name:          "demo-app",
		BuildStrategy: model.AppBuildStrategyStaticSite,
	}, "demo-app.tgz", archiveBytes)

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App       model.App       `json:"app"`
		Operation model.Operation `json:"operation"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.App.ID == "" {
		t.Fatal("expected app id in response")
	}
	if response.Operation.ID == "" {
		t.Fatal("expected operation id in response")
	}
	if response.Operation.Type != model.OperationTypeImport {
		t.Fatalf("expected import operation, got %q", response.Operation.Type)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected upload source type, got %q", op.DesiredSource.Type)
	}
	if op.DesiredSource.UploadID == "" {
		t.Fatal("expected upload id on queued source")
	}

	upload, archiveData, err := s.GetSourceUploadArchive(op.DesiredSource.UploadID)
	if err != nil {
		t.Fatalf("get source upload archive: %v", err)
	}
	if upload.TenantID != tenant.ID {
		t.Fatalf("expected upload tenant %q, got %q", tenant.ID, upload.TenantID)
	}
	if len(archiveData) == 0 {
		t.Fatal("expected stored archive bytes")
	}
}

func TestGetSourceUploadArchiveRequiresDownloadToken(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Download Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	archiveBytes := mustTarGz(t, map[string]string{
		"index.html": "<h1>download</h1>\n",
	})
	upload, err := s.CreateSourceUpload(tenant.ID, "site.tgz", "application/gzip", archiveBytes)
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	missingTokenReq := httptest.NewRequest(http.MethodGet, "/v1/source-uploads/"+upload.ID+"/archive", nil)
	missingTokenRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingTokenRecorder, missingTokenReq)
	if missingTokenRecorder.Code != http.StatusBadRequest {
		t.Fatalf("expected missing token status %d, got %d body=%s", http.StatusBadRequest, missingTokenRecorder.Code, missingTokenRecorder.Body.String())
	}

	invalidTokenReq := httptest.NewRequest(http.MethodGet, "/v1/source-uploads/"+upload.ID+"/archive?download_token=wrong", nil)
	invalidTokenRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(invalidTokenRecorder, invalidTokenReq)
	if invalidTokenRecorder.Code != http.StatusNotFound {
		t.Fatalf("expected invalid token status %d, got %d body=%s", http.StatusNotFound, invalidTokenRecorder.Code, invalidTokenRecorder.Body.String())
	}

	validTokenReq := httptest.NewRequest(http.MethodGet, "/v1/source-uploads/"+upload.ID+"/archive", nil)
	validTokenQuery := url.Values{}
	validTokenQuery.Set("download_token", upload.DownloadToken)
	validTokenReq.URL.RawQuery = validTokenQuery.Encode()
	validTokenRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(validTokenRecorder, validTokenReq)
	if validTokenRecorder.Code != http.StatusOK {
		t.Fatalf("expected valid token status %d, got %d body=%s", http.StatusOK, validTokenRecorder.Code, validTokenRecorder.Body.String())
	}
	if got := validTokenRecorder.Body.Bytes(); !bytes.Equal(got, archiveBytes) {
		t.Fatalf("unexpected archive response body length=%d want=%d", len(got), len(archiveBytes))
	}
}

func newImportUploadMultipartBody(t *testing.T, req importUploadRequest, archiveName string, archiveBytes []byte) (*bytes.Buffer, string) {
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
	part, err := writer.CreateFormFile("archive", archiveName)
	if err != nil {
		t.Fatalf("create archive part: %v", err)
	}
	if _, err := part.Write(archiveBytes); err != nil {
		t.Fatalf("write archive part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, writer.FormDataContentType()
}

func mustTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	for name, content := range files {
		header := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(content)),
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatalf("write tar header: %v", err)
		}
		if _, err := tarWriter.Write([]byte(content)); err != nil {
			t.Fatalf("write tar content: %v", err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return buffer.Bytes()
}
