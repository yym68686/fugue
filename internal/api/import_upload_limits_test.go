package api

import (
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSourceUploadConcurrencyLimitReturns429WithRetryAfter(t *testing.T) {
	t.Parallel()

	server := &Server{sourceUploadSlots: make(chan struct{}, 1)}
	firstRequest := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", http.NoBody)
	firstRecorder := httptest.NewRecorder()
	finish, ok := server.beginSourceUploadRequest(firstRecorder, firstRequest)
	if !ok {
		t.Fatal("expected the first source upload to acquire the slot")
	}
	defer finish()

	secondRequest := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", http.NoBody)
	secondRecorder := httptest.NewRecorder()
	if secondFinish, secondOK := server.beginSourceUploadRequest(secondRecorder, secondRequest); secondOK {
		secondFinish()
		t.Fatal("expected the second source upload to be rejected")
	}
	if secondRecorder.Code != http.StatusTooManyRequests {
		t.Fatalf("expected status %d, got %d", http.StatusTooManyRequests, secondRecorder.Code)
	}
	if got := secondRecorder.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("expected Retry-After 5, got %q", got)
	}
}

func TestParseImportUploadMultipartTimesOutStalledBody(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", reader)
	req.Header.Set("Content-Type", "multipart/form-data; boundary=stalled-upload")
	stopDeadline := startSourceUploadDeadline(req, 10*time.Millisecond)
	defer stopDeadline()
	recorder := httptest.NewRecorder()

	err := parseImportUploadMultipart(recorder, req)
	if got := importUploadErrorStatus(err); got != http.StatusRequestTimeout {
		t.Fatalf("expected status %d, got %d from %v", http.StatusRequestTimeout, got, err)
	}
}

func TestParseImportUploadMultipartRejectsUnsupportedMediaType(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	err := parseImportUploadMultipart(recorder, req)
	assertImportUploadErrorResponse(t, recorder, err, http.StatusUnsupportedMediaType)
}

func TestParseImportUploadMultipartRejectsKnownOversizedRequestBeforeReading(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", http.NoBody)
	req.Header.Set("Content-Type", "multipart/form-data; boundary=bounded-upload")
	req.ContentLength = maxSourceUploadRequestBytes + 1
	recorder := httptest.NewRecorder()

	err := parseImportUploadMultipart(recorder, req)
	assertImportUploadErrorResponse(t, recorder, err, http.StatusRequestEntityTooLarge)
}

func TestValidateImportUploadMultipartFieldsRejectsOversizedArchive(t *testing.T) {
	t.Parallel()

	err := validateImportUploadMultipartFields(&multipart.Form{
		Value: map[string][]string{"request": {"{}"}},
		File: map[string][]*multipart.FileHeader{
			"archive": {{Filename: "source.tgz", Size: maxSourceUploadArchiveBytes + 1}},
		},
	})
	if got := importUploadErrorStatus(err); got != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status %d, got %d from %v", http.StatusRequestEntityTooLarge, got, err)
	}
}

func TestDecodeImportUploadMultipartRejectsUnsupportedArchiveAs415(t *testing.T) {
	t.Parallel()

	body, contentType := newImportUploadMultipartBody(
		t,
		importUploadRequest{Name: "invalid-archive"},
		"source.zip",
		[]byte("not-a-zip"),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/apps/import-upload", body)
	req.Header.Set("Content-Type", contentType)
	recorder := httptest.NewRecorder()
	if err := parseImportUploadMultipart(recorder, req); err != nil {
		t.Fatalf("parse multipart request: %v", err)
	}
	t.Cleanup(func() {
		if req.MultipartForm != nil {
			_ = req.MultipartForm.RemoveAll()
		}
	})

	_, _, _, err := decodeImportUploadMultipart(req)
	if got := importUploadErrorStatus(err); got != http.StatusUnsupportedMediaType {
		t.Fatalf("expected status %d, got %d from %v", http.StatusUnsupportedMediaType, got, err)
	}
}

func assertImportUploadErrorResponse(t *testing.T, recorder *httptest.ResponseRecorder, err error, wantStatus int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected upload request error with status %d", wantStatus)
	}
	writeImportUploadError(recorder, err)
	if recorder.Code != wantStatus {
		t.Fatalf("expected status %d, got %d body=%s", wantStatus, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Error string `json:"error"`
	}
	if decodeErr := json.Unmarshal(recorder.Body.Bytes(), &response); decodeErr != nil {
		t.Fatalf("decode error response: %v", decodeErr)
	}
	if strings.TrimSpace(response.Error) == "" {
		t.Fatalf("expected a stable JSON error message, got %s", recorder.Body.String())
	}
}
