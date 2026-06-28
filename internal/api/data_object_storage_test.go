package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDataPresignTTLClamp(t *testing.T) {
	t.Setenv("FUGUE_DATA_PRESIGN_TTL", "1m")
	if got := dataPresignTTL(); got != minDataPresignTTL {
		t.Fatalf("expected minimum TTL %s, got %s", minDataPresignTTL, got)
	}
	t.Setenv("FUGUE_DATA_PRESIGN_TTL", "72h")
	if got := dataPresignTTL(); got != maxDataPresignTTL {
		t.Fatalf("expected maximum TTL %s, got %s", maxDataPresignTTL, got)
	}
	t.Setenv("FUGUE_DATA_PRESIGN_TTL", "30m")
	if got := dataPresignTTL(); got != 30*time.Minute {
		t.Fatalf("expected configured TTL 30m, got %s", got)
	}
	t.Setenv("FUGUE_DATA_PRESIGN_TTL", "not-a-duration")
	if got := dataPresignTTL(); got != defaultDataPresignTTL {
		t.Fatalf("expected default TTL %s, got %s", defaultDataPresignTTL, got)
	}
}

func TestPresignedURLScopesObjectAndMultipartPart(t *testing.T) {
	backend, err := newDataObjectBackend(model.DataBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
		Region:   "auto",
		Prefix:   "tenant-a/workspace-a",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
		Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderCloudflareR2),
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	objectKey := "blobs/sha256/aa/bb/" + strings.Repeat("a", 64)
	putURL, _, err := backend.presignPut(context.Background(), objectKey, 15*time.Minute)
	if err != nil {
		t.Fatalf("presign put: %v", err)
	}
	parsed, err := url.Parse(putURL)
	if err != nil {
		t.Fatalf("parse put url: %v", err)
	}
	if !strings.Contains(parsed.Path, "/bucket/tenant-a/workspace-a/"+objectKey) {
		t.Fatalf("presigned put path is not object scoped: %s", parsed.String())
	}
	partURL, _, err := backend.presignUploadPart(context.Background(), objectKey, "upload-1", 7, 15*time.Minute)
	if err != nil {
		t.Fatalf("presign part: %v", err)
	}
	parsed, err = url.Parse(partURL)
	if err != nil {
		t.Fatalf("parse part url: %v", err)
	}
	if parsed.Query().Get("uploadId") != "upload-1" || parsed.Query().Get("partNumber") != "7" {
		t.Fatalf("presigned part URL is not scoped to upload and part: %s", parsed.String())
	}
}

func TestPutObjectMultipartUploadsPartsAndCompletes(t *testing.T) {
	fake := newFakeS3MultipartServer(t, 0)
	defer fake.Close()
	backend := newTestDataObjectBackend(t, fake.URL)
	payload := bytes.Repeat([]byte("a"), int(dataMultipartMinPartSize*2+17))

	if err := backend.putObjectMultipart(context.Background(), "backups/control-plane.dump", bytes.NewReader(payload), int64(len(payload)), dataMultipartMinPartSize); err != nil {
		t.Fatalf("put multipart object: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.createCalls != 1 {
		t.Fatalf("expected one create multipart call, got %d", fake.createCalls)
	}
	if fake.abortCalls != 0 {
		t.Fatalf("expected no abort call, got %d", fake.abortCalls)
	}
	if fake.completeCalls != 1 {
		t.Fatalf("expected one complete call, got %d", fake.completeCalls)
	}
	if got := len(fake.parts); got != 3 {
		t.Fatalf("expected 3 uploaded parts, got %d", got)
	}
	if int64(len(fake.parts[1])) != dataMultipartMinPartSize || int64(len(fake.parts[2])) != dataMultipartMinPartSize || len(fake.parts[3]) != 17 {
		t.Fatalf("unexpected part sizes: part1=%d part2=%d part3=%d", len(fake.parts[1]), len(fake.parts[2]), len(fake.parts[3]))
	}
	for _, want := range []string{"<PartNumber>1</PartNumber>", "<ETag>etag-1</ETag>", "<PartNumber>2</PartNumber>", "<ETag>etag-2</ETag>", "<PartNumber>3</PartNumber>", "<ETag>etag-3</ETag>"} {
		if !strings.Contains(fake.completeBody, want) {
			t.Fatalf("complete request missing %q: %s", want, fake.completeBody)
		}
	}
}

func TestPutObjectMultipartAbortsOnPartFailure(t *testing.T) {
	fake := newFakeS3MultipartServer(t, 2)
	defer fake.Close()
	backend := newTestDataObjectBackend(t, fake.URL)
	payload := bytes.Repeat([]byte("b"), int(dataMultipartMinPartSize*2+1))

	err := backend.putObjectMultipart(context.Background(), "backups/control-plane.dump", bytes.NewReader(payload), int64(len(payload)), dataMultipartMinPartSize)
	if err == nil {
		t.Fatal("expected multipart upload to fail")
	}
	if !strings.Contains(err.Error(), "upload multipart part 2") {
		t.Fatalf("expected part failure, got %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.createCalls != 1 {
		t.Fatalf("expected one create multipart call, got %d", fake.createCalls)
	}
	if fake.completeCalls != 0 {
		t.Fatalf("expected no complete call, got %d", fake.completeCalls)
	}
	if fake.abortCalls != 1 {
		t.Fatalf("expected one abort call, got %d", fake.abortCalls)
	}
}

func TestDataMultipartPartSizeForObjectKeepsPartCountWithinS3Limit(t *testing.T) {
	size := dataMultipartPartSize*dataMultipartMaxParts + 1
	partSize, err := dataMultipartPartSizeForObject(size)
	if err != nil {
		t.Fatalf("part size for object: %v", err)
	}
	if partSize <= dataMultipartPartSize {
		t.Fatalf("expected dynamic part size above default, got %d", partSize)
	}
	if partSize%dataMultipartMinPartSize != 0 {
		t.Fatalf("expected part size rounded to S3 minimum multiple, got %d", partSize)
	}
	parts := ((size - 1) / partSize) + 1
	if parts > dataMultipartMaxParts {
		t.Fatalf("expected at most %d parts, got %d with part size %d", dataMultipartMaxParts, parts, partSize)
	}
}

type fakeS3MultipartServer struct {
	*httptest.Server
	t             *testing.T
	mu            sync.Mutex
	failPart      int32
	createCalls   int
	completeCalls int
	abortCalls    int
	parts         map[int32][]byte
	completeBody  string
}

func newFakeS3MultipartServer(t *testing.T, failPart int32) *fakeS3MultipartServer {
	t.Helper()
	fake := &fakeS3MultipartServer{t: t, failPart: failPart, parts: map[int32][]byte{}}
	fake.Server = httptest.NewServer(http.HandlerFunc(fake.handle))
	return fake
}

func (f *fakeS3MultipartServer) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && hasQueryKey(r, "uploads"):
		f.mu.Lock()
		f.createCalls++
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<CreateMultipartUploadResult><Bucket>bucket</Bucket><Key>backups/control-plane.dump</Key><UploadId>upload-1</UploadId></CreateMultipartUploadResult>`))
	case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") == "upload-1":
		partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
		if err != nil {
			http.Error(w, "invalid part number", http.StatusBadRequest)
			return
		}
		if int32(partNumber) == f.failPart {
			http.Error(w, "part failed", http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if r.ContentLength != int64(len(body)) {
			http.Error(w, fmt.Sprintf("content length %d did not match body %d", r.ContentLength, len(body)), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.parts[int32(partNumber)] = body
		f.mu.Unlock()
		w.Header().Set("ETag", fmt.Sprintf(`"etag-%d"`, partNumber))
	case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") == "upload-1":
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		f.mu.Lock()
		f.completeCalls++
		f.completeBody = string(body)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><ETag>&quot;etag-final&quot;</ETag></CompleteMultipartUploadResult>`))
	case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") == "upload-1":
		f.mu.Lock()
		f.abortCalls++
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		f.t.Errorf("unexpected fake s3 request %s %s", r.Method, r.URL.String())
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

func newTestDataObjectBackend(t *testing.T, endpoint string) *dataObjectBackend {
	t.Helper()
	backend, err := newDataObjectBackend(model.DataBackend{
		Name:     "fake-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: endpoint,
		Region:   "us-east-1",
		Prefix:   "",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
		Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderS3),
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	return backend
}
