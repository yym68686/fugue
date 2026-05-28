package api

import (
	"context"
	"net/url"
	"strings"
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
