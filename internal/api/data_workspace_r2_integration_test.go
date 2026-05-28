package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestCloudflareR2MultipartResumeIntegration(t *testing.T) {
	if os.Getenv("FUGUE_DATA_INTEGRATION_R2") != "1" {
		t.Skip("set FUGUE_DATA_INTEGRATION_R2=1 with R2 backend env vars to run")
	}
	bucket := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_BUCKET"))
	accessKey := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY"))
	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_ENDPOINT")), "/")
	if endpoint == "" {
		accountID := strings.TrimSpace(os.Getenv("FUGUE_DATA_R2_ACCOUNT_ID"))
		if accountID != "" {
			endpoint = fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)
		}
	}
	if bucket == "" || accessKey == "" || secretKey == "" || endpoint == "" {
		t.Skip("R2 integration requires FUGUE_DATA_BACKEND_BUCKET, FUGUE_DATA_BACKEND_ACCESS_KEY_ID, FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY, and FUGUE_DATA_R2_ACCOUNT_ID or FUGUE_DATA_BACKEND_ENDPOINT")
	}
	prefix := strings.Trim(strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_PREFIX")), "/")
	prefix = path.Join(prefix, "integration", fmt.Sprintf("r2-%d", time.Now().UnixNano()))
	backend, err := newDataObjectBackend(model.DataBackend{
		Name:     "r2-integration",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   bucket,
		Region:   "auto",
		Endpoint: endpoint,
		Prefix:   prefix,
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			Token:           strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_SESSION_TOKEN")),
		},
		Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderCloudflareR2),
	})
	if err != nil {
		t.Fatalf("create R2 backend: %v", err)
	}
	ctx := context.Background()
	sourcePath := filepathForR2IntegrationSource(t, dataMultipartPartSize+1024)
	digest, err := sha256FileForR2Integration(sourcePath)
	if err != nil {
		t.Fatalf("hash source: %v", err)
	}
	objectKey := model.DataObjectKey(digest)
	defer func() {
		_ = backend.deleteObjects(ctx, []string{backend.objectKey(objectKey)})
	}()
	uploadID, err := backend.createMultipartUpload(ctx, objectKey)
	if err != nil {
		t.Fatalf("create multipart upload: %v", err)
	}
	defer func() {
		_ = backend.abortMultipartUpload(ctx, objectKey, uploadID)
	}()
	parts, err := (&Server{}).presignDataUploadParts(ctx, backend, objectKey, uploadID, dataMultipartPartSize+1024, dataMultipartPartSize)
	if err != nil {
		t.Fatalf("presign parts: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected two multipart parts, got %+v", parts)
	}
	firstETag, err := uploadR2IntegrationPart(parts[0].UploadURL, sourcePath, parts[0].Offset, parts[0].Size)
	if err != nil {
		t.Fatalf("upload first part: %v", err)
	}
	listed, err := backend.listMultipartParts(ctx, objectKey, uploadID)
	if err != nil {
		t.Fatalf("list uploaded parts: %v", err)
	}
	if len(listed) != 1 || listed[0].PartNumber != 1 || listed[0].ETag == "" {
		t.Fatalf("expected only first uploaded part, got %+v", listed)
	}
	parts[0].ETag = firstETag
	parts[0].Completed = true
	secondETag, err := uploadR2IntegrationPart(parts[1].UploadURL, sourcePath, parts[1].Offset, parts[1].Size)
	if err != nil {
		t.Fatalf("upload second part: %v", err)
	}
	parts[1].ETag = secondETag
	parts[1].Completed = true
	if err := backend.completeMultipartUpload(ctx, objectKey, uploadID, parts); err != nil {
		t.Fatalf("complete multipart upload: %v", err)
	}
	exists, err := backend.headObject(ctx, objectKey)
	if err != nil {
		t.Fatalf("head completed object: %v", err)
	}
	if !exists {
		t.Fatal("completed object does not exist")
	}
	body, _, err := backend.getObject(ctx, objectKey)
	if err != nil {
		t.Fatalf("get completed object: %v", err)
	}
	defer body.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, body); err != nil {
		t.Fatalf("read completed object: %v", err)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != digest {
		t.Fatalf("downloaded R2 object checksum mismatch: got %s want %s", got, digest)
	}
}

func filepathForR2IntegrationSource(t *testing.T, size int64) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "fugue-r2-source-*")
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	defer file.Close()
	chunk := []byte("fugue-r2-integration-data\n")
	var written int64
	for written < size {
		n := int64(len(chunk))
		if remaining := size - written; remaining < n {
			n = remaining
		}
		if _, err := file.Write(chunk[:n]); err != nil {
			t.Fatalf("write source: %v", err)
		}
		written += n
	}
	return file.Name()
}

func sha256FileForR2Integration(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func uploadR2IntegrationPart(uploadURL, sourcePath string, offset, size int64) (string, error) {
	file, err := os.Open(sourcePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	req, err := http.NewRequest(http.MethodPut, uploadURL, io.NewSectionReader(file, offset, size))
	if err != nil {
		return "", err
	}
	req.ContentLength = size
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return "", fmt.Errorf("upload part returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	etag := strings.Trim(resp.Header.Get("ETag"), "\"")
	if etag == "" {
		return "", fmt.Errorf("upload part did not return an ETag")
	}
	return etag, nil
}
