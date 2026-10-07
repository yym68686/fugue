package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestPushDoesNotAcknowledgeFailedJournal(t *testing.T) {
	dir := t.TempDir()
	blocked := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blocked, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	cache := &imageCache{manifestDir: blocked, registry: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) })}
	req := httptest.NewRequest(http.MethodPut, "/v2/apps/sample/manifests/unique", strings.NewReader(`{"schemaVersion":2}`))
	rec := httptest.NewRecorder()
	cache.serveRegistryWrite(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("acknowledged non-durable push: %d %s", rec.Code, rec.Body.String())
	}
}
