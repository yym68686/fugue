package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthOnlyReportsVerifiedOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner, ownerErr := "api", error(nil)
	h := healthHandler(ctx, func(context.Context) (string, error) { return owner, ownerErr })
	request := func(path string, status int, body string) {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != status || w.Body.String() != body {
			t.Fatalf("%s returned %d %q; want %d %q", path, w.Code, w.Body.String(), status, body)
		}
	}
	request("/healthz", 200, "ok\n")
	request("/readyz", 200, "ok\n")
	request("/metrics", 200, "fugue_database_import_worker_store_available 1\nfugue_database_import_worker_owns_execution 0\n")
	owner = "worker"
	request("/metrics", 200, "fugue_database_import_worker_store_available 1\nfugue_database_import_worker_owns_execution 1\n")
	ownerErr = errors.New("sensitive dependency error")
	request("/readyz", 503, "unavailable\n")
	request("/healthz", 200, "ok\n")
	request("/metrics", 200, "fugue_database_import_worker_store_available 0\n")
	ownerErr = nil
	cancel()
	request("/readyz", 503, "unavailable\n")
}
