package api

import (
	"context"
	"encoding/json"
	"fugue/internal/auth"
	"fugue/internal/store"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestAdminStorageRequiresPlatformAdminAndReportsUnknowns(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := db.CreateTenant("Storage owner")
	_, admin, _ := db.CreateAPIKey(tenant.ID, "admin", []string{"platform.admin"})
	_, reader, _ := db.CreateAPIKey(tenant.ID, "reader", []string{"storage.admin"})
	s := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	for _, tc := range []struct {
		token  string
		status int
	}{{reader, 403}, {admin, 200}} {
		req := httptest.NewRequest(http.MethodGet, "/v1/admin/storage", nil)
		req.Header.Set("Authorization", "Bearer "+tc.token)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != tc.status {
			t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
		}
		if tc.status == 200 {
			var out adminStorageOverview
			if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if out.BucketsComplete || out.LonghornComplete || len(out.Warnings) == 0 || !strings.HasPrefix(out.BillingStatus, "unavailable") {
				t.Fatalf("unknown sources reported as complete: %+v", out)
			}
		}
	}
}

func TestAdminStorageObservesUntrackedRemoteWithoutDeleting(t *testing.T) {
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("mutating inventory request: %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/longhorn-system/backups") {
			t.Fatalf("unexpected inventory path: %s", r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items":[{"metadata":{"name":"untracked-backup"},"status":{"state":"Completed","volumeName":"example-volume","backupTargetName":"default"}}]}`))
	}))
	defer remote.Close()
	s := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	out := adminStorageOverview{}
	s.adminStorageLonghorn(context.Background(), &kubeLogsClient{baseURL: remote.URL, client: remote.Client()}, &out)
	if !out.LonghornComplete || len(out.Longhorn) != 1 || out.Longhorn[0].Status != "untracked" || out.Longhorn[0].CleanupState != "blocked" {
		t.Fatalf("untracked remote was lost: %+v", out)
	}
}
