package api

import (
	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorageMaintenanceRequiresAdminAndPersistsPreview(t *testing.T) {
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := db.CreateTenant("test")
	_, admin, _ := db.CreateAPIKey(tenant.ID, "admin", []string{"platform.admin"})
	_, reader, _ := db.CreateAPIKey(tenant.ID, "reader", []string{"storage.admin"})
	s := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	for _, tc := range []struct {
		token, body string
		status      int
	}{
		{reader, `{"kind":"longhorn-reconcile"}`, 403},
		{admin, `{"kind":"unsupported"}`, 400},
		{admin, `{"kind":"longhorn-reconcile","scope_id":"foreign"}`, 400},
		{admin, `{"kind":"longhorn-reconcile"}`, 202},
	} {
		r := httptest.NewRequest("POST", "/v1/admin/storage/jobs", strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("status %d want %d: %s", w.Code, tc.status, w.Body.String())
		}
	}
	jobs, err := db.ListStorageMaintenanceJobs()
	if err != nil || len(jobs) != 1 || jobs[0].Execute || jobs[0].Status != "queued" {
		t.Fatalf("jobs %+v %v", jobs, err)
	}
	result, err := s.runStorageMaintenance(t.Context(), model.StorageMaintenanceJob{Kind: "retention"})
	if err != nil || result["preview"] != true {
		t.Fatalf("preview %+v %v", result, err)
	}
}
