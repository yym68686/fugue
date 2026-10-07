package api

import (
	"fugue/internal/auth"
	"fugue/internal/store"
	"net/http"
	"path/filepath"
	"testing"
)

func TestImageOrphanPolicyAdminScopeCASAndValidation(t *testing.T) {
	st := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant("sample")
	if err != nil {
		t.Fatal(err)
	}
	_, admin, err := st.CreateAPIKey(tenant.ID, "admin", []string{"platform.admin"})
	if err != nil {
		t.Fatal(err)
	}
	_, user, err := st.CreateAPIKey(tenant.ID, "reader", []string{"apps.read"})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(st, auth.New(st, ""), nil, ServerConfig{})
	body := map[string]any{"expected_generation": 0, "mode": "retire", "repository_prefixes": []string{"apps"}, "quarantine_seconds": 600, "minimum_observations": 2, "inventory_max_age_seconds": 7200}
	if r := performJSONRequest(t, server, http.MethodPut, "/v1/admin/image-cache/orphan-policy", user, body); r.Code != http.StatusForbidden {
		t.Fatal(r.Code, r.Body.String())
	}
	if r := performJSONRequest(t, server, http.MethodPut, "/v1/admin/image-cache/orphan-policy", admin, body); r.Code != http.StatusOK {
		t.Fatal(r.Code, r.Body.String())
	}
	if r := performJSONRequest(t, server, http.MethodPut, "/v1/admin/image-cache/orphan-policy", admin, body); r.Code != http.StatusConflict {
		t.Fatal(r.Code, r.Body.String())
	}
	body["expected_generation"] = 1
	body["quarantine_seconds"] = 1
	if r := performJSONRequest(t, server, http.MethodPut, "/v1/admin/image-cache/orphan-policy", admin, body); r.Code != http.StatusBadRequest {
		t.Fatal(r.Code, r.Body.String())
	}
	if r := performFormRequest(t, server, http.MethodGet, "/v1/admin/image-cache/orphans", admin, nil); r.Code != http.StatusOK {
		t.Fatal(r.Code, r.Body.String())
	}
}
