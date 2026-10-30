package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestAppMailHostnameRequiresExactOwnedApplicationSubdomain(t *testing.T) {
	for _, host := range []string{"apps.example.test", "nested.app.apps.example.test", "foreign.example.test", "-invalid.apps.example.test"} {
		if _, err := appMailHostname(model.App{Route: &model.AppRoute{Hostname: host}}, "apps.example.test"); err == nil {
			t.Fatal("accepted", host)
		}
	}
	if host, err := appMailHostname(model.App{Route: &model.AppRoute{Hostname: "app.apps.example.test"}}, "apps.example.test"); err != nil || host != "app.apps.example.test" {
		t.Fatal(host, err)
	}
}

func TestAppMailAPIRequiresOwnershipCASAndReportsUnreconciledIntent(t *testing.T) {
	s, server, writer, admin, app, _ := setupAppDomainTestServer(t)
	if err := s.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: app.ID, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	for _, input := range []any{map[string]any{"state": "active"}, map[string]any{"state": "active", "expected_generation": 0, "local_part": "invalid@name"}, map[string]any{"state": "active", "expected_generation": 0, "extra": true}} {
		if res := performJSONRequest(t, server, http.MethodPut, "/v1/apps/"+app.ID+"/mail", writer, input); res.Code != 400 {
			t.Fatal(res.Code, res.Body.String())
		}
	}
	res := performJSONRequest(t, server, http.MethodPut, "/v1/apps/"+app.ID+"/mail", writer, map[string]any{"state": "active", "expected_generation": 0})
	if res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	var observed struct {
		Binding    model.AppMailBinding `json:"binding"`
		Reconciled bool                 `json:"reconciled"`
		Error      string               `json:"error"`
	}
	mustDecodeJSON(t, res, &observed)
	if observed.Reconciled || observed.Error == "" || observed.Binding.Generation != 1 {
		t.Fatal("unavailable provider reported success", observed)
	}
	if res = performJSONRequest(t, server, http.MethodPut, "/v1/apps/"+app.ID+"/mail", writer, map[string]any{"state": "suspended", "expected_generation": 0}); res.Code != 409 {
		t.Fatal(res.Code, res.Body.String())
	}
	if res = performJSONRequest(t, server, http.MethodGet, "/v1/admin/mail", writer, nil); res.Code != 403 {
		t.Fatal("nonadmin read platform config", res.Code)
	}
	if res = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/mail", admin, nil); res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	other, _ := s.CreateTenant("Other mail tenant")
	_, key, err := s.CreateAPIKey(other.ID, "other", []string{"app.read", "app.write"})
	if err != nil {
		t.Fatal(err)
	}
	if res = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/mail", key, nil); res.Code != 403 && res.Code != 404 {
		t.Fatal("cross tenant mail read", res.Code)
	}
	if res = performJSONRequest(t, server, http.MethodPut, "/v1/apps/"+app.ID+"/mail", key, map[string]any{"state": "revoked", "expected_generation": 1}); res.Code != 403 && res.Code != 404 {
		t.Fatal("cross tenant mail write", res.Code)
	}
}

func TestAppMailProviderObservationMustMatchSavedGenerationAndOwner(t *testing.T) {
	b := model.AppMailBinding{AppID: "app", TenantID: "tenant", ProjectID: "project", Hostname: "app.apps.example.test", Generation: 3, State: "suspended"}
	value := map[string]any{"appId": b.AppID, "tenantId": b.TenantID, "projectId": b.ProjectID, "hostname": b.Hostname, "generation": b.Generation, "state": b.State}
	raw, _ := json.Marshal(value)
	if err := validateMailObservation(raw, b); err != nil {
		t.Fatal(err)
	}
	for key, altered := range map[string]any{"tenantId": "other", "projectId": "other", "hostname": "other.apps.example.test", "generation": 2, "state": "active"} {
		copy := map[string]any{}
		for k, v := range value {
			copy[k] = v
		}
		copy[key] = altered
		raw, _ = json.Marshal(copy)
		if err := validateMailObservation(raw, b); err == nil {
			t.Fatal("accepted stale or foreign observation", key)
		}
	}
}
