package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestPlatformDomainBindingAPIAllowsAdminBindingPlatformRootAndSubdomain(t *testing.T) {
	t.Parallel()

	_, server, apiKey, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	tenantRecorder := performJSONRequest(t, server, http.MethodPut, "/v1/admin/domains/www.fugue.pro", apiKey, map[string]any{
		"app_id": app.ID,
	})
	if tenantRecorder.Code != http.StatusForbidden {
		t.Fatalf("expected tenant key to be forbidden, got %d body=%s", tenantRecorder.Code, tenantRecorder.Body.String())
	}

	rootRecorder := performJSONRequest(t, server, http.MethodPut, "/v1/admin/domains/fugue.pro", platformAdminKey, map[string]any{
		"app_id": app.ID,
	})
	if rootRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, rootRecorder.Code, rootRecorder.Body.String())
	}
	var rootResponse struct {
		Binding model.PlatformDomainBinding `json:"binding"`
	}
	mustDecodeJSON(t, rootRecorder, &rootResponse)
	if rootResponse.Binding.Hostname != "fugue.pro" ||
		rootResponse.Binding.AppID != app.ID ||
		rootResponse.Binding.RoutePolicy != model.EdgeRoutePolicyEnabled ||
		rootResponse.Binding.TLSStatus != model.AppDomainTLSStatusReady {
		t.Fatalf("unexpected root binding: %+v", rootResponse.Binding)
	}

	subdomainRecorder := performJSONRequest(t, server, http.MethodPut, "/v1/admin/domains/www.fugue.pro", platformAdminKey, map[string]any{
		"app_id":        app.ID,
		"route_policy":  model.EdgeRoutePolicyCanary,
		"edge_group_id": "edge-group-country-us",
	})
	if subdomainRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, subdomainRecorder.Code, subdomainRecorder.Body.String())
	}
	var subdomainResponse struct {
		Binding model.PlatformDomainBinding `json:"binding"`
	}
	mustDecodeJSON(t, subdomainRecorder, &subdomainResponse)
	if subdomainResponse.Binding.Hostname != "www.fugue.pro" ||
		subdomainResponse.Binding.RoutePolicy != model.EdgeRoutePolicyCanary ||
		subdomainResponse.Binding.EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("unexpected subdomain binding: %+v", subdomainResponse.Binding)
	}

	listRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/domains?zone=fugue.pro", platformAdminKey, nil)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, listRecorder.Code, listRecorder.Body.String())
	}
	var listResponse struct {
		Bindings []model.PlatformDomainBinding `json:"bindings"`
	}
	mustDecodeJSON(t, listRecorder, &listResponse)
	if len(listResponse.Bindings) != 2 {
		t.Fatalf("expected two platform domain bindings, got %+v", listResponse.Bindings)
	}
}

func TestPlatformDomainBindingAPIRejectsReservedSubzones(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	recorder := performJSONRequest(t, server, http.MethodPut, "/v1/admin/domains/d-test.dns.fugue.pro", platformAdminKey, map[string]any{
		"app_id": app.ID,
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
}
