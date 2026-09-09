package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestBillingSummariesRequiresPlatformAdminAndKeepsAllTenants(t *testing.T) {
	state, server, tenantKey, app := setupSearchTestServer(t)
	_, adminKey, err := state.CreateAPIKey(app.TenantID, "platform", []string{"platform.admin"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := state.CreateTenant("other tenant")
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/billing/summaries?tenant_ids=" + app.TenantID + "," + other.ID + "," + app.TenantID + "&include_current_usage=false"
	denied := performJSONRequest(t, server, http.MethodGet, path, tenantKey, nil)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", denied.Code)
	}
	response := performJSONRequest(t, server, http.MethodGet, path, adminKey, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Billings []model.TenantBillingSummary `json:"billings"`
	}
	mustDecodeJSON(t, response, &body)
	if len(body.Billings) != 2 || body.Billings[0].TenantID != app.TenantID || body.Billings[1].TenantID != other.ID {
		t.Fatalf("wrong tenant order/count: %+v", body.Billings)
	}
	for _, summary := range body.Billings {
		if summary.CurrentUsage != nil {
			t.Fatal("usage must be absent when disabled")
		}
	}
	missing := performJSONRequest(t, server, http.MethodGet, "/v1/billing/summaries?tenant_ids="+app.TenantID+",missing&include_current_usage=false", adminKey, nil)
	if missing.Code != http.StatusOK {
		t.Fatalf("absent tenant should be explicit: %d", missing.Code)
	}
	var absent struct {
		Billings []model.TenantBillingSummary `json:"billings"`
		Missing  []string                     `json:"missing_tenant_ids"`
	}
	mustDecodeJSON(t, missing, &absent)
	if len(absent.Billings) != 1 || len(absent.Missing) != 1 || absent.Missing[0] != "missing" {
		t.Fatalf("wrong absent tenant result: %+v", absent)
	}
}
