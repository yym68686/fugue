package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlatformAppListPreservesPaginationAndSubsequentWrites(t *testing.T) {
	t.Parallel()
	stateStore, server, _, app := setupSearchTestServer(t)
	_, key, err := stateStore.CreateAPIKey(app.TenantID, "platform-reader", []string{"platform.admin"})
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/apps?include_resource_usage=false&include_live_status=false"
	initial := performJSONRequest(t, server, http.MethodGet, path, key, nil)
	if initial.Code != http.StatusOK {
		t.Fatalf("initial response: %d", initial.Code)
	}
	if _, err := stateStore.CreateApp(app.TenantID, app.ProjectID, "second-service", "", model.AppSpec{
		Image: "ghcr.io/example/second:latest", Ports: []int{8080}, Replicas: 1,
	}); err != nil {
		t.Fatal(err)
	}
	latest := performJSONRequest(t, server, http.MethodGet, path, key, nil)
	var list struct {
		Apps []model.App `json:"apps"`
	}
	mustDecodeJSON(t, latest, &list)
	if len(list.Apps) != 2 {
		t.Fatalf("write not visible in next list: %d apps", len(list.Apps))
	}
	page := performJSONRequest(t, server, http.MethodGet, path+"&limit=1", key, nil)
	var paged struct {
		Apps []model.App      `json:"apps"`
		Page *appListPageInfo `json:"page_info"`
	}
	mustDecodeJSON(t, page, &paged)
	if len(paged.Apps) != 1 || paged.Page == nil || !paged.Page.HasNextPage {
		t.Fatalf("platform pagination ignored: count=%d page=%+v", len(paged.Apps), paged.Page)
	}
	invalid := performJSONRequest(t, server, http.MethodGet, path+"&limit=201", key, nil)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid platform limit: expected 400, got %d", invalid.Code)
	}
}

func TestBillingReadsChangesWrittenByAnotherProcess(t *testing.T) {
	t.Parallel()
	stateStore, server, key, app := setupSearchTestServer(t)
	path := "/v1/billing?include_current_usage=false"
	initial := performJSONRequest(t, server, http.MethodGet, path, key, nil)
	if initial.Code != http.StatusOK {
		t.Fatalf("initial billing response: %d", initial.Code)
	}
	updated, err := stateStore.SetTenantBillingBalance(app.TenantID, 12345, nil)
	if err != nil {
		t.Fatal(err)
	}
	latest := performJSONRequest(t, server, http.MethodGet, path, key, nil)
	var response struct {
		Billing model.TenantBillingSummary `json:"billing"`
	}
	mustDecodeJSON(t, latest, &response)
	if response.Billing.BalanceMicroCents != updated.BalanceMicroCents {
		t.Fatalf("billing reused a process-local stale balance: got %d want %d", response.Billing.BalanceMicroCents, updated.BalanceMicroCents)
	}
	if response.Billing.CurrentUsage != nil {
		t.Fatal("include_current_usage=false unexpectedly returned usage")
	}
}

func TestGalleryDoesNotServeExpiredSummaryIndefinitely(t *testing.T) {
	t.Parallel()
	_, server, _, app := setupSearchTestServer(t)
	principal := model.Principal{TenantID: app.TenantID}
	key := consoleGalleryCacheKey(principal, false)
	server.consoleGalleryCache.byKey[key] = expiringResponseCacheEntry[consoleGalleryResponse]{
		value:     consoleGalleryResponse{Projects: []consoleProjectSummary{{ID: "expired-project"}}},
		expiresAt: time.Now().Add(-time.Hour), ok: true,
	}
	response, err := server.cachedConsoleGalleryResponse(context.Background(), principal, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Projects) != 1 || response.Projects[0].ID != app.ProjectID {
		t.Fatalf("expired summary substituted for current projects: %+v", response.Projects)
	}
}

func TestBillingServerTimingDistinguishesLedgerAndUsage(t *testing.T) {
	t.Parallel()
	_, server, key, _ := setupSearchTestServer(t)
	response := performJSONRequest(t, server, http.MethodGet, "/v1/billing?include_current_usage=true", key, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("billing response: %d", response.Code)
	}
	header := response.Header().Get("Server-Timing")
	for _, name := range []string{"billing_summary", "billing_current_usage", "billing_usage_apps", "billing_usage_runtimes", "billing_usage_inventory"} {
		if !strings.Contains(header, name+";dur=") {
			t.Fatalf("missing %s timing in %q", name, header)
		}
	}
	response = performJSONRequest(t, server, http.MethodGet, "/v1/billing?include_current_usage=false", key, nil)
	if strings.Contains(response.Header().Get("Server-Timing"), "billing_usage_") {
		t.Fatal("usage timing present when usage was not requested")
	}
}
