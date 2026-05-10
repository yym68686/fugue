package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestEdgeRoutesBundleDerivesPlatformAndCustomDomainRoutes(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname:    "www.example.com",
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Status:      model.AppDomainStatusVerified,
		TLSStatus:   model.AppDomainTLSStatusReady,
		RouteTarget: server.primaryCustomDomainTarget(app),
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("put verified app domain: %v", err)
	}
	if _, err := storeState.CreateApp(app.TenantID, app.ProjectID, "internal-only", "", model.AppSpec{
		Image:       "ghcr.io/example/internal-only:latest",
		Ports:       []int{8080},
		Replicas:    1,
		RuntimeID:   model.DefaultManagedRuntimeID,
		NetworkMode: model.AppNetworkModeInternal,
	}); err != nil {
		t.Fatalf("create internal app: %v", err)
	}
	if _, err := storeState.CreateApp(app.TenantID, app.ProjectID, "background-only", "", model.AppSpec{
		Image:       "ghcr.io/example/background-only:latest",
		Replicas:    1,
		RuntimeID:   model.DefaultManagedRuntimeID,
		NetworkMode: model.AppNetworkModeBackground,
	}); err != nil {
		t.Fatalf("create background app: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("ETag") == "" {
		t.Fatal("expected route bundle ETag header")
	}

	var bundle model.EdgeRouteBundle
	mustDecodeJSON(t, recorder, &bundle)
	if bundle.Version == "" {
		t.Fatalf("expected bundle version, got %+v", bundle)
	}
	if len(bundle.Routes) != 2 {
		t.Fatalf("expected platform and custom-domain route, got %+v", bundle.Routes)
	}
	platform := edgeRouteByHostAndKind(bundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform)
	if platform == nil {
		t.Fatalf("expected platform route, got %+v", bundle.Routes)
	}
	if platform.Status != model.EdgeRouteStatusActive || platform.TLSPolicy != model.EdgeRouteTLSPolicyPlatform {
		t.Fatalf("expected active platform route, got %+v", platform)
	}
	if !strings.Contains(platform.UpstreamURL, ".svc.cluster.local:8080") {
		t.Fatalf("expected service DNS upstream, got %+v", platform)
	}

	custom := edgeRouteByHostAndKind(bundle.Routes, "www.example.com", model.EdgeRouteKindCustomDomain)
	if custom == nil {
		t.Fatalf("expected custom-domain route, got %+v", bundle.Routes)
	}
	if custom.Status != model.EdgeRouteStatusActive || custom.TLSPolicy != model.EdgeRouteTLSPolicyCustomDomain {
		t.Fatalf("expected active custom-domain route, got %+v", custom)
	}
	if custom.Hostname == server.primaryCustomDomainTarget(app) {
		t.Fatalf("expected route bundle to contain real Host, not CNAME target: %+v", custom)
	}
	if len(bundle.TLSAllowlist) != 1 || bundle.TLSAllowlist[0].Hostname != "www.example.com" {
		t.Fatalf("expected custom domain TLS allowlist, got %+v", bundle.TLSAllowlist)
	}
}

func TestEdgeRoutesBundleSupportsGroupFilterAndConditionalFetch(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "HK",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	deployAppForEdgeRouteTest(t, storeState, app)

	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(first, req)
	if first.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, first.Code, first.Body.String())
	}
	etag := first.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag")
	}
	var bundle model.EdgeRouteBundle
	mustDecodeJSON(t, first, &bundle)
	if len(bundle.Routes) != 1 {
		t.Fatalf("expected one route for country edge group, got %+v", bundle.Routes)
	}
	if bundle.Routes[0].EdgeGroupID != "edge-group-country-hk" {
		t.Fatalf("expected HK edge group, got %+v", bundle.Routes[0])
	}

	second := httptest.NewRecorder()
	conditional := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	conditional.Header.Set("If-None-Match", etag)
	server.Handler().ServeHTTP(second, conditional)
	if second.Code != http.StatusNotModified {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusNotModified, second.Code, second.Body.String())
	}
}

func TestEdgeRouteBindingDerivesNonActiveStatuses(t *testing.T) {
	t.Parallel()

	server := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes", nil)
	runtimes := map[string]model.Runtime{
		model.DefaultManagedRuntimeID: {
			ID:     model.DefaultManagedRuntimeID,
			Status: model.RuntimeStatusActive,
		},
	}

	disabled := server.deriveEdgeRouteBinding(req, model.App{
		ID:       "app_disabled",
		TenantID: "tenant_demo",
		Name:     "disabled",
		Route:    &model.AppRoute{Hostname: "disabled.fugue.pro", ServicePort: 8080},
		Spec: model.AppSpec{
			Replicas:  0,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
	}, "disabled.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes)
	if disabled.Status != model.EdgeRouteStatusDisabled || disabled.UpstreamURL != "" {
		t.Fatalf("expected disabled route without upstream, got %+v", disabled)
	}

	missingRuntime := server.deriveEdgeRouteBinding(req, model.App{
		ID:       "app_missing_runtime",
		TenantID: "tenant_demo",
		Name:     "missing-runtime",
		Route:    &model.AppRoute{Hostname: "missing-runtime.fugue.pro", ServicePort: 8080},
		Spec: model.AppSpec{
			Replicas:  1,
			RuntimeID: "runtime_missing",
		},
		Status: model.AppStatus{CurrentReplicas: 1},
	}, "missing-runtime.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes)
	if missingRuntime.Status != model.EdgeRouteStatusRuntimeMissing || missingRuntime.UpstreamURL != "" {
		t.Fatalf("expected runtime-missing route without upstream, got %+v", missingRuntime)
	}

	unavailable := server.deriveEdgeRouteBinding(req, model.App{
		ID:       "app_unavailable",
		TenantID: "tenant_demo",
		Name:     "unavailable",
		Route:    &model.AppRoute{Hostname: "unavailable.fugue.pro", ServicePort: 8080},
		Spec: model.AppSpec{
			Replicas:  1,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		Status: model.AppStatus{CurrentReplicas: 0},
	}, "unavailable.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes)
	if unavailable.Status != model.EdgeRouteStatusUnavailable || unavailable.UpstreamURL != "" {
		t.Fatalf("expected unavailable route without upstream, got %+v", unavailable)
	}
}

func edgeRouteByHostAndKind(routes []model.EdgeRouteBinding, hostname, kind string) *model.EdgeRouteBinding {
	for index := range routes {
		if routes[index].Hostname == hostname && routes[index].RouteKind == kind {
			return &routes[index]
		}
	}
	return nil
}

func deployAppForEdgeRouteTest(t *testing.T, storeState edgeRouteTestStore, app model.App) model.App {
	t.Helper()
	specCopy := app.Spec
	deployOp, err := storeState.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		ExecutionMode:   model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := storeState.CompleteManagedOperationWithResult(deployOp.ID, "", "deployed", &specCopy, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}
	reloaded, err := storeState.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload deployed app: %v", err)
	}
	return reloaded
}

type edgeRouteTestStore interface {
	CreateApp(string, string, string, string, model.AppSpec) (model.App, error)
	CreateOperation(model.Operation) (model.Operation, error)
	CompleteManagedOperationWithResult(string, string, string, *model.AppSpec, *model.AppSource) (model.Operation, error)
	GetApp(string) (model.App, error)
}
