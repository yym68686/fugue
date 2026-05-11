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
	if platform.RoutePolicy != model.EdgeRoutePolicyRouteAOnly {
		t.Fatalf("expected platform route to default to Route A only, got %+v", platform)
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
	if custom.RoutePolicy != model.EdgeRoutePolicyRouteAOnly {
		t.Fatalf("expected custom-domain route to default to Route A only, got %+v", custom)
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

	repeated := httptest.NewRecorder()
	repeatedReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(repeated, repeatedReq)
	if repeated.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, repeated.Code, repeated.Body.String())
	}
	var repeatedBundle model.EdgeRouteBundle
	mustDecodeJSON(t, repeated, &repeatedBundle)
	if repeatedBundle.Version != bundle.Version {
		t.Fatalf("expected unchanged route content to keep stable version, first=%s repeated=%s", bundle.Version, repeatedBundle.Version)
	}
	if repeated.Header().Get("ETag") != etag {
		t.Fatalf("expected unchanged route content to keep stable ETag, first=%s repeated=%s", etag, repeated.Header().Get("ETag"))
	}

	second := httptest.NewRecorder()
	conditional := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	conditional.Header.Set("If-None-Match", etag)
	server.Handler().ServeHTTP(second, conditional)
	if second.Code != http.StatusNotModified {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusNotModified, second.Code, second.Body.String())
	}

	secondApp, err := storeState.CreateAppWithRoute(app.TenantID, app.ProjectID, "second", "", model.AppSpec{
		Image:     "ghcr.io/example/second:latest",
		Ports:     []int{9090},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	}, model.AppRoute{
		Hostname:    "second.fugue.pro",
		BaseDomain:  "fugue.pro",
		PublicURL:   "https://second.fugue.pro",
		ServicePort: 9090,
	})
	if err != nil {
		t.Fatalf("create second app: %v", err)
	}
	deployAppForEdgeRouteTest(t, storeState, secondApp)

	changed := httptest.NewRecorder()
	changedReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	changedReq.Header.Set("If-None-Match", etag)
	server.Handler().ServeHTTP(changed, changedReq)
	if changed.Code != http.StatusOK {
		t.Fatalf("expected route content change to return status %d, got %d body=%s", http.StatusOK, changed.Code, changed.Body.String())
	}
	var changedBundle model.EdgeRouteBundle
	mustDecodeJSON(t, changed, &changedBundle)
	if changedBundle.Version == bundle.Version {
		t.Fatalf("expected route content change to update version %s", bundle.Version)
	}
	if len(changedBundle.Routes) != 2 {
		t.Fatalf("expected two routes after content change, got %+v", changedBundle.Routes)
	}
}

func TestEdgeRoutePolicyOptInGatesBundleByHostname(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "HK",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	deployAppForEdgeRouteTest(t, storeState, app)

	initial := httptest.NewRecorder()
	initialReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret", nil)
	server.Handler().ServeHTTP(initial, initialReq)
	if initial.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, initial.Code, initial.Body.String())
	}
	var initialBundle model.EdgeRouteBundle
	mustDecodeJSON(t, initial, &initialBundle)
	if len(initialBundle.Routes) != 1 {
		t.Fatalf("expected one initial route, got %+v", initialBundle.Routes)
	}
	initialRoute := initialBundle.Routes[0]
	if initialRoute.EdgeGroupID != "edge-group-country-hk" ||
		initialRoute.FallbackEdgeGroupID != defaultEdgeGroupID ||
		initialRoute.RoutePolicy != model.EdgeRoutePolicyRouteAOnly {
		t.Fatalf("expected default HK Route A-only binding, got %+v", initialRoute)
	}

	hkBefore := httptest.NewRecorder()
	hkReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(hkBefore, hkReq)
	if hkBefore.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, hkBefore.Code, hkBefore.Body.String())
	}
	var hkBundle model.EdgeRouteBundle
	mustDecodeJSON(t, hkBefore, &hkBundle)
	if len(hkBundle.Routes) != 1 {
		t.Fatalf("expected route to appear in derived HK bundle before opt-in, got %+v", hkBundle.Routes)
	}

	put := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/demo.fugue.pro", platformAdminKey, map[string]any{
		"edge_group_id": "edge-group-country-us",
		"route_policy":  model.EdgeRoutePolicyCanary,
	})
	if put.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
	}
	var putResponse struct {
		Policy model.EdgeRoutePolicy `json:"policy"`
	}
	mustDecodeJSON(t, put, &putResponse)
	if putResponse.Policy.Hostname != "demo.fugue.pro" ||
		putResponse.Policy.EdgeGroupID != "edge-group-country-us" ||
		putResponse.Policy.RoutePolicy != model.EdgeRoutePolicyCanary ||
		!putResponse.Policy.Enabled {
		t.Fatalf("unexpected stored policy: %+v", putResponse.Policy)
	}

	mismatch := httptest.NewRecorder()
	mismatchReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret", nil)
	server.Handler().ServeHTTP(mismatch, mismatchReq)
	if mismatch.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, mismatch.Code, mismatch.Body.String())
	}
	var mismatchBundle model.EdgeRouteBundle
	mustDecodeJSON(t, mismatch, &mismatchBundle)
	if mismatchBundle.Version == initialBundle.Version {
		t.Fatalf("expected edge route policy change to update bundle version %s", initialBundle.Version)
	}
	if len(mismatchBundle.Routes) != 1 {
		t.Fatalf("expected one mismatched route, got %+v", mismatchBundle.Routes)
	}
	mismatchedRoute := mismatchBundle.Routes[0]
	if mismatchedRoute.EdgeGroupID != "edge-group-country-hk" ||
		mismatchedRoute.PolicyEdgeGroupID != "edge-group-country-us" ||
		mismatchedRoute.RoutePolicy != model.EdgeRoutePolicyRouteAOnly ||
		mismatchedRoute.Status != model.EdgeRouteStatusUnavailable ||
		!strings.Contains(mismatchedRoute.StatusReason, "does not match runtime edge group") {
		t.Fatalf("expected mismatched policy to stay Route A-only on runtime group, got %+v", mismatchedRoute)
	}

	hkAfter := httptest.NewRecorder()
	hkAfterReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(hkAfter, hkAfterReq)
	if hkAfter.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, hkAfter.Code, hkAfter.Body.String())
	}
	var hkAfterBundle model.EdgeRouteBundle
	mustDecodeJSON(t, hkAfter, &hkAfterBundle)
	if len(hkAfterBundle.Routes) != 1 || hkAfterBundle.Routes[0].RoutePolicy != model.EdgeRoutePolicyRouteAOnly {
		t.Fatalf("expected route to remain HK Route A-only after mismatched US opt-in, got %+v", hkAfterBundle.Routes)
	}

	usAfter := httptest.NewRecorder()
	usReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-us", nil)
	server.Handler().ServeHTTP(usAfter, usReq)
	if usAfter.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, usAfter.Code, usAfter.Body.String())
	}
	var usBundle model.EdgeRouteBundle
	mustDecodeJSON(t, usAfter, &usBundle)
	if len(usBundle.Routes) != 0 {
		t.Fatalf("expected mismatched policy to keep US bundle empty, got %+v", usBundle.Routes)
	}

	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-hk-1",
		EdgeGroupID: "edge-group-country-hk",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy HK edge node: %v", err)
	}
	hkPut := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/demo.fugue.pro", platformAdminKey, map[string]any{
		"edge_group_id": "edge-group-country-hk",
		"route_policy":  model.EdgeRoutePolicyCanary,
	})
	if hkPut.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, hkPut.Code, hkPut.Body.String())
	}
	hkLocal := httptest.NewRecorder()
	hkLocalReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(hkLocal, hkLocalReq)
	if hkLocal.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, hkLocal.Code, hkLocal.Body.String())
	}
	var hkLocalBundle model.EdgeRouteBundle
	mustDecodeJSON(t, hkLocal, &hkLocalBundle)
	if len(hkLocalBundle.Routes) != 1 ||
		hkLocalBundle.Routes[0].EdgeGroupID != "edge-group-country-hk" ||
		hkLocalBundle.Routes[0].RuntimeEdgeGroupID != "edge-group-country-hk" ||
		hkLocalBundle.Routes[0].RoutePolicy != model.EdgeRoutePolicyCanary ||
		hkLocalBundle.Routes[0].UpstreamScope != model.EdgeRouteUpstreamScopeLocalService {
		t.Fatalf("expected local HK runtime canary route, got %+v", hkLocalBundle.Routes)
	}

	deleted := performJSONRequest(t, server, http.MethodDelete, "/v1/edge/route-policies/demo.fugue.pro", platformAdminKey, nil)
	if deleted.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, deleted.Code, deleted.Body.String())
	}
	reverted := httptest.NewRecorder()
	revertedReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret", nil)
	server.Handler().ServeHTTP(reverted, revertedReq)
	if reverted.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, reverted.Code, reverted.Body.String())
	}
	var revertedBundle model.EdgeRouteBundle
	mustDecodeJSON(t, reverted, &revertedBundle)
	if len(revertedBundle.Routes) != 1 ||
		revertedBundle.Routes[0].RoutePolicy != model.EdgeRoutePolicyRouteAOnly ||
		revertedBundle.Routes[0].EdgeGroupID != "edge-group-country-hk" {
		t.Fatalf("expected deleted policy to restore derived Route A-only binding, got %+v", revertedBundle.Routes)
	}
}

func TestDerivedEdgeGroupIDForRuntimeUsesClusterNodeLabelsFallback(t *testing.T) {
	t.Parallel()

	runtimeObj := model.Runtime{
		ID:   "runtime_us",
		Type: model.RuntimeTypeManagedOwned,
	}
	edgeGroupID := derivedEdgeGroupIDForRuntime(runtimeObj, true, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "US",
	})
	if edgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected node country label fallback, got %q", edgeGroupID)
	}

	runtimeObj.Labels = map[string]string{runtimepkg.LocationCountryCodeLabelKey: "HK"}
	edgeGroupID = derivedEdgeGroupIDForRuntime(runtimeObj, true, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "US",
	})
	if edgeGroupID != "edge-group-country-hk" {
		t.Fatalf("expected runtime labels to take precedence over node labels, got %q", edgeGroupID)
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
	}, "disabled.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes, nil)
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
	}, "missing-runtime.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes, nil)
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
	}, "unavailable.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes, nil)
	if unavailable.Status != model.EdgeRouteStatusUnavailable || unavailable.UpstreamURL != "" {
		t.Fatalf("expected unavailable route without upstream, got %+v", unavailable)
	}
}

func TestEdgeRouteBundleVersionIgnoresNonContentMetadata(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(5 * time.Minute)
	bundle := model.EdgeRouteBundle{
		GeneratedAt: time.Date(2026, 5, 10, 1, 0, 0, 0, time.UTC),
		EdgeID:      "edge-a",
		EdgeGroupID: "edge-group-country-hk",
		Routes: []model.EdgeRouteBinding{
			{
				Hostname:            "demo.fugue.pro",
				RouteKind:           model.EdgeRouteKindPlatform,
				AppID:               "app_demo",
				TenantID:            "tenant_demo",
				RuntimeID:           model.DefaultManagedRuntimeID,
				EdgeGroupID:         "edge-group-country-hk",
				FallbackEdgeGroupID: defaultEdgeGroupID,
				RoutePolicy:         model.EdgeRoutePolicyCanary,
				UpstreamKind:        model.EdgeRouteUpstreamKindKubernetesService,
				UpstreamURL:         "http://app-demo.default.svc.cluster.local:8080",
				ServicePort:         8080,
				TLSPolicy:           model.EdgeRouteTLSPolicyPlatform,
				Streaming:           true,
				Status:              model.EdgeRouteStatusActive,
				RouteGeneration:     "routegen_old",
				CreatedAt:           createdAt,
				UpdatedAt:           updatedAt,
			},
		},
		TLSAllowlist: []model.EdgeTLSAllowlistEntry{
			{
				Hostname:  "www.example.com",
				AppID:     "app_demo",
				TenantID:  "tenant_demo",
				Status:    model.AppDomainStatusVerified,
				TLSStatus: model.AppDomainTLSStatusReady,
			},
		},
	}

	baseVersion := edgeRouteBundleVersion(bundle)
	baseRouteGeneration := edgeRouteGeneration(bundle.Routes[0])
	metadataOnly := cloneEdgeRouteBundleForTest(bundle)
	metadataOnly.GeneratedAt = bundle.GeneratedAt.Add(10 * time.Minute)
	metadataOnly.EdgeID = "edge-b"
	metadataOnly.EdgeGroupID = "edge-group-country-us"
	metadataOnly.Routes[0].RouteGeneration = "routegen_new"
	metadataOnly.Routes[0].CreatedAt = createdAt.Add(24 * time.Hour)
	metadataOnly.Routes[0].UpdatedAt = updatedAt.Add(24 * time.Hour)

	if got := edgeRouteBundleVersion(metadataOnly); got != baseVersion {
		t.Fatalf("expected non-content metadata changes to keep bundle version stable, base=%s got=%s", baseVersion, got)
	}
	if got := edgeRouteGeneration(metadataOnly.Routes[0]); got != baseRouteGeneration {
		t.Fatalf("expected non-content metadata changes to keep route generation stable, base=%s got=%s", baseRouteGeneration, got)
	}

	routeContentChanged := cloneEdgeRouteBundleForTest(bundle)
	routeContentChanged.Routes[0].ServicePort = 9090
	if got := edgeRouteBundleVersion(routeContentChanged); got == baseVersion {
		t.Fatalf("expected route content change to update bundle version %s", baseVersion)
	}
	if got := edgeRouteGeneration(routeContentChanged.Routes[0]); got == baseRouteGeneration {
		t.Fatalf("expected route content change to update route generation %s", baseRouteGeneration)
	}

	tlsContentChanged := cloneEdgeRouteBundleForTest(bundle)
	tlsContentChanged.TLSAllowlist[0].TLSStatus = model.AppDomainTLSStatusPending
	if got := edgeRouteBundleVersion(tlsContentChanged); got == baseVersion {
		t.Fatalf("expected TLS allowlist content change to update bundle version %s", baseVersion)
	}
}

func cloneEdgeRouteBundleForTest(bundle model.EdgeRouteBundle) model.EdgeRouteBundle {
	bundle.Routes = append([]model.EdgeRouteBinding(nil), bundle.Routes...)
	bundle.TLSAllowlist = append([]model.EdgeTLSAllowlistEntry(nil), bundle.TLSAllowlist...)
	return bundle
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
