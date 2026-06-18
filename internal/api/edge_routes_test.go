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

func TestEdgePodLiveServingStateSuppressesRecentCaddyRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 9, 37, 0, 0, time.UTC)
	finishedAt := now.Add(-30 * time.Second)
	pod := kubePodInfo{}
	pod.Spec.Containers = []struct {
		Name  string `json:"name"`
		Image string `json:"image,omitempty"`
	}{{Name: "edge"}, {Name: "caddy"}}
	pod.Status.Phase = "Running"
	pod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  "edge",
		Ready: true,
		State: kubeRuntimeState{Running: &struct{}{}},
	}, {
		Name:      "caddy",
		Ready:     true,
		State:     kubeRuntimeState{Running: &struct{}{}},
		LastState: kubeRuntimeState{Terminated: &kubeStateDetail{Reason: "OOMKilled", ExitCode: 137, FinishedAt: &finishedAt}},
	}}

	state := edgePodLiveServingState(pod, now)
	if state.Serving || !strings.Contains(state.Reason, "restarted recently") {
		t.Fatalf("expected recent caddy restart to suppress serving, got %+v", state)
	}
}

func TestEdgePodLiveServingStateRequiresExpectedCaddyStatus(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 9, 37, 0, 0, time.UTC)
	pod := kubePodInfo{}
	pod.Spec.Containers = []struct {
		Name  string `json:"name"`
		Image string `json:"image,omitempty"`
	}{{Name: "edge"}, {Name: "caddy"}}
	pod.Status.Phase = "Running"
	pod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  "edge",
		Ready: true,
		State: kubeRuntimeState{Running: &struct{}{}},
	}}

	state := edgePodLiveServingState(pod, now)
	if state.Serving || !strings.Contains(state.Reason, "status is missing") {
		t.Fatalf("expected missing caddy status to suppress serving, got %+v", state)
	}
}

func TestEdgeNodeRouteServingCapableUsesLiveCaddyState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 9, 37, 0, 0, time.UTC)
	node := model.EdgeNode{
		ID:                 "edge-node-a",
		Status:             model.EdgeHealthHealthy,
		Healthy:            true,
		RouteBundleVersion: "routegen_a",
		LastHeartbeatAt:    &now,
	}
	live := map[string]edgeLiveServingState{
		"edge-node-a": {Serving: false, Reason: "caddy container restarted recently"},
	}

	if edgeNodeRouteServingCapableWithLive(node, now, live) {
		t.Fatal("expected live caddy state to make the edge node non-serving")
	}
}

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
	if _, err := storeState.PutAppDomain(model.AppDomain{
		Hostname:  "www.fugue.pro",
		AppID:     app.ID,
		TenantID:  app.TenantID,
		Status:    model.AppDomainStatusVerified,
		TLSStatus: model.AppDomainTLSStatusReady,
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("put verified platform domain binding: %v", err)
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
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")

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
	if len(bundle.Routes) != 3 {
		t.Fatalf("expected platform, platform-domain, and custom-domain route, got %+v", bundle.Routes)
	}
	platform := edgeRouteByHostAndKind(bundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform)
	if platform == nil {
		t.Fatalf("expected platform route, got %+v", bundle.Routes)
	}
	if platform.Status != model.EdgeRouteStatusActive || platform.TLSPolicy != model.EdgeRouteTLSPolicyPlatform {
		t.Fatalf("expected active platform route, got %+v", platform)
	}
	if platform.RoutePolicy != model.EdgeRoutePolicyEnabled {
		t.Fatalf("expected platform route to default to edge, got %+v", platform)
	}
	if platform.CachePolicyID != defaultStaticAssetCachePolicyID || platform.CacheNamespace == "" || platform.DeploymentGeneration == "" {
		t.Fatalf("expected platform route to carry static asset cache policy, got %+v", platform)
	}
	if len(bundle.CachePolicies) != 2 ||
		bundle.CachePolicies[0].ID != defaultStaticAssetCachePolicyID ||
		bundle.CachePolicies[1].ID != defaultHTMLDocumentCachePolicyID {
		t.Fatalf("expected route bundle to include default cache policy, got %+v", bundle.CachePolicies)
	}
	for _, want := range defaultHTMLDocumentVaryAllowlist {
		if !testStringSliceContainsFold(bundle.CachePolicies[1].VaryAllowlist, want) {
			t.Fatalf("expected HTML cache vary allowlist to include %q, got %+v", want, bundle.CachePolicies[1].VaryAllowlist)
		}
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
	if custom.RoutePolicy != model.EdgeRoutePolicyEnabled {
		t.Fatalf("expected custom-domain route to default to edge, got %+v", custom)
	}
	if custom.Hostname == server.primaryCustomDomainTarget(app) {
		t.Fatalf("expected route bundle to contain real Host, not CNAME target: %+v", custom)
	}
	platformDomain := edgeRouteByHostAndKind(bundle.Routes, "www.fugue.pro", model.EdgeRouteKindPlatformDomain)
	if platformDomain == nil {
		t.Fatalf("expected platform-domain route, got %+v", bundle.Routes)
	}
	if platformDomain.TLSPolicy != model.EdgeRouteTLSPolicyPlatform || platformDomain.RoutePolicy != model.EdgeRoutePolicyEnabled {
		t.Fatalf("unexpected platform-domain route: %+v", platformDomain)
	}
	if len(bundle.TLSAllowlist) != 2 ||
		bundle.TLSAllowlist[0].Hostname != "www.example.com" ||
		bundle.TLSAllowlist[1].Hostname != "www.fugue.pro" {
		t.Fatalf("expected custom and platform domain TLS allowlist, got %+v", bundle.TLSAllowlist)
	}
}

func TestEdgeRoutesBundlePublishesProjectRouteTable(t *testing.T) {
	t.Parallel()

	storeState, server, apiKey, _, frontend, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	frontend = deployAppForEdgeRouteTest(t, storeState, frontend)
	backend, err := storeState.CreateApp(frontend.TenantID, frontend.ProjectID, "api", "", model.AppSpec{
		Image:     "ghcr.io/example/api:latest",
		Ports:     []int{9000},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create backend app: %v", err)
	}
	backend = deployAppForEdgeRouteTest(t, storeState, backend)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")

	put := performJSONRequest(t, server, http.MethodPut, "/v1/projects/"+frontend.ProjectID+"/routes", apiKey, map[string]any{
		"domains": []map[string]any{
			{"name": "production", "hostname": "0-0.fugue.pro"},
			{"name": "api", "hostname": "api.fugue.pro"},
			{"name": "staging", "hostname": "api2.fugue.pro"},
		},
		"entrypoints": []map[string]any{
			{"name": "production", "domain": "production", "routes": []map[string]any{
				{"path_prefix": "/v1", "service": "api"},
				{"path_prefix": "/", "service": "demo"},
			}},
			{"name": "api", "domain": "api", "routes": []map[string]any{
				{"path_prefix": "/v1", "service": "api"},
			}},
			{"name": "staging", "domain": "staging", "routes": []map[string]any{
				{"path_prefix": "/v1", "service": "api"},
				{"path_prefix": "/", "service": "demo"},
			}},
		},
	})
	if put.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
	}

	getTraffic := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+backend.ID+"/traffic", apiKey, nil)
	if getTraffic.Code != http.StatusOK {
		t.Fatalf("expected get backend traffic status %d, got %d body=%s", http.StatusOK, getTraffic.Code, getTraffic.Body.String())
	}
	var trafficResponse appTrafficResponse
	mustDecodeJSON(t, getTraffic, &trafficResponse)
	createCandidate := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+backend.ID+"/releases", apiKey, appReleaseCreateRequest{
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: "https://api-candidate.example.internal",
		Status:      model.AppReleaseStatusReady,
	})
	if createCandidate.Code != http.StatusCreated {
		t.Fatalf("expected create backend release status %d, got %d body=%s", http.StatusCreated, createCandidate.Code, createCandidate.Body.String())
	}
	var releaseResponse appReleaseResponse
	mustDecodeJSON(t, createCandidate, &releaseResponse)
	stableWeight := 90
	candidateWeight := 10
	patchTraffic := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+backend.ID+"/traffic", apiKey, appTrafficPatchRequest{
		Mode:               model.AppTrafficModeCanary,
		CandidateReleaseID: releaseResponse.Release.ID,
		StableWeight:       &stableWeight,
		CandidateWeight:    &candidateWeight,
	})
	if patchTraffic.Code != http.StatusOK {
		t.Fatalf("expected patch backend traffic status %d, got %d body=%s", http.StatusOK, patchTraffic.Code, patchTraffic.Body.String())
	}

	found, err := storeState.GetAppByRoutePrefix("api2.fugue.pro", "/v1")
	if err != nil {
		t.Fatalf("lookup project route table route: %v", err)
	}
	if found.ID != backend.ID {
		t.Fatalf("expected staging /v1 to resolve to backend %s, got %s", backend.ID, found.ID)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeRouteBundle
	mustDecodeJSON(t, recorder, &bundle)

	apiRoute := edgeRouteByHostKindAndPath(bundle.Routes, "api.fugue.pro", model.EdgeRouteKindPlatformDomain, "/v1")
	if apiRoute == nil {
		t.Fatalf("expected api.fugue.pro /v1 route, got %+v", bundle.Routes)
	}
	if apiRoute.AppID != backend.ID || apiRoute.ServicePort != 9000 {
		t.Fatalf("expected api route to target backend, got %+v", apiRoute)
	}
	if len(apiRoute.Upstreams) != 2 {
		t.Fatalf("expected api route to carry weighted release upstreams, got %+v", apiRoute)
	}
	if apiRoute.Upstreams[0].Role != model.AppReleaseRoleStable ||
		apiRoute.Upstreams[0].ReleaseID != trafficResponse.Traffic.StableReleaseID ||
		apiRoute.Upstreams[0].Weight != stableWeight {
		t.Fatalf("unexpected api stable upstream: %+v", apiRoute.Upstreams[0])
	}
	if apiRoute.Upstreams[1].Role != model.AppReleaseRoleCandidate ||
		apiRoute.Upstreams[1].ReleaseID != releaseResponse.Release.ID ||
		apiRoute.Upstreams[1].Weight != candidateWeight ||
		apiRoute.Upstreams[1].UpstreamURL != "https://api-candidate.example.internal" {
		t.Fatalf("unexpected api candidate upstream: %+v", apiRoute.Upstreams[1])
	}
	rootRoute := edgeRouteByHostKindAndPath(bundle.Routes, "0-0.fugue.pro", model.EdgeRouteKindPlatformDomain, "/")
	if rootRoute == nil {
		t.Fatalf("expected production root route, got %+v", bundle.Routes)
	}
	if rootRoute.AppID != frontend.ID || rootRoute.ServicePort != 8080 {
		t.Fatalf("expected production root route to target frontend, got %+v", rootRoute)
	}
	stagingRoute := edgeRouteByHostKindAndPath(bundle.Routes, "api2.fugue.pro", model.EdgeRouteKindPlatformDomain, "/v1")
	if stagingRoute == nil || stagingRoute.AppID != backend.ID {
		t.Fatalf("expected staging /v1 route to target backend, got %+v", stagingRoute)
	}
}

func TestEdgeRoutesBundlePublishesAppReleaseTrafficUpstreams(t *testing.T) {
	t.Parallel()

	storeState, server, apiKey, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-default-1", defaultEdgeGroupID, "203.0.113.20")

	getTraffic := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/traffic", apiKey, nil)
	if getTraffic.Code != http.StatusOK {
		t.Fatalf("expected get traffic status %d, got %d body=%s", http.StatusOK, getTraffic.Code, getTraffic.Body.String())
	}
	var trafficResponse appTrafficResponse
	mustDecodeJSON(t, getTraffic, &trafficResponse)
	if trafficResponse.Traffic.StableReleaseID == "" {
		t.Fatalf("expected default traffic policy to have stable release id: %+v", trafficResponse.Traffic)
	}

	createCandidate := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/releases", apiKey, appReleaseCreateRequest{
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: "https://candidate.example.internal",
		Status:      model.AppReleaseStatusReady,
	})
	if createCandidate.Code != http.StatusCreated {
		t.Fatalf("expected create release status %d, got %d body=%s", http.StatusCreated, createCandidate.Code, createCandidate.Body.String())
	}
	var releaseResponse appReleaseResponse
	mustDecodeJSON(t, createCandidate, &releaseResponse)

	stableWeight := 90
	candidateWeight := 10
	patchTraffic := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/traffic", apiKey, appTrafficPatchRequest{
		Mode:               model.AppTrafficModeCanary,
		CandidateReleaseID: releaseResponse.Release.ID,
		StableWeight:       &stableWeight,
		CandidateWeight:    &candidateWeight,
	})
	if patchTraffic.Code != http.StatusOK {
		t.Fatalf("expected patch traffic status %d, got %d body=%s", http.StatusOK, patchTraffic.Code, patchTraffic.Body.String())
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected edge routes status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeRouteBundle
	mustDecodeJSON(t, recorder, &bundle)

	route := edgeRouteByHostAndKind(bundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform)
	if route == nil {
		t.Fatalf("expected demo platform route, got %+v", bundle.Routes)
	}
	if len(route.Upstreams) != 2 {
		t.Fatalf("expected weighted stable/candidate upstreams, got %+v", route)
	}
	if route.Upstreams[0].Role != model.AppReleaseRoleStable ||
		route.Upstreams[0].ReleaseID != trafficResponse.Traffic.StableReleaseID ||
		route.Upstreams[0].Weight != stableWeight {
		t.Fatalf("unexpected stable upstream: %+v", route.Upstreams[0])
	}
	if route.Upstreams[1].Role != model.AppReleaseRoleCandidate ||
		route.Upstreams[1].ReleaseID != releaseResponse.Release.ID ||
		route.Upstreams[1].Weight != candidateWeight ||
		route.Upstreams[1].UpstreamURL != "https://candidate.example.internal" {
		t.Fatalf("unexpected candidate upstream: %+v", route.Upstreams[1])
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
	if second.Code != http.StatusOK {
		t.Fatalf("expected signed route bundle refresh status %d, got %d body=%s", http.StatusOK, second.Code, second.Body.String())
	}
	var secondBundle model.EdgeRouteBundle
	mustDecodeJSON(t, second, &secondBundle)
	if secondBundle.Version != bundle.Version {
		t.Fatalf("expected conditional signed refresh to keep content version %s, got %s", bundle.Version, secondBundle.Version)
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

func TestEdgeRoutePolicyUnhealthyPolicyGroupDoesNotDowngradeToRouteA(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	deployAppForEdgeRouteTest(t, storeState, app)

	put := performJSONRequest(t, server, http.MethodPut, "/v1/edge/route-policies/demo.fugue.pro", platformAdminKey, map[string]any{
		"edge_group_id": "edge-group-country-hk",
		"route_policy":  model.EdgeRoutePolicyCanary,
	})
	if put.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, put.Code, put.Body.String())
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeRouteBundle
	mustDecodeJSON(t, recorder, &bundle)
	route := edgeRouteByHostAndKind(bundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform)
	if route == nil {
		t.Fatalf("expected platform route in bundle: %+v", bundle.Routes)
	}
	if route.RoutePolicy != model.EdgeRoutePolicyCanary || route.Status != model.EdgeRouteStatusUnavailable {
		t.Fatalf("expected unavailable canary route to remain edge traffic policy, got %+v", route)
	}
}

func TestEdgeRoutePolicyCanaryUsesNearestHealthyEdgeGroup(t *testing.T) {
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
		initialRoute.RoutePolicy != model.EdgeRoutePolicyEnabled ||
		initialRoute.Status != model.EdgeRouteStatusUnavailable ||
		initialRoute.StatusReason != "edge group has no healthy edge nodes" {
		t.Fatalf("expected default HK edge binding to degrade without healthy edges, got %+v", initialRoute)
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

	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-us-1",
		EdgeGroupID: "edge-group-country-us",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy US edge node: %v", err)
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

	usFallback := httptest.NewRecorder()
	usFallbackReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret", nil)
	server.Handler().ServeHTTP(usFallback, usFallbackReq)
	if usFallback.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, usFallback.Code, usFallback.Body.String())
	}
	var usFallbackBundle model.EdgeRouteBundle
	mustDecodeJSON(t, usFallback, &usFallbackBundle)
	if usFallbackBundle.Version == initialBundle.Version {
		t.Fatalf("expected edge route policy change to update bundle version %s", initialBundle.Version)
	}
	if len(usFallbackBundle.Routes) != 1 {
		t.Fatalf("expected one US fallback route, got %+v", usFallbackBundle.Routes)
	}
	fallbackRoute := usFallbackBundle.Routes[0]
	if fallbackRoute.EdgeGroupID != "edge-group-country-us" ||
		fallbackRoute.RuntimeEdgeGroupID != "edge-group-country-hk" ||
		fallbackRoute.PolicyEdgeGroupID != "edge-group-country-us" ||
		fallbackRoute.FallbackEdgeGroupID != "" ||
		fallbackRoute.RoutePolicy != model.EdgeRoutePolicyCanary ||
		fallbackRoute.UpstreamScope != model.EdgeRouteUpstreamScopeLocalService {
		t.Fatalf("expected US nearest-edge fallback route for HK runtime, got %+v", fallbackRoute)
	}

	hkAfter := httptest.NewRecorder()
	hkAfterReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(hkAfter, hkAfterReq)
	if hkAfter.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, hkAfter.Code, hkAfter.Body.String())
	}
	var hkAfterBundle model.EdgeRouteBundle
	mustDecodeJSON(t, hkAfter, &hkAfterBundle)
	if len(hkAfterBundle.Routes) != 1 {
		t.Fatalf("expected HK bundle to keep the fallback route for global serving, got %+v", hkAfterBundle.Routes)
	}
	if hkAfterBundle.Routes[0].EdgeGroupID != "edge-group-country-us" ||
		hkAfterBundle.Routes[0].PolicyEdgeGroupID != "edge-group-country-us" ||
		hkAfterBundle.Routes[0].RoutePolicy != model.EdgeRoutePolicyCanary {
		t.Fatalf("expected HK bundle to carry the active US fallback route, got %+v", hkAfterBundle.Routes[0])
	}

	usAfter := httptest.NewRecorder()
	usReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-us", nil)
	server.Handler().ServeHTTP(usAfter, usReq)
	if usAfter.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, usAfter.Code, usAfter.Body.String())
	}
	var usBundle model.EdgeRouteBundle
	mustDecodeJSON(t, usAfter, &usBundle)
	if len(usBundle.Routes) != 1 || usBundle.Routes[0].EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected US bundle to receive nearest-edge fallback route, got %+v", usBundle.Routes)
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
	if len(revertedBundle.Routes) != 2 ||
		edgeRouteByHostKindAndGroup(revertedBundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform, "edge-group-country-hk") == nil ||
		edgeRouteByHostKindAndGroup(revertedBundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform, "edge-group-country-us") == nil {
		t.Fatalf("expected deleted policy to restore default platform edge binding, got %+v", revertedBundle.Routes)
	}
}

func TestPlatformRoutesDefaultToHealthyEdgeGroups(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "HK",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	deployAppForEdgeRouteTest(t, storeState, app)
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-us-1",
		EdgeGroupID: "edge-group-country-us",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy US edge node: %v", err)
	}
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-de-1",
		EdgeGroupID: "edge-group-country-de",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy DE edge node: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-us", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeRouteBundle
	mustDecodeJSON(t, recorder, &bundle)
	if len(bundle.Routes) != 2 {
		t.Fatalf("expected generated platform hostname to be published to all healthy edge groups, got %+v", bundle.Routes)
	}
	for _, edgeGroupID := range []string{"edge-group-country-us", "edge-group-country-de"} {
		route := edgeRouteByHostKindAndGroup(bundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform, edgeGroupID)
		if route == nil {
			t.Fatalf("expected generated platform hostname in %s bundle: %+v", edgeGroupID, bundle.Routes)
		}
		if route.Hostname != "demo.fugue.pro" ||
			route.RouteKind != model.EdgeRouteKindPlatform ||
			route.RoutePolicy != model.EdgeRoutePolicyEnabled ||
			route.RuntimeEdgeGroupID != "edge-group-country-hk" {
			t.Fatalf("unexpected global platform route in %s bundle: %+v", edgeGroupID, route)
		}
	}

	hk := httptest.NewRecorder()
	hkReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(hk, hkReq)
	if hk.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, hk.Code, hk.Body.String())
	}
	var hkBundle model.EdgeRouteBundle
	mustDecodeJSON(t, hk, &hkBundle)
	if len(hkBundle.Routes) != 2 {
		t.Fatalf("expected HK bundle to receive the global platform route set, got %+v", hkBundle.Routes)
	}
	for _, edgeGroupID := range []string{"edge-group-country-us", "edge-group-country-de"} {
		if edgeRouteByHostKindAndGroup(hkBundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform, edgeGroupID) == nil {
			t.Fatalf("expected HK bundle to include %s route copy, got %+v", edgeGroupID, hkBundle.Routes)
		}
	}

	de := httptest.NewRecorder()
	deReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-de", nil)
	server.Handler().ServeHTTP(de, deReq)
	if de.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, de.Code, de.Body.String())
	}
	var deBundle model.EdgeRouteBundle
	mustDecodeJSON(t, de, &deBundle)
	if len(deBundle.Routes) != 2 {
		t.Fatalf("expected DE bundle to receive the global platform route set, got %+v", deBundle.Routes)
	}
	for _, edgeGroupID := range []string{"edge-group-country-us", "edge-group-country-de"} {
		route := edgeRouteByHostKindAndGroup(deBundle.Routes, "demo.fugue.pro", model.EdgeRouteKindPlatform, edgeGroupID)
		if route == nil || route.RuntimeEdgeGroupID != "edge-group-country-hk" {
			t.Fatalf("expected DE bundle to include %s route copy, got %+v", edgeGroupID, deBundle.Routes)
		}
	}
}

func TestPlatformRoutesBootstrapPendingEdgeGroupReceivesBundle(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.EnsureManagedSharedLocationLabels(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "HK",
	}); err != nil {
		t.Fatalf("set managed shared location labels: %v", err)
	}
	deployAppForEdgeRouteTest(t, storeState, app)
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:                 "edge-hk-1",
		EdgeGroupID:        "edge-group-country-hk",
		Status:             model.EdgeHealthDegraded,
		Healthy:            true,
		ServingGeneration:  "routegen_bootstrap",
		LKGGeneration:      "routegen_bootstrap",
		RouteBundleVersion: "routegen_bootstrap",
	}); err != nil {
		t.Fatalf("record bootstrap-capable HK edge node: %v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bundle model.EdgeRouteBundle
	mustDecodeJSON(t, recorder, &bundle)
	if len(bundle.Routes) != 1 {
		t.Fatalf("expected bootstrap-capable HK bundle to receive the public route, got %+v", bundle.Routes)
	}
	route := bundle.Routes[0]
	if route.Hostname != "demo.fugue.pro" ||
		route.RouteKind != model.EdgeRouteKindPlatform ||
		route.RoutePolicy != model.EdgeRoutePolicyEnabled ||
		route.RuntimeEdgeGroupID != "edge-group-country-hk" ||
		route.EdgeGroupID != "edge-group-country-hk" ||
		route.PolicyEdgeGroupID != "" ||
		route.FallbackEdgeGroupID != "" {
		t.Fatalf("unexpected bootstrap route bundle: %+v", route)
	}
}

func TestConfiguredPlatformRouteFansOutToHealthyEdgeGroups(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.platformRoutes = parsePlatformRoutes(`{"routes":[{
		"hostname":"api.fugue.pro",
		"kind":"control-plane-api",
		"upstream_url":"http://fugue-fugue.fugue-system.svc.cluster.local:80",
		"edge_group_mode":"region_aware"
	}]}`, nil)
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-us-1",
		EdgeGroupID: "edge-group-country-us",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy US edge node: %v", err)
	}
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          "edge-de-1",
		EdgeGroupID: "edge-group-country-de",
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy DE edge node: %v", err)
	}

	for _, edgeGroupID := range []string{"edge-group-country-us", "edge-group-country-de"} {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id="+edgeGroupID, nil)
		server.Handler().ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected status %d for %s, got %d body=%s", http.StatusOK, edgeGroupID, recorder.Code, recorder.Body.String())
		}
		var bundle model.EdgeRouteBundle
		mustDecodeJSON(t, recorder, &bundle)
		route := edgeRouteByHostKindAndGroup(bundle.Routes, "api.fugue.pro", model.EdgeRouteKindControlPlaneAPI, edgeGroupID)
		if route == nil {
			t.Fatalf("expected configured platform route in %s bundle: %+v", edgeGroupID, bundle.Routes)
		}
		if route.RoutePolicy != model.EdgeRoutePolicyEnabled ||
			route.UpstreamKind != model.EdgeRouteUpstreamKindKubernetesService ||
			route.UpstreamScope != model.EdgeRouteUpstreamScopeCluster ||
			route.UpstreamURL != "http://fugue-fugue.fugue-system.svc.cluster.local:80" ||
			route.TLSPolicy != model.EdgeRouteTLSPolicyPlatform ||
			route.Status != model.EdgeRouteStatusActive {
			t.Fatalf("unexpected configured platform route: %+v", route)
		}
	}

	hk := httptest.NewRecorder()
	hkReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token=edge-secret&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(hk, hkReq)
	if hk.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, hk.Code, hk.Body.String())
	}
	var hkBundle model.EdgeRouteBundle
	mustDecodeJSON(t, hk, &hkBundle)
	if edgeRouteByHostAndKind(hkBundle.Routes, "api.fugue.pro", model.EdgeRouteKindControlPlaneAPI) == nil {
		t.Fatalf("configured platform route must be present on fallback bundles too: %+v", hkBundle.Routes)
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

	nonHTTP := server.deriveEdgeRouteBinding(req, model.App{
		ID:       "app_redis",
		TenantID: "tenant_demo",
		Name:     "redis",
		Route:    &model.AppRoute{Hostname: "redis.fugue.pro", ServicePort: 6379},
		Source:   &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "redis:8-alpine"},
		Spec: model.AppSpec{
			Replicas:  1,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		Status: model.AppStatus{CurrentReplicas: 1},
	}, "redis.fugue.pro", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, time.Time{}, time.Time{}, runtimes, nil)
	if nonHTTP.Status != model.EdgeRouteStatusUnavailable || nonHTTP.UpstreamURL != "" || !strings.Contains(nonHTTP.StatusReason, "non-HTTP") {
		t.Fatalf("expected known non-HTTP app route to be unavailable, got %+v", nonHTTP)
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

func edgeRouteByHostKindAndPath(routes []model.EdgeRouteBinding, hostname, kind, pathPrefix string) *model.EdgeRouteBinding {
	for index := range routes {
		if routes[index].Hostname == hostname && routes[index].RouteKind == kind && model.NormalizeAppRoutePathPrefix(routes[index].PathPrefix) == model.NormalizeAppRoutePathPrefix(pathPrefix) {
			return &routes[index]
		}
	}
	return nil
}

func edgeRouteByHostKindAndGroup(routes []model.EdgeRouteBinding, hostname, kind, edgeGroupID string) *model.EdgeRouteBinding {
	for index := range routes {
		if routes[index].Hostname == hostname && routes[index].RouteKind == kind && routes[index].EdgeGroupID == edgeGroupID {
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

type edgeRouteHeartbeatStore interface {
	UpdateEdgeHeartbeat(model.EdgeNode) (model.EdgeNode, model.EdgeGroup, error)
}

func recordHealthyEdgeForRouteTest(t *testing.T, storeState edgeRouteHeartbeatStore, id, groupID, publicIPv4 string) {
	t.Helper()
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:          id,
		EdgeGroupID: groupID,
		PublicIPv4:  publicIPv4,
		Status:      model.EdgeHealthHealthy,
		Healthy:     true,
	}); err != nil {
		t.Fatalf("record healthy edge node: %v", err)
	}
}

func testStringSliceContainsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(want)) {
			return true
		}
	}
	return false
}
