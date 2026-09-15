package api

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestPlatformIntentProjectionRequiresPlatformAdmin(t *testing.T) {
	state, server, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	for _, test := range []struct {
		name, token string
		status      int
	}{{"anonymous", "", http.StatusUnauthorized}, {"tenant", tenant, http.StatusForbidden}} {
		t.Run(test.name, func(t *testing.T) {
			response := performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-config/routes/project", test.token, nil)
			if response.Code != test.status {
				t.Fatalf("expected %d got %d", test.status, response.Code)
			}
		})
	}
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-config/routes/project", admin, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected admin result %d %s", response.Code, response.Body.String())
	}
	var projection platformIntentProjectionResponse
	mustDecodeJSON(t, response, &projection)
	if projection.MigrationReady || len(projection.Issues) == 0 || len(projection.Intent.Routes) != 1 || projection.RouteCount != 1 || projection.RuntimeSnapshot.IntentGeneration != projection.Intent.Generation || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("invalid draft: %+v", projection)
	}
	if projection.Policy.SchemaVersion != platformconfig.SchemaVersion || projection.Policy.Scope != platformconfig.GlobalScopeKey || projection.Policy.Generation == "" {
		t.Fatalf("policy projection missing: %+v", projection.Policy)
	}
	if projection.Intent.Routes[0].EdgeGroupMode != model.PlatformRouteEdgeGroupModeAllHealthy {
		t.Fatal("invalid group mode mapping")
	}
	if projection.BusinessSnapshotRevision == "" || projection.BusinessSnapshotAt.IsZero() {
		t.Fatal("business snapshot provenance missing")
	}
	for _, issue := range projection.Issues {
		if issue.Code == "transaction_snapshot_not_frozen" {
			t.Fatal("transactional business snapshot not used")
		}
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("projection modified artifacts")
	}
}

func TestBusinessRouteDraftUsesFreshRuntimeObservationForReleaseFacts(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	app := model.App{ID: "app-a", TenantID: "tenant-a", Spec: model.AppSpec{Replicas: 1}, ObservedStatus: &model.AppObservedStatus{RuntimeID: "runtime-a", ObservedAt: now}}
	snapshot := model.EdgeRouteIntentSnapshot{GeneratedAt: now, Routes: []model.EdgeRouteIntent{{Hostname: "app.example.test", PathPrefix: "/", AppID: app.ID, TenantID: app.TenantID, RuntimeID: "runtime-a", ServicePort: 80}}}
	release := model.AppRelease{ID: "release-a", AppID: app.ID, TenantID: app.TenantID, Status: model.AppReleaseStatusServing, RuntimeID: "runtime-a", UpdatedAt: old, UpstreamURL: "http://app:80"}
	traffic := model.AppTrafficPolicy{ID: "traffic-a", AppID: app.ID, TenantID: app.TenantID, Mode: model.AppTrafficModeSingle, StableReleaseID: release.ID, StableWeight: 100}
	result, err := projectBusinessRouteDraft(snapshot, map[string]model.App{app.ID: app}, map[string]model.App{app.ID: app}, nil, nil, []model.AppTrafficPolicy{traffic}, []model.AppRelease{release}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RuntimeSnapshot.Releases) != 1 || !result.RuntimeSnapshot.Releases[0].ObservedAt.Equal(now) {
		t.Fatalf("release observation did not use runtime evidence: %+v", result.RuntimeSnapshot.Releases)
	}
}

func TestBusinessRouteDraftRetainsEvidenceTimeAndDesiredIntent(t *testing.T) {
	captured := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	observedAt := captured.Add(-time.Hour)
	app := model.App{ID: "app-test", TenantID: "tenant-test", Name: "test", Spec: model.AppSpec{Replicas: 1, RuntimeID: "runtime-wanted"}}
	apps := map[string]model.App{app.ID: app}
	observed := map[string]model.App{app.ID: {ObservedStatus: &model.AppObservedStatus{ObservedAt: observedAt, Fresh: false}}}
	route := model.EdgeRouteIntent{Hostname: "route.example", PathPrefix: "/", AppID: app.ID, TenantID: app.TenantID, RuntimeID: app.Spec.RuntimeID,
		RouteKind: model.EdgeRouteKindPlatform, RoutePolicy: model.EdgeRoutePolicyEnabled, TargetGroupMode: model.EdgeRouteIntentGroupModeAllGroups,
		UpstreamURL: "http://observed-target:8080", ServicePort: 8080, OriginStatus: model.EdgeRouteStatusActive,
		Upstreams: []model.EdgeRouteUpstream{{Weight: 100, UpstreamURL: "http://observed-target:8080"}},
	}
	snapshot := model.EdgeRouteIntentSnapshot{GeneratedAt: captured, Routes: []model.EdgeRouteIntent{route}}
	first, err := projectBusinessRouteDraft(snapshot, apps, observed, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.MigrationReady || !first.RuntimeSnapshot.Origins[0].ObservedAt.Equal(observedAt) || !first.Intent.Routes[0].Enabled {
		t.Fatal("draft renewed stale evidence or changed intent")
	}
	if first.Policy.Generation == "" {
		t.Fatal("policy generation missing")
	}
	snapshot.GeneratedAt = captured.Add(time.Minute)
	snapshot.Routes[0].OriginStatus = model.EdgeRouteStatusDisabled
	snapshot.Routes[0].UpstreamURL = ""
	snapshot.Routes[0].Upstreams = nil
	snapshot.Routes[0].RuntimeID = "runtime-observed-other"
	second, err := projectBusinessRouteDraft(snapshot, apps, observed, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Intent, second.Intent) || !second.RuntimeSnapshot.Origins[0].ObservedAt.Equal(observedAt) {
		t.Fatal("observed availability changed desired configuration or evidence time")
	}
	if first.Policy.Generation != second.Policy.Generation {
		t.Fatalf("runtime observation changed policy generation: %q != %q", first.Policy.Generation, second.Policy.Generation)
	}
	if second.RuntimeSnapshot.Origins[0].RuntimeID != "runtime-observed-other" {
		t.Fatal("runtime mismatch was hidden")
	}
	ref := second.Intent.Routes[0].OriginRef
	extra := route
	extra.Hostname = "earlier.example"
	snapshot.Routes = append([]model.EdgeRouteIntent{extra}, snapshot.Routes...)
	third, err := projectBusinessRouteDraft(snapshot, apps, observed, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range third.Intent.Routes {
		if r.Hostname == route.Hostname && r.OriginRef != ref {
			t.Fatal("origin identity depends on route order")
		}
	}
	if _, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: first.Intent, Policy: platformconfig.PolicySnapshot{Generation: "policy", MaxStaleSeconds: 60}, RuntimeSnapshot: first.RuntimeSnapshot}); err == nil {
		t.Fatal("stale draft compiled as fresh")
	}
}

func TestBusinessRouteDraftPreservesConfiguredPlatformMaintenance(t *testing.T) {
	route := model.EdgeRouteIntent{Hostname: "platform.example", PathPrefix: "/", TargetGroupMode: model.EdgeRouteIntentGroupModePinnedGroup, PinnedEdgeGroupID: "edge-group-test", OriginStatus: model.EdgeRouteStatusUnavailable}
	configured := model.PlatformRoute{Hostname: route.Hostname, UpstreamURL: "http://configured:8080", Status: model.EdgeRouteStatusUnavailable, StatusReason: "planned maintenance"}
	result, err := projectBusinessRouteDraft(model.EdgeRouteIntentSnapshot{Routes: []model.EdgeRouteIntent{route}}, nil, nil, []model.PlatformRoute{configured}, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := result.Intent.Routes[0]
	if got.EdgeGroupMode != model.PlatformRouteEdgeGroupModePinned || got.EdgeGroupID != "edge-group-test" || got.Status != configured.Status || got.StatusReason != configured.StatusReason || got.UpstreamURL != configured.UpstreamURL {
		t.Fatalf("platform configuration changed: %+v", got)
	}
}
