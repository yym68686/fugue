package api

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"fugue/internal/store"
)

func TestProjectHostRoutePlanRejectsStaleAndKeepsOwnership(t *testing.T) {
	st, srv, token, admin, owner, _ := setupAppDomainTestServer(t)
	spec := model.AppSpec{Image: "example.test/web:v1", Ports: []int{3000}, Replicas: 1, RuntimeID: "runtime_managed_shared"}
	web, err := st.CreateApp(owner.TenantID, owner.ProjectID, "web", "", spec)
	if err != nil {
		t.Fatal(err)
	}
	host := "customer.example.net"
	claim, err := st.PutAppDomain(model.AppDomain{Hostname: host, AppID: owner.ID, TenantID: owner.TenantID, Status: model.AppDomainStatusVerified, DNSStatus: model.AppDomainDNSStatusReady, TLSStatus: model.AppDomainTLSStatusReady})
	if err != nil {
		t.Fatal(err)
	}
	base := "/v1/projects/" + owner.ProjectID + "/routes/"
	edit := model.ProjectHostRouteEdit{Hostname: host, Paths: map[string]string{"/": web.ID, "/v1": owner.ID}}
	preview := func(edit model.ProjectHostRouteEdit, status int) model.ProjectHostRoutePlan {
		t.Helper()
		rec := performJSONRequest(t, srv, http.MethodPost, base+"host-plan", token, edit)
		if rec.Code != status {
			t.Fatalf("plan status %d, want %d: %s", rec.Code, status, rec.Body)
		}
		var plan model.ProjectHostRoutePlan
		if status == http.StatusOK {
			mustDecodeJSON(t, rec, &plan)
		}
		return plan
	}
	apply := func(plan model.ProjectHostRoutePlan, status int, state string) {
		t.Helper()
		rec := performJSONRequest(t, srv, http.MethodPost, base+"host-apply", token, plan)
		if rec.Code != status {
			t.Fatalf("apply status %d, want %d: %s", rec.Code, status, rec.Body)
		}
		if status == http.StatusOK {
			var result model.ProjectHostRoutePlan
			mustDecodeJSON(t, rec, &result)
			if result.State != state || result.PlanID != plan.PlanID || !maps.Equal(result.After, plan.After) {
				t.Fatalf("unexpected apply result: %+v", result)
			}
		}
	}
	status := func(plan model.ProjectHostRoutePlan, state, reason string) {
		t.Helper()
		rec := performJSONRequest(t, srv, http.MethodPost, base+"host-status", admin, plan)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
		var result model.ProjectHostRoutePublicationStatus
		mustDecodeJSON(t, rec, &result)
		if result.State != state || !strings.Contains(result.Reason, reason) {
			t.Fatalf("publication status: got %+v, want %s containing %q", result, state, reason)
		}
	}
	preview(model.ProjectHostRouteEdit{Hostname: host, Paths: map[string]string{"/": web.ID}}, http.StatusConflict)
	original := preview(edit, http.StatusOK)
	if !maps.Equal(original.Before, map[string]string{"/": owner.ID}) || !maps.Equal(original.After, edit.Paths) {
		t.Fatalf("wrong effective routing preview: %+v", original)
	}
	if _, err := st.GetProjectRouteTable(owner.ProjectID); err == nil {
		t.Fatal("preview wrote an explicit table")
	}
	status(original, "pending", "has not been applied")
	other := model.ProjectRouteEntrypoint{Name: "other", Domain: owner.Route.Hostname, Routes: []model.ProjectRouteEntrypointRoute{{PathPrefix: "/health", AppID: owner.ID}}}
	if _, err = st.PutProjectRouteTable(owner.ProjectID, model.ProjectRouteTable{Entrypoints: []model.ProjectRouteEntrypoint{other}}); err != nil {
		t.Fatal(err)
	}
	apply(original, http.StatusConflict, "")
	status(original, "superseded", "desired routes differ")
	current := preview(edit, http.StatusOK)
	apply(current, http.StatusOK, "saved")
	status(current, "pending", "no exact full global publication")
	apply(current, http.StatusOK, "already_current")
	table, err := st.GetProjectRouteTable(owner.ProjectID)
	if err != nil || len(table.Entrypoints) != 2 || !reflect.DeepEqual(table.Entrypoints[0], model.NormalizeProjectRouteEntrypoint(other)) {
		t.Fatalf("unrelated host overwritten: %+v %v", table, err)
	}
	rollback := preview(model.ProjectHostRouteEdit{Hostname: host, RemovePaths: []string{"/", "/v1"}}, http.StatusOK)
	if !maps.Equal(rollback.After, original.Before) {
		t.Fatalf("rollback did not restore inherited route: %+v", rollback)
	}
	apply(rollback, http.StatusOK, "saved")
	after, err := st.GetAppDomain(host)
	if err != nil || !reflect.DeepEqual(claim, after) {
		t.Fatalf("hostname ownership or verification mutated: %+v %v", after, err)
	}
}

func TestProjectHostRoutePostgresSerializesPlans(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable Postgres")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires a disposable loopback fugue_test database")
	}
	st := store.New("", address)
	ctx := context.Background()
	if err = schemamigrate.MigrateEdgeInstanceFencing(ctx, address); err != nil {
		t.Fatal(err)
	}
	if err = st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant(model.NewID("host-plan"))
	if err != nil {
		t.Fatal(err)
	}
	project, err := st.CreateProject(tenant.ID, "project", "")
	if err != nil {
		t.Fatal(err)
	}
	spec := model.AppSpec{Image: "example.test/app:v1", Ports: []int{3000}, Replicas: 1, RuntimeID: "runtime_managed_shared"}
	owner, err := st.CreateApp(tenant.ID, project.ID, "api", "", spec)
	if err != nil {
		t.Fatal(err)
	}
	web, err := st.CreateApp(tenant.ID, project.ID, "web", "", spec)
	if err != nil {
		t.Fatal(err)
	}
	host := strings.ReplaceAll(tenant.ID, "_", "-") + ".example.net"
	_, err = st.PutAppDomain(model.AppDomain{Hostname: host, AppID: owner.ID, TenantID: tenant.ID, Status: model.AppDomainStatusVerified, TLSStatus: model.AppDomainTLSStatusReady})
	if err != nil {
		t.Fatal(err)
	}
	edit := model.ProjectHostRouteEdit{Hostname: host, Paths: map[string]string{"/": web.ID, "/api": owner.ID}}
	var plan model.ProjectHostRoutePlan
	err = st.EditProjectHostRoutes(ctx, project.ID, host, false, func(in store.ProjectHostRouteInputs) (*model.ProjectRouteTable, error) {
		var err error
		plan, _, _, err = buildProjectHostRoutePlan(in, edit)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	save := func(in store.ProjectHostRouteInputs) (*model.ProjectRouteTable, error) {
		current, next, _, err := buildProjectHostRoutePlan(in, edit)
		if err != nil {
			return nil, err
		}
		if current.PlanID != plan.PlanID {
			return nil, errProjectHostPlanChanged
		}
		return &next, nil
	}
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- st.EditProjectHostRoutes(ctx, project.ID, host, true, save) }()
	}
	a, b := <-results, <-results
	if !(a == nil && errors.Is(b, errProjectHostPlanChanged) || b == nil && errors.Is(a, errProjectHostPlanChanged)) {
		t.Fatalf("concurrent apply did not compare against committed table: %v, %v", a, b)
	}
}

func TestProjectHostPublicationStatusRequiresExactFullRoutes(t *testing.T) {
	plan := model.ProjectHostRoutePlan{Edit: model.ProjectHostRouteEdit{Hostname: "customer.example.net"}, After: map[string]string{"/": "web", "/api": "api"}, HostnameOwnerAppID: "api"}
	for _, scenario := range []string{"exact full", "only gray", "unhealthy group", "old root", "missing api", "wrong certificate owner"} {
		t.Run(scenario, func(t *testing.T) {
			host := plan.Edit.Hostname
			snapshot := model.EdgeRouteIntentSnapshot{
				Generation:     "routes",
				TrafficRelease: &model.TrafficReleaseBinding{ReleaseID: "release", ReleaseChannel: model.PlatformArtifactReleaseChannelFull},
				TLSAllowlist:   []model.EdgeTLSAllowlistEntry{{Hostname: host, AppID: "api", Status: model.AppDomainStatusVerified}},
			}
			for path, app := range plan.After {
				snapshot.Routes = append(snapshot.Routes, model.EdgeRouteIntent{Hostname: host, PathPrefix: path, AppID: app, OriginStatus: model.EdgeRouteStatusActive, RoutePolicy: model.EdgeRoutePolicyEnabled})
			}
			healthy := map[string]bool{"group-a": true, "group-b": true}
			switch scenario {
			case "only gray":
				snapshot.TrafficRelease.ReleaseChannel = model.PlatformArtifactReleaseChannelGray
			case "unhealthy group":
				healthy["group-b"] = false
			case "old root":
				for i := range snapshot.Routes {
					snapshot.Routes[i].AppID = "api"
				}
			case "missing api":
				snapshot.Routes = snapshot.Routes[:1]
			case "wrong certificate owner":
				snapshot.TLSAllowlist[0].AppID = "web"
			}
			got := projectHostPublicationStatus(plan, snapshot, healthy)
			if (got.State == "published") != (scenario == "exact full") {
				t.Fatalf("unexpected production status: %+v", got)
			}
		})
	}
}
