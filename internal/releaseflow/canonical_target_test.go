package releaseflow

import (
	"context"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCanonicalStableTargetRequiresCompleteOwnedReference(t *testing.T) {
	app := model.App{ID: "app-neutral", TenantID: "tenant-neutral", Name: "service", Spec: model.AppSpec{Image: "registry.example.test/service:one", RuntimeID: "runtime-a", Ports: []int{8080}}}
	upstream := "http://" + runtime.RuntimeAppServiceName(app) + "." + runtime.NamespaceForTenant(app.TenantID) + ".svc.cluster.local:8080"
	for _, scenario := range []string{"valid", "foreign app", "foreign tenant", "other runtime", "other image", "no snapshot", "snapshot image", "snapshot runtime", "foreign upstream", "wrong port", "partial identity", "explicit revision", "candidate"} {
		t.Run(scenario, func(t *testing.T) {
			spec := app.Spec
			r := model.AppRelease{ID: "release", AppID: app.ID, TenantID: app.TenantID, Role: model.AppReleaseRoleStable, RuntimeID: app.Spec.RuntimeID, ResolvedImageRef: app.Spec.Image, UpstreamURL: upstream, SpecSnapshot: &spec, Status: model.AppReleaseStatusReady}
			switch scenario {
			case "foreign app":
				r.AppID = "other"
			case "foreign tenant":
				r.TenantID = "other"
			case "other runtime":
				r.RuntimeID = "other"
			case "other image":
				r.ResolvedImageRef = "other"
			case "no snapshot":
				r.SpecSnapshot = nil
			case "snapshot image":
				spec.Image = "other"
			case "snapshot runtime":
				spec.RuntimeID = "other"
			case "foreign upstream":
				r.UpstreamURL = "http://other.svc.cluster.local:8080"
			case "wrong port":
				r.UpstreamURL = upstream + "0"
			case "partial identity":
				r.ServiceName = "explicit"
			case "explicit revision":
				r.DeploymentName = "revision"
				r.ServiceName = "revision"
			case "candidate":
				r.Role = model.AppReleaseRoleCandidate
			}
			got := CanonicalStableTarget(app, r)
			if scenario == "valid" || scenario == "no snapshot" {
				if got.DeploymentName != runtime.RuntimeAppResourceName(app) || got.ServiceName != runtime.RuntimeAppServiceName(app) {
					t.Fatal("canonical workload missing")
				}
				got.DeploymentName = ""
				got.ServiceName = ""
			}
			if !reflect.DeepEqual(got, r) {
				t.Fatal("identity resolver changed other release state")
			}
		})
	}
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	service := AppReleaseService{Store: s, ServiceURLForApp: func(context.Context, model.App) string { return upstream }}
	r, err := service.EnsureStableRelease(context.Background(), app)
	if err != nil || r.DeploymentName != runtime.RuntimeAppResourceName(app) || r.ServiceName != runtime.RuntimeAppServiceName(app) {
		t.Fatal("stable initializer omitted identity", err)
	}
	r, err = service.CreateRelease(context.Background(), app, CreateReleaseRequest{Role: model.AppReleaseRoleStable, ResolvedImageRef: app.Spec.Image, Status: model.AppReleaseStatusReady})
	if err != nil || r.DeploymentName != runtime.RuntimeAppResourceName(app) {
		t.Fatal("stable create omitted canonical identity", err)
	}
}
