package store

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func runtimeReadFixture(t *testing.T, postgres bool) (*Store, model.App, model.App) {
	t.Helper()
	s := newStoreTestDatabase(t, "")
	if postgres {
		s = billingBatchPGStore(t)
		if err := s.ensureDatabaseReady(); err != nil {
			t.Fatal(err)
		}
	} else if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant(model.NewID("runtime_read_test"))
	if err != nil {
		t.Fatal(err)
	}
	if postgres {
		t.Cleanup(func() {
			if _, err := s.db.Exec(`DELETE FROM fugue_tenants WHERE id=$1`, tenant.ID); err != nil {
				t.Error(err)
			}
		})
	}
	project, err := s.CreateProject(tenant.ID, "runtime-read-test", "")
	if err != nil {
		t.Fatal(err)
	}
	create := func(name string) model.App {
		app, err := s.CreateApp(tenant.ID, project.ID, name, "", model.AppSpec{Image: "registry.example/runtime:latest", Ports: []int{8080}, RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1})
		if err != nil {
			t.Fatal(err)
		}
		return app
	}
	return s, create("first"), create("second")
}

func TestAppReleasesByIDsMatchFilteredFullList(t *testing.T) {
	for _, backend := range []string{"file", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s, app, other := runtimeReadFixture(t, backend == "postgres")
			makeRelease := func(appID, role, status string) model.AppRelease {
				r, err := s.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: appID, Role: role, Status: status, SpecSnapshot: &model.AppSpec{Image: "registry.example/immutable", Env: map[string]string{"PROBE": "synthetic"}}, ServiceName: "service-test"})
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			first := makeRelease(app.ID, model.AppReleaseRoleStable, model.AppReleaseStatusServing)
			second := makeRelease(app.ID, model.AppReleaseRolePrevious, model.AppReleaseStatusDraining)
			retired := makeRelease(app.ID, model.AppReleaseRoleRetired, model.AppReleaseStatusRetired)
			foreign := makeRelease(other.ID, model.AppReleaseRoleStable, model.AppReleaseStatusServing)
			makeRelease(app.ID, model.AppReleaseRolePrevious, model.AppReleaseStatusServing)
			ids := []string{second.ID, " " + first.ID + " ", second.ID, retired.ID, foreign.ID, "missing"}
			membership := trimmedStringSet(ids)
			filters := []model.AppReleaseFilter{
				{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true},
				{TenantID: app.TenantID, AppID: app.ID},
				{TenantID: "other-tenant", AppID: app.ID, PlatformAdmin: true},
				{TenantID: app.TenantID, AppID: other.ID},
				{TenantID: app.TenantID, AppID: app.ID, IncludeRetired: true},
				{TenantID: app.TenantID, AppID: app.ID, ActiveOnly: true},
				{TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRolePrevious},
			}
			verify := func() {
				t.Helper()
				for _, filter := range filters {
					full, err := s.ListAppReleases(filter)
					if err != nil {
						t.Fatal(err)
					}
					want := []model.AppRelease{}
					for _, r := range full {
						if _, ok := membership[r.ID]; ok {
							want = append(want, r)
						}
					}
					got, err := s.ListAppReleasesByIDs(context.Background(), filter, ids)
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("filtered membership/snapshot/order mismatch for %+v: err=%v", filter, err)
					}
				}
			}
			verify()
			first.SpecSnapshot.Env["PROBE"] = "new-value"
			if _, err := s.UpdateAppRelease(first); err != nil {
				t.Fatal(err)
			}
			verify()
			if got, err := s.ListAppReleasesByIDs(context.Background(), filters[0], []string{"", " "}); err != nil || len(got) != 0 {
				t.Fatalf("empty membership widened query: %v %v", got, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := s.ListAppReleasesByIDs(ctx, filters[0], ids); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation ignored: %v", err)
			}
		})
	}
}
