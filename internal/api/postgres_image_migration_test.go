package api

import (
	"fugue/internal/model"
	"fugue/internal/pgimage"
	"fugue/internal/store"
	"net/http"
	"reflect"
	"testing"
)

func TestPostgresImageMigrationPlanRequiresExplicitApplyAndReusesIntent(t *testing.T) {
	for _, independent := range []bool{false, true} {
		name := "app-owned"
		if independent {
			name = "independent"
		}
		t.Run(name, func(t *testing.T) { testPostgresImageMigrationAdmission(t, independent) })
	}
}

func independentImageMigrationFixture(t *testing.T) (*store.Store, *Server, string, model.App, model.BackingService) {
	t.Helper()
	s, server, key, owner, owned := managedPostgresLifecycleFixture(t, 0)
	app, err := s.CreateApp(owner.TenantID, owner.ProjectID, "independent-client", "", model.AppSpec{Image: "registry.invalid/client:1", Replicas: 1, RuntimeID: owner.Spec.RuntimeID})
	if err != nil {
		t.Fatal(err)
	}
	p := model.CloneAppPostgresSpec(owned.Spec.Postgres)
	p.ServiceName = "independent-postgres"
	svc, err := s.CreateBackingService(app.TenantID, app.ProjectID, "independent-postgres", "", model.BackingServiceSpec{Postgres: p})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BindBackingService(app.TenantID, app.ID, svc.ID, "postgres", nil); err != nil {
		t.Fatal(err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	return s, server, key, app, svc
}

func testPostgresImageMigrationAdmission(t *testing.T, independent bool) {
	fixture := func() (*store.Store, *Server, string, model.App, model.BackingService) {
		return managedPostgresLifecycleFixture(t, 0)
	}
	if independent {
		fixture = func() (*store.Store, *Server, string, model.App, model.BackingService) {
			return independentImageMigrationFixture(t)
		}
	}
	s, server, key, app, svc := fixture()
	url := "/v1/backing-services/" + svc.ID + "/migrate-image"
	before, _ := s.GetBackingService(svc.ID)
	opsBefore, _ := s.ListOperationsByApp(app.TenantID, false, app.ID)
	planResponse := performJSONRequest(t, server, http.MethodPost, url, key, map[string]any{})
	if planResponse.Code != 200 {
		t.Fatalf("plan: %d %s", planResponse.Code, planResponse.Body.String())
	}
	var plan struct {
		Plan   pgimage.Plan `json:"plan"`
		DryRun bool         `json:"dry_run"`
	}
	mustDecodeJSON(t, planResponse, &plan)
	wantOwnership := "app_owned"
	if independent {
		wantOwnership = "independent"
	}
	if plan.Plan.Ownership != wantOwnership || plan.Plan.AppID != app.ID {
		t.Fatalf("wrong ownership/anchor: %+v", plan.Plan)
	}
	ops, _ := s.ListOperationsByApp(app.TenantID, false, app.ID)
	if !plan.DryRun || len(ops) != len(opsBefore) || !plan.Plan.LivePreflightPending || !plan.Plan.RequiresDowntime {
		t.Fatalf("plan mutated state or hid preflight: %+v", plan)
	}
	for _, request := range []map[string]any{{"apply": true}, {"apply": true, "acknowledge_downtime": true, "expected_config_hash": "stale"}} {
		res := performJSONRequest(t, server, http.MethodPost, url, key, request)
		if res.Code != 400 && res.Code != 409 {
			t.Fatalf("unsafe apply accepted: %d", res.Code)
		}
	}
	request := map[string]any{"apply": true, "acknowledge_downtime": true, "expected_config_hash": plan.Plan.ConfigHash}
	var first model.Operation
	for i := 0; i < 2; i++ {
		res := performJSONRequest(t, server, http.MethodPost, url, key, request)
		if res.Code != 202 {
			t.Fatalf("apply: %d %s", res.Code, res.Body.String())
		}
		assertResponseOmitsSecrets(t, res.Body.String(), "lifecycle-secret")
		var body struct {
			Operation model.Operation `json:"operation"`
		}
		mustDecodeJSON(t, res, &body)
		if i == 0 {
			first = body.Operation
		} else if body.Operation.ID != first.ID {
			t.Fatal("retry created duplicate")
		}
	}
	after, _ := s.GetBackingService(svc.ID)
	if !reflect.DeepEqual(before.Spec, after.Spec) {
		t.Fatal("target image persisted before execution")
	}
	if after.OwnerAppID != before.OwnerAppID {
		t.Fatal("independent service was adopted")
	}
	if _, err := s.UnbindBackingService(app.Bindings[0].ID); err != store.ErrConflict {
		t.Fatalf("unbind bypassed migration lease: %v", err)
	}
	deleted := performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID+"?force=true", key, nil)
	if deleted.Code != http.StatusConflict {
		t.Fatalf("force delete bypassed migration interlock: %d %s", deleted.Code, deleted.Body.String())
	}
}

func TestIndependentImageMigrationRejectsUnboundAndCrossProjectConsumers(t *testing.T) {
	for _, scenario := range []string{"unbound", "cross-project", "read-only-scope", "stale-binding"} {
		t.Run(scenario, func(t *testing.T) {
			s, server, key, app, svc := independentImageMigrationFixture(t)
			url := "/v1/backing-services/" + svc.ID + "/migrate-image"
			plan := performJSONRequest(t, server, http.MethodPost, url, key, map[string]any{})
			var body struct {
				Plan pgimage.Plan `json:"plan"`
			}
			mustDecodeJSON(t, plan, &body)
			request := map[string]any{}
			want := http.StatusConflict
			if scenario == "read-only-scope" {
				var err error
				_, key, err = s.CreateAPIKey(app.TenantID, "reader", []string{"app.read"})
				if err != nil {
					t.Fatal(err)
				}
				want = http.StatusForbidden
			} else {
				if _, err := s.UnbindBackingService(app.Bindings[0].ID); err != nil {
					t.Fatal(err)
				}
				if scenario == "cross-project" {
					project, err := s.CreateProject(app.TenantID, "other-project", "")
					if err != nil {
						t.Fatal(err)
					}
					other, err := s.CreateApp(app.TenantID, project.ID, "other-client", "", model.AppSpec{Image: "registry.invalid/other:1", Replicas: 1, RuntimeID: app.Spec.RuntimeID})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := s.BindBackingService(app.TenantID, other.ID, svc.ID, "postgres", nil); err != nil {
						t.Fatal(err)
					}
				} else if scenario == "stale-binding" {
					if _, err := s.BindBackingService(app.TenantID, app.ID, svc.ID, "postgres", nil); err != nil {
						t.Fatal(err)
					}
					request = map[string]any{"apply": true, "acknowledge_downtime": true, "expected_config_hash": body.Plan.ConfigHash}
				}
			}
			response := performJSONRequest(t, server, http.MethodPost, url, key, request)
			if response.Code != want {
				t.Fatalf("unsafe %s target accepted: %d %s", scenario, response.Code, response.Body.String())
			}
			ops, err := s.ListOperationsByApp(app.TenantID, false, app.ID)
			if err != nil || len(ops) != 0 {
				t.Fatalf("rejected migration created operation: %d %v", len(ops), err)
			}
		})
	}
}
