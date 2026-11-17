package api

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/pgretire/retiretest"
	"fugue/internal/store"
)

func TestPostgresSourceRetirementAdmission(t *testing.T) {
	for _, independent := range []bool{false, true} {
		name := "owned"
		if independent {
			name = "independent"
		}
		t.Run(name, func(t *testing.T) {
			fixture := managedPostgresLifecycleFixture
			var st *store.Store
			var server *Server
			var tenantKey string
			var app model.App
			var svc model.BackingService
			if independent {
				st, server, tenantKey, app, svc = independentImageMigrationFixture(t)
			} else {
				st, server, tenantKey, app, svc = fixture(t, 0)
			}
			const adminKey = "synthetic-retirement-admin"
			server.auth = auth.New(st, adminKey)
			target, err := store.PostgresImageMigrationTargetForApp(app, svc.ID)
			if err != nil {
				t.Fatal(err)
			}
			objects := retiretest.New(app, *target.Service)
			mutations := 0
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					mutations++
					w.WriteHeader(400)
					return
				}
				retiretest.ServeRead(objects, w, r)
			}))
			defer kube.Close()
			server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
				return &managedAppStatusClient{client: kube.Client(), baseURL: kube.URL}, nil
			}
			path := "/v1/backing-services/" + svc.ID + "/retire-source"
			before, _ := st.GetBackingService(svc.ID)
			priorOps, _ := st.ListOperationsByApp(app.TenantID, false, app.ID)
			if response := performJSONRequest(t, server, http.MethodPost, path, tenantKey, map[string]any{}); response.Code != 403 {
				t.Fatalf("tenant could retire source: %d", response.Code)
			}
			preview := performJSONRequest(t, server, http.MethodPost, path, adminKey, map[string]any{})
			if preview.Code != 200 {
				t.Fatalf("plan: %d %s", preview.Code, preview.Body.String())
			}
			var plan struct {
				Plan   model.PostgresSourceRetirementPlan `json:"plan"`
				DryRun bool                               `json:"dry_run"`
			}
			mustDecodeJSON(t, preview, &plan)
			ops, _ := st.ListOperationsByApp(app.TenantID, false, app.ID)
			if !plan.DryRun || !plan.Plan.PreserveStorage || len(ops) != len(priorOps) || mutations != 0 {
				t.Fatal("preview caused changes")
			}
			for _, request := range []map[string]any{{"apply": true}, {"apply": true, "expected_source_uid": "wrong", "expected_plan_hash": plan.Plan.PlanHash}} {
				res := performJSONRequest(t, server, http.MethodPost, path, adminKey, request)
				if res.Code != 400 && res.Code != 409 {
					t.Fatalf("unconfirmed apply accepted: %d", res.Code)
				}
			}
			var first model.Operation
			for i := 0; i < 2; i++ {
				res := performJSONRequest(t, server, http.MethodPost, path, adminKey, map[string]any{"apply": true, "expected_source_uid": plan.Plan.SourceUID, "expected_plan_hash": plan.Plan.PlanHash})
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
				} else if first.ID != body.Operation.ID {
					t.Fatal("repeated apply duplicated operation")
				}
			}
			after, _ := st.GetBackingService(svc.ID)
			if !reflect.DeepEqual(before.Spec, after.Spec) || before.OwnerAppID != after.OwnerAppID || mutations != 0 {
				t.Fatal("admission mutated runtime or desired state")
			}
			if _, err = st.UnbindBackingService(app.Bindings[0].ID); err != store.ErrConflict {
				t.Fatal("binding changed during retirement")
			}
		})
	}
}
