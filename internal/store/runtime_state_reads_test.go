package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func runtimeReadFixture(t *testing.T, postgres bool) (*Store, model.App, model.App) {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "state.json"))
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

func TestRuntimeStateOperationIDsFilterBeforeHydration(t *testing.T) {
	for _, backend := range []string{"file", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			postgres := backend == "postgres"
			s, app, other := runtimeReadFixture(t, postgres)
			base := time.Now().UTC().Truncate(time.Microsecond)
			statuses := []string{model.OperationStatusPending, model.OperationStatusCompleted, model.OperationStatusRunning, model.OperationStatusWaitingAgent, model.OperationStatusFailed, model.OperationStatusCanceled, model.OperationStatusPending, model.OperationStatusRunning}
			rows := make([]model.Operation, 0, len(statuses))
			for i, status := range statuses {
				op := model.Operation{ID: model.NewID("runtime_read_op"), TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeImport, Status: status, CreatedAt: base.Add(time.Duration(i) * time.Second), DesiredSource: &model.AppSource{Type: model.AppSourceTypeUpload}, DesiredSpec: &model.AppSpec{Env: map[string]string{"SYNTHETIC": strings.Repeat("payload", 10000)}}}
				if i == 6 {
					op.DesiredSource = nil
				}
				if i == 7 {
					op.AppID = other.ID
				}
				rows = append(rows, op)
			}
			persist := func() {
				t.Helper()
				if !postgres {
					if err := s.withLockedState(true, func(st *model.State) error { st.Operations = rows; return nil }); err != nil {
						t.Fatal(err)
					}
					return
				}
				for _, op := range rows {
					var source any
					if op.DesiredSource != nil {
						raw, err := json.Marshal(op.DesiredSource)
						if err != nil {
							t.Fatal(err)
						}
						source = raw
					}
					spec, err := json.Marshal(op.DesiredSpec)
					if err != nil {
						t.Fatal(err)
					}
					_, err = s.db.Exec(`INSERT INTO fugue_operations(id,tenant_id,type,status,execution_mode,requested_by_type,requested_by_id,app_id,desired_spec_json,desired_source_json,created_at,updated_at) VALUES($1,$2,$3,$4,'managed','bootstrap','test',$5,$6::jsonb,$7::jsonb,$8,$8) ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status,desired_source_json=EXCLUDED.desired_source_json`, op.ID, op.TenantID, op.Type, op.Status, op.AppID, spec, source, op.CreatedAt)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			check := func() {
				t.Helper()
				full, err := s.ListOperationsWithDesiredSourceByApp(app.TenantID, false, app.ID)
				if err != nil {
					t.Fatal(err)
				}
				want := []string{}
				for _, op := range full {
					if isActiveOperationStatus(op.Status) {
						want = append(want, op.ID)
					}
				}
				got, err := s.ListActiveSourceOperationIDsByApp(context.Background(), app.TenantID, false, app.ID)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("membership/order differs: got=%v want=%v err=%v", got, want, err)
				}
			}
			persist()
			check()
			got, err := s.ListActiveSourceOperationIDsByApp(context.Background(), app.TenantID, false, app.ID)
			if err != nil || !reflect.DeepEqual(got, []string{rows[0].ID, rows[2].ID, rows[3].ID}) {
				t.Fatalf("wrong active IDs: %v err=%v", got, err)
			}
			if got, err := s.ListActiveSourceOperationIDsByApp(context.Background(), "other-tenant", false, app.ID); err != nil || len(got) != 0 {
				t.Fatalf("tenant isolation failed: %v %v", got, err)
			}
			if got, err := s.ListActiveSourceOperationIDsByApp(context.Background(), "other-tenant", true, app.ID); err != nil || len(got) != 3 {
				t.Fatalf("admin semantics changed: %v %v", got, err)
			}
			if got, err := s.ListActiveSourceOperationIDsByApp(context.Background(), app.TenantID, true, " "); err != nil || len(got) != 0 {
				t.Fatalf("blank app widened query: %v %v", got, err)
			}
			rows[0].Status = model.OperationStatusCompleted
			rows[2].DesiredSource = nil
			rows[3].Status = model.OperationStatusFailed
			persist()
			check()
			if postgres {
				// Historical bodies are irrelevant to current pending membership. This
				// invalid retired payload makes an accidental full reader fail decoding.
				if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_spec_json='"not-an-app-spec"'::jsonb WHERE id=$1`, rows[1].ID); err != nil {
					t.Fatal(err)
				}
				if _, err := s.ListOperationsWithDesiredSourceByApp(app.TenantID, false, app.ID); err == nil {
					t.Fatal("fixture did not exercise historical hydration")
				}
				if got, err := s.ListActiveSourceOperationIDsByApp(context.Background(), app.TenantID, false, app.ID); err != nil || len(got) != 0 {
					t.Fatalf("historical payload affected membership: %v %v", got, err)
				}
				// The existing PostgreSQL reader uses IS NOT NULL, which includes
				// JSON null. Keep that membership distinct from an SQL NULL source.
				if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_spec_json=NULL WHERE id=$1`, rows[1].ID); err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.Exec(`UPDATE fugue_operations SET desired_source_json='null'::jsonb WHERE id=$1`, rows[6].ID); err != nil {
					t.Fatal(err)
				}
				check()
				if got, err := s.ListActiveSourceOperationIDsByApp(context.Background(), app.TenantID, false, app.ID); err != nil || !reflect.DeepEqual(got, []string{rows[6].ID}) {
					t.Fatalf("JSON-null source membership changed: %v %v", got, err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := s.ListActiveSourceOperationIDsByApp(ctx, app.TenantID, false, app.ID); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation ignored: %v", err)
			}
		})
	}
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
