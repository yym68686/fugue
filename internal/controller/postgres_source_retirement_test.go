package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/pgretire/retiretest"
	"fugue/internal/store"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

func TestPostgresSourceRetirementExecutor(t *testing.T) {
	for _, independent := range []bool{false, true} {
		for _, scenario := range []string{"complete", "lost-cluster-response", "lost-pvc-response", "lost-pod-response", "source-running", "sql-unhealthy", "changed-after-intent", "protected-changed-after-intent"} {
			t.Run(fmt.Sprintf("independent=%v/%s", independent, scenario), func(t *testing.T) {
				st := newStoreTestDatabase(t, "")
				if err := st.Init(); err != nil {
					t.Fatal(err)
				}
				tenant, err := st.CreateTenant("retirement")
				if err != nil {
					t.Fatal(err)
				}
				project, err := st.CreateProject(tenant.ID, "database", "")
				if err != nil {
					t.Fatal(err)
				}
				pg := &model.AppPostgresSpec{Database: "example", User: "example", Password: "synthetic-secret", StorageSize: "1Gi", ServiceName: "current", Instances: 1}
				spec := model.AppSpec{Image: "registry.invalid/client:1", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Postgres: pg}
				if independent {
					spec.Postgres = nil
				}
				app, err := st.CreateApp(tenant.ID, project.ID, "consumer", "", spec)
				if err != nil {
					t.Fatal(err)
				}
				if independent {
					svc, e := st.CreateBackingService(tenant.ID, project.ID, "database", "", model.BackingServiceSpec{Postgres: pg})
					if e != nil {
						t.Fatal(e)
					}
					if _, e = st.BindBackingService(tenant.ID, app.ID, svc.ID, "postgres", nil); e != nil {
						t.Fatal(e)
					}
				}
				// Admission verifies the plan against the persisted app and binding.
				app, err = st.GetApp(app.ID)
				if err != nil {
					t.Fatal(err)
				}
				svc := app.BackingServices[0]
				originalService, _ := st.GetBackingService(svc.ID)
				target, err := store.PostgresImageMigrationTargetForApp(app, svc.ID)
				if err != nil {
					t.Fatal(err)
				}
				svc = *target.Service
				objects := retiretest.New(app, svc)
				protectedPVCPath := pgretire.Path("fg-"+strings.ReplaceAll(app.TenantID, "_", "-"), "persistentvolumeclaims", "unowned-archive")
				protectedBefore := pgretire.Hash([]any{objects[protectedPVCPath], objects["/api/v1/persistentvolumes/protected-pv"]})
				p, err := pgretire.Inspect(context.Background(), retiretest.Get(objects), app, svc)
				if err != nil {
					t.Fatal(err)
				}
				op, err := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, ServiceID: svc.ID, Type: pgretire.OperationType, DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{SourceRetirement: &p}}})
				if err != nil {
					t.Fatal(err)
				}
				op, found, err := st.TryClaimPendingOperation(op.ID)
				if err != nil || !found {
					t.Fatalf("claim: %v", err)
				}
				before, _ := st.GetApp(app.ID)
				mutations, lost := 0, false
				sourcePath := pgretire.Path(p.Namespace, "clusters", p.SourceCluster)
				podPath := pgretire.Path(p.Namespace, "pods", "previous-1")
				pvcPath := pgretire.Path(p.Namespace, "persistentvolumeclaims", "previous-1")
				pvPath := "/api/v1/persistentvolumes/source-pv"
				kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet {
						retiretest.ServeRead(objects, w, r)
						return
					}
					proof, e := st.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{pgretire.IntentEvidence}, Limit: 1})
					if e != nil || len(proof) != 1 {
						t.Error("mutation without durable intent")
					}
					path := r.URL.Path
					obj := objects[path]
					if obj == nil {
						t.Errorf("mutation of missing or unexpected resource: %s", path)
						w.WriteHeader(404)
						return
					}
					switch r.Method {
					case http.MethodPatch:
						if path != sourcePath && path != pvPath && path != pvcPath {
							t.Errorf("forbidden PATCH: %s", path)
							w.WriteHeader(400)
							return
						}
						raw, _ := io.ReadAll(r.Body)
						var fields []map[string]any
						json.Unmarshal(raw, &fields)
						if len(fields) < 3 || fields[0]["path"] != "/metadata/uid" || fields[1]["path"] != "/metadata/resourceVersion" || fields[0]["op"] != "test" || fields[1]["op"] != "test" {
							t.Fatal("missing optimistic identity fence")
						}
						for _, field := range fields[2:] {
							allowed := (path == sourcePath && field["path"] == "/metadata/annotations") || (path == pvPath && field["path"] == "/spec/persistentVolumeReclaimPolicy" && field["value"] == "Retain") || (path == pvcPath && (field["path"] == "/metadata/ownerReferences" || field["path"] == "/metadata/annotations" || field["path"] == "/metadata/labels"))
							if !allowed {
								t.Error("unexpected patch field", field)
							}
						}
						patch, e := jsonpatch.DecodePatch(raw)
						if e != nil {
							t.Fatal(e)
						}
						original, _ := json.Marshal(obj)
						updated, e := patch.Apply(original)
						if e != nil {
							t.Fatal(e)
						}
						json.Unmarshal(updated, &obj)
						objects[path] = obj
					case http.MethodDelete:
						if path != sourcePath && path != podPath {
							t.Errorf("forbidden DELETE: %s", path)
							w.WriteHeader(400)
							return
						}
						if pgretire.Owned(objects[pvcPath], p.SourceUID) || pgretire.Object(objects[pvPath]["spec"])["persistentVolumeReclaimPolicy"] != "Retain" {
							t.Fatal("deletion before storage protection")
						}
						var options map[string]any
						json.NewDecoder(r.Body).Decode(&options)
						pre := pgretire.Object(options["preconditions"])
						if pre["uid"] != pgretire.Meta(obj)["uid"] || pre["resourceVersion"] != pgretire.Meta(obj)["resourceVersion"] {
							t.Fatal("delete lacks UID/RV preconditions")
						}
						if path == sourcePath && options["propagationPolicy"] != "Orphan" {
							t.Fatal("source delete not orphaned")
						}
						if path == podPath && options["gracePeriodSeconds"] != float64(30) {
							t.Fatal("pod removal is not graceful")
						}
						delete(objects, path)
						if path == sourcePath {
							delete(pgretire.Meta(objects[podPath]), "ownerReferences") // Orphan GC keeps the exact Pod.
						}
					default:
						t.Errorf("forbidden mutation %s", r.Method)
						w.WriteHeader(400)
						return
					}
					mutations++
					if scenario == "changed-after-intent" && mutations == 1 {
						pgretire.Meta(objects[pgretire.Path(p.Namespace, "clusters", p.TargetCluster)])["uid"] = "replacement"
					}
					if scenario == "protected-changed-after-intent" && mutations == 1 {
						pgretire.Meta(objects[protectedPVCPath])["annotations"] = map[string]any{"changed": "externally"}
					}
					if !lost && ((scenario == "lost-cluster-response" && path == sourcePath && r.Method == http.MethodDelete) || (scenario == "lost-pvc-response" && path == pvcPath) || (scenario == "lost-pod-response" && path == podPath)) {
						lost = true
						http.Error(w, "response lost after commit", 500)
						return
					}
					json.NewEncoder(w).Encode(obj)
				}))
				defer kube.Close()
				s := &Service{Store: st, Config: config.ControllerConfig{KubectlApply: true}, Logger: log.New(io.Discard, "", 0), newKubeClient: func(string) (*kubeClient, error) { return &kubeClient{client: kube.Client(), baseURL: kube.URL}, nil }, postgresSourceRetirementProbe: func(context.Context, *kubeClient, model.PostgresSourceRetirementPlan, model.PostgresSourceRetirementPod) error {
					if scenario == "source-running" {
						return fmt.Errorf("source still running")
					}
					return nil
				}, managedPostgresPrimarySQLProbe: func(context.Context, string, string, model.AppPostgresSpec) error {
					if scenario == "sql-unhealthy" {
						return fmt.Errorf("SQL unavailable")
					}
					return nil
				}}
				err = s.executePostgresSourceRetirement(context.Background(), op, before)
				if scenario == "source-running" || scenario == "sql-unhealthy" {
					if err == nil || mutations != 0 {
						t.Fatalf("unsafe preflight: %v mutations=%d", err, mutations)
					}
					return
				}
				if scenario != "complete" {
					if !errors.Is(err, errPostgresSourceRetirementUnsettled) {
						t.Fatalf("unknown mutation lost interlock: %v", err)
					}
					for _, action := range []func() error{func() error { _, e := st.CancelOperation(op.ID, "cancel"); return e }, func() error { _, e := st.RequeueManagedOperationGuarded(op, "requeue"); return e }, func() error { _, e := st.UnbindBackingService(app.Bindings[0].ID); return e }} {
						if !errors.Is(action(), store.ErrConflict) {
							t.Fatal("retirement lease bypassed")
						}
					}
					if scenario == "changed-after-intent" || scenario == "protected-changed-after-intent" {
						if mutations != 1 {
							t.Fatal("continued after identity change")
						}
						return
					}
					err = s.executePostgresSourceRetirement(context.Background(), op, before)
				}
				if err != nil {
					t.Fatal(err)
				}
				final, _ := st.GetOperation(op.ID)
				after, _ := st.GetApp(app.ID)
				current, _ := st.GetBackingService(svc.ID)
				if final.Status != model.OperationStatusCompleted || mutations != 5 {
					t.Fatalf("retirement did not settle: %s mutations=%d", final.Status, mutations)
				}
				if !reflect.DeepEqual(before.Spec, after.Spec) || !reflect.DeepEqual(before.Status, after.Status) || !reflect.DeepEqual(before.Bindings, after.Bindings) || !before.UpdatedAt.Equal(after.UpdatedAt) || !reflect.DeepEqual(originalService.Spec, current.Spec) || originalService.OwnerAppID != current.OwnerAppID {
					t.Fatal("retirement changed active service/consumer")
				}
				if objects[sourcePath] != nil || objects[podPath] != nil || objects[pvcPath] == nil || objects[pvPath] == nil {
					t.Fatal("wrong retirement footprint")
				}
				if protectedBefore != pgretire.Hash([]any{objects[protectedPVCPath], objects["/api/v1/persistentvolumes/protected-pv"]}) {
					t.Fatal("retirement modified protected unowned storage")
				}
			})
		}
	}
}
