package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

func TestPostgresPlacementOperationWritesOnlyExactExistingCluster(t *testing.T) {
	for _, scenario := range []string{"stage", "release hold", "foreign", "missing", "replaced", "concurrent", "inactive", "ordinary deploy"} {
		t.Run(scenario, func(t *testing.T) {
			st := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, _ := st.CreateTenant("Operations")
			project, _ := st.CreateProject(tenant.ID, "applications", "")
			source, _, _ := st.CreateRuntime(tenant.ID, "source", model.RuntimeTypeManagedOwned, "", nil)
			destination, _, _ := st.CreateRuntime(tenant.ID, "destination", model.RuntimeTypeManagedOwned, "", nil)
			app, err := st.CreateApp(tenant.ID, project.ID, "sample", "", model.AppSpec{RuntimeID: source.ID, Image: "example.invalid/missing:historical", Replicas: 1, Postgres: &model.AppPostgresSpec{ServiceName: "sample-postgres", RuntimeID: source.ID, Database: "sample", User: "owner", Password: "fixture", StorageSize: "1Gi", Instances: 1}})
			if err != nil {
				t.Fatal(err)
			}
			app.Spec.Postgres = &model.AppPostgresSpec{ServiceName: "sample-postgres", RuntimeID: source.ID, Database: "sample", User: "owner", Password: "fixture", StorageSize: "1Gi", Instances: 2, PrimaryPlacementPendingRebalance: scenario != "release hold"}
			app, err = store.OverlayDesiredManagedPostgres(app)
			if err != nil {
				t.Fatal(err)
			}
			kind := model.OperationTypeDatabaseLocalize
			if scenario == "ordinary deploy" {
				kind = model.OperationTypeDeploy
			}
			final := *cloneControllerAppSpec(&app.Spec)
			op, err := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: kind, SourceRuntimeID: source.ID, TargetRuntimeID: destination.ID, DesiredSpec: &final})
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "inactive" {
				if _, err := st.FailOperation(op.ID, "test"); err != nil {
					t.Fatal(err)
				}
			}
			ns := runtime.NamespaceForTenant(tenant.ID)
			clusterPath := cloudNativePGClusterAPIPath(ns, "sample-postgres")
			managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
			managed["metadata"].(map[string]any)["uid"] = "managed-uid"
			var live map[string]any
			for _, obj := range runtime.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil) {
				if cloudNativePGObject(obj) {
					live = cloneKubeMap(obj)
				}
			}
			meta := objectMapField(live, "metadata")
			meta["uid"], meta["resourceVersion"] = "cluster-uid", "10"
			meta["annotations"] = map[string]any{"external.example/setting": "keep", runtime.CloudNativePGReconcilePodSpecAnno: runtime.CloudNativePGReconcilePodSpecHold}
			spec := objectMapField(live, "spec")
			spec["instances"] = 1
			spec["imageName"] = "example.invalid/postgres:previous"
			spec["resources"] = map[string]any{"requests": map[string]any{"cpu": "150m"}}
			spec["bootstrap"] = map[string]any{"initdb": map[string]any{"secret": map[string]any{"name": "legacy-credential"}}}
			spec["managed"] = map[string]any{"roles": []any{map[string]any{"name": "legacy-owner", "passwordSecret": map[string]any{"name": "legacy-credential"}}}}
			objectMapField(spec, "storage")["resizeInUseVolumes"] = false
			before := cloneKubeMap(live)
			writes, reads := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					switch r.URL.Path {
					case managedAppAPIPath(ns, runtime.ManagedAppResourceName(app)):
						_ = json.NewEncoder(w).Encode(managed)
					case clusterPath:
						reads++
						if scenario == "missing" {
							http.NotFound(w, r)
							return
						}
						if scenario == "foreign" {
							objectMapField(meta, "labels")[runtime.FugueLabelTenantID] = "other"
						}
						if scenario == "replaced" && reads > 1 {
							meta["uid"] = "replacement"
						}
						_ = json.NewEncoder(w).Encode(live)
					case "/api/v1/namespaces/" + ns + "/persistentvolumeclaims":
						_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
					default:
						http.NotFound(w, r)
					}
					return
				}
				if r.Method != http.MethodPatch || r.URL.Path != clusterPath || r.Header.Get("Content-Type") != "application/json-patch+json" {
					t.Errorf("unrelated write: %s %s", r.Method, r.URL.Path)
					http.Error(w, "forbidden", 403)
					return
				}
				var operations []map[string]any
				if err := json.NewDecoder(r.Body).Decode(&operations); err != nil {
					t.Error(err)
					return
				}
				if scenario == "concurrent" {
					meta["resourceVersion"] = "11"
				}
				encoded, _ := json.Marshal(operations)
				patch, err := jsonpatch.DecodePatch(encoded)
				if err != nil {
					t.Error(err)
					return
				}
				original, _ := json.Marshal(live)
				next, err := patch.Apply(original)
				if err != nil {
					http.Error(w, "conflict", 409)
					return
				}
				if err := json.Unmarshal(next, &live); err != nil {
					t.Error(err)
					return
				}
				writes++
				_ = json.NewEncoder(w).Encode(live)
			}))
			defer server.Close()
			client := &kubeClient{baseURL: server.URL, client: server.Client(), namespace: ns}
			svc := &Service{Store: st, newKubeClient: func(string) (*kubeClient, error) { return client, nil }}
			placements := map[string][]runtime.SchedulingConstraints{"sample-postgres": {{NodeSelector: map[string]string{"kubernetes.io/hostname": "destination"}}}}
			err = svc.applyManagedPostgresOperationObjects(context.Background(), op.ID, app, placements)
			if scenario != "stage" && scenario != "release hold" {
				if err == nil || writes != 0 {
					t.Fatalf("unsafe mutation: err=%v writes=%d", err, writes)
				}
				return
			}
			if err != nil || writes != 1 {
				t.Fatalf("apply: err=%v writes=%d", err, writes)
			}
			actual := objectMapField(live, "spec")
			for _, field := range []string{"bootstrap", "managed", "imageName", "resources", "enableSuperuserAccess"} {
				if !reflect.DeepEqual(actual[field], objectMapField(before, "spec")[field]) {
					t.Fatal("placement changed", field)
				}
			}
			if actual["instances"] != float64(2) || objectMapField(actual, "storage")["resizeInUseVolumes"] != false {
				t.Fatal("placement or resize policy lost")
			}
			annotations := objectMapField(objectMapField(live, "metadata"), "annotations")
			if annotations["external.example/setting"] != "keep" {
				t.Fatal("external annotation lost")
			}
			_, held := annotations[runtime.CloudNativePGReconcilePodSpecAnno]
			if held != (scenario == "stage") {
				t.Fatal("hold did not follow operation phase")
			}
		})
	}
}

func TestPostgresPlacementPatchRequiresConcurrencyIdentity(t *testing.T) {
	for _, identity := range []map[string]any{{}, {"uid": "one"}, {"resourceVersion": "1"}} {
		_, err := managedPostgresPlacementOperationPatch(map[string]any{"metadata": identity}, map[string]any{})
		if err == nil || !strings.Contains(err.Error(), "concurrency") {
			t.Fatal("missing identity accepted")
		}
	}
}
