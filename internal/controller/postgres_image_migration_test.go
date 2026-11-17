package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/pgimage"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestPostgresImageMigrationRecoveryReadFailurePreservesRunningOperation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	op := model.Operation{ID: "migration", AppID: "client", TenantID: "tenant", Type: pgimage.OperationType, Status: model.OperationStatusRunning, ExecutionMode: model.ExecutionModeManaged}
	state := model.State{Apps: []model.App{{ID: op.AppID, TenantID: op.TenantID, Status: model.AppStatus{Phase: "deleted"}}}, Operations: []model.Operation{op}}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	s := &Service{Store: store.New(path), Config: config.ControllerConfig{KubectlApply: true}, Logger: log.New(io.Discard, "", 0)}
	if err = s.recoverCompletedManagedDeploys(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, err := s.Store.GetOperation(op.ID)
	if err != nil || current.Status != model.OperationStatusRunning {
		t.Fatalf("app read failure released an unknown mutation: status=%s err=%v", current.Status, err)
	}
}

func TestPostgresImageMigrationExecutorPreservesDataAndRecoversLostPatch(t *testing.T) {
	for _, ownership := range []string{"app-owned", "independent"} {
		t.Run(ownership, func(t *testing.T) { testPostgresImageMigrationExecutor(t, ownership == "independent") })
	}
}

func testPostgresImageMigrationExecutor(t *testing.T, independent bool) {
	for _, scenario := range []string{"ready", "lost-patch-response", "changed-volume", "wrong-source-digest", "fenced"} {
		t.Run(scenario, func(t *testing.T) {
			state := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("Image Migration")
			if err != nil {
				t.Fatal(err)
			}
			project, err := state.CreateProject(tenant.ID, "database", "")
			if err != nil {
				t.Fatal(err)
			}
			appSpec := model.AppSpec{Image: "registry.invalid/client:1", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Postgres: &model.AppPostgresSpec{Database: "example", User: "example", Password: "test-secret", StorageSize: "1Gi", Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512, CPULimitMilliCores: 200, MemoryLimitMebibytes: 768}}}
			postgresSpec := appSpec.Postgres
			if independent {
				appSpec.Postgres = nil
			}
			app, err := state.CreateApp(tenant.ID, project.ID, "database-client", "", appSpec)
			if err != nil {
				t.Fatal(err)
			}
			if independent {
				service, err := state.CreateBackingService(tenant.ID, project.ID, "independent-postgres", "", model.BackingServiceSpec{Postgres: postgresSpec})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := state.BindBackingService(tenant.ID, app.ID, service.ID, "postgres", nil); err != nil {
					t.Fatal(err)
				}
				app, err = state.GetApp(app.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			service := app.BackingServices[0]
			postgres := store.ManagedPostgresSpecForOperation(app, service.ID)
			catalog, err := pgimage.ApprovedCatalog()
			if err != nil {
				t.Fatal(err)
			}
			op, err := state.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID, Type: pgimage.OperationType, DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{Image: catalog.DefaultImage, ImageMigrationConfigHash: store.PostgresImageMigrationConfigHash(app, service, catalog.DefaultImage)}}})
			if err != nil {
				t.Fatal(err)
			}
			op, found, err := state.TryClaimPendingOperation(op.ID)
			if err != nil || !found {
				t.Fatalf("claim: %v", err)
			}
			before, _ := state.GetApp(app.ID)
			ns, name := runtime.NamespaceForTenant(app.TenantID), postgres.ServiceName
			var cluster, pod map[string]any
			json.Unmarshal([]byte(`{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","metadata":{"uid":"cluster-uid","generation":1,"resourceVersion":"1"},"spec":{"instances":1,"storage":{"size":"1Gi"},"affinity":{"nodeSelector":{}}},"status":{"instances":1,"readyInstances":1,"currentPrimary":"database-1","targetPrimary":"database-1","systemID":"123456","conditions":[{"type":"Ready","status":"True"}]}}`), &cluster)
			imageObject(cluster["metadata"])["name"] = name
			imageObject(cluster["metadata"])["namespace"] = ns
			spec := imageObject(cluster["spec"])
			spec["primaryUpdateStrategy"] = "unsupervised"
			spec["primaryUpdateMethod"] = "restart"
			source := strings.Split(catalog.PostgresBase, "@")[0]
			spec["imageName"] = source
			imageObject(imageObject(spec["affinity"])["nodeSelector"])[runtime.RuntimeIDLabelKey] = postgres.RuntimeID
			resources := map[string]any{"requests": map[string]any{"cpu": "100m", "memory": "512Mi"}, "limits": map[string]any{"cpu": "200m", "memory": "768Mi"}}
			spec["resources"] = resources
			json.Unmarshal([]byte(`{"metadata":{"name":"database-1","uid":"old-pod-uid","generation":1,"resourceVersion":"1","labels":{"role":"primary","cnpg.io/instanceRole":"primary"},"ownerReferences":[{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","uid":"cluster-uid","controller":true}]},"spec":{"nodeName":"node-a","containers":[{"name":"postgres"}],"volumes":[{"name":"pgdata","persistentVolumeClaim":{"claimName":"database-1"}}],"initContainers":[{"name":"bootstrap-controller"}]},"status":{"phase":"Running","observedGeneration":1,"conditions":[{"type":"Ready","status":"True"}],"initContainerStatuses":[{"name":"bootstrap-controller","state":{"terminated":{"exitCode":0}}}],"containerStatuses":[{"name":"postgres","ready":true,"restartCount":0,"state":{"running":{"startedAt":"2026-01-01T00:00:00Z"}}}]}}`), &pod)
			imageObject(pod["metadata"])["namespace"] = ns
			imageObject(imageObject(pod["metadata"])["labels"])[cloudNativePGClusterLabel] = name
			imageObjects(imageObject(pod["metadata"])["ownerReferences"])[0]["name"] = name
			main := imageContainer(pod)
			main["resources"] = resources
			main["image"] = source
			status := imageObjects(imageObject(pod["status"])["containerStatuses"])[0]
			status["resources"] = resources
			status["imageID"] = catalog.PostgresBase
			if scenario == "wrong-source-digest" {
				status["imageID"] = "registry.invalid/pg@sha256:other"
			}
			if scenario == "fenced" {
				imageObject(cluster["metadata"])["annotations"] = map[string]any{"cnpg.io/fencedInstances": `["*"]`}
			}
			patches, probes := 0, 0
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				var response any
				switch {
				case r.Method == http.MethodPatch:
					if r.URL.Path != cloudNativePGClusterAPIPath(ns, name) || r.Header.Get("Content-Type") != "application/json-patch+json" {
						t.Error("unexpected mutation", r.Method, r.URL.Path)
					}
					proofs, e := state.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{pgimage.IntentEvidence}, Limit: 1})
					if e != nil || len(proofs) != 1 {
						t.Error("PATCH sent before durable intent")
					}
					var patch []map[string]any
					json.NewDecoder(r.Body).Decode(&patch)
					if len(patch) != 4 || patch[0]["op"] != "test" || patch[1]["op"] != "test" || patch[2]["path"] != "/spec" || patch[3]["op"] != "replace" || patch[3]["path"] != "/spec/imageName" || patch[3]["value"] != catalog.DefaultImage {
						t.Error("image patch changed unexpected fields", patch)
					}
					patches++
					spec["imageName"] = catalog.DefaultImage
					imageObject(pod["metadata"])["uid"] = "new-pod-uid"
					imageContainer(pod)["image"] = catalog.DefaultImage
					status["imageID"] = catalog.DefaultImage
					imageContainer(pod)["command"] = []any{"/usr/local/bin/fugue-cnpg-manager", "instance", "run"}
					delete(imageObject(pod["spec"]), "initContainers")
					if scenario == "lost-patch-response" {
						http.Error(w, "response lost after commit", 500)
						return
					}
					response = cluster
				case r.Method != http.MethodGet:
					t.Error("forbidden mutation", r.Method, r.URL.Path)
				case r.URL.Path == cloudNativePGClusterAPIPath(ns, name):
					response = cluster
				case strings.HasSuffix(r.URL.Path, "/pods/database-1"):
					response = pod
				case strings.HasSuffix(r.URL.Path, "/pods"):
					response = map[string]any{"items": []any{pod}}
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/database-1"):
					response = map[string]any{"metadata": map[string]any{"uid": "pvc-uid"}, "spec": map[string]any{"volumeName": "pv-data"}, "status": map[string]any{"phase": "Bound"}}
				case r.URL.Path == "/api/v1/persistentvolumes/pv-data":
					uid := "pv-uid"
					if patches > 0 && scenario == "changed-volume" {
						uid = "replacement-pv"
					}
					response = map[string]any{"metadata": map[string]any{"uid": uid}, "spec": map[string]any{"claimRef": map[string]any{"uid": "pvc-uid", "namespace": ns, "name": "database-1"}}}
				case r.URL.Path == "/api/v1/nodes":
					response = map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": "node-a", "labels": map[string]any{runtime.RuntimeIDLabelKey: postgres.RuntimeID}}}}}
				case r.URL.Path == "/api/v1/nodes/node-a":
					response = map[string]any{"metadata": map[string]any{"name": "node-a"}, "status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}
				case strings.HasSuffix(r.URL.Path, "/deployments"):
					response = map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"generation": 1}, "spec": map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": catalog.OperatorImage}}}}}, "status": map[string]any{"observedGeneration": 1, "readyReplicas": 1, "updatedReplicas": 1}}}}
				default:
					t.Error("unexpected read", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer kube.Close()
			s := &Service{Store: state, Config: config.ControllerConfig{KubectlApply: true}, Logger: log.New(io.Discard, "", 0), newKubeClient: func(ns string) (*kubeClient, error) {
				return &kubeClient{client: kube.Client(), baseURL: kube.URL, namespace: ns}, nil
			}, postgresImageMigrationProbe: func(context.Context, *kubeClient, string, model.AppPostgresSpec, managedPostgresResizeSnapshot, map[string]any) error {
				probes++
				return nil
			}}
			err = s.executePostgresImageMigration(context.Background(), op, before)
			if scenario == "wrong-source-digest" || scenario == "fenced" {
				if err == nil || patches != 0 || probes != 0 {
					t.Fatalf("unsafe preflight passed: patches=%d probes=%d err=%v", patches, probes, err)
				}
				return
			}
			if scenario == "changed-volume" || scenario == "lost-patch-response" {
				if !errors.Is(err, errPostgresImageMigrationUnsettled) {
					t.Fatalf("accepted mutation lost interlock: %v", err)
				}
				active, _ := state.GetOperation(op.ID)
				current, _ := state.GetBackingService(service.ID)
				if active.Status != model.OperationStatusRunning || current.Spec.Postgres.Image != "" {
					t.Fatal("unverified mutation marked terminal or persisted early")
				}
				if _, err = state.CancelOperation(op.ID, "cancel"); !errors.Is(err, store.ErrConflict) {
					t.Fatal("accepted mutation became cancellable")
				}
				if _, err = state.RequeueManagedOperationGuarded(op, "requeue"); !errors.Is(err, store.ErrConflict) {
					t.Fatal("accepted mutation became requeueable")
				}
				if scenario == "changed-volume" {
					return
				}
				err = s.executePostgresImageMigration(context.Background(), op, before)
			}
			if err != nil {
				t.Fatal(err)
			}
			final, _ := state.GetOperation(op.ID)
			current, _ := state.GetBackingService(service.ID)
			after, _ := state.GetApp(app.ID)
			if final.Status != model.OperationStatusCompleted || current.Spec.Postgres.Image != catalog.DefaultImage || patches != 1 || probes != 2 {
				t.Fatalf("unexpected terminal state: %s patches=%d probes=%d", final.Status, patches, probes)
			}
			if current.OwnerAppID != service.OwnerAppID {
				t.Fatal("migration changed independent ownership")
			}
			if !reflect.DeepEqual(before.Spec, after.Spec) || !reflect.DeepEqual(before.Status, after.Status) || !reflect.DeepEqual(before.Bindings, after.Bindings) || !before.UpdatedAt.Equal(after.UpdatedAt) {
				t.Fatal("migration mutated the consumer application")
			}
		})
	}
}
