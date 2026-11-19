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
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/pgimage"
	"fugue/internal/runtime"
	"fugue/internal/storagerecovery"
	"fugue/internal/store"
)

func TestPostgresImageDrainPreservesConcurrentDatabaseSettings(t *testing.T) {
	for _, having := range []string{"none", "postgresql", "parameters"} {
		t.Run(having, func(t *testing.T) {
			spec := map[string]any{"smartShutdownTimeout": float64(180), "stopDelay": float64(1800)}
			switch having {
			case "postgresql":
				spec["postgresql"] = map[string]any{"syncReplicaElectionConstraint": map[string]any{"enabled": true}}
			case "parameters":
				spec["postgresql"] = map[string]any{"parameters": map[string]any{"log_min_duration_statement": "-1"}}
			}
			cluster := map[string]any{"spec": spec}
			before := imagePreservedSpecHash(cluster)
			var w postgresImageWitness
			if err := configurePostgresImageDrain(&w, cluster, "op", 10); err != nil {
				t.Fatal(err)
			}
			spec["smartShutdownTimeout"] = float64(10)
			if spec["postgresql"] == nil {
				spec["postgresql"] = map[string]any{}
			}
			postgres := imageObject(spec["postgresql"])
			if postgres["parameters"] == nil {
				postgres["parameters"] = map[string]any{}
			}
			params := imageObject(postgres["parameters"])
			params[pgimage.DrainMarker] = w.Drain.Marker
			original, staged, err := originalImageDrainSpec(cluster, w.Drain)
			if err != nil || !staged || imagePreservedSpecHash(original) != before {
				t.Fatalf("staged settings do not reconstruct original: %v", err)
			}
			params["shared_buffers"] = "64MB"
			original, _, err = originalImageDrainSpec(cluster, w.Drain)
			if err == nil && imagePreservedSpecHash(original) == before {
				t.Fatal("normalization masked independently changed parameters")
			}
		})
	}
}

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
	scenarios := []string{"ready", "lost-patch-response", "changed-volume", "wrong-source-digest", "fenced", "resized-up", "resized-down", "resized-unlimited", "resized-lost-patch-response", "resized-lost-before-patch", "resized-changed-volume", "resized-unapplied", "resized-extra-resource-drift", "resized-extra-resource-preserved", "unpersisted-resource-drift", "resized-cas-conflict", "resized-replacement-drift", "resized-bootstrap-patch-lost-patch-response", "custom-pod-patch"}
	if independent {
		scenarios = append(scenarios, "cold-target-lost-patch-response", "cold-target-marker-removed", "fast-ready", "fast-blocked-before-stage", "fast-blocked-after-stage", "fast-stage-response-lost", "fast-image-lost-patch-response", "fast-restore-response-lost")
	}
	for _, scenario := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			statePath := filepath.Join(t.TempDir(), "state.json")
			state := store.New(statePath)
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
			if _, err = state.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 4000, MemoryMebibytes: 8192, StorageGibibytes: 20}); err != nil {
				t.Fatal(err)
			}
			appSpec := model.AppSpec{Image: "registry.invalid/client:1", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Postgres: &model.AppPostgresSpec{Database: "example", User: "example", Password: "test-secret", StorageSize: "1Gi", Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512, CPULimitMilliCores: 200, MemoryLimitMebibytes: 768}}}
			if scenario == "resized-unlimited" {
				appSpec.Postgres.Resources.CPULimitMilliCores = 0
			}
			postgresSpec := appSpec.Postgres
			coldTarget := strings.HasPrefix(scenario, "cold-target-")
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
			if coldTarget {
				persisted, err := state.GetBackingService(service.ID)
				if err != nil {
					t.Fatal(err)
				}
				current := *persisted.Spec.Postgres
				desired := app.Spec
				desired.Postgres = model.CloneAppPostgresSpec(&current)
				recovery, err := state.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID, Type: storagerecovery.OperationType, TargetRuntimeID: current.RuntimeID, DesiredSpec: &desired})
				if err != nil {
					t.Fatal(err)
				}
				claimedRecovery, claimed, err := state.TryClaimPendingOperation(recovery.ID)
				if err != nil || !claimed {
					t.Fatalf("claim cold recovery: %v", err)
				}
				next := current
				next.RuntimeID = claimedRecovery.TargetRuntimeID
				next.ServiceName = "restored-cluster"
				next.EndpointServiceName = model.PostgresEndpointName(current)
				if _, err := state.CommitColdPostgresTarget(recovery.ID, store.PostgresSpecFingerprint(current), next); err != nil {
					t.Fatal(err)
				}
				if _, err := state.CompleteManagedOperationWithResult(recovery.ID, "", "verified synthetic recovery", &app.Spec, nil); err != nil {
					t.Fatal(err)
				}
				app, err = state.GetApp(app.ID)
				if err != nil {
					t.Fatal(err)
				}
				service = app.BackingServices[0]
			}
			resized := strings.HasPrefix(scenario, "resized-")
			if resized {
				target := *service.Spec.Postgres.Resources
				target.MemoryMebibytes, target.MemoryLimitMebibytes = 1024, 2048
				if scenario == "resized-down" {
					target.MemoryMebibytes, target.MemoryLimitMebibytes = 128, 384
				}
				resize, err := state.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID, Type: model.OperationTypeDatabaseResize, RequestedByType: model.ActorTypeSystem, RequestedByID: store.ManagedPostgresAutoMemoryRequestActorPrefix + "/synthetic", DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{RuntimeResources: &target}}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = state.CompleteManagedOperation(resize.ID, "", "verified synthetic resize"); err != nil {
					t.Fatal(err)
				}
				app, err = state.GetApp(app.ID)
				if err != nil {
					t.Fatal(err)
				}
				service = app.BackingServices[0]
			}
			postgres := store.ManagedPostgresSpecForOperation(app, service.ID)
			catalog, err := pgimage.ApprovedCatalog()
			if err != nil {
				t.Fatal(err)
			}
			fast := strings.HasPrefix(scenario, "fast-")
			budget := 0
			if fast {
				budget = 15
			}
			op, err := state.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID, Type: pgimage.OperationType, DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{Image: catalog.DefaultImage, ImageMigrationConfigHash: store.PostgresImageMigrationPlanHash(app, service, catalog.DefaultImage, budget), ImageMigrationDrainBudgetSeconds: budget}}})
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
			params := map[string]any{"shared_buffers": "128MB"}
			spec["smartShutdownTimeout"], spec["stopDelay"] = float64(180), float64(1800)
			spec["postgresql"] = map[string]any{"parameters": params}
			source := strings.Split(catalog.PostgresBase, "@")[0]
			spec["imageName"] = source
			imageObject(imageObject(spec["affinity"])["nodeSelector"])[runtime.RuntimeIDLabelKey] = postgres.RuntimeID
			resourceMap := func(target model.ResourceSpec) map[string]any {
				kube, err := managedPostgresKubeResources(target)
				if err != nil {
					t.Fatal(err)
				}
				b, _ := json.Marshal(kube)
				var m map[string]any
				json.Unmarshal(b, &m)
				return m
			}
			resources := resourceMap(model.ManagedPostgresRuntimeResources(*postgres))
			spec["resources"] = resourceMap(*postgres.Resources)
			if scenario == "resized-extra-resource-drift" {
				imageObject(resources["requests"])["ephemeral-storage"] = "1Gi"
			}
			if scenario == "resized-extra-resource-preserved" {
				imageObject(resources["requests"])["ephemeral-storage"] = "1Gi"
				imageObject(imageObject(spec["resources"])["requests"])["ephemeral-storage"] = "1Gi"
			}
			if scenario == "unpersisted-resource-drift" {
				imageObject(resources["requests"])["memory"] = "640Mi"
			}
			json.Unmarshal([]byte(`{"metadata":{"name":"database-1","uid":"old-pod-uid","generation":1,"resourceVersion":"1","labels":{"role":"primary","cnpg.io/instanceRole":"primary"},"ownerReferences":[{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","uid":"cluster-uid","controller":true}]},"spec":{"nodeName":"node-a","containers":[{"name":"postgres"}],"volumes":[{"name":"pgdata","persistentVolumeClaim":{"claimName":"database-1"}}],"initContainers":[{"name":"bootstrap-controller"}]},"status":{"phase":"Running","observedGeneration":1,"conditions":[{"type":"Ready","status":"True"}],"initContainerStatuses":[{"name":"bootstrap-controller","state":{"terminated":{"exitCode":0}}}],"containerStatuses":[{"name":"postgres","ready":true,"restartCount":0,"state":{"running":{"startedAt":"2026-01-01T00:00:00Z"}}}]}}`), &pod)
			imageObject(pod["metadata"])["namespace"] = ns
			imageObject(imageObject(pod["metadata"])["labels"])[cloudNativePGClusterLabel] = name
			imageObjects(imageObject(pod["metadata"])["ownerReferences"])[0]["name"] = name
			main := imageContainer(pod)
			main["resources"] = resources
			main["image"] = source
			status := imageObjects(imageObject(pod["status"])["containerStatuses"])[0]
			status["resources"] = resources
			if scenario == "resized-unapplied" {
				status["resources"] = spec["resources"]
			}
			status["imageID"] = catalog.PostgresBase
			if scenario == "wrong-source-digest" {
				status["imageID"] = "registry.invalid/pg@sha256:other"
			}
			if scenario == "fenced" {
				imageObject(cluster["metadata"])["annotations"] = map[string]any{"cnpg.io/fencedInstances": `["*"]`}
			}
			marker, endpoint := coldStateName(service.ID), model.PostgresEndpointName(*postgres)
			var coldStateJSON []byte
			if coldTarget {
				imageObject(cluster["metadata"])["annotations"] = map[string]any{coldMigrationAnnotation: marker}
				imageObject(cluster["metadata"])["labels"] = map[string]any{runtime.FugueLabelBackingServiceID: service.ID}
				imageObject(pod["status"])["podIP"] = "10.42.0.7"
				coldStateJSON, err = json.Marshal(coldPostgresState{Version: 1, ServiceID: service.ID, SourceName: "previous", SourceUID: "previous-uid", TargetName: name, TargetUID: "cluster-uid", Endpoint: endpoint, SystemID: "123456", Phase: "complete"})
				if err != nil {
					t.Fatal(err)
				}
			}
			bootstrapPatch := strings.Contains(scenario, "bootstrap-patch")
			const retainedAnnotation = "fugue.io/test-retained"
			if bootstrapPatch || scenario == "custom-pod-patch" {
				value := migrationTestBootstrapPodPatch
				if !bootstrapPatch {
					value = `[{"op":"remove","path":"/spec/containers/0/resources"}]`
				}
				imageObject(cluster["metadata"])["annotations"] = map[string]any{imageMigrationPodPatchAnnotation: value, retainedAnnotation: "preserve"}
			}
			patches, probes := 0, 0
			var baselineSpec map[string]any
			b, _ := json.Marshal(spec)
			json.Unmarshal(b, &baselineSpec)
			drainWrites, drainSamples := 0, 0
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
					} else if coldTarget && proofs[0].Payload["cold_migration"] != marker {
						t.Error("PATCH sent without the marked target recorded in durable intent")
					}
					var patch []map[string]any
					json.NewDecoder(r.Body).Decode(&patch)
					if fast && len(patch) == 5 && patch[3]["path"] == "/spec/smartShutdownTimeout" {
						if pgimage.Hash(patch[2]["value"]) != pgimage.Hash(spec) {
							t.Error("drain did not guard full Cluster spec")
						}
						drainWrites++
						spec["smartShutdownTimeout"] = patch[3]["value"]
						if patch[4]["path"] != "/spec/postgresql/parameters/"+pgimage.DrainMarker {
							t.Error("did not patch the exact acknowledgement parameter")
						}
						if patch[4]["op"] == "add" {
							params[pgimage.DrainMarker] = patch[4]["value"]
						} else {
							delete(params, pgimage.DrainMarker)
						}
						if (scenario == "fast-stage-response-lost" && drainWrites == 1) || (scenario == "fast-restore-response-lost" && drainWrites == 2) {
							http.Error(w, "response lost after drain PATCH", 500)
							return
						}
						json.NewEncoder(w).Encode(cluster)
						return
					}
					if fast && (drainWrites != 1 || params[pgimage.DrainMarker] == nil || drainSamples < 3) {
						t.Error("image changed before temporary policy was loaded")
					}
					expectedPatches := 4
					if resized {
						expectedPatches = 5
					}
					if bootstrapPatch {
						expectedPatches += 2
					}
					if len(patch) != expectedPatches || patch[0]["op"] != "test" || patch[1]["op"] != "test" || patch[2]["path"] != "/spec" || pgimage.Hash(patch[2]["value"]) != pgimage.Hash(spec) || patch[3]["op"] != "replace" || patch[3]["path"] != "/spec/imageName" || patch[3]["value"] != catalog.DefaultImage {
						t.Error("image patch changed unexpected fields", patch)
					}
					if scenario == "resized-cas-conflict" || (scenario == "resized-lost-before-patch" && patches == 0) {
						patches++
						http.Error(w, "uncertain or conflicting write", http.StatusConflict)
						return
					}
					patches++
					spec["imageName"] = catalog.DefaultImage
					if resized {
						if patch[4]["op"] != "replace" || patch[4]["path"] != "/spec/resources" || pgimage.Hash(patch[4]["value"]) != pgimage.Hash(resources) {
							t.Fatal("restart template was not aligned atomically", patch)
						}
						spec["resources"] = patch[4]["value"]
					}
					if bootstrapPatch {
						expected := removeImageMigrationPodPatch(migrationTestBootstrapPodPatch)
						if !reflect.DeepEqual(patch[len(patch)-2:], expected) {
							t.Errorf("missing atomic exact annotation removal: %v", patch)
							http.Error(w, "stale patch would block replacement", http.StatusConflict)
							return
						}
						if len(proofs) != 1 || proofs[0].Payload["bootstrap_pod_patch"] != migrationTestBootstrapPodPatch {
							t.Error("annotation removal not pinned in durable intent")
						}
						delete(imageObject(imageObject(cluster["metadata"])["annotations"]), imageMigrationPodPatchAnnotation)
					}
					imageObject(pod["metadata"])["uid"] = "new-pod-uid"
					imageContainer(pod)["image"] = catalog.DefaultImage
					status["imageID"] = catalog.DefaultImage
					imageContainer(pod)["command"] = []any{"/usr/local/bin/fugue-cnpg-manager", "instance", "run"}
					delete(imageObject(pod["spec"]), "initContainers")
					if scenario == "cold-target-marker-removed" {
						delete(imageObject(cluster["metadata"]), "annotations")
					}
					if scenario == "resized-replacement-drift" {
						imageContainer(pod)["resources"] = resourceMap(*postgres.Resources)
					}
					if strings.HasSuffix(scenario, "lost-patch-response") {
						http.Error(w, "response lost after commit", 500)
						return
					}
					response = cluster
				case r.Method != http.MethodGet:
					t.Error("forbidden mutation", r.Method, r.URL.Path)
				case r.URL.Path == cloudNativePGClusterAPIPath(ns, name):
					response = cluster
				case coldTarget && r.URL.Path == coldStatePath(ns, service.ID):
					response = map[string]any{"metadata": map[string]any{"resourceVersion": "1"}, "data": map[string]string{"state.json": string(coldStateJSON)}}
				case coldTarget && r.URL.Path == cloudNativePGClusterAPIPath(ns, "previous"):
					http.NotFound(w, r)
					return
				case coldTarget && r.URL.Path == "/api/v1/namespaces/"+ns+"/services/"+endpoint:
					response = map[string]any{"metadata": map[string]any{"name": endpoint, "namespace": ns, "uid": "stable-uid"}, "spec": map[string]any{"selector": map[string]string{cloudNativePGClusterLabel: name, cloudNativePGInstanceRoleLabel: "primary"}}}
				case coldTarget && strings.HasSuffix(r.URL.Path, "/endpointslices"):
					response = map[string]any{"items": []any{map[string]any{
						"metadata":  map[string]any{"namespace": ns, "labels": map[string]string{"kubernetes.io/service-name": endpoint}, "ownerReferences": []any{map[string]any{"apiVersion": "v1", "kind": "Service", "name": endpoint, "uid": "stable-uid"}}},
						"endpoints": []any{map[string]any{"addresses": []string{"10.42.0.7"}, "conditions": map[string]bool{"ready": true}, "targetRef": map[string]any{"kind": "Pod", "name": "database-1", "namespace": ns, "uid": imageObject(pod["metadata"])["uid"]}}},
					}}}
				case fast && r.URL.Path == "/api/v1/namespaces/"+ns+"/services/"+endpoint:
					http.NotFound(w, r)
					return
				case strings.HasSuffix(r.URL.Path, "/pods/database-1"):
					response = pod
				case strings.HasSuffix(r.URL.Path, "/pods"):
					response = map[string]any{"items": []any{pod}}
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/database-1"):
					response = map[string]any{"metadata": map[string]any{"uid": "pvc-uid"}, "spec": map[string]any{"volumeName": "pv-data"}, "status": map[string]any{"phase": "Bound"}}
				case r.URL.Path == "/api/v1/persistentvolumes/pv-data":
					uid := "pv-uid"
					if patches > 0 && strings.HasSuffix(scenario, "changed-volume") {
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
			}, postgresImageDrainProbe: func(_ context.Context, _ *kubeClient, _ postgresImageWitness) pgimage.DrainObservation {
				drainSamples++
				at := time.Now()
				marker, _ := params[pgimage.DrainMarker].(string)
				if scenario == "fast-blocked-before-stage" || (scenario == "fast-blocked-after-stage" && marker != "") {
					return pgimage.DrainObservation{Status: "blocked", SampledAt: &at, Reason: "long transaction", LoadedMarker: marker, MarkerFromFile: marker != ""}
				}
				return pgimage.DrainObservation{Status: "eligible", SampledAt: &at, LoadedMarker: marker, MarkerFromFile: marker != ""}
			}}
			err = s.executePostgresImageMigration(context.Background(), op, before)
			if fast {
				if strings.HasSuffix(scenario, "response-lost") || scenario == "fast-image-lost-patch-response" {
					if !errors.Is(err, errPostgresImageMigrationUnsettled) {
						t.Fatalf("uncertain fast drain did not retain lock: %v", err)
					}
					err = s.executePostgresImageMigration(context.Background(), op, before)
				}
				if strings.HasPrefix(scenario, "fast-blocked") {
					if err == nil || errors.Is(err, errPostgresImageMigrationUnsettled) || patches != 0 || params[pgimage.DrainMarker] != nil || spec["smartShutdownTimeout"] != float64(180) {
						t.Fatalf("unsafe transaction did not abort cleanly: image patches=%d drain writes=%d err=%v", patches, drainWrites, err)
					}
					return
				}
				if err != nil || drainWrites != 2 || params[pgimage.DrainMarker] != nil || spec["smartShutdownTimeout"] != float64(180) || patches != 1 {
					t.Fatalf("fast drain did not restore the original timeout after exactly one image replacement: writes=%d patches=%d err=%v", drainWrites, patches, err)
				}
			}
			if scenario == "wrong-source-digest" || scenario == "fenced" || scenario == "resized-unapplied" || scenario == "resized-extra-resource-drift" || scenario == "unpersisted-resource-drift" || scenario == "custom-pod-patch" {
				if err == nil || patches != 0 || probes != 0 {
					t.Fatalf("unsafe preflight passed: patches=%d probes=%d err=%v", patches, probes, err)
				}
				if scenario == "custom-pod-patch" && (!strings.Contains(err.Error(), "cnpg.io/podPatch") || errors.Is(err, errPostgresImageMigrationUnsettled)) {
					t.Fatalf("custom patch must fail before intent: %v", err)
				}
				return
			}
			if !fast && (strings.HasSuffix(scenario, "changed-volume") || strings.HasSuffix(scenario, "lost-patch-response") || scenario == "resized-lost-before-patch" || scenario == "resized-cas-conflict" || scenario == "resized-replacement-drift" || scenario == "cold-target-marker-removed") {
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
				if strings.HasSuffix(scenario, "changed-volume") || scenario == "resized-cas-conflict" || scenario == "resized-replacement-drift" || scenario == "cold-target-marker-removed" {
					return
				}
				// Reopen durable state to simulate recovery by a fresh controller.
				s.Store = store.New(statePath)
				err = s.executePostgresImageMigration(context.Background(), op, before)
			}
			if err != nil {
				t.Fatal(err)
			}
			final, _ := state.GetOperation(op.ID)
			current, _ := state.GetBackingService(service.ID)
			after, _ := state.GetApp(app.ID)
			expectedCalls := 1
			if scenario == "resized-lost-before-patch" {
				expectedCalls = 2
			}
			if final.Status != model.OperationStatusCompleted || current.Spec.Postgres.Image != catalog.DefaultImage || patches != expectedCalls || probes != 2 {
				t.Fatalf("unexpected terminal state: %s patches=%d probes=%d", final.Status, patches, probes)
			}
			if resized {
				if !reflect.DeepEqual(current.Spec.Postgres.Resources, postgres.RuntimeResources) || !reflect.DeepEqual(current.Spec.Postgres.RuntimeResources, postgres.RuntimeResources) {
					t.Fatal("migration lost the authorized resource target")
				}
				baselineSpec["resources"] = resources
			}
			baselineSpec["imageName"] = catalog.DefaultImage
			if pgimage.Hash(baselineSpec) != pgimage.Hash(spec) {
				t.Fatal("migration changed another Cluster field")
			}
			if bootstrapPatch && !reflect.DeepEqual(imageObject(cluster["metadata"])["annotations"], map[string]any{retainedAnnotation: "preserve"}) {
				t.Fatal("migration did not remove only the obsolete bootstrap patch")
			}
			if current.OwnerAppID != service.OwnerAppID {
				t.Fatal("migration changed independent ownership")
			}
			if coldTarget && (current.Spec.Postgres.EndpointServiceName != endpoint || imageObject(imageObject(cluster["metadata"])["annotations"])[coldMigrationAnnotation] != marker) {
				t.Fatal("image migration changed the stable endpoint or the cold-recovery marker")
			}
			if !reflect.DeepEqual(before.Spec, after.Spec) || !reflect.DeepEqual(before.Status, after.Status) || !reflect.DeepEqual(before.Bindings, after.Bindings) || !before.UpdatedAt.Equal(after.UpdatedAt) {
				t.Fatal("migration mutated the consumer application")
			}
		})
	}
}

func TestImageRestartWitnessRejectsPartialOrUnrelatedChanges(t *testing.T) {
	for _, scenario := range []string{"source", "target", "image-without-resources", "resources-without-image", "storage-drift", "target-envelope-drift", "missing-target", "changed-authorization", "extra-resource-drift"} {
		t.Run(scenario, func(t *testing.T) {
			target := model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 1024, MemoryLimitMebibytes: 1536}
			resources := map[string]any{"requests": map[string]any{"cpu": "100m", "memory": "1Gi", "ephemeral-storage": "1Gi"}, "limits": map[string]any{"memory": "1536Mi"}}
			old := map[string]any{"requests": map[string]any{"cpu": "100m", "memory": "512Mi", "ephemeral-storage": "1Gi"}, "limits": map[string]any{"memory": "768Mi"}}
			cluster := map[string]any{"metadata": map[string]any{"uid": "cluster"}, "spec": map[string]any{"imageName": "source", "resources": old, "storage": map[string]any{"size": "1Gi"}}}
			w := postgresImageWitness{ClusterUID: "cluster", SpecHash: imagePreservedSpecHash(cluster), SourceImage: "source", TargetImage: "target", SystemID: "123", Pod: "database-1", PodUID: "old", Node: "node-a", ResourcesHash: pgimage.Hash(resources), RestartResources: resources, RestartSpecHash: imageRestartSpecHash(cluster, resources), RestartResourceTarget: &target}
			pod := map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "postgres", "resources": resources}}}}
			actual, err := managedPostgresKubeResources(target)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := managedPostgresResizeSnapshot{Observation: managedPostgresResizeObservation{PodName: w.Pod, NodeName: w.Node, DesiredResources: actual, ActualResources: &actual}}
			snapshot.Cluster.Status.SystemID = w.SystemID
			spec := imageObject(cluster["spec"])
			if scenario != "source" && scenario != "resources-without-image" {
				spec["imageName"] = "target"
			}
			if scenario != "source" && scenario != "image-without-resources" {
				spec["resources"] = resources
			}
			switch scenario {
			case "storage-drift":
				imageObject(spec["storage"])["size"] = "2Gi"
			case "target-envelope-drift":
				spec["resources"] = old
			case "missing-target":
				w.RestartResourceTarget = nil
			case "changed-authorization":
				w.RestartResourceTarget.MemoryMebibytes = 640
			case "extra-resource-drift":
				spec["resources"] = map[string]any{"requests": map[string]any{"cpu": "100m", "memory": "1Gi"}, "limits": map[string]any{"memory": "1536Mi"}}
			}
			err = validateImageWitness(w, cluster, pod, snapshot)
			if (err == nil) != (scenario == "source" || scenario == "target") {
				t.Fatalf("unexpected witness validation: %v", err)
			}
		})
	}
}
