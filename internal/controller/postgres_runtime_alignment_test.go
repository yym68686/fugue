package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestBestMatchingManagedSharedRuntimeIDPrefersMostSpecificSelector(t *testing.T) {
	t.Parallel()

	nodeLabels := map[string]string{
		runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
		runtimepkg.LocationCountryCodeLabelKey: "us",
	}
	runtimes := []model.Runtime{
		{
			ID:     "runtime_managed_shared",
			Type:   model.RuntimeTypeManagedShared,
			Status: model.RuntimeStatusActive,
		},
		{
			ID:     "runtime_managed_shared_loc_country-us-demo",
			Type:   model.RuntimeTypeManagedShared,
			Status: model.RuntimeStatusActive,
			Labels: map[string]string{
				runtimepkg.LocationCountryCodeLabelKey: "us",
			},
		},
	}

	if got := bestMatchingManagedSharedRuntimeID(nodeLabels, runtimes); got != "runtime_managed_shared_loc_country-us-demo" {
		t.Fatalf("expected location runtime, got %q", got)
	}
}

func TestManagedPostgresPlacementMutationRejectsPVCOnlyEvidence(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Runtime Alignment Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	ownedRuntime, _, err := stateStore.CreateRuntime(tenant.ID, "owned-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create owned runtime: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	}}); err != nil {
		t.Fatalf("sync shared location runtimes: %v", err)
	}

	runtimes, err := stateStore.ListRuntimes("", true)
	if err != nil {
		t.Fatalf("list runtimes: %v", err)
	}

	sharedRuntimeID := ""
	for _, runtimeObj := range runtimes {
		if runtimeObj.Type != model.RuntimeTypeManagedShared {
			continue
		}
		if runtimeObj.Labels[runtimepkg.LocationCountryCodeLabelKey] == "us" {
			sharedRuntimeID = runtimeObj.ID
			break
		}
	}
	if sharedRuntimeID == "" {
		t.Fatal("expected US managed shared runtime")
	}

	app := model.App{
		ID:       "app_demo",
		TenantID: tenant.ID,
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			RuntimeID: ownedRuntime.ID,
			Replicas:  1,
		},
		Bindings: []model.ServiceBinding{
			{
				ID:        "binding_pg",
				TenantID:  tenant.ID,
				AppID:     "app_demo",
				ServiceID: "svc_pg",
				Alias:     "postgres",
			},
		},
		BackingServices: []model.BackingService{
			{
				ID:          "svc_pg",
				TenantID:    tenant.ID,
				ProjectID:   "project_demo",
				OwnerAppID:  "app_demo",
				Name:        "demo-postgres",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Database:    "demo",
						User:        "demo",
						Password:    "secret",
						ServiceName: "demo-postgres",
						RuntimeID:   ownedRuntime.ID,
						Instances:   1,
					},
				},
			},
		},
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	clusterName := "demo-postgres"
	primaryPodName := "demo-postgres-2"
	sharedNodeName := "instance-us-1"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, clusterName):
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": clusterName,
				},
				"spec": map[string]any{
					"instances": 1,
				},
				"status": map[string]any{
					"currentPrimary": primaryPodName,
				},
			}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods/" + primaryPodName:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name":              primaryPodName,
					"creationTimestamp": "2026-04-07T00:00:00Z",
				},
				"spec": map[string]any{
					"volumes": []map[string]any{
						{
							"name": "pgdata",
							"persistentVolumeClaim": map[string]any{
								"claimName": primaryPodName,
							},
						},
					},
				},
				"status": map[string]any{
					"phase": "Pending",
				},
			}); err != nil {
				t.Fatalf("encode pod: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims/" + primaryPodName:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": primaryPodName,
					"annotations": map[string]any{
						pvcSelectedNodeAnnotation: sharedNodeName,
					},
				},
				"spec": map[string]any{
					"volumeName": "pvc-demo",
				},
			}); err != nil {
				t.Fatalf("encode pvc: %v", err)
			}
		case "/api/v1/nodes/" + sharedNodeName:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sharedNodeName,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   sharedNodeName,
					},
				},
			}); err != nil {
				t.Fatalf("encode node: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}

	_, changed, err := svc.managedPostgresPlacementMutationForObservedPrimary(context.Background(), app)
	if err != nil {
		t.Fatalf("plan managed postgres placement: %v", err)
	}
	if changed {
		t.Fatal("PVC selected-node evidence must not authorize a placement mutation without a serving Pod/IP witness")
	}
}

func TestObservedManagedPostgresDesiredAppConsumesOfflineFailoverTarget(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Observed Failover Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{
		{runtimepkg.LocationCountryCodeLabelKey: "jp"},
		{runtimepkg.LocationCountryCodeLabelKey: "us"},
	}); err != nil {
		t.Fatalf("sync managed shared runtimes: %v", err)
	}
	runtimes, err := stateStore.ListRuntimes("", true)
	if err != nil {
		t.Fatalf("list runtimes: %v", err)
	}
	sourceRuntimeID := ""
	targetRuntimeID := ""
	for _, runtimeObj := range runtimes {
		if runtimeObj.Type != model.RuntimeTypeManagedShared {
			continue
		}
		switch runtimeObj.Labels[runtimepkg.LocationCountryCodeLabelKey] {
		case "jp":
			sourceRuntimeID = runtimeObj.ID
		case "us":
			targetRuntimeID = runtimeObj.ID
		}
	}
	if sourceRuntimeID == "" || targetRuntimeID == "" {
		t.Fatalf("expected managed shared source/target runtimes, got source=%q target=%q", sourceRuntimeID, targetRuntimeID)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: sourceRuntimeID,
		Resources: &model.ResourceSpec{
			CPUMilliCores:   100,
			MemoryMebibytes: 128,
		},
		Postgres: &model.AppPostgresSpec{
			ServiceName:                      "demo-postgres",
			RuntimeID:                        sourceRuntimeID,
			FailoverTargetRuntimeID:          targetRuntimeID,
			Instances:                        2,
			SynchronousReplicas:              1,
			PrimaryPlacementPendingRebalance: true,
			Resources: &model.ResourceSpec{
				CPUMilliCores:   100,
				MemoryMebibytes: 128,
			},
		},
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		DockerfilePath: "Dockerfile",
		CommitSHA:      "head",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{
		{runtimepkg.LocationCountryCodeLabelKey: "us"},
	}); err != nil {
		t.Fatalf("resync managed shared runtimes: %v", err)
	}
	sourceRuntime, err := stateStore.GetRuntime(sourceRuntimeID)
	if err != nil {
		t.Fatalf("get source runtime: %v", err)
	}
	if sourceRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected source runtime offline, got %q", sourceRuntime.Status)
	}
	targetRuntime, err := stateStore.GetRuntime(targetRuntimeID)
	if err != nil {
		t.Fatalf("get target runtime: %v", err)
	}
	if targetRuntime.Status != model.RuntimeStatusActive {
		t.Fatalf("expected target runtime active, got %q", targetRuntime.Status)
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	clusterName := "demo-postgres"
	primaryPodName := "demo-postgres-4"
	targetNodeName := "node-target"
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, clusterName):
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": clusterName,
				},
				"spec": map[string]any{
					"instances": 2,
				},
				"status": map[string]any{
					"readyInstances": 2,
					"currentPrimary": primaryPodName,
					"targetPrimary":  primaryPodName,
				},
			}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods/" + primaryPodName:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": primaryPodName,
				},
				"spec": map[string]any{
					"nodeName": targetNodeName,
				},
				"status": map[string]any{
					"phase":      "Running",
					"podIP":      "10.42.0.44",
					"conditions": []map[string]any{{"type": "Ready", "status": "True"}},
				},
			}); err != nil {
				t.Fatalf("encode pod: %v", err)
			}
		case "/api/v1/nodes/" + targetNodeName:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": targetNodeName,
					"labels": map[string]any{
						runtimepkg.RuntimeIDLabelKey: targetRuntime.ID,
						kubeHostnameLabelKey:         targetNodeName,
					},
				},
			}); err != nil {
				t.Fatalf("encode node: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}

	updatedApp, changed, err := svc.observedManagedPostgresDesiredApp(context.Background(), app)
	if err != nil {
		t.Fatalf("sync observed managed postgres desired app: %v", err)
	}
	if !changed {
		t.Fatal("expected observed managed postgres desired app to change state")
	}

	postgres := store.OwnedManagedPostgresSpec(updatedApp)
	if postgres == nil {
		t.Fatal("expected updated app to retain managed postgres")
	}
	if got := postgres.RuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected postgres runtime %q, got %q", targetRuntime.ID, got)
	}
	if got := postgres.FailoverTargetRuntimeID; got != "" {
		t.Fatalf("expected failover target to be cleared, got %q", got)
	}
	if got := postgres.Instances; got != 1 {
		t.Fatalf("expected instances=1, got %d", got)
	}
	if got := postgres.SynchronousReplicas; got != 0 {
		t.Fatalf("expected synchronous replicas=0, got %d", got)
	}
	if postgres.PrimaryPlacementPendingRebalance {
		t.Fatal("expected placement pending rebalance to be cleared")
	}
	if got := postgres.PrimaryNodeName; got != targetNodeName {
		t.Fatalf("expected primary node %q, got %q", targetNodeName, got)
	}

	storedApp, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get synced app: %v", err)
	}
	storedPostgres := store.OwnedManagedPostgresSpec(storedApp)
	if storedPostgres == nil {
		t.Fatal("expected stored app to retain managed postgres")
	}
	if got := storedPostgres.RuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected stored postgres runtime %q, got %q", targetRuntime.ID, got)
	}
	if got := storedPostgres.FailoverTargetRuntimeID; got != "" {
		t.Fatalf("expected stored failover target cleared, got %q", got)
	}
}

func TestObservedManagedPostgresDesiredAppCorrectsBoundSameRuntimeNode(t *testing.T) {
	t.Parallel()

	stateStore, app, service, runtimeObj := newBoundManagedPostgresPlacementControllerFixture(t)
	svc := managedPostgresPlacementControllerService(t, stateStore, app, service, runtimeObj.ID, "node-current")

	updated, changed, err := svc.observedManagedPostgresDesiredApp(context.Background(), app)
	if err != nil {
		t.Fatalf("correct bound same-runtime node: %v", err)
	}
	if !changed {
		t.Fatal("expected same-runtime primary node correction")
	}
	if updated.Spec.Postgres != nil {
		t.Fatalf("bound correction leaked postgres into app spec: %+v", updated.Spec.Postgres)
	}
	postgres := store.OwnedManagedPostgresSpec(updated)
	if postgres == nil || postgres.RuntimeID != runtimeObj.ID || postgres.PrimaryNodeName != "node-current" {
		t.Fatalf("bound correction did not persist exact runtime/node: %+v", postgres)
	}
	storedService, err := stateStore.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get corrected bound service: %v", err)
	}
	if got := storedService.Spec.Postgres.PrimaryNodeName; got != "node-current" {
		t.Fatalf("stored bound primary node = %q, want node-current", got)
	}
}

func TestObservedManagedPostgresDesiredAppRejectsUnconsumedRuntimeMismatch(t *testing.T) {
	t.Parallel()

	stateStore, app, service, sourceRuntime := newBoundManagedPostgresPlacementControllerFixture(t)
	observedRuntime, _, err := stateStore.CreateRuntime(app.TenantID, "observed-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create observed runtime: %v", err)
	}
	svc := managedPostgresPlacementControllerService(t, stateStore, app, service, observedRuntime.ID, "node-observed")

	updated, changed, err := svc.observedManagedPostgresDesiredApp(context.Background(), app)
	if err != nil {
		t.Fatalf("reject unconsumed runtime mismatch: %v", err)
	}
	if changed {
		t.Fatalf("runtime mismatch without legal failover consumption changed app: %+v", updated)
	}
	storedService, err := stateStore.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get service after rejected runtime mismatch: %v", err)
	}
	if got := storedService.Spec.Postgres.RuntimeID; got != sourceRuntime.ID {
		t.Fatalf("rejected runtime mismatch changed desired runtime to %q", got)
	}
	if got := storedService.Spec.Postgres.PrimaryNodeName; got != "node-stored" {
		t.Fatalf("rejected runtime mismatch copied cross-runtime node %q", got)
	}
}

func TestAppWithBackingServicePostgresKeepsSwitchoverStageEphemeral(t *testing.T) {
	t.Parallel()

	stateStore, app, service, _ := newBoundManagedPostgresPlacementControllerFixture(t)
	stage := *model.CloneAppPostgresSpec(service.Spec.Postgres)
	stage.FailoverTargetRuntimeID = "runtime-target"
	stage.Instances = 2
	stage.SynchronousReplicas = 1
	rendered, err := appWithBackingServicePostgres(service.ID, app, stage)
	if err != nil {
		t.Fatalf("build ephemeral backing-service stage: %v", err)
	}
	renderedPostgres := store.OwnedManagedPostgresSpec(rendered)
	if renderedPostgres == nil || renderedPostgres.FailoverTargetRuntimeID != stage.FailoverTargetRuntimeID {
		t.Fatalf("rendered stage did not contain target state: %+v", renderedPostgres)
	}
	stored, err := stateStore.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get persisted service after ephemeral stage: %v", err)
	}
	if stored.Spec.Postgres.FailoverTargetRuntimeID != "" || stored.Spec.Postgres.Instances != service.Spec.Postgres.Instances {
		t.Fatalf("ephemeral stage changed persisted backing service: %+v", stored.Spec.Postgres)
	}
}

func newBoundManagedPostgresPlacementControllerFixture(
	t *testing.T,
) (*store.Store, model.App, model.BackingService, model.Runtime) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init bound placement store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Bound Placement")
	if err != nil {
		t.Fatalf("create bound placement tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "bound-placement", "")
	if err != nil {
		t.Fatalf("create bound placement project: %v", err)
	}
	runtimeObj, _, err := stateStore.CreateRuntime(tenant.ID, "source-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create bound placement runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "bound-placement", "", model.AppSpec{
		Image: "ghcr.io/example/bound:1", Replicas: 1, RuntimeID: runtimeObj.ID,
	})
	if err != nil {
		t.Fatalf("create bound placement app: %v", err)
	}
	service, err := stateStore.CreateBackingService(tenant.ID, project.ID, "bound-postgres", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database: "bound", User: "bound", Password: "secret", ServiceName: "bound-postgres",
			RuntimeID: runtimeObj.ID, PrimaryNodeName: "node-stored", StorageSize: "1Gi", Instances: 1,
		},
	})
	if err != nil {
		t.Fatalf("create bound placement service: %v", err)
	}
	if _, err := stateStore.BindBackingService(tenant.ID, app.ID, service.ID, "postgres", nil); err != nil {
		t.Fatalf("bind placement service: %v", err)
	}
	app, err = stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload bound placement app: %v", err)
	}
	return stateStore, app, service, runtimeObj
}

func managedPostgresPlacementControllerService(
	t *testing.T,
	stateStore *store.Store,
	app model.App,
	service model.BackingService,
	observedRuntimeID, observedNodeName string,
) *Service {
	t.Helper()
	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	clusterName := model.NormalizePostgresServiceName(service.Spec.Postgres.ServiceName, "")
	primaryPod := clusterName + "-1"
	primaryIP := "10.42.0.51"
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, clusterName):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": clusterName},
				"spec":     map[string]any{"instances": 1},
				"status": map[string]any{
					"readyInstances": 1, "currentPrimary": primaryPod, "targetPrimary": primaryPod,
				},
			})
		case "/api/v1/namespaces/" + namespace + "/pods/" + primaryPod:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": primaryPod},
				"spec":     map[string]any{"nodeName": observedNodeName},
				"status": map[string]any{
					"phase": "Running", "podIP": primaryIP,
					"conditions": []map[string]any{{"type": "Ready", "status": "True"}},
				},
			})
		case "/api/v1/nodes/" + observedNodeName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name":   observedNodeName,
					"labels": map[string]any{runtimepkg.RuntimeIDLabelKey: observedRuntimeID},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(kubeServer.Close)
	return &Service{
		Store: stateStore, Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client: kubeServer.Client(), baseURL: kubeServer.URL, bearerToken: "test", namespace: namespace,
			}, nil
		},
	}
}

func TestExecuteManagedOperationDeployDoesNotConsumeLivePostgresPlacement(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Deploy Alignment Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	ownedRuntime, _, err := stateStore.CreateRuntime(tenant.ID, "owned-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create owned runtime: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	}}); err != nil {
		t.Fatalf("sync shared location runtimes: %v", err)
	}

	runtimes, err := stateStore.ListRuntimes("", true)
	if err != nil {
		t.Fatalf("list runtimes: %v", err)
	}
	sharedRuntimeID := ""
	for _, runtimeObj := range runtimes {
		if runtimeObj.Type != model.RuntimeTypeManagedShared {
			continue
		}
		if runtimeObj.Labels[runtimepkg.LocationCountryCodeLabelKey] == "us" {
			sharedRuntimeID = runtimeObj.ID
			break
		}
	}
	if sharedRuntimeID == "" {
		t.Fatal("expected US managed shared runtime")
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: ownedRuntime.ID,
		Postgres: &model.AppPostgresSpec{
			ServiceName: "demo-postgres",
			RuntimeID:   ownedRuntime.ID,
			Instances:   1,
		},
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		DockerfilePath: "Dockerfile",
		ComposeService: "web",
		CommitSHA:      "oldcommit",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Image = "ghcr.io/example/demo:new"
	desiredSource := *app.Source
	desiredSource.ComposeService = "api"
	desiredSource.CommitSHA = "newcommit"

	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &desiredSpec,
		DesiredSource: &desiredSource,
	})
	if err != nil {
		t.Fatalf("create deploy op: %v", err)
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	clusterName := "demo-postgres"
	primaryPodName := "demo-postgres-2"
	sharedNodeName := "instance-us-1"
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, clusterName):
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": clusterName,
				},
				"spec": map[string]any{
					"instances": 1,
				},
				"status": map[string]any{
					"currentPrimary": primaryPodName,
				},
			}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods/" + primaryPodName:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name":              primaryPodName,
					"creationTimestamp": "2026-04-07T00:00:00Z",
				},
				"spec": map[string]any{
					"nodeName": sharedNodeName,
				},
				"status": map[string]any{
					"phase": "Running",
				},
			}); err != nil {
				t.Fatalf("encode pod: %v", err)
			}
		case "/api/v1/nodes/" + sharedNodeName:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sharedNodeName,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   sharedNodeName,
					},
				},
			}); err != nil {
				t.Fatalf("encode node: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	svc := &Service{
		Store:    stateStore,
		Renderer: runtimepkg.Renderer{BaseDir: t.TempDir()},
		Logger:   log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}

	if err := svc.executeManagedOperation(context.Background(), op); err != nil {
		t.Fatalf("execute managed deploy: %v", err)
	}

	completedOp, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get completed op: %v", err)
	}
	if completedOp.Status != model.OperationStatusCompleted {
		t.Fatalf("expected completed deploy op, got %q", completedOp.Status)
	}

	storedApp, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get deployed app: %v", err)
	}
	if storedApp.Source == nil {
		t.Fatal("expected deployed app source to be preserved")
	}
	if got := storedApp.Source.CommitSHA; got != "newcommit" {
		t.Fatalf("expected app source commit newcommit, got %q", got)
	}
	if got := storedApp.Source.ComposeService; got != "api" {
		t.Fatalf("expected app compose service api, got %q", got)
	}
	ownedPostgres := store.OwnedManagedPostgresSpec(storedApp)
	if ownedPostgres == nil {
		t.Fatal("expected deployed app postgres backing service to be preserved")
	}
	if got := ownedPostgres.RuntimeID; got != ownedRuntime.ID {
		t.Fatalf("active deploy consumed live postgres runtime %q; want stored runtime %q", got, ownedRuntime.ID)
	}

	manifestBytes, err := os.ReadFile(completedOp.ManifestPath)
	if err != nil {
		t.Fatalf("read rendered manifest: %v", err)
	}
	manifest := string(manifestBytes)
	wantAlias := runtimepkg.ComposeServiceAliasName(app.ProjectID, "api")
	if !strings.Contains(manifest, "name: "+wantAlias) {
		t.Fatalf("expected rendered manifest to include desired compose alias %q, got:\n%s", wantAlias, manifest)
	}
	oldAlias := runtimepkg.ComposeServiceAliasName(app.ProjectID, "web")
	if oldAlias != "" && strings.Contains(manifest, "name: "+oldAlias) {
		t.Fatalf("expected rendered manifest to stop using old compose alias %q", oldAlias)
	}
}
