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

func TestAlignManagedPostgresRuntimeToObservedPrimaryUsesPVCSelectedNode(t *testing.T) {
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

	alignedSpec, changed, err := svc.alignManagedPostgresRuntimeToObservedPrimary(context.Background(), app)
	if err != nil {
		t.Fatalf("align managed postgres runtime: %v", err)
	}
	if !changed {
		t.Fatal("expected managed postgres runtime alignment to change desired spec")
	}
	if alignedSpec.Postgres == nil {
		t.Fatal("expected aligned spec to include managed postgres")
	}
	if got := alignedSpec.Postgres.RuntimeID; got != sharedRuntimeID {
		t.Fatalf("expected aligned postgres runtime %q, got %q", sharedRuntimeID, got)
	}
	if got := alignedSpec.RuntimeID; got != ownedRuntime.ID {
		t.Fatalf("expected app runtime to stay %q, got %q", ownedRuntime.ID, got)
	}
	if got := alignedSpec.Postgres.FailoverTargetRuntimeID; got != "" {
		t.Fatalf("expected failover target to stay empty, got %q", got)
	}
}

func TestExecuteManagedOperationDeployUsesDesiredSourceAndAlignedPostgresRuntime(t *testing.T) {
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
	if got := ownedPostgres.RuntimeID; got != sharedRuntimeID {
		t.Fatalf("expected aligned postgres runtime %q, got %q", sharedRuntimeID, got)
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
