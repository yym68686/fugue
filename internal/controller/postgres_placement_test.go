package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestManagedPostgresPlacementRequestUsesRuntimeTarget(t *testing.T) {
	t.Parallel()

	request := managedPostgresPlacementRequest(model.AppPostgresSpec{
		Resources:        &model.ResourceSpec{CPUMilliCores: 500, MemoryMebibytes: 1024},
		RuntimeResources: &model.ResourceSpec{CPUMilliCores: 150, MemoryMebibytes: 640},
	})
	if request.cpuMilli != 150 || request.memoryBytes != 640*1024*1024 {
		t.Fatalf("expected runtime target placement request, got %+v", request)
	}
}

func TestKubePodRequestsMatchesRestartableInitSchedulingSemantics(t *testing.T) {
	t.Parallel()

	always := "Always"
	pod := kubePod{}
	pod.Spec.Containers = []kubeContainerSpec{
		{Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "100m", "memory": "100Mi", "ephemeral-storage": "20Mi",
		}}},
		{Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "50m", "memory": "50Mi", "ephemeral-storage": "10Mi",
		}}},
	}
	pod.Spec.InitContainers = []kubeContainerSpec{
		{RestartPolicy: &always, Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "25m", "memory": "32Mi", "ephemeral-storage": "5Mi",
		}}},
		{Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "300m", "memory": "768Mi", "ephemeral-storage": "40Mi",
		}}},
		{RestartPolicy: &always, Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "10m", "memory": "16Mi", "ephemeral-storage": "2Mi",
		}}},
		{Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "50m", "memory": "64Mi", "ephemeral-storage": "100Mi",
		}}},
	}

	request := kubePodRequests(pod)
	if request.cpuMilli != 325 {
		t.Fatalf("expected init-stage CPU peak of 325m, got %dm", request.cpuMilli)
	}
	if request.memoryBytes != 800*1024*1024 {
		t.Fatalf("expected init-stage memory peak of 800Mi, got %d", request.memoryBytes)
	}
	if request.ephemeralBytes != 107*1024*1024 {
		t.Fatalf("expected later init-stage ephemeral peak of 107Mi, got %d", request.ephemeralBytes)
	}

	deployment := kubeDeployment{}
	deployment.Spec.Template.Spec.Containers = pod.Spec.Containers
	deployment.Spec.Template.Spec.InitContainers = pod.Spec.InitContainers
	if got := deploymentTemplateRequests(deployment); got != request {
		t.Fatalf("deployment and live Pod accounting diverged: deployment=%+v pod=%+v", got, request)
	}
}

func TestManagedReadyAppNodesByNodeKeepsOnlyServingPods(t *testing.T) {
	t.Parallel()

	app := model.App{ID: "app_demo", Name: "demo", Spec: model.AppSpec{Replicas: 1}}
	ready := kubePod{}
	ready.Metadata.Name = "app-demo-abc"
	ready.Spec.NodeName = "node-a"
	ready.Status.Phase = "Running"
	ready.Status.Conditions = []kubePodCondition{{Type: "Ready", Status: "True"}}
	pending := ready
	pending.Metadata.Name = "app-demo-pending"
	pending.Spec.NodeName = "node-b"
	pending.Status.Phase = "Pending"
	deleted := ready
	deleted.Metadata.Name = "app-demo-deleted"
	deleted.Metadata.DeletionTimestamp = "2026-09-03T00:00:00Z"
	other := ready
	other.Metadata.Name = "other-app-abc"

	got := managedReadyAppNodesByNode(app, []kubePod{ready, pending, deleted, other})
	if len(got) != 1 {
		t.Fatalf("expected one serving app node, got %#v", got)
	}
	if _, ok := got["node-a"]; !ok {
		t.Fatalf("expected node-a serving app pin, got %#v", got)
	}
}

func TestManagedPostgresPlacementsCanonicalizesMixedCaseServiceName(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Placement Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	}}); err != nil {
		t.Fatalf("sync shared runtimes: %v", err)
	}

	sourceRuntimeID := model.DefaultManagedRuntimeID
	targetRuntime, _, err := stateStore.CreateRuntime(tenant.ID, "standby-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create standby runtime: %v", err)
	}

	app := model.App{
		ID:       "app_demo",
		TenantID: tenant.ID,
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			RuntimeID: sourceRuntimeID,
			Postgres: &model.AppPostgresSpec{
				Database:                "demo",
				User:                    "demo",
				Password:                "secret",
				ServiceName:             "MecGod",
				RuntimeID:               sourceRuntimeID,
				FailoverTargetRuntimeID: targetRuntime.ID,
				Instances:               2,
				SynchronousReplicas:     1,
			},
		},
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	primaryPodName := "mecgod-1"
	sourceNodeName := "shared-us-1"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, "mecgod"):
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": "mecgod",
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
					"name": primaryPodName,
				},
				"spec": map[string]any{
					"nodeName": sourceNodeName,
				},
			}); err != nil {
				t.Fatalf("encode pod: %v", err)
			}
		case "/api/v1/nodes/" + sourceNodeName:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sourceNodeName,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   sourceNodeName,
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

	placements, err := svc.managedPostgresPlacements(context.Background(), app)
	if err != nil {
		t.Fatalf("resolve postgres placements: %v", err)
	}

	servicePlacements := placements["mecgod"]
	if len(servicePlacements) != 2 {
		t.Fatalf("expected two placements, got %d", len(servicePlacements))
	}
	if got := servicePlacements[0].NodeSelector[kubeHostnameLabelKey]; got != sourceNodeName {
		t.Fatalf("expected shared primary hostname %q, got %q", sourceNodeName, got)
	}
	if len(servicePlacements[0].NodeSelector) != 1 {
		t.Fatalf("expected exact primary hostname selector, got %#v", servicePlacements[0].NodeSelector)
	}
	if got := servicePlacements[1].NodeSelector[runtimepkg.RuntimeIDLabelKey]; got != targetRuntime.ID {
		t.Fatalf("expected standby runtime selector %q, got %q", targetRuntime.ID, got)
	}
	if got := servicePlacements[1].NodeSelector[runtimepkg.TenantIDLabelKey]; got != tenant.ID {
		t.Fatalf("expected standby tenant selector %q, got %q", tenant.ID, got)
	}
	if len(servicePlacements[1].Tolerations) != 1 {
		t.Fatalf("expected standby runtime toleration, got %#v", servicePlacements[1].Tolerations)
	}
}

func TestManagedPostgresPlacementsChoosesNonOverlappingSharedSourceNode(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Placement Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{
		{
			runtimepkg.LocationCountryCodeLabelKey: "us",
		},
		{
			runtimepkg.RegionLabelKey: "us-west1",
		},
	}); err != nil {
		t.Fatalf("sync shared runtimes: %v", err)
	}

	sourceRuntimeID := managedSharedRuntimeIDForLabels(t, stateStore, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})
	targetRuntimeID := managedSharedRuntimeIDForLabels(t, stateStore, map[string]string{
		runtimepkg.RegionLabelKey: "us-west1",
	})

	app := model.App{
		ID:       "app_demo",
		TenantID: tenant.ID,
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			RuntimeID: sourceRuntimeID,
			Postgres: &model.AppPostgresSpec{
				Database:                "demo",
				User:                    "demo",
				Password:                "secret",
				ServiceName:             "demo-postgres",
				RuntimeID:               sourceRuntimeID,
				FailoverTargetRuntimeID: targetRuntimeID,
				Instances:               2,
				SynchronousReplicas:     1,
			},
		},
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	sharedEastNode := "shared-east"
	sharedWestNode := "shared-west"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, "demo-postgres"):
			http.NotFound(w, r)
		case "/api/v1/nodes":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": sharedWestNode,
						},
					},
					{
						"metadata": map[string]any{
							"name": sharedEastNode,
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode node list: %v", err)
			}
		case "/api/v1/nodes/" + sharedEastNode:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sharedEastNode,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   sharedEastNode,
					},
				},
				"status": map[string]any{
					"conditions": []map[string]any{
						{"type": "Ready", "status": "True"},
						{"type": "DiskPressure", "status": "False"},
					},
					"allocatable": map[string]any{
						"cpu":               "2",
						"memory":            "4Gi",
						"ephemeral-storage": "20Gi",
					},
				},
			}); err != nil {
				t.Fatalf("encode east node: %v", err)
			}
		case "/api/v1/nodes/" + sharedWestNode:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sharedWestNode,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						runtimepkg.RegionLabelKey:              "us-west1",
						kubeHostnameLabelKey:                   sharedWestNode,
					},
				},
				"status": map[string]any{
					"conditions": []map[string]any{
						{"type": "Ready", "status": "True"},
						{"type": "DiskPressure", "status": "False"},
					},
					"allocatable": map[string]any{
						"cpu":               "4",
						"memory":            "8Gi",
						"ephemeral-storage": "30Gi",
					},
				},
			}); err != nil {
				t.Fatalf("encode west node: %v", err)
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

	placements, err := svc.managedPostgresPlacements(context.Background(), app)
	if err != nil {
		t.Fatalf("resolve postgres placements: %v", err)
	}

	servicePlacements := placements["demo-postgres"]
	if len(servicePlacements) != 2 {
		t.Fatalf("expected two placements, got %d", len(servicePlacements))
	}
	if got := servicePlacements[0].NodeSelector[kubeHostnameLabelKey]; got != sharedEastNode {
		t.Fatalf("expected non-overlapping shared source node %q, got %q", sharedEastNode, got)
	}
	if got := servicePlacements[1].NodeSelector[runtimepkg.RegionLabelKey]; got != "us-west1" {
		t.Fatalf("expected standby region selector %q, got %q", "us-west1", got)
	}
	if got := servicePlacements[1].NodeSelector[runtimepkg.SharedPoolLabelKey]; got != runtimepkg.SharedPoolLabelValue {
		t.Fatalf("expected standby shared-pool selector %q, got %q", runtimepkg.SharedPoolLabelValue, got)
	}
}

func TestManagedPostgresPlacementsPinsSharedPrimaryToLegacyPVCNode(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Placement Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{
		{runtimepkg.LocationCountryCodeLabelKey: "us"},
		{runtimepkg.LocationCountryCodeLabelKey: "hk"},
	}); err != nil {
		t.Fatalf("sync shared runtimes: %v", err)
	}

	sourceRuntimeID := managedSharedRuntimeIDForLabels(t, stateStore, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})
	targetRuntimeID := managedSharedRuntimeIDForLabels(t, stateStore, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "hk",
	})

	app := model.App{
		ID:       "app_demo",
		TenantID: tenant.ID,
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			RuntimeID: sourceRuntimeID,
			Postgres: &model.AppPostgresSpec{
				Database:                "demo",
				User:                    "demo",
				Password:                "secret",
				ServiceName:             "demo-postgres",
				RuntimeID:               sourceRuntimeID,
				FailoverTargetRuntimeID: targetRuntimeID,
				Instances:               2,
				SynchronousReplicas:     1,
			},
		},
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	currentPrimary := "demo-postgres-1"
	standbyPod := "demo-postgres-2"
	sourceNode := "legacy-control-plane-existing-pv"
	largerSourceNode := "shared-us-larger"
	targetNode := "shared-hk-primary"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, "demo-postgres"):
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": "demo-postgres",
				},
				"status": map[string]any{
					"currentPrimary": currentPrimary,
				},
			}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods/" + currentPrimary:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": currentPrimary,
				},
				"spec": map[string]any{
					"nodeName": targetNode,
				},
				"status": map[string]any{
					"phase": "Running",
				},
			}); err != nil {
				t.Fatalf("encode current primary pod: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":              currentPrimary,
							"creationTimestamp": "2026-04-07T00:00:00Z",
						},
						"spec": map[string]any{
							"nodeName": targetNode,
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
					{
						"metadata": map[string]any{
							"name":              standbyPod,
							"creationTimestamp": "2026-04-07T00:01:00Z",
						},
						"spec": map[string]any{
							"volumes": []map[string]any{
								{
									"name": "pgdata",
									"persistentVolumeClaim": map[string]any{
										"claimName": standbyPod,
									},
								},
							},
						},
						"status": map[string]any{
							"phase": "Pending",
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode pod list: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims/" + standbyPod:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": standbyPod,
				},
				"spec": map[string]any{
					"volumeName": "pv-demo-postgres-2",
				},
			}); err != nil {
				t.Fatalf("encode standby pvc: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": standbyPod,
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode pvc list: %v", err)
			}
		case "/api/v1/persistentvolumes/pv-demo-postgres-2":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": "pv-demo-postgres-2",
				},
				"spec": map[string]any{
					"nodeAffinity": map[string]any{
						"required": map[string]any{
							"nodeSelectorTerms": []map[string]any{
								{
									"matchExpressions": []map[string]any{
										{
											"key":      kubeHostnameLabelKey,
											"operator": "In",
											"values":   []string{sourceNode},
										},
									},
								},
							},
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode standby pv: %v", err)
			}
		case "/api/v1/nodes":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{"metadata": map[string]any{"name": sourceNode}},
					{"metadata": map[string]any{"name": largerSourceNode}},
				},
			}); err != nil {
				t.Fatalf("encode node list: %v", err)
			}
		case "/api/v1/nodes/" + sourceNode:
			encodeReadyPlacementNode(t, w, sourceNode, "2", "4Gi", "20Gi")
		case "/api/v1/nodes/" + largerSourceNode:
			encodeSharedPlacementNode(t, w, largerSourceNode, "us", "8", "16Gi", "80Gi")
		case "/api/v1/nodes/" + targetNode:
			encodeSharedPlacementNode(t, w, targetNode, "hk", "2", "4Gi", "20Gi")
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

	placements, err := svc.managedPostgresPlacements(context.Background(), app)
	if err != nil {
		t.Fatalf("resolve postgres placements: %v", err)
	}

	servicePlacements := placements["demo-postgres"]
	if len(servicePlacements) != 2 {
		t.Fatalf("expected two placements, got %d", len(servicePlacements))
	}
	if got := servicePlacements[0].NodeSelector[kubeHostnameLabelKey]; got != sourceNode {
		t.Fatalf("expected existing legacy pv node %q, got %q", sourceNode, got)
	}
	if got := servicePlacements[1].NodeSelector[runtimepkg.LocationCountryCodeLabelKey]; got != "hk" {
		t.Fatalf("expected standby country selector %q, got %q", "hk", got)
	}
}

func TestManagedPostgresPlacementsChoosesHealthiestSharedSourceNodeWithoutFailoverTarget(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Placement Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	}}); err != nil {
		t.Fatalf("sync shared runtimes: %v", err)
	}

	sourceRuntimeID := managedSharedRuntimeIDForLabels(t, stateStore, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})

	app := model.App{
		ID:       "app_demo",
		TenantID: tenant.ID,
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			RuntimeID: sourceRuntimeID,
			Postgres: &model.AppPostgresSpec{
				Database:    "demo",
				User:        "demo",
				Password:    "secret",
				ServiceName: "demo-postgres",
				RuntimeID:   sourceRuntimeID,
			},
		},
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	sharedHealthyNode := "shared-healthy-large"
	sharedSmallNode := "shared-healthy-small"
	sharedDiskPressureNode := "shared-disk-pressure"
	sharedTaintedNode := "shared-tainted"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, "demo-postgres"):
			http.NotFound(w, r)
		case "/api/v1/nodes":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{"metadata": map[string]any{"name": sharedTaintedNode}},
					{"metadata": map[string]any{"name": sharedDiskPressureNode}},
					{"metadata": map[string]any{"name": sharedSmallNode}},
					{"metadata": map[string]any{"name": sharedHealthyNode}},
				},
			}); err != nil {
				t.Fatalf("encode node list: %v", err)
			}
		case "/api/v1/nodes/" + sharedHealthyNode:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sharedHealthyNode,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   sharedHealthyNode,
					},
				},
				"status": map[string]any{
					"conditions": []map[string]any{
						{"type": "Ready", "status": "True"},
						{"type": "DiskPressure", "status": "False"},
					},
					"allocatable": map[string]any{
						"cpu":               "4",
						"memory":            "8Gi",
						"ephemeral-storage": "30Gi",
					},
				},
			}); err != nil {
				t.Fatalf("encode healthy node: %v", err)
			}
		case "/api/v1/nodes/" + sharedSmallNode:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sharedSmallNode,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   sharedSmallNode,
					},
				},
				"status": map[string]any{
					"conditions": []map[string]any{
						{"type": "Ready", "status": "True"},
						{"type": "DiskPressure", "status": "False"},
					},
					"allocatable": map[string]any{
						"cpu":               "2",
						"memory":            "4Gi",
						"ephemeral-storage": "10Gi",
					},
				},
			}); err != nil {
				t.Fatalf("encode small node: %v", err)
			}
		case "/api/v1/nodes/" + sharedDiskPressureNode:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sharedDiskPressureNode,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   sharedDiskPressureNode,
					},
				},
				"status": map[string]any{
					"conditions": []map[string]any{
						{"type": "Ready", "status": "True"},
						{"type": "DiskPressure", "status": "True"},
					},
					"allocatable": map[string]any{
						"cpu":               "8",
						"memory":            "16Gi",
						"ephemeral-storage": "40Gi",
					},
				},
			}); err != nil {
				t.Fatalf("encode disk-pressure node: %v", err)
			}
		case "/api/v1/nodes/" + sharedTaintedNode:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": sharedTaintedNode,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   sharedTaintedNode,
					},
				},
				"spec": map[string]any{
					"taints": []map[string]any{
						{"key": "node.kubernetes.io/disk-pressure", "effect": "NoSchedule"},
					},
				},
				"status": map[string]any{
					"conditions": []map[string]any{
						{"type": "Ready", "status": "True"},
						{"type": "DiskPressure", "status": "False"},
					},
					"allocatable": map[string]any{
						"cpu":               "10",
						"memory":            "24Gi",
						"ephemeral-storage": "50Gi",
					},
				},
			}); err != nil {
				t.Fatalf("encode tainted node: %v", err)
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

	placements, err := svc.managedPostgresPlacements(context.Background(), app)
	if err != nil {
		t.Fatalf("resolve postgres placements: %v", err)
	}

	servicePlacements := placements["demo-postgres"]
	if len(servicePlacements) != 1 {
		t.Fatalf("expected one placement, got %d", len(servicePlacements))
	}
	if got := servicePlacements[0].NodeSelector[kubeHostnameLabelKey]; got != sharedHealthyNode {
		t.Fatalf("expected healthiest shared node %q, got %q", sharedHealthyNode, got)
	}
	if len(servicePlacements[0].NodeSelector) != 1 {
		t.Fatalf("expected exact primary hostname selector, got %#v", servicePlacements[0].NodeSelector)
	}
}

func TestManagedPostgresPlacementsAvoidsSharedNodeWithoutRequestHeadroom(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Placement Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	}}); err != nil {
		t.Fatalf("sync shared runtimes: %v", err)
	}

	sourceRuntimeID := managedSharedRuntimeIDForLabels(t, stateStore, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})

	app := model.App{
		ID:       "app_demo",
		TenantID: tenant.ID,
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			RuntimeID: sourceRuntimeID,
			Postgres: &model.AppPostgresSpec{
				Database:    "demo",
				User:        "demo",
				Password:    "secret",
				ServiceName: "demo-postgres",
				RuntimeID:   sourceRuntimeID,
			},
		},
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	sharedLargeFullNode := "shared-large-full"
	sharedSmallFreeNode := "shared-small-free"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, "demo-postgres"):
			http.NotFound(w, r)
		case "/api/v1/pods":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": "existing-postgres-1",
						},
						"spec": map[string]any{
							"nodeName": sharedLargeFullNode,
							"containers": []map[string]any{
								{
									"name": "postgres",
									"resources": map[string]any{
										"requests": map[string]string{
											"cpu":    "3900m",
											"memory": "7800Mi",
										},
									},
								},
							},
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode pod list: %v", err)
			}
		case "/api/v1/nodes":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{"metadata": map[string]any{"name": sharedLargeFullNode}},
					{"metadata": map[string]any{"name": sharedSmallFreeNode}},
				},
			}); err != nil {
				t.Fatalf("encode node list: %v", err)
			}
		case "/api/v1/nodes/" + sharedLargeFullNode:
			encodeSharedPlacementNode(t, w, sharedLargeFullNode, "us", "4", "8Gi", "80Gi")
		case "/api/v1/nodes/" + sharedSmallFreeNode:
			encodeSharedPlacementNode(t, w, sharedSmallFreeNode, "us", "2", "4Gi", "20Gi")
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

	placements, err := svc.managedPostgresPlacements(context.Background(), app)
	if err != nil {
		t.Fatalf("resolve postgres placements: %v", err)
	}

	servicePlacements := placements["demo-postgres"]
	if len(servicePlacements) != 1 {
		t.Fatalf("expected one placement, got %d", len(servicePlacements))
	}
	if got := servicePlacements[0].NodeSelector[kubeHostnameLabelKey]; got != sharedSmallFreeNode {
		t.Fatalf("expected shared node with request headroom %q, got %q", sharedSmallFreeNode, got)
	}
}

func encodeSharedPlacementNode(t *testing.T, w http.ResponseWriter, name, country, cpu, memory, storage string) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(map[string]any{
		"metadata": map[string]any{
			"name": name,
			"labels": map[string]any{
				runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
				runtimepkg.LocationCountryCodeLabelKey: country,
				kubeHostnameLabelKey:                   name,
			},
		},
		"status": map[string]any{
			"conditions": []map[string]any{
				{"type": "Ready", "status": "True"},
				{"type": "DiskPressure", "status": "False"},
			},
			"allocatable": map[string]any{
				"cpu":               cpu,
				"memory":            memory,
				"ephemeral-storage": storage,
			},
		},
	}); err != nil {
		t.Fatalf("encode node %s: %v", name, err)
	}
}

func encodeReadyPlacementNode(t *testing.T, w http.ResponseWriter, name, cpu, memory, storage string) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(map[string]any{
		"metadata": map[string]any{
			"name": name,
			"labels": map[string]any{
				kubeHostnameLabelKey: name,
			},
		},
		"status": map[string]any{
			"conditions": []map[string]any{
				{"type": "Ready", "status": "True"},
				{"type": "DiskPressure", "status": "False"},
			},
			"allocatable": map[string]any{
				"cpu":               cpu,
				"memory":            memory,
				"ephemeral-storage": storage,
			},
		},
	}); err != nil {
		t.Fatalf("encode node %s: %v", name, err)
	}
}

func managedSharedRuntimeIDForLabels(t *testing.T, stateStore *store.Store, labels map[string]string) string {
	t.Helper()

	runtimes, err := stateStore.ListRuntimes("", true)
	if err != nil {
		t.Fatalf("list runtimes: %v", err)
	}
	for _, runtimeObj := range runtimes {
		if runtimeObj.Type != model.RuntimeTypeManagedShared {
			continue
		}
		matched := true
		for key, value := range labels {
			if runtimeObj.Labels[key] != value {
				matched = false
				break
			}
		}
		if matched {
			return runtimeObj.ID
		}
	}
	t.Fatalf("managed shared runtime with labels %#v not found", labels)
	return ""
}
