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

func TestManagedPostgresPlacementsPinsSharedPrimaryToObservedPrimaryNode(t *testing.T) {
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
				ServiceName:             "demo-postgres",
				RuntimeID:               sourceRuntimeID,
				FailoverTargetRuntimeID: targetRuntime.ID,
				Instances:               2,
				SynchronousReplicas:     1,
			},
		},
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	primaryPodName := "demo-postgres-1"
	sourceNodeName := "shared-us-1"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, "demo-postgres"):
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": "demo-postgres",
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

	servicePlacements := placements["demo-postgres"]
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
