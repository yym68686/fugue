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

func TestDatabaseSwitchoverSpecClearsPendingPlacementRebalance(t *testing.T) {
	t.Parallel()

	base := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: "runtime_app",
	}
	postgres := &model.AppPostgresSpec{
		Database:                         "demo",
		User:                             "demo",
		Password:                         "secret",
		RuntimeID:                        "runtime_source",
		FailoverTargetRuntimeID:          "runtime_target",
		Instances:                        2,
		SynchronousReplicas:              1,
		PrimaryPlacementPendingRebalance: true,
	}

	got := databaseSwitchoverSpec(base, postgres, "runtime_target", "runtime_source")
	if got.Postgres == nil {
		t.Fatalf("expected postgres spec, got %+v", got)
	}
	if got.Postgres.PrimaryPlacementPendingRebalance {
		t.Fatalf("expected explicit switchover to clear pending placement hold, got %+v", got.Postgres)
	}
}

func TestDatabaseLocalizeSpecClearsFailoverAndPinsTargetNode(t *testing.T) {
	t.Parallel()

	base := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: "runtime_app",
	}
	postgres := &model.AppPostgresSpec{
		Database:                         "demo",
		User:                             "demo",
		Password:                         "secret",
		RuntimeID:                        "runtime_source",
		FailoverTargetRuntimeID:          "runtime_target",
		Instances:                        2,
		SynchronousReplicas:              1,
		PrimaryPlacementPendingRebalance: true,
	}

	stage := databaseLocalizeSpec(base, postgres, "runtime_app", "instance-us-1", false, true)
	if stage.Postgres == nil {
		t.Fatalf("expected postgres spec, got %+v", stage)
	}
	if stage.Postgres.RuntimeID != "runtime_app" || stage.Postgres.FailoverTargetRuntimeID != "" || stage.Postgres.PrimaryNodeName != "instance-us-1" {
		t.Fatalf("unexpected stage postgres placement: %+v", stage.Postgres)
	}
	if stage.Postgres.Instances != 2 || stage.Postgres.SynchronousReplicas != 0 || !stage.Postgres.PrimaryPlacementPendingRebalance {
		t.Fatalf("unexpected stage postgres lifecycle: %+v", stage.Postgres)
	}

	final := databaseLocalizeSpec(base, postgres, "runtime_app", "instance-us-1", true, false)
	if final.Postgres.Instances != 1 || final.Postgres.SynchronousReplicas != 0 || final.Postgres.PrimaryPlacementPendingRebalance {
		t.Fatalf("unexpected final postgres lifecycle: %+v", final.Postgres)
	}
}

func TestSelectManagedPostgresSwitchoverTargetMatchesManagedSharedLocationNode(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := stateStore.SyncManagedSharedLocationRuntimes([]map[string]string{{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	}}); err != nil {
		t.Fatalf("sync shared location runtimes: %v", err)
	}

	targetRuntimeID := managedSharedRuntimeIDForLabels(t, stateStore, map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})
	namespace := "tenant-demo"
	clusterName := "demo-postgres"
	currentPrimary := "demo-postgres-1"
	standbyPod := "demo-postgres-2"
	standbyNode := "instance-us-1"

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/namespaces/" + namespace + "/pods":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":              currentPrimary,
							"creationTimestamp": "2026-04-07T00:00:00Z",
						},
						"spec": map[string]any{
							"nodeName": "instance-hk-1",
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
							"nodeName": standbyNode,
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
				},
			}); err != nil {
				t.Fatalf("encode pod list: %v", err)
			}
		case "/api/v1/nodes/" + standbyNode:
			if err := json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name": standbyNode,
					"labels": map[string]any{
						runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
						runtimepkg.LocationCountryCodeLabelKey: "us",
						kubeHostnameLabelKey:                   standbyNode,
					},
				},
			}); err != nil {
				t.Fatalf("encode standby node: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}

	got, err := svc.selectManagedPostgresSwitchoverTarget(context.Background(), client, namespace, clusterName, targetRuntimeID, currentPrimary)
	if err != nil {
		t.Fatalf("select switchover target: %v", err)
	}
	if got != standbyPod {
		t.Fatalf("expected standby pod %q, got %q", standbyPod, got)
	}
}
