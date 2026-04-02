package controller

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestQueueAutomaticFailoversCreatesOperationForOfflineRuntime(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := stateStore.CreateRuntime(tenant.ID, "source-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := stateStore.CreateRuntime(tenant.ID, "target-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: sourceRuntime.ID,
		Failover: &model.AppFailoverSpec{
			TargetRuntimeID: targetRuntime.ID,
			Auto:            true,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	stateBytes, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store: %v", err)
	}
	var state model.State
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		t.Fatalf("decode store: %v", err)
	}
	for index := range state.Runtimes {
		if state.Runtimes[index].ID == sourceRuntime.ID {
			state.Runtimes[index].Status = model.RuntimeStatusOffline
		}
	}
	updatedStateBytes, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatalf("encode store: %v", err)
	}
	if err := os.WriteFile(storePath, updatedStateBytes, 0o600); err != nil {
		t.Fatalf("write store: %v", err)
	}

	svc := New(stateStore, config.ControllerConfig{}, log.New(io.Discard, "", 0))
	if err := svc.queueAutomaticFailovers(); err != nil {
		t.Fatalf("queue automatic failovers: %v", err)
	}

	ops, err := stateStore.ListOperations(tenant.ID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}
	if got := ops[0].Type; got != model.OperationTypeFailover {
		t.Fatalf("expected failover operation, got %q", got)
	}
	if got := ops[0].AppID; got != app.ID {
		t.Fatalf("expected failover app %q, got %q", app.ID, got)
	}
	if got := ops[0].TargetRuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected target runtime %q, got %q", targetRuntime.ID, got)
	}
}

func TestDecorateManagedAppObjectsWithFenceEpochUpdatesServiceAndDeploymentSelectors(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}
	objects := runtime.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil)
	decorateManagedAppObjectsWithFenceEpoch(objects, app, "7")

	deployment := objects[0]
	service := objects[1]
	deploymentSelector := deployment["spec"].(map[string]any)["selector"].(map[string]any)["matchLabels"].(map[string]string)
	if got := deploymentSelector[runtime.FugueLabelFenceEpoch]; got != "7" {
		t.Fatalf("expected deployment fence epoch 7, got %#v", got)
	}
	serviceSelector := service["spec"].(map[string]any)["selector"].(map[string]string)
	if got := serviceSelector[runtime.FugueLabelFenceEpoch]; got != "7" {
		t.Fatalf("expected service fence epoch 7, got %#v", got)
	}
}
