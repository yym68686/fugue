package controller

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestManagedClusterRuntimeForOnlineMigration(t *testing.T) {
	tests := []struct {
		name    string
		runtime model.Runtime
		want    bool
	}{
		{
			name:    "managed shared",
			runtime: model.Runtime{Type: model.RuntimeTypeManagedShared},
			want:    true,
		},
		{
			name: "managed owned cluster",
			runtime: model.Runtime{
				Type:           model.RuntimeTypeManagedOwned,
				ConnectionMode: model.MachineConnectionModeCluster,
			},
			want: true,
		},
		{
			name:    "managed owned agent",
			runtime: model.Runtime{Type: model.RuntimeTypeManagedOwned, ConnectionMode: model.MachineConnectionModeAgent},
			want:    false,
		},
		{
			name:    "external owned",
			runtime: model.Runtime{Type: model.RuntimeTypeExternalOwned},
			want:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := managedClusterRuntimeForOnlineMigration(test.runtime); got != test.want {
				t.Fatalf("managedClusterRuntimeForOnlineMigration(%+v) = %v, want %v", test.runtime, got, test.want)
			}
		})
	}
}

func TestApplyManagedMigrationOnlineRolloutIntentRequiresManagedRuntimes(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	source, _, err := stateStore.CreateRuntime("", "source", model.RuntimeTypeManagedShared, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	target, _, err := stateStore.CreateRuntime("", "target", model.RuntimeTypeManagedShared, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	current := model.App{ID: "app_demo", Name: "demo", Spec: model.AppSpec{
		Image: "registry.example/demo:v1", Ports: []int{8080}, Replicas: 1, RuntimeID: source.ID,
	}}
	desired := current
	desired.Spec.RuntimeID = target.ID
	op := model.Operation{Type: model.OperationTypeMigrate, SourceRuntimeID: source.ID, TargetRuntimeID: target.ID, DesiredSpec: &desired.Spec}
	svc := &Service{Store: stateStore}
	if err := svc.applyManagedMigrationOnlineRolloutIntent(op, current, &desired); err != nil {
		t.Fatalf("apply online migration intent: %v", err)
	}
	if desired.Spec.RolloutIntent != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected managed stateless migration intent, got %q", desired.Spec.RolloutIntent)
	}

	external, _, err := stateStore.CreateRuntime("", "external", model.RuntimeTypeExternalOwned, "", nil)
	if err != nil {
		t.Fatalf("create external runtime: %v", err)
	}
	externalDesired := current
	externalDesired.Spec.RuntimeID = external.ID
	externalOp := model.Operation{Type: model.OperationTypeMigrate, SourceRuntimeID: source.ID, TargetRuntimeID: external.ID, DesiredSpec: &externalDesired.Spec}
	if err := svc.applyManagedMigrationOnlineRolloutIntent(externalOp, current, &externalDesired); err != nil {
		t.Fatalf("apply external migration intent: %v", err)
	}
	if externalDesired.Spec.RolloutIntent != "" {
		t.Fatalf("external migration must not receive managed online intent, got %q", externalDesired.Spec.RolloutIntent)
	}
}
