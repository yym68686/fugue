package api

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/oomrightsizing"
	"fugue/internal/store"
)

func TestOOMTerminationAdapterPreservesKubernetesEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-2 * time.Minute)
	old := now.Add(-time.Hour)
	var pod kubePodInfo
	pod.Metadata.Name = "demo-abc"
	pod.Status.ContainerStatuses = []kubeContainerStatus{
		{
			Name:         "demo",
			RestartCount: 1,
			LastState: kubeRuntimeState{
				Terminated: &kubeStateDetail{Reason: "OOMKilled", ExitCode: 137, FinishedAt: &recent},
			},
		},
		{
			Name: "old",
			LastState: kubeRuntimeState{
				Terminated: &kubeStateDetail{Reason: "OOMKilled", ExitCode: 137, FinishedAt: &old},
			},
		},
		{
			Name: "error",
			LastState: kubeRuntimeState{
				Terminated: &kubeStateDetail{Reason: "Error", ExitCode: 1, FinishedAt: &recent},
			},
		},
	}

	got := oomTerminations([]kubePodInfo{pod})
	if len(got) != 3 || got[0].Pod != "demo-abc" || got[0].Container != "demo" || got[0].State != "last-state" ||
		got[0].Reason != "OOMKilled" || got[0].Restarts != 1 || !recent.Equal(*got[0].FinishedAt) {
		t.Fatalf("termination evidence changed: %+v", got)
	}
}

func TestQueueOOMRightSizingDeployHonorsTenantEnvelopeOnOwnedRuntime(t *testing.T) {
	t.Parallel()

	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Owned OOM Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := stateStore.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    2_000,
		MemoryMebibytes:  1_200,
		StorageGibibytes: 20,
	}); err != nil {
		t.Fatalf("update tenant envelope: %v", err)
	}
	runtimeObj, _, err := stateStore.CreateRuntime(tenant.ID, "owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "", runtimeObj.ID)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
		Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512},
		Postgres: &model.AppPostgresSpec{
			Database:  "demo",
			User:      "demo",
			Password:  "secret",
			RuntimeID: runtimeObj.ID,
			Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	err = server.queueOOMRightSizingDeploy(app, []oomrightsizing.Target{{
		Kind: model.ClusterNodeWorkloadKindBackingService,
		ID:   app.BackingServices[0].ID,
		EventKeys: []string{
			"demo-postgres-1/postgres/last-state/1/2026-06-10T11:58:00Z",
		},
	}})
	if !errors.Is(err, store.ErrBillingCapExceeded) {
		t.Fatalf("expected tenant envelope to reject owned-runtime growth, got %v", err)
	}
}

func TestQueueOOMRightSizingDeployGrowsAppAndPostgres(t *testing.T) {
	t.Parallel()

	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("OOM Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{
			CPUMilliCores:        500,
			MemoryMebibytes:      512,
			MemoryLimitMebibytes: 512,
		},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
			Resources: &model.ResourceSpec{
				CPUMilliCores:        500,
				MemoryMebibytes:      1024,
				MemoryLimitMebibytes: 1024,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected postgres backing service, got %+v", app.BackingServices)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	err = server.queueOOMRightSizingDeploy(app, []oomrightsizing.Target{
		{
			Kind: model.ClusterNodeWorkloadKindApp,
			ID:   app.ID,
			EventKeys: []string{
				"demo-abc/demo/last-state/1/2026-06-10T11:58:00Z",
			},
		},
		{
			Kind: model.ClusterNodeWorkloadKindBackingService,
			ID:   app.BackingServices[0].ID,
			EventKeys: []string{
				"demo-postgres-1/postgres/last-state/1/2026-06-10T11:59:00Z",
			},
		},
	})
	if err != nil {
		t.Fatalf("queue OOM right-sizing deploy: %v", err)
	}

	operations, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	if len(operations) != 1 || operations[0].DesiredSpec == nil {
		t.Fatalf("expected one deploy operation, got %+v", operations)
	}
	desired := operations[0].DesiredSpec
	if desired.Resources == nil || desired.Resources.MemoryMebibytes != 768 || desired.Resources.MemoryLimitMebibytes != 896 {
		t.Fatalf("unexpected app OOM resources: %+v", desired.Resources)
	}
	if desired.Postgres == nil || desired.Postgres.Resources == nil ||
		desired.Postgres.Resources.MemoryMebibytes != 1536 ||
		desired.Postgres.Resources.MemoryLimitMebibytes != 1664 {
		t.Fatalf("unexpected postgres OOM resources: %+v", desired.Postgres)
	}
	if operations[0].RequestedByID == model.OperationRequestedByOOMRightSizing {
		t.Fatalf("expected event fingerprint in requested_by_id, got %q", operations[0].RequestedByID)
	}
}

func TestQueueOOMRightSizingDeployDeduplicatesEventAcrossServers(t *testing.T) {
	t.Parallel()

	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("OOM Dedupe Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{
			CPUMilliCores:        500,
			MemoryMebibytes:      512,
			MemoryLimitMebibytes: 512,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	targets := []oomrightsizing.Target{{
		Kind: model.ClusterNodeWorkloadKindApp,
		ID:   app.ID,
		EventKeys: []string{
			"demo-abc/demo/last-state/1/2026-06-10T11:58:00Z",
		},
	}}

	first := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	second := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	if err := first.queueOOMRightSizingDeploy(app, targets); err != nil {
		t.Fatalf("first queue OOM right-sizing deploy: %v", err)
	}
	if err := second.queueOOMRightSizingDeploy(app, targets); err != nil {
		t.Fatalf("duplicate queue OOM right-sizing deploy: %v", err)
	}

	operations, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	if len(operations) != 1 {
		t.Fatalf("expected one operation for the shared event, got %+v", operations)
	}
	duplicate := operations[0]
	if _, err := stateStore.CreateOperation(duplicate); !errors.Is(err, store.ErrInvalidInput) {
		t.Fatalf("generic operation writer accepted OOM actor, got %v", err)
	}
	if _, err := stateStore.EnqueueOOMRightSizingDeploy(app, duplicate.RequestedByID, *duplicate.DesiredSpec, duplicate.DesiredSource); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected durable event fingerprint conflict through scoped admission, got %v", err)
	}
}
