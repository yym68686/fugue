package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestManagedAndExternalOperationFlow(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "web project")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "nginx", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:   500,
		MemoryMebibytes: 1024,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}

	deploySpec := app.Spec
	deploySpec.Replicas = 2
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}

	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected pending operation")
	}
	if claimed.ID != deployOp.ID || claimed.Status != model.OperationStatusRunning {
		t.Fatalf("unexpected claimed deploy operation: %+v", claimed)
	}

	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/nginx.yaml", "done"); err != nil {
		t.Fatalf("complete managed operation: %v", err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after deploy: %v", err)
	}
	if app.Status.CurrentReplicas != 2 || app.Spec.Replicas != 2 {
		t.Fatalf("expected replicas=2 after deploy, got status=%d spec=%d", app.Status.CurrentReplicas, app.Spec.Replicas)
	}

	token, secret, err := s.CreateEnrollmentToken(tenant.ID, "worker", time.Hour)
	if err != nil {
		t.Fatalf("create enrollment token: %v", err)
	}
	if token.ID == "" || secret == "" {
		t.Fatal("expected enrollment token secret")
	}
	externalRuntime, runtimeKey, err := s.ConsumeEnrollmentToken(secret, "tenant-vps-1", "https://vps.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("consume enrollment token: %v", err)
	}
	if runtimeKey == "" {
		t.Fatal("expected runtime key")
	}

	migrateOp, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeMigrate,
		AppID:           app.ID,
		TargetRuntimeID: externalRuntime.ID,
	})
	if err != nil {
		t.Fatalf("create migrate operation: %v", err)
	}
	claimed, found, err = s.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim migrate operation: %v", err)
	}
	if !found {
		t.Fatal("expected migrate operation")
	}
	if claimed.ID != migrateOp.ID || claimed.Status != model.OperationStatusWaitingAgent || claimed.AssignedRuntimeID != externalRuntime.ID {
		t.Fatalf("unexpected claimed migrate operation: %+v", claimed)
	}

	ops, err := s.ListAssignedOperations(externalRuntime.ID)
	if err != nil {
		t.Fatalf("list assigned operations: %v", err)
	}
	if len(ops) != 1 || ops[0].ID != migrateOp.ID {
		t.Fatalf("expected migrate operation assigned to runtime, got %+v", ops)
	}

	if _, err := s.CompleteAgentOperation(migrateOp.ID, externalRuntime.ID, "/tmp/nginx-external.yaml", "migrated"); err != nil {
		t.Fatalf("complete agent operation: %v", err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after migrate: %v", err)
	}
	if app.Status.CurrentRuntimeID != externalRuntime.ID || app.Spec.RuntimeID != externalRuntime.ID {
		t.Fatalf("expected app runtime=%s, got status=%s spec=%s", externalRuntime.ID, app.Status.CurrentRuntimeID, app.Spec.RuntimeID)
	}
}

func TestMigrateOperationAppliesDesiredSpecAndSource(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "web project")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:   500,
		MemoryMebibytes: 1024,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}

	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:       model.AppSourceTypeGitHubPublic,
		RepoURL:    "https://github.com/example/demo",
		RepoBranch: "main",
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	externalRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-vps-1", model.RuntimeTypeExternalOwned, "https://vps.example.com", nil)
	if err != nil {
		t.Fatalf("create external runtime: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Image = "registry.pull.example/fugue-apps/demo:git-new"
	desiredSpec.Ports = []int{8080}
	desiredSpec.RuntimeID = externalRuntime.ID
	desiredSource := model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		CommitSHA:        "newcommit",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-new",
		ComposeDependsOn: []string{"redis"},
	}

	migrateOp, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeMigrate,
		AppID:           app.ID,
		TargetRuntimeID: externalRuntime.ID,
		DesiredSpec:     &desiredSpec,
		DesiredSource:   &desiredSource,
	})
	if err != nil {
		t.Fatalf("create migrate operation: %v", err)
	}

	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim migrate operation: %v", err)
	}
	if !found {
		t.Fatal("expected migrate operation")
	}
	if claimed.ID != migrateOp.ID || claimed.Status != model.OperationStatusWaitingAgent || claimed.AssignedRuntimeID != externalRuntime.ID {
		t.Fatalf("unexpected claimed migrate operation: %+v", claimed)
	}

	if _, err := s.CompleteAgentOperation(migrateOp.ID, externalRuntime.ID, "/tmp/nginx-external.yaml", "migrated"); err != nil {
		t.Fatalf("complete agent migrate operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after migrate: %v", err)
	}
	if got := app.Spec.RuntimeID; got != externalRuntime.ID {
		t.Fatalf("expected runtime %q, got %q", externalRuntime.ID, got)
	}
	if got := app.Spec.Image; got != desiredSpec.Image {
		t.Fatalf("expected image %q, got %q", desiredSpec.Image, got)
	}
	if got := app.Route.ServicePort; got != 8080 {
		t.Fatalf("expected route service port 8080, got %d", got)
	}
	if app.Source == nil {
		t.Fatal("expected source to be updated")
	}
	if got := app.Source.CommitSHA; got != desiredSource.CommitSHA {
		t.Fatalf("expected commit %q, got %q", desiredSource.CommitSHA, got)
	}
	if got := app.Source.ResolvedImageRef; got != desiredSource.ResolvedImageRef {
		t.Fatalf("expected resolved image ref %q, got %q", desiredSource.ResolvedImageRef, got)
	}
	if app.Status.Phase != "migrated" {
		t.Fatalf("expected phase migrated, got %q", app.Status.Phase)
	}
}

func TestSyncManagedOwnedClusterRuntimeStatuses(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	_, secretReady, err := s.CreateNodeKey(tenant.ID, "ready")
	if err != nil {
		t.Fatalf("create ready node key: %v", err)
	}
	_, runtimeReady, err := s.BootstrapClusterNode(secretReady, "cluster-ready", "https://ready.example.com", nil, "cluster-ready", "cluster-ready")
	if err != nil {
		t.Fatalf("bootstrap ready node: %v", err)
	}
	_, secretNotReady, err := s.CreateNodeKey(tenant.ID, "not-ready")
	if err != nil {
		t.Fatalf("create not-ready node key: %v", err)
	}
	_, runtimeNotReady, err := s.BootstrapClusterNode(secretNotReady, "cluster-not-ready", "https://not-ready.example.com", nil, "cluster-not-ready", "cluster-not-ready")
	if err != nil {
		t.Fatalf("bootstrap not-ready node: %v", err)
	}
	readyNodeName := runtimeReady.ClusterNodeName
	if readyNodeName == "" {
		readyNodeName = runtimeReady.Name
	}
	notReadyNodeName := runtimeNotReady.ClusterNodeName
	if notReadyNodeName == "" {
		notReadyNodeName = runtimeNotReady.Name
	}

	changed, err := s.SyncManagedOwnedClusterRuntimeStatuses(map[string]bool{
		readyNodeName:    true,
		notReadyNodeName: false,
	})
	if err != nil {
		t.Fatalf("sync managed-owned cluster runtime statuses: %v", err)
	}
	if changed != 1 {
		t.Fatalf("expected 1 runtime status change, got %d", changed)
	}

	runtimeReady, err = s.GetRuntime(runtimeReady.ID)
	if err != nil {
		t.Fatalf("get ready runtime: %v", err)
	}
	if runtimeReady.Status != model.RuntimeStatusActive {
		t.Fatalf("expected ready runtime active, got %q", runtimeReady.Status)
	}
	runtimeNotReady, err = s.GetRuntime(runtimeNotReady.ID)
	if err != nil {
		t.Fatalf("get not-ready runtime: %v", err)
	}
	if runtimeNotReady.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected not-ready runtime offline, got %q", runtimeNotReady.Status)
	}

	changed, err = s.SyncManagedOwnedClusterRuntimeStatuses(map[string]bool{
		readyNodeName:    false,
		notReadyNodeName: true,
	})
	if err != nil {
		t.Fatalf("resync managed-owned cluster runtime statuses: %v", err)
	}
	if changed != 2 {
		t.Fatalf("expected 2 runtime status changes, got %d", changed)
	}
}

func TestStatefulFailoverOperationUsesAppFailoverPolicy(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Failover Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "source-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "target-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: sourceRuntime.ID,
		Workspace: &model.AppWorkspaceSpec{},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
		},
		Failover: &model.AppFailoverSpec{
			TargetRuntimeID: targetRuntime.ID,
			Auto:            true,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	op, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeFailover,
		AppID:    app.ID,
	})
	if err != nil {
		t.Fatalf("create failover operation: %v", err)
	}
	if got := op.SourceRuntimeID; got != sourceRuntime.ID {
		t.Fatalf("expected source runtime %q, got %q", sourceRuntime.ID, got)
	}
	if got := op.TargetRuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected target runtime %q, got %q", targetRuntime.ID, got)
	}

	if _, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeFailover,
		AppID:    app.ID,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected failover conflict while another operation is in flight, got %v", err)
	}
}

func TestDatabaseSwitchoverOperationUsesManagedPostgresPrimaryAndPreservesAppStatus(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Database Switchover")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appRuntime, _, err := s.CreateRuntime(tenant.ID, "app-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create app runtime: %v", err)
	}
	databaseSource, _, err := s.CreateRuntime(tenant.ID, "db-source", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create database source runtime: %v", err)
	}
	databaseTarget, _, err := s.CreateRuntime(tenant.ID, "db-target", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create database target runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: appRuntime.ID,
		Postgres: &model.AppPostgresSpec{
			Database:  "demo",
			RuntimeID: databaseSource.ID,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	currentApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get current app: %v", err)
	}
	currentDatabase := OwnedManagedPostgresSpec(currentApp)
	if currentDatabase == nil {
		t.Fatalf("expected owned managed postgres spec, got app=%+v", currentApp)
	}
	if got := currentDatabase.RuntimeID; got != databaseSource.ID {
		t.Fatalf("expected owned managed postgres runtime %q, got %q", databaseSource.ID, got)
	}

	deploySpec := app.Spec
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "deployed"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	switchoverOp, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeDatabaseSwitchover,
		AppID:           app.ID,
		TargetRuntimeID: databaseTarget.ID,
	})
	if err != nil {
		t.Fatalf("create database switchover operation: %v", err)
	}
	if got := switchoverOp.SourceRuntimeID; got != databaseSource.ID {
		t.Fatalf("expected database source runtime %q, got %q", databaseSource.ID, got)
	}
	if got := switchoverOp.TargetRuntimeID; got != databaseTarget.ID {
		t.Fatalf("expected database target runtime %q, got %q", databaseTarget.ID, got)
	}

	finalSpec := deploySpec
	finalSpec.Postgres = &model.AppPostgresSpec{
		Database:                "demo",
		RuntimeID:               databaseTarget.ID,
		FailoverTargetRuntimeID: databaseSource.ID,
		Instances:               2,
		SynchronousReplicas:     1,
	}
	if _, err := s.CompleteManagedOperationWithResult(
		switchoverOp.ID,
		"/tmp/demo-db.yaml",
		"managed postgres switched over",
		&finalSpec,
		nil,
	); err != nil {
		t.Fatalf("complete database switchover operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Status.Phase != "deployed" {
		t.Fatalf("expected app phase to remain deployed, got %q", app.Status.Phase)
	}
	if app.Status.LastOperationID != deployOp.ID {
		t.Fatalf("expected last operation to stay %q, got %q", deployOp.ID, app.Status.LastOperationID)
	}
	if app.Status.LastMessage != "deployed" {
		t.Fatalf("expected last message to stay deployed, got %q", app.Status.LastMessage)
	}
	if app.Spec.Postgres != nil {
		t.Fatalf("expected app spec postgres to remain externalized, got %+v", app.Spec.Postgres)
	}
	currentDatabase = OwnedManagedPostgresSpec(app)
	if currentDatabase == nil {
		t.Fatal("expected owned managed postgres spec after switchover")
	}
	if got := currentDatabase.RuntimeID; got != databaseTarget.ID {
		t.Fatalf("expected owned managed postgres runtime %q, got %q", databaseTarget.ID, got)
	}
	if got := currentDatabase.FailoverTargetRuntimeID; got != databaseSource.ID {
		t.Fatalf("expected owned managed postgres failover runtime %q, got %q", databaseSource.ID, got)
	}
	if len(app.BackingServices) != 1 || app.BackingServices[0].Spec.Postgres == nil {
		t.Fatalf("expected one managed postgres backing service, got %+v", app.BackingServices)
	}
	if got := app.BackingServices[0].Spec.Postgres.RuntimeID; got != databaseTarget.ID {
		t.Fatalf("expected backing service postgres runtime %q, got %q", databaseTarget.ID, got)
	}
	if got := app.BackingServices[0].Spec.Postgres.FailoverTargetRuntimeID; got != databaseSource.ID {
		t.Fatalf("expected backing service postgres failover runtime %q, got %q", databaseSource.ID, got)
	}
}

func TestFailoverOperationPreservesManagedPostgresPlacement(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Failover Postgres Continuity")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "source-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "target-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: sourceRuntime.ID,
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
		},
		Failover: &model.AppFailoverSpec{
			TargetRuntimeID: targetRuntime.ID,
			Auto:            true,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	failoverSpec := FailoverDesiredSpec(app, targetRuntime.ID)
	if failoverSpec == nil {
		t.Fatal("expected failover desired spec")
	}
	if failoverSpec.RuntimeID != targetRuntime.ID {
		t.Fatalf("expected failover desired runtime %q, got %q", targetRuntime.ID, failoverSpec.RuntimeID)
	}
	if failoverSpec.Postgres == nil {
		t.Fatal("expected failover desired postgres spec")
	}
	if got := failoverSpec.Postgres.RuntimeID; got != sourceRuntime.ID {
		t.Fatalf("expected failover desired postgres runtime %q, got %q", sourceRuntime.ID, got)
	}

	op, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeFailover,
		AppID:    app.ID,
	})
	if err != nil {
		t.Fatalf("create failover operation: %v", err)
	}

	if _, err := s.CompleteManagedOperationWithResult(op.ID, "/tmp/demo-failover.yaml", "failed over", failoverSpec, nil); err != nil {
		t.Fatalf("complete failover operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if got := app.Spec.RuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected app runtime %q after failover, got %q", targetRuntime.ID, got)
	}
	if app.Spec.Postgres != nil {
		t.Fatalf("expected app spec postgres to remain externalized, got %+v", app.Spec.Postgres)
	}
	if got := app.Status.Phase; got != "failed-over" {
		t.Fatalf("expected failed-over phase, got %q", got)
	}
	if got := app.Status.CurrentRuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected current runtime %q, got %q", targetRuntime.ID, got)
	}

	currentDatabase := OwnedManagedPostgresSpec(app)
	if currentDatabase == nil {
		t.Fatal("expected managed postgres spec after failover")
	}
	if got := currentDatabase.RuntimeID; got != sourceRuntime.ID {
		t.Fatalf("expected managed postgres runtime to stay pinned to %q, got %q", sourceRuntime.ID, got)
	}
	if got := currentDatabase.Instances; got != 1 {
		t.Fatalf("expected managed postgres instances 1, got %d", got)
	}
	if len(app.BackingServices) != 1 || app.BackingServices[0].Spec.Postgres == nil {
		t.Fatalf("expected one managed postgres backing service, got %+v", app.BackingServices)
	}
	if got := app.BackingServices[0].Spec.Postgres.RuntimeID; got != sourceRuntime.ID {
		t.Fatalf("expected backing service postgres runtime %q, got %q", sourceRuntime.ID, got)
	}
}

func TestOwnedManagedPostgresSpecPreservesPendingPlacementWhileFailoverEnabled(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Postgres Pending Rebalance")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "source", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "target", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: sourceRuntime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database:                         "demo",
			Password:                         "secret",
			RuntimeID:                        sourceRuntime.ID,
			FailoverTargetRuntimeID:          targetRuntime.ID,
			Instances:                        2,
			SynchronousReplicas:              1,
			PrimaryPlacementPendingRebalance: true,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	currentDatabase := OwnedManagedPostgresSpec(app)
	if currentDatabase == nil {
		t.Fatal("expected owned managed postgres spec")
	}
	if !currentDatabase.PrimaryPlacementPendingRebalance {
		t.Fatalf("expected pending placement hold to survive normalization, got %+v", currentDatabase)
	}
}

func TestMigrateOperationRejectsExternalRuntimeWhenAppHasBoundManagedPostgres(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Bound Managed Postgres")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "source", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	externalRuntime, _, err := s.CreateRuntime(tenant.ID, "target", model.RuntimeTypeExternalOwned, "", nil)
	if err != nil {
		t.Fatalf("create external runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: sourceRuntime.ID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	service, err := s.CreateBackingService(tenant.ID, project.ID, "shared-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backing service: %v", err)
	}
	if _, err := s.BindBackingService(tenant.ID, app.ID, service.ID, "", nil); err != nil {
		t.Fatalf("bind backing service: %v", err)
	}

	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeMigrate,
		AppID:           app.ID,
		TargetRuntimeID: externalRuntime.ID,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid input for external runtime with bound managed postgres, got %v", err)
	}
}

func TestSharedNodeKeyBootstrapsMultipleNodesAndCanBeRevoked(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Shared Nodes")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	key, secret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	if key.ID == "" || secret == "" {
		t.Fatal("expected node key secret")
	}
	if key.Hash != "" {
		t.Fatal("expected redacted node key hash")
	}

	issuedKey, nodeA, runtimeKeyA, err := s.BootstrapNode(secret, "worker", "https://a.example.com", map[string]string{"zone": "a"}, "", "")
	if err != nil {
		t.Fatalf("bootstrap first node: %v", err)
	}
	if issuedKey.ID != key.ID {
		t.Fatalf("expected issued key id %s, got %s", key.ID, issuedKey.ID)
	}
	if runtimeKeyA == "" {
		t.Fatal("expected first runtime key")
	}
	if nodeA.NodeKeyID != key.ID {
		t.Fatalf("expected nodeA NodeKeyID=%s, got %s", key.ID, nodeA.NodeKeyID)
	}
	if nodeA.Name != "worker" {
		t.Fatalf("expected first node name worker, got %s", nodeA.Name)
	}

	_, nodeB, runtimeKeyB, err := s.BootstrapNode(secret, "worker", "https://b.example.com", map[string]string{"zone": "b"}, "", "")
	if err != nil {
		t.Fatalf("bootstrap second node: %v", err)
	}
	if runtimeKeyB == "" {
		t.Fatal("expected second runtime key")
	}
	if nodeB.ID == nodeA.ID {
		t.Fatal("expected distinct node ids")
	}
	if nodeB.Name != "worker-2" {
		t.Fatalf("expected second node name worker-2, got %s", nodeB.Name)
	}

	keys, err := s.ListNodeKeys(tenant.ID, false)
	if err != nil {
		t.Fatalf("list node keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 node key, got %d", len(keys))
	}
	if keys[0].LastUsedAt == nil {
		t.Fatal("expected node key last_used_at to be populated")
	}

	nodes, err := s.ListNodes(tenant.ID, false)
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	revoked, err := s.RevokeNodeKey(key.ID)
	if err != nil {
		t.Fatalf("revoke node key: %v", err)
	}
	if revoked.Status != model.NodeKeyStatusRevoked || revoked.RevokedAt == nil {
		t.Fatalf("expected revoked node key, got %+v", revoked)
	}

	_, _, _, err = s.BootstrapNode(secret, "worker", "https://c.example.com", nil, "", "")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict after revoke, got %v", err)
	}
}

func TestNodeAndKeyDefaultsWhenNamesAreOmitted(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	clusterTenant, err := s.CreateTenant("Cluster Tenant")
	if err != nil {
		t.Fatalf("create cluster tenant: %v", err)
	}
	clusterKey, clusterSecret, err := s.CreateNodeKey(clusterTenant.ID, "")
	if err != nil {
		t.Fatalf("create cluster node key: %v", err)
	}
	if clusterKey.Label != "default" {
		t.Fatalf("expected default node key label, got %q", clusterKey.Label)
	}
	_, clusterRuntime, err := s.BootstrapClusterNode(clusterSecret, "", "https://cluster.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node without name: %v", err)
	}
	if clusterRuntime.Name != "node" {
		t.Fatalf("expected default cluster runtime name node, got %q", clusterRuntime.Name)
	}
	if clusterRuntime.AccessMode != model.RuntimeAccessModePrivate {
		t.Fatalf("expected private access mode for joined cluster runtime, got %q", clusterRuntime.AccessMode)
	}

	externalTenant, err := s.CreateTenant("External Tenant")
	if err != nil {
		t.Fatalf("create external tenant: %v", err)
	}
	_, externalSecret, err := s.CreateNodeKey(externalTenant.ID, "")
	if err != nil {
		t.Fatalf("create external node key: %v", err)
	}
	_, externalRuntime, runtimeKey, err := s.BootstrapNode(externalSecret, "", "https://external.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap external node without name: %v", err)
	}
	if runtimeKey == "" {
		t.Fatal("expected runtime key from bootstrap node")
	}
	if externalRuntime.Name != "node" {
		t.Fatalf("expected default external runtime name node, got %q", externalRuntime.Name)
	}
	if externalRuntime.AccessMode != model.RuntimeAccessModePrivate {
		t.Fatalf("expected private access mode for joined external runtime, got %q", externalRuntime.AccessMode)
	}

	enrollTenant, err := s.CreateTenant("Enroll Tenant")
	if err != nil {
		t.Fatalf("create enroll tenant: %v", err)
	}
	_, enrollSecret, err := s.CreateEnrollmentToken(enrollTenant.ID, "worker", time.Hour)
	if err != nil {
		t.Fatalf("create enrollment token: %v", err)
	}
	enrolledRuntime, enrolledKey, err := s.ConsumeEnrollmentToken(enrollSecret, "", "https://enroll.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("consume enrollment token without name: %v", err)
	}
	if enrolledKey == "" {
		t.Fatal("expected runtime key from enrollment")
	}
	if enrolledRuntime.Name != "node" {
		t.Fatalf("expected default enrolled runtime name node, got %q", enrolledRuntime.Name)
	}
	if enrolledRuntime.AccessMode != model.RuntimeAccessModePrivate {
		t.Fatalf("expected private access mode for enrolled runtime, got %q", enrolledRuntime.AccessMode)
	}
}

func TestDisableEnableAndDeleteAPIKey(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("API Key Lifecycle")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	key, secret, err := s.CreateAPIKey(tenant.ID, "preview", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	if key.Status != model.APIKeyStatusActive {
		t.Fatalf("expected active status on create, got %q", key.Status)
	}

	if _, err := s.AuthenticateAPIKey(secret); err != nil {
		t.Fatalf("authenticate api key before disable: %v", err)
	}

	disabled, err := s.DisableAPIKey(key.ID)
	if err != nil {
		t.Fatalf("disable api key: %v", err)
	}
	if disabled.Status != model.APIKeyStatusDisabled {
		t.Fatalf("expected disabled status, got %q", disabled.Status)
	}
	if disabled.DisabledAt == nil {
		t.Fatal("expected disabled_at to be set")
	}
	if _, err := s.AuthenticateAPIKey(secret); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for disabled key auth, got %v", err)
	}

	enabled, err := s.EnableAPIKey(key.ID)
	if err != nil {
		t.Fatalf("enable api key: %v", err)
	}
	if enabled.Status != model.APIKeyStatusActive {
		t.Fatalf("expected active status after enable, got %q", enabled.Status)
	}
	if enabled.DisabledAt != nil {
		t.Fatalf("expected disabled_at to be cleared, got %v", enabled.DisabledAt)
	}
	if _, err := s.AuthenticateAPIKey(secret); err != nil {
		t.Fatalf("authenticate api key after enable: %v", err)
	}

	deleted, err := s.DeleteAPIKey(key.ID)
	if err != nil {
		t.Fatalf("delete api key: %v", err)
	}
	if deleted.ID != key.ID {
		t.Fatalf("expected deleted key id %q, got %q", key.ID, deleted.ID)
	}
	if _, err := s.AuthenticateAPIKey(secret); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted key auth, got %v", err)
	}
	if _, err := s.GetAPIKey(key.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted key lookup, got %v", err)
	}
}

func TestEnsureDefaultProjectReusesExistingProject(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Default Project Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	projectA, err := s.EnsureDefaultProject(tenant.ID)
	if err != nil {
		t.Fatalf("ensure default project first call: %v", err)
	}
	if projectA.Name != "default" {
		t.Fatalf("expected default project name, got %q", projectA.Name)
	}
	if projectA.Description != "default project" {
		t.Fatalf("expected default project description, got %q", projectA.Description)
	}

	projectB, err := s.EnsureDefaultProject(tenant.ID)
	if err != nil {
		t.Fatalf("ensure default project second call: %v", err)
	}
	if projectA.ID != projectB.ID {
		t.Fatalf("expected same default project id, got %s and %s", projectA.ID, projectB.ID)
	}

	projects, err := s.ListProjects(tenant.ID)
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	if len(projects) != 1 {
		t.Fatalf("expected 1 project after ensure default project, got %d", len(projects))
	}
}

func TestCreateAppConvertsInlinePostgresToBackingService(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Stateful Create")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.DefaultManagedPostgresBillingResources()); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "root",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	if app.Spec.Postgres != nil {
		t.Fatal("expected inline postgres to be removed from app spec")
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected 1 backing service, got %d", len(app.BackingServices))
	}
	if len(app.Bindings) != 1 {
		t.Fatalf("expected 1 binding, got %d", len(app.Bindings))
	}
	service := app.BackingServices[0]
	if service.OwnerAppID != app.ID {
		t.Fatalf("expected owner_app_id=%s, got %s", app.ID, service.OwnerAppID)
	}
	if service.Spec.Postgres == nil {
		t.Fatal("expected postgres backing service spec")
	}
	if got := service.Spec.Postgres.Database; got != "demo" {
		t.Fatalf("expected database demo, got %q", got)
	}
	if got := service.Spec.Postgres.RuntimeID; got != app.Spec.RuntimeID {
		t.Fatalf("expected postgres runtime %q, got %q", app.Spec.RuntimeID, got)
	}
	if got := service.Spec.Postgres.Image; got != "" {
		t.Fatalf("expected official postgres image to be stripped, got %q", got)
	}
	if got := service.Spec.Postgres.Instances; got != 1 {
		t.Fatalf("expected default postgres instances 1, got %d", got)
	}
	if got := service.Spec.Postgres.SynchronousReplicas; got != 0 {
		t.Fatalf("expected default synchronous replicas 0 for single-instance postgres, got %d", got)
	}
	if got := app.Bindings[0].Env["DB_HOST"]; got != model.PostgresRWServiceName(service.Spec.Postgres.ServiceName) {
		t.Fatalf("expected binding DB_HOST=%q, got %q", model.PostgresRWServiceName(service.Spec.Postgres.ServiceName), got)
	}

	persisted, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if persisted.Spec.Postgres != nil {
		t.Fatal("expected persisted app spec without inline postgres")
	}
	if len(persisted.BackingServices) != 1 || len(persisted.Bindings) != 1 {
		t.Fatalf("expected persisted backing resources, got services=%d bindings=%d", len(persisted.BackingServices), len(persisted.Bindings))
	}
}

func TestCreateAppRejectsReservedCNPGPostgresUser(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Stateful Create Validation")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	_, err = s.CreateApp(tenant.ID, project.ID, "fugue-web", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Image:    "postgres:16-alpine",
			User:     "postgres",
			Password: "secret",
		},
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
}

func TestDeployOperationConvertsInlinePostgresToBackingServiceOnComplete(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Stateful Deploy")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    750,
		MemoryMebibytes:  1536,
		StorageGibibytes: 1,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Postgres = &model.AppPostgresSpec{
		Image:    "postgres:17.6-alpine",
		Database: "demo",
		User:     "root",
		Password: "secret",
	}

	op, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &desiredSpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected deploy operation")
	}

	completed, err := s.CompleteManagedOperation(op.ID, "/tmp/demo.yaml", "deployed")
	if err != nil {
		t.Fatalf("complete managed operation: %v", err)
	}
	if completed.DesiredSpec == nil {
		t.Fatal("expected desired spec on completed operation")
	}
	if completed.DesiredSpec.Postgres != nil {
		t.Fatal("expected completed operation desired spec without inline postgres")
	}

	persisted, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if persisted.Spec.Postgres != nil {
		t.Fatal("expected persisted app spec without inline postgres after deploy")
	}
	if len(persisted.BackingServices) != 1 {
		t.Fatalf("expected 1 backing service after deploy, got %d", len(persisted.BackingServices))
	}
	if len(persisted.Bindings) != 1 {
		t.Fatalf("expected 1 binding after deploy, got %d", len(persisted.Bindings))
	}
	if persisted.BackingServices[0].Spec.Postgres == nil {
		t.Fatal("expected postgres backing service spec after deploy")
	}
	if got := persisted.BackingServices[0].Spec.Postgres.Database; got != "demo" {
		t.Fatalf("expected database demo after deploy, got %q", got)
	}
	if got := persisted.BackingServices[0].Spec.Postgres.RuntimeID; got != app.Spec.RuntimeID {
		t.Fatalf("expected postgres runtime %q after deploy, got %q", app.Spec.RuntimeID, got)
	}
	if got := persisted.BackingServices[0].Spec.Postgres.Image; got != "" {
		t.Fatalf("expected official postgres image to be stripped after deploy, got %q", got)
	}
}

func TestDeployOperationRejectsReservedCNPGPostgresUser(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Stateful Deploy Validation")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Postgres = &model.AppPostgresSpec{
		User:     "postgres",
		Password: "secret",
	}

	_, err = s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &desiredSpec,
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
}

func TestCreateAppRejectsManagedPostgresFailoverTargetMatchingPrimary(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Stateful Failover Validation")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	_, err = s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			FailoverTargetRuntimeID: "runtime_managed_shared",
			Password:                "secret",
		},
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
}

func TestRuntimeReferencedByStateIncludesContinuityTargets(t *testing.T) {
	t.Parallel()

	state := &model.State{
		Apps: []model.App{
			{
				ID: "app_demo",
				Spec: model.AppSpec{
					RuntimeID: "runtime_app_primary",
					Failover: &model.AppFailoverSpec{
						TargetRuntimeID: "runtime_app_failover",
						Auto:            true,
					},
				},
				Status: model.AppStatus{
					CurrentRuntimeID: "runtime_app_primary",
				},
			},
		},
		BackingServices: []model.BackingService{
			{
				ID:     "service_demo",
				Status: model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						RuntimeID:               "runtime_db_primary",
						FailoverTargetRuntimeID: "runtime_db_failover",
					},
				},
			},
		},
		Operations: []model.Operation{
			{
				ID: "op_demo",
				DesiredSpec: &model.AppSpec{
					RuntimeID: "runtime_op_primary",
					Failover: &model.AppFailoverSpec{
						TargetRuntimeID: "runtime_op_failover",
					},
					Postgres: &model.AppPostgresSpec{
						RuntimeID:               "runtime_op_db_primary",
						FailoverTargetRuntimeID: "runtime_op_db_failover",
					},
				},
			},
		},
	}

	referencedRuntimeIDs := []string{
		"runtime_app_primary",
		"runtime_app_failover",
		"runtime_db_primary",
		"runtime_db_failover",
		"runtime_op_primary",
		"runtime_op_failover",
		"runtime_op_db_primary",
		"runtime_op_db_failover",
	}
	for _, runtimeID := range referencedRuntimeIDs {
		if !runtimeReferencedByState(state, runtimeID) {
			t.Fatalf("expected runtime %q to be referenced", runtimeID)
		}
	}
}

func TestBootstrapNodeReusesRuntimeByFingerprint(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Machine Reuse")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}

	_, runtimeA, runtimeKeyA, err := s.BootstrapNode(nodeSecret, "worker", "https://a.example.com", map[string]string{"zone": "a"}, "alicehk2", "fingerprint-1")
	if err != nil {
		t.Fatalf("bootstrap first machine: %v", err)
	}
	_, runtimeB, runtimeKeyB, err := s.BootstrapNode(nodeSecret, "worker", "https://b.example.com", map[string]string{"zone": "b"}, "alicehk2-renamed", "fingerprint-1")
	if err != nil {
		t.Fatalf("bootstrap same machine again: %v", err)
	}

	if runtimeA.ID != runtimeB.ID {
		t.Fatalf("expected same runtime id, got %s and %s", runtimeA.ID, runtimeB.ID)
	}
	if runtimeKeyA == runtimeKeyB {
		t.Fatal("expected runtime key rotation on machine re-bootstrap")
	}
	if runtimeB.Endpoint != "https://b.example.com" {
		t.Fatalf("expected updated runtime endpoint, got %q", runtimeB.Endpoint)
	}
	if runtimeB.MachineName != "alicehk2-renamed" {
		t.Fatalf("expected updated machine_name, got %q", runtimeB.MachineName)
	}
	if runtimeB.ConnectionMode != model.MachineConnectionModeAgent {
		t.Fatalf("expected agent connection mode, got %q", runtimeB.ConnectionMode)
	}
	if runtimeB.FingerprintPrefix == "" || runtimeB.FingerprintHash == "" {
		t.Fatal("expected fingerprint metadata on reused runtime")
	}

	nodes, err := s.ListNodes(tenant.ID, false)
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 compatibility node runtime, got %d", len(nodes))
	}
}

func TestListRuntimesByNodeKey(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Node Key Usage")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	key, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}

	if _, _, _, err := s.BootstrapNode(nodeSecret, "worker-a", "https://a.example.com", nil, "worker-a", "fingerprint-a"); err != nil {
		t.Fatalf("bootstrap machine a: %v", err)
	}
	if _, _, _, err := s.BootstrapNode(nodeSecret, "worker-b", "https://b.example.com", nil, "worker-b", "fingerprint-b"); err != nil {
		t.Fatalf("bootstrap machine b: %v", err)
	}

	runtimes, err := s.ListRuntimesByNodeKey(key.ID, tenant.ID, false)
	if err != nil {
		t.Fatalf("list runtimes by node key: %v", err)
	}
	if len(runtimes) != 2 {
		t.Fatalf("expected 2 runtimes for node key, got %d", len(runtimes))
	}
}

func TestRuntimeSharingGrantControlsVisibilityAndUsage(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Runtime Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	grantee, err := s.CreateTenant("Runtime Grantee")
	if err != nil {
		t.Fatalf("create grantee tenant: %v", err)
	}
	project, err := s.CreateProject(grantee.ID, "shared-apps", "")
	if err != nil {
		t.Fatalf("create grantee project: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "shared-worker", "https://shared-worker.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	visible, err := s.RuntimeVisibleToTenant(runtimeObj.ID, grantee.ID, false)
	if err != nil {
		t.Fatalf("check pre-grant visibility: %v", err)
	}
	if visible {
		t.Fatal("expected runtime to be hidden before grant")
	}
	if _, err := s.CreateApp(grantee.ID, project.ID, "before-share", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound before grant, got %v", err)
	}

	grant, err := s.GrantRuntimeAccess(runtimeObj.ID, owner.ID, grantee.ID)
	if err != nil {
		t.Fatalf("grant runtime access: %v", err)
	}
	if grant.RuntimeID != runtimeObj.ID || grant.TenantID != grantee.ID {
		t.Fatalf("unexpected runtime grant: %+v", grant)
	}

	visible, err = s.RuntimeVisibleToTenant(runtimeObj.ID, grantee.ID, false)
	if err != nil {
		t.Fatalf("check granted visibility: %v", err)
	}
	if !visible {
		t.Fatal("expected runtime to be visible after grant")
	}
	nodes, err := s.ListNodes(grantee.ID, false)
	if err != nil {
		t.Fatalf("list grantee nodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != runtimeObj.ID {
		t.Fatalf("expected granted tenant to see shared node, got %+v", nodes)
	}
	if _, err := s.CreateApp(grantee.ID, project.ID, "after-share", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
	}); err != nil {
		t.Fatalf("create app on granted runtime: %v", err)
	}

	removed, err := s.RevokeRuntimeAccess(runtimeObj.ID, owner.ID, grantee.ID)
	if err != nil {
		t.Fatalf("revoke runtime access: %v", err)
	}
	if !removed {
		t.Fatal("expected runtime grant to be removed")
	}
	visible, err = s.RuntimeVisibleToTenant(runtimeObj.ID, grantee.ID, false)
	if err != nil {
		t.Fatalf("check post-revoke visibility: %v", err)
	}
	if visible {
		t.Fatal("expected runtime to be hidden after revoke")
	}
	nodes, err = s.ListNodes(grantee.ID, false)
	if err != nil {
		t.Fatalf("list grantee nodes after revoke: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected no shared nodes after revoke, got %+v", nodes)
	}
	if _, err := s.CreateApp(grantee.ID, project.ID, "after-revoke", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after revoke, got %v", err)
	}
}

func TestRuntimePlatformSharedVisibleToAllTenants(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Platform Shared Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	consumer, err := s.CreateTenant("Platform Shared Consumer")
	if err != nil {
		t.Fatalf("create consumer tenant: %v", err)
	}
	project, err := s.CreateProject(consumer.ID, "shared-project", "")
	if err != nil {
		t.Fatalf("create consumer project: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "cluster-public", "https://cluster-public.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	runtimeObj, err = s.SetRuntimeAccessMode(runtimeObj.ID, owner.ID, model.RuntimeAccessModePlatformShared)
	if err != nil {
		t.Fatalf("set runtime access mode: %v", err)
	}
	if runtimeObj.AccessMode != model.RuntimeAccessModePlatformShared {
		t.Fatalf("expected platform-shared access mode, got %q", runtimeObj.AccessMode)
	}

	visible, err := s.RuntimeVisibleToTenant(runtimeObj.ID, consumer.ID, false)
	if err != nil {
		t.Fatalf("check platform-shared visibility: %v", err)
	}
	if !visible {
		t.Fatal("expected platform-shared runtime to be visible")
	}
	runtimes, err := s.ListRuntimes(consumer.ID, false)
	if err != nil {
		t.Fatalf("list visible runtimes: %v", err)
	}
	found := false
	for _, candidate := range runtimes {
		if candidate.ID == runtimeObj.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected platform-shared runtime %s in visible runtime list", runtimeObj.ID)
	}
	nodes, err := s.ListNodes(consumer.ID, false)
	if err != nil {
		t.Fatalf("list visible nodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != runtimeObj.ID {
		t.Fatalf("expected platform-shared node in visible node list, got %+v", nodes)
	}
	if _, err := s.CreateApp(consumer.ID, project.ID, "platform-shared-app", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
	}); err != nil {
		t.Fatalf("create app on platform-shared runtime: %v", err)
	}
}

func TestRuntimePublicAccessTransfersAccruedBalanceToOwner(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Public Runtime Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	consumer, err := s.CreateTenant("Public Runtime Consumer")
	if err != nil {
		t.Fatalf("create consumer tenant: %v", err)
	}
	project, err := s.CreateProject(consumer.ID, "public-runtime-project", "")
	if err != nil {
		t.Fatalf("create consumer project: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "public-node", "https://public-node.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}
	runtimeObj, err = s.SetRuntimeAccessMode(runtimeObj.ID, owner.ID, model.RuntimeAccessModePublic)
	if err != nil {
		t.Fatalf("set runtime public access mode: %v", err)
	}
	offer, err := normalizeRuntimePublicOffer(model.RuntimePublicOffer{
		ReferenceBundle: model.BillingResourceSpec{
			CPUMilliCores:    2000,
			MemoryMebibytes:  4096,
			StorageGibibytes: 30,
		},
		ReferenceMonthlyPriceMicroCents: 400 * microCentsPerCent,
	})
	if err != nil {
		t.Fatalf("normalize public offer: %v", err)
	}
	runtimeObj, err = s.SetRuntimePublicOffer(runtimeObj.ID, owner.ID, offer)
	if err != nil {
		t.Fatalf("set runtime public offer: %v", err)
	}
	if runtimeObj.PublicOffer == nil {
		t.Fatal("expected runtime public offer to be saved")
	}

	appSpec := model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
		Resources: &model.ResourceSpec{
			CPUMilliCores:   1000,
			MemoryMebibytes: 1024,
		},
		Workspace: &model.AppWorkspaceSpec{
			StorageSize: "10Gi",
		},
	}
	app, err := s.CreateApp(consumer.ID, project.ID, "public-runtime-app", "", appSpec)
	if err != nil {
		t.Fatalf("create app on public runtime: %v", err)
	}
	deployOp, err := s.CreateOperation(model.Operation{
		AppID:       app.ID,
		DesiredSpec: cloneAppSpec(&appSpec),
		TenantID:    consumer.ID,
		Type:        model.OperationTypeDeploy,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/manifests/public-runtime.yaml", "public runtime deploy finished"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	startConsumerBalance := int64(10_000 * microCentsPerCent)
	startOwnerBalance := int64(0)
	staleAccruedAt := time.Now().UTC().Add(-2 * time.Hour)
	if err := s.withLockedState(true, func(state *model.State) error {
		consumerBilling := ensureTenantBillingRecord(state, consumer.ID, staleAccruedAt)
		consumerBilling.ManagedCap = model.BillingResourceSpec{}
		consumerBilling.BalanceMicroCents = startConsumerBalance
		consumerBilling.LastAccruedAt = staleAccruedAt
		consumerBilling.UpdatedAt = staleAccruedAt
		appendTenantBillingEvent(state, newTenantBillingBalanceAdjustedEvent(
			consumer.ID,
			0,
			startConsumerBalance,
			staleAccruedAt,
			map[string]string{"source": "test-seed"},
		))

		ownerBilling := ensureTenantBillingRecord(state, owner.ID, staleAccruedAt)
		ownerBilling.ManagedCap = model.BillingResourceSpec{}
		ownerBilling.BalanceMicroCents = startOwnerBalance
		ownerBilling.LastAccruedAt = staleAccruedAt
		ownerBilling.UpdatedAt = staleAccruedAt
		appendTenantBillingEvent(state, newTenantBillingBalanceAdjustedEvent(
			owner.ID,
			0,
			startOwnerBalance,
			staleAccruedAt,
			map[string]string{"source": "test-seed"},
		))
		return nil
	}); err != nil {
		t.Fatalf("seed billing timestamps: %v", err)
	}

	consumerSummary, err := s.GetTenantBillingSummary(consumer.ID)
	if err != nil {
		t.Fatalf("get consumer billing summary: %v", err)
	}
	expectedHourlyRate := publicRuntimeOfferHourlyRateMicroCents(*runtimeObj.PublicOffer, model.BillingResourceSpec{
		CPUMilliCores:    1000,
		MemoryMebibytes:  1024,
		StorageGibibytes: 10,
	})
	elapsedNanos := consumerSummary.LastAccruedAt.Sub(staleAccruedAt).Nanoseconds()
	expectedTransfer := expectedHourlyRate * elapsedNanos / int64(time.Hour)
	if consumerSummary.BalanceMicroCents != startConsumerBalance-expectedTransfer {
		t.Fatalf("expected consumer balance %d, got %d", startConsumerBalance-expectedTransfer, consumerSummary.BalanceMicroCents)
	}
	if consumerSummary.HourlyRateMicroCents != expectedHourlyRate {
		t.Fatalf("expected public runtime hourly rate %d, got %d", expectedHourlyRate, consumerSummary.HourlyRateMicroCents)
	}
	if len(consumerSummary.Events) == 0 || consumerSummary.Events[0].Type != model.BillingEventTypePublicRuntimeDebit {
		t.Fatalf("expected latest consumer event %q, got %+v", model.BillingEventTypePublicRuntimeDebit, consumerSummary.Events)
	}

	ownerSummary, err := s.GetTenantBillingSummary(owner.ID)
	if err != nil {
		t.Fatalf("get owner billing summary: %v", err)
	}
	if ownerSummary.BalanceMicroCents != startOwnerBalance+expectedTransfer {
		t.Fatalf("expected owner balance %d, got %d", startOwnerBalance+expectedTransfer, ownerSummary.BalanceMicroCents)
	}
	if len(ownerSummary.Events) == 0 || ownerSummary.Events[0].Type != model.BillingEventTypePublicRuntimeCredit {
		t.Fatalf("expected latest owner event %q, got %+v", model.BillingEventTypePublicRuntimeCredit, ownerSummary.Events)
	}
}

func TestSyncManagedSharedLocationRuntimesMaterializesSelectableTargets(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	if err := s.SyncManagedSharedLocationRuntimes([]map[string]string{
		{runtimepkg.LocationCountryCodeLabelKey: "HK"},
		{runtimepkg.LocationCountryCodeLabelKey: "JP"},
		{runtimepkg.LocationCountryCodeLabelKey: "hk"},
	}); err != nil {
		t.Fatalf("sync managed shared location runtimes: %v", err)
	}

	baseRuntime, err := s.GetRuntime(managedSharedRuntimeID)
	if err != nil {
		t.Fatalf("get base managed shared runtime: %v", err)
	}
	if got := baseRuntime.Labels[runtimepkg.LocationCountryCodeLabelKey]; got != "" {
		t.Fatalf("expected base managed shared runtime to stay unconstrained, got country code %q", got)
	}

	hkSpec := buildManagedSharedLocationRuntimeSpec(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "hk",
	})
	jpSpec := buildManagedSharedLocationRuntimeSpec(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "jp",
	})
	hkRuntime, err := s.GetRuntime(hkSpec.ID)
	if err != nil {
		t.Fatalf("get hong kong managed shared runtime: %v", err)
	}
	if hkRuntime.Type != model.RuntimeTypeManagedShared {
		t.Fatalf("expected hong kong runtime type %q, got %q", model.RuntimeTypeManagedShared, hkRuntime.Type)
	}
	if got := hkRuntime.Labels[runtimepkg.LocationCountryCodeLabelKey]; got != "hk" {
		t.Fatalf("expected hong kong runtime country code %q, got %q", "hk", got)
	}

	jpRuntime, err := s.GetRuntime(jpSpec.ID)
	if err != nil {
		t.Fatalf("get japan managed shared runtime: %v", err)
	}
	if got := jpRuntime.Labels[runtimepkg.LocationCountryCodeLabelKey]; got != "jp" {
		t.Fatalf("expected japan runtime country code %q, got %q", "jp", got)
	}
}

func TestSyncManagedSharedLocationRuntimesDeletesUnusedTargetsAndKeepsReferencedOnesOffline(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	if err := s.SyncManagedSharedLocationRuntimes([]map[string]string{
		{runtimepkg.LocationCountryCodeLabelKey: "HK"},
		{runtimepkg.LocationCountryCodeLabelKey: "JP"},
		{runtimepkg.LocationCountryCodeLabelKey: "US"},
	}); err != nil {
		t.Fatalf("initial sync managed shared location runtimes: %v", err)
	}

	hkSpec := buildManagedSharedLocationRuntimeSpec(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "hk",
	})
	jpSpec := buildManagedSharedLocationRuntimeSpec(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "jp",
	})
	usSpec := buildManagedSharedLocationRuntimeSpec(map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})
	if _, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: jpSpec.ID,
	}); err != nil {
		t.Fatalf("create app on japan shared runtime: %v", err)
	}

	if err := s.SyncManagedSharedLocationRuntimes([]map[string]string{
		{runtimepkg.LocationCountryCodeLabelKey: "HK"},
	}); err != nil {
		t.Fatalf("resync managed shared location runtimes: %v", err)
	}

	if _, err := s.GetRuntime(hkSpec.ID); err != nil {
		t.Fatalf("expected hong kong runtime to remain active: %v", err)
	}
	if _, err := s.GetRuntime(usSpec.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected unreferenced united states runtime to be removed, got %v", err)
	}

	jpRuntime, err := s.GetRuntime(jpSpec.ID)
	if err != nil {
		t.Fatalf("expected referenced japan runtime to remain readable: %v", err)
	}
	if jpRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected referenced japan runtime to become offline, got %q", jpRuntime.Status)
	}
}

func TestBootstrapNodeTransfersOwnershipAcrossTenants(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	ownerA, err := s.CreateTenant("Owner A")
	if err != nil {
		t.Fatalf("create owner A tenant: %v", err)
	}
	ownerB, err := s.CreateTenant("Owner B")
	if err != nil {
		t.Fatalf("create owner B tenant: %v", err)
	}
	viewer, err := s.CreateTenant("Viewer")
	if err != nil {
		t.Fatalf("create viewer tenant: %v", err)
	}
	keyA, secretA, err := s.CreateNodeKey(ownerA.ID, "default")
	if err != nil {
		t.Fatalf("create node key A: %v", err)
	}
	keyB, secretB, err := s.CreateNodeKey(ownerB.ID, "default")
	if err != nil {
		t.Fatalf("create node key B: %v", err)
	}

	_, runtimeA, runtimeKeyA, err := s.BootstrapNode(secretA, "worker", "https://owner-a.example.com", map[string]string{"zone": "a"}, "worker-a", "transfer-fingerprint")
	if err != nil {
		t.Fatalf("bootstrap node for owner A: %v", err)
	}
	if runtimeKeyA == "" {
		t.Fatal("expected runtime key for owner A bootstrap")
	}
	if _, err := s.GrantRuntimeAccess(runtimeA.ID, ownerA.ID, viewer.ID); err != nil {
		t.Fatalf("grant runtime access before transfer: %v", err)
	}

	visible, err := s.RuntimeVisibleToTenant(runtimeA.ID, viewer.ID, false)
	if err != nil {
		t.Fatalf("check viewer visibility before transfer: %v", err)
	}
	if !visible {
		t.Fatal("expected viewer to see granted runtime before transfer")
	}

	_, runtimeB, runtimeKeyB, err := s.BootstrapNode(secretB, "worker", "https://owner-b.example.com", map[string]string{"zone": "b"}, "worker-b", "transfer-fingerprint")
	if err != nil {
		t.Fatalf("bootstrap node for owner B: %v", err)
	}
	if runtimeKeyB == "" {
		t.Fatal("expected runtime key for owner B bootstrap")
	}
	if runtimeB.ID == runtimeA.ID {
		t.Fatalf("expected ownership transfer to create or reuse owner B runtime, got same runtime id %s", runtimeB.ID)
	}
	if runtimeB.TenantID != ownerB.ID {
		t.Fatalf("expected runtime tenant %s, got %s", ownerB.ID, runtimeB.TenantID)
	}
	if runtimeB.NodeKeyID != keyB.ID {
		t.Fatalf("expected runtime node key %s, got %s", keyB.ID, runtimeB.NodeKeyID)
	}
	if runtimeB.AccessMode != model.RuntimeAccessModePrivate {
		t.Fatalf("expected transferred runtime to default to private, got %q", runtimeB.AccessMode)
	}

	oldRuntime, err := s.GetRuntime(runtimeA.ID)
	if err != nil {
		t.Fatalf("get old runtime after transfer: %v", err)
	}
	if oldRuntime.TenantID != ownerA.ID {
		t.Fatalf("expected old runtime to remain attached to owner A history, got tenant %s", oldRuntime.TenantID)
	}
	if oldRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected old runtime to be offline, got %q", oldRuntime.Status)
	}
	if oldRuntime.NodeKeyID != "" {
		t.Fatalf("expected old runtime node key to be cleared, got %q", oldRuntime.NodeKeyID)
	}
	if oldRuntime.FingerprintHash != "" || oldRuntime.FingerprintPrefix != "" {
		t.Fatalf("expected old runtime fingerprint to be cleared, got prefix=%q hash=%q", oldRuntime.FingerprintPrefix, oldRuntime.FingerprintHash)
	}
	if oldRuntime.AgentKeyHash != "" || oldRuntime.AgentKeyPrefix != "" {
		t.Fatalf("expected old runtime agent key to be cleared, got prefix=%q hash=%q", oldRuntime.AgentKeyPrefix, oldRuntime.AgentKeyHash)
	}
	if oldRuntime.AccessMode != model.RuntimeAccessModePrivate {
		t.Fatalf("expected old runtime access mode to reset to private, got %q", oldRuntime.AccessMode)
	}

	visible, err = s.RuntimeVisibleToTenant(runtimeA.ID, viewer.ID, false)
	if err != nil {
		t.Fatalf("check viewer visibility after transfer: %v", err)
	}
	if visible {
		t.Fatal("expected viewer access to old runtime to be revoked by transfer")
	}
	visible, err = s.RuntimeVisibleToTenant(runtimeB.ID, viewer.ID, false)
	if err != nil {
		t.Fatalf("check viewer visibility for new owner runtime: %v", err)
	}
	if visible {
		t.Fatal("expected transferred runtime to be private by default")
	}

	usagesA, err := s.ListRuntimesByNodeKey(keyA.ID, ownerA.ID, false)
	if err != nil {
		t.Fatalf("list node key A usages: %v", err)
	}
	if len(usagesA) != 0 {
		t.Fatalf("expected old node key to have no runtime usages after transfer, got %+v", usagesA)
	}
	usagesB, err := s.ListRuntimesByNodeKey(keyB.ID, ownerB.ID, false)
	if err != nil {
		t.Fatalf("list node key B usages: %v", err)
	}
	if len(usagesB) != 1 || usagesB[0].ID != runtimeB.ID {
		t.Fatalf("expected new node key to own transferred runtime, got %+v", usagesB)
	}
}

func TestBootstrapClusterNodeNormalizesKubernetesNodeName(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Cluster Name Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}

	_, runtimeObj, err := s.BootstrapClusterNode(
		secret,
		"VM-0-17-ubuntu-2",
		"https://cluster.example.com",
		nil,
		"VM-0-17-ubuntu",
		"cluster-name-fingerprint",
	)
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	if runtimeObj.Name != "vm-0-17-ubuntu-2" {
		t.Fatalf("expected normalized runtime name, got %q", runtimeObj.Name)
	}
	if runtimeObj.ClusterNodeName != "vm-0-17-ubuntu-2" {
		t.Fatalf("expected normalized cluster node name, got %q", runtimeObj.ClusterNodeName)
	}
	if runtimeObj.MachineName != "VM-0-17-ubuntu" {
		t.Fatalf("expected original machine name to be preserved, got %q", runtimeObj.MachineName)
	}
}

func TestBootstrapClusterNodeTransfersOwnershipAcrossTenants(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	ownerA, err := s.CreateTenant("Cluster Owner A")
	if err != nil {
		t.Fatalf("create owner A tenant: %v", err)
	}
	ownerB, err := s.CreateTenant("Cluster Owner B")
	if err != nil {
		t.Fatalf("create owner B tenant: %v", err)
	}
	viewer, err := s.CreateTenant("Cluster Viewer")
	if err != nil {
		t.Fatalf("create viewer tenant: %v", err)
	}
	_, secretA, err := s.CreateNodeKey(ownerA.ID, "default")
	if err != nil {
		t.Fatalf("create node key A: %v", err)
	}
	keyB, secretB, err := s.CreateNodeKey(ownerB.ID, "default")
	if err != nil {
		t.Fatalf("create node key B: %v", err)
	}

	_, runtimeA, err := s.BootstrapClusterNode(secretA, "worker", "https://cluster-a.example.com", map[string]string{"zone": "a"}, "worker-a", "cluster-transfer-fingerprint")
	if err != nil {
		t.Fatalf("bootstrap cluster node for owner A: %v", err)
	}
	if _, err := s.GrantRuntimeAccess(runtimeA.ID, ownerA.ID, viewer.ID); err != nil {
		t.Fatalf("grant cluster runtime access before transfer: %v", err)
	}
	if _, err := s.SetRuntimeAccessMode(runtimeA.ID, ownerA.ID, model.RuntimeAccessModePlatformShared); err != nil {
		t.Fatalf("set cluster runtime platform-shared before transfer: %v", err)
	}
	if _, err := s.SetRuntimePoolMode(runtimeA.ID, model.RuntimePoolModeInternalShared); err != nil {
		t.Fatalf("set cluster runtime shared-pool mode before transfer: %v", err)
	}

	visible, err := s.RuntimeVisibleToTenant(runtimeA.ID, viewer.ID, false)
	if err != nil {
		t.Fatalf("check viewer visibility before cluster transfer: %v", err)
	}
	if !visible {
		t.Fatal("expected viewer to see cluster runtime before transfer")
	}

	_, runtimeB, err := s.BootstrapClusterNode(secretB, "worker", "https://cluster-b.example.com", map[string]string{"zone": "b"}, "worker-b", "cluster-transfer-fingerprint")
	if err != nil {
		t.Fatalf("bootstrap cluster node for owner B: %v", err)
	}
	if runtimeB.ID == runtimeA.ID {
		t.Fatalf("expected ownership transfer to allocate owner B runtime, got same runtime id %s", runtimeB.ID)
	}
	if runtimeB.NodeKeyID != keyB.ID {
		t.Fatalf("expected transferred cluster runtime node key %s, got %s", keyB.ID, runtimeB.NodeKeyID)
	}
	if runtimeB.AccessMode != model.RuntimeAccessModePrivate {
		t.Fatalf("expected transferred cluster runtime to reset to private, got %q", runtimeB.AccessMode)
	}
	if runtimeB.PoolMode != model.RuntimePoolModeDedicated {
		t.Fatalf("expected transferred cluster runtime to default to dedicated pool mode, got %q", runtimeB.PoolMode)
	}
	if runtimeB.ClusterNodeName == "" {
		t.Fatal("expected transferred cluster runtime to keep a cluster node name")
	}

	oldRuntime, err := s.GetRuntime(runtimeA.ID)
	if err != nil {
		t.Fatalf("get old cluster runtime after transfer: %v", err)
	}
	if oldRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected old cluster runtime to be offline, got %q", oldRuntime.Status)
	}
	if oldRuntime.NodeKeyID != "" {
		t.Fatalf("expected old cluster runtime node key to be cleared, got %q", oldRuntime.NodeKeyID)
	}
	if oldRuntime.ClusterNodeName != "" {
		t.Fatalf("expected old cluster runtime cluster node name to be cleared, got %q", oldRuntime.ClusterNodeName)
	}
	if oldRuntime.FingerprintHash != "" || oldRuntime.FingerprintPrefix != "" {
		t.Fatalf("expected old cluster runtime fingerprint to be cleared, got prefix=%q hash=%q", oldRuntime.FingerprintPrefix, oldRuntime.FingerprintHash)
	}
	if oldRuntime.AccessMode != model.RuntimeAccessModePrivate {
		t.Fatalf("expected old cluster runtime access mode to reset to private, got %q", oldRuntime.AccessMode)
	}
	if oldRuntime.PoolMode != model.RuntimePoolModeDedicated {
		t.Fatalf("expected old cluster runtime pool mode to reset to dedicated, got %q", oldRuntime.PoolMode)
	}

	visible, err = s.RuntimeVisibleToTenant(runtimeA.ID, viewer.ID, false)
	if err != nil {
		t.Fatalf("check old cluster runtime visibility after transfer: %v", err)
	}
	if visible {
		t.Fatal("expected old cluster runtime visibility to be revoked after transfer")
	}
	visible, err = s.RuntimeVisibleToTenant(runtimeB.ID, viewer.ID, false)
	if err != nil {
		t.Fatalf("check new cluster runtime visibility after transfer: %v", err)
	}
	if visible {
		t.Fatal("expected new cluster runtime to default to private after transfer")
	}

	nodesB, err := s.ListNodes(ownerB.ID, false)
	if err != nil {
		t.Fatalf("list owner B nodes: %v", err)
	}
	found := false
	for _, node := range nodesB {
		if node.ID == runtimeB.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected owner B to see transferred cluster runtime %s", runtimeB.ID)
	}
}

func TestSetRuntimePoolModeOnlyAllowsManagedOwnedRuntimes(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Pool Mode Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, clusterSecret, err := s.CreateNodeKey(tenant.ID, "cluster")
	if err != nil {
		t.Fatalf("create cluster node key: %v", err)
	}
	_, managedRuntime, err := s.BootstrapClusterNode(clusterSecret, "worker", "https://worker.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	updatedRuntime, err := s.SetRuntimePoolMode(managedRuntime.ID, model.RuntimePoolModeInternalShared)
	if err != nil {
		t.Fatalf("set managed runtime pool mode: %v", err)
	}
	if updatedRuntime.PoolMode != model.RuntimePoolModeInternalShared {
		t.Fatalf("expected managed runtime pool mode %q, got %q", model.RuntimePoolModeInternalShared, updatedRuntime.PoolMode)
	}

	_, externalSecret, err := s.CreateNodeKey(tenant.ID, "external")
	if err != nil {
		t.Fatalf("create external node key: %v", err)
	}
	_, externalRuntime, _, err := s.BootstrapNode(externalSecret, "agent", "https://agent.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap external node: %v", err)
	}
	if _, err := s.SetRuntimePoolMode(externalRuntime.ID, model.RuntimePoolModeInternalShared); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for external runtime pool mode, got %v", err)
	}
	if _, err := s.SetRuntimePoolMode("runtime_managed_shared", model.RuntimePoolModeInternalShared); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for built-in shared runtime pool mode, got %v", err)
	}
}

func TestEnsureRuntimeMetadataBackfillsLegacyMachineState(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	tenantID := "tenant_legacy"
	state := model.State{
		Machines: []model.Machine{
			{
				ID:                "machine_old",
				TenantID:          tenantID,
				Name:              "alicehk2",
				ConnectionMode:    model.MachineConnectionModeAgent,
				Status:            model.RuntimeStatusActive,
				Endpoint:          "https://worker.example.com",
				NodeKeyID:         "nk_1",
				RuntimeID:         "runtime_old",
				RuntimeName:       "worker-old",
				FingerprintPrefix: model.SecretPrefix("fingerprint-1"),
				FingerprintHash:   model.HashSecret("fingerprint-1"),
				LastSeenAt:        &now,
				CreatedAt:         now,
				UpdatedAt:         now,
			},
		},
		Runtimes: []model.Runtime{
			{
				ID:        "runtime_old",
				TenantID:  tenantID,
				Name:      "worker-old",
				Type:      model.RuntimeTypeExternalOwned,
				Status:    model.RuntimeStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
	}

	ensureRuntimeMetadata(&state)

	if len(state.Runtimes) != 1 {
		t.Fatalf("expected 1 runtime after metadata backfill, got %d", len(state.Runtimes))
	}
	runtime := state.Runtimes[0]
	if runtime.MachineName != "alicehk2" {
		t.Fatalf("expected machine_name alicehk2, got %q", runtime.MachineName)
	}
	if runtime.ConnectionMode != model.MachineConnectionModeAgent {
		t.Fatalf("expected connection mode agent, got %q", runtime.ConnectionMode)
	}
	if runtime.Endpoint != "https://worker.example.com" {
		t.Fatalf("expected endpoint from legacy machine, got %q", runtime.Endpoint)
	}
	if runtime.NodeKeyID != "nk_1" {
		t.Fatalf("expected node key nk_1, got %q", runtime.NodeKeyID)
	}
	if runtime.FingerprintPrefix == "" || runtime.FingerprintHash == "" {
		t.Fatal("expected fingerprint metadata from legacy machine")
	}
	if runtime.LastSeenAt == nil {
		t.Fatal("expected last_seen_at from legacy machine")
	}
}

func TestDeleteTenantRemovesTenantOwnedResources(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Delete Me")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	otherTenant, err := s.CreateTenant("Keep Me")
	if err != nil {
		t.Fatalf("create other tenant: %v", err)
	}

	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, _, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"project.write"}); err != nil {
		t.Fatalf("create api key: %v", err)
	}
	token, enrollSecret, err := s.CreateEnrollmentToken(tenant.ID, "external", time.Hour)
	if err != nil {
		t.Fatalf("create enrollment token: %v", err)
	}
	if token.ID == "" || enrollSecret == "" {
		t.Fatal("expected enrollment token secret")
	}
	if _, runtimeKey, err := s.ConsumeEnrollmentToken(enrollSecret, "external-1", "https://node2.example.com", map[string]string{"zone": "b"}, "", ""); err != nil {
		t.Fatalf("consume enrollment token: %v", err)
	} else if runtimeKey == "" {
		t.Fatal("expected runtime key")
	}

	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "cluster")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, managedRuntime, err := s.BootstrapClusterNode(nodeSecret, "worker-1", "https://node1.example.com", map[string]string{"zone": "a"}, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	app, err := s.CreateAppWithRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	desiredSpec := app.Spec
	if _, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &desiredSpec,
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if err := s.AppendAuditEvent(model.AuditEvent{
		TenantID:   tenant.ID,
		ActorType:  model.ActorTypeBootstrap,
		ActorID:    "bootstrap",
		Action:     "tenant.test",
		TargetType: "tenant",
		TargetID:   tenant.ID,
	}); err != nil {
		t.Fatalf("append audit event: %v", err)
	}

	deletedTenant, err := s.DeleteTenant(tenant.ID)
	if err != nil {
		t.Fatalf("delete tenant: %v", err)
	}
	if deletedTenant.ID != tenant.ID {
		t.Fatalf("expected deleted tenant id %s, got %s", tenant.ID, deletedTenant.ID)
	}

	if _, err := s.GetTenant(tenant.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted tenant, got %v", err)
	}

	tenants, err := s.ListTenants()
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	if len(tenants) != 1 || tenants[0].ID != otherTenant.ID {
		t.Fatalf("expected only other tenant to remain, got %+v", tenants)
	}

	projects, err := s.ListProjects(tenant.ID)
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	if len(projects) != 0 {
		t.Fatalf("expected no projects for deleted tenant, got %+v", projects)
	}

	keys, err := s.ListAPIKeys(tenant.ID, false)
	if err != nil {
		t.Fatalf("list api keys: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected no api keys for deleted tenant, got %+v", keys)
	}

	tokens, err := s.ListEnrollmentTokens(tenant.ID)
	if err != nil {
		t.Fatalf("list enrollment tokens: %v", err)
	}
	if len(tokens) != 0 {
		t.Fatalf("expected no enrollment tokens for deleted tenant, got %+v", tokens)
	}

	nodeKeys, err := s.ListNodeKeys(tenant.ID, false)
	if err != nil {
		t.Fatalf("list node keys: %v", err)
	}
	if len(nodeKeys) != 0 {
		t.Fatalf("expected no node keys for deleted tenant, got %+v", nodeKeys)
	}

	nodes, err := s.ListNodes(tenant.ID, false)
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected no nodes for deleted tenant, got %+v", nodes)
	}

	apps, err := s.ListApps(tenant.ID, false)
	if err != nil {
		t.Fatalf("list apps: %v", err)
	}
	if len(apps) != 0 {
		t.Fatalf("expected no apps for deleted tenant, got %+v", apps)
	}

	ops, err := s.ListOperations(tenant.ID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no operations for deleted tenant, got %+v", ops)
	}

	events, err := s.ListAuditEvents("", true)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected tenant audit events to be deleted, got %+v", events)
	}

	runtimes, err := s.ListRuntimes("", true)
	if err != nil {
		t.Fatalf("list runtimes: %v", err)
	}
	foundShared := false
	for _, runtime := range runtimes {
		if runtime.ID == "runtime_managed_shared" {
			foundShared = true
		}
		if runtime.ID == managedRuntime.ID {
			t.Fatalf("expected tenant managed runtime %s to be deleted", managedRuntime.ID)
		}
	}
	if !foundShared {
		t.Fatal("expected runtime_managed_shared to remain after deleting tenant")
	}
}

func TestCreateImportedAppRejectsDuplicateHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Imported Apps")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	spec := model.AppSpec{
		Image:     "127.0.0.1:30500/fugue-apps/demo:latest",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	source := model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyStaticSite,
	}
	route := model.AppRoute{
		Hostname:    "demo.app.example.com",
		BaseDomain:  "app.example.com",
		PublicURL:   "https://demo.app.example.com",
		ServicePort: 80,
	}

	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", spec, source, route)
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	if app.Route == nil || app.Route.Hostname != route.Hostname {
		t.Fatalf("expected route hostname %s, got %+v", route.Hostname, app.Route)
	}

	_, err = s.CreateImportedApp(tenant.ID, project.ID, "demo-2", "", spec, source, route)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for duplicate hostname, got %v", err)
	}

	found, err := s.GetAppByHostname(route.Hostname)
	if err != nil {
		t.Fatalf("lookup app by hostname: %v", err)
	}
	if found.ID != app.ID {
		t.Fatalf("expected app id %s, got %s", app.ID, found.ID)
	}
}

func TestUpdateAppRouteReleasesPreviousHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Route Updates")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateAppWithRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "127.0.0.1:30500/fugue-apps/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	updated, err := s.UpdateAppRoute(app.ID, model.AppRoute{
		Hostname:   "fresh.apps.example.com",
		BaseDomain: "apps.example.com",
		PublicURL:  "https://fresh.apps.example.com",
	})
	if err != nil {
		t.Fatalf("update app route: %v", err)
	}
	if updated.Route == nil || updated.Route.Hostname != "fresh.apps.example.com" {
		t.Fatalf("expected updated hostname fresh.apps.example.com, got %+v", updated.Route)
	}
	if updated.Route.ServicePort != 8080 {
		t.Fatalf("expected service port 8080 to be preserved, got %d", updated.Route.ServicePort)
	}

	found, err := s.GetAppByHostname("fresh.apps.example.com")
	if err != nil {
		t.Fatalf("lookup updated hostname: %v", err)
	}
	if found.ID != app.ID {
		t.Fatalf("expected updated hostname to resolve to app %s, got %s", app.ID, found.ID)
	}
	if _, err := s.GetAppByHostname("demo.apps.example.com"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected old hostname to be released, got %v", err)
	}
}

func TestUpdateAppRouteRejectsDuplicateHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Route Conflicts")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	spec := model.AppSpec{
		Image:     "127.0.0.1:30500/fugue-apps/demo:latest",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	app, err := s.CreateAppWithRoute(tenant.ID, project.ID, "demo", "", spec, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, err = s.CreateAppWithRoute(tenant.ID, project.ID, "taken", "", spec, model.AppRoute{
		Hostname:    "taken.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://taken.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create taken app: %v", err)
	}

	_, err = s.UpdateAppRoute(app.ID, model.AppRoute{
		Hostname:   "taken.apps.example.com",
		BaseDomain: "apps.example.com",
		PublicURL:  "https://taken.apps.example.com",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for duplicate hostname update, got %v", err)
	}
}

func TestCreateImportedAppAllowsPendingImportPlaceholder(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Pending Import")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyAuto,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create placeholder imported app: %v", err)
	}
	if app.Status.Phase != "importing" {
		t.Fatalf("expected importing phase, got %q", app.Status.Phase)
	}
	if app.Spec.Image != "" {
		t.Fatalf("expected empty image placeholder, got %q", app.Spec.Image)
	}
}

func TestDeployOperationUpdatesImportedAppSource(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Rebuild")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "127.0.0.1:30500/fugue-apps/demo:git-old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		SourceDir:     "",
		BuildStrategy: model.AppBuildStrategyStaticSite,
		CommitSHA:     "oldcommit",
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	spec.Image = "127.0.0.1:30500/fugue-apps/demo:git-new"
	source := model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		SourceDir:     "dist",
		BuildStrategy: model.AppBuildStrategyStaticSite,
		CommitSHA:     "newcommit",
	}

	op, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &spec,
		DesiredSource: &source,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}

	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim operation: %v", err)
	} else if !found {
		t.Fatal("expected pending operation")
	}

	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/demo.yaml", "rebuilt"); err != nil {
		t.Fatalf("complete managed operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Spec.Image != spec.Image {
		t.Fatalf("expected image %s, got %s", spec.Image, app.Spec.Image)
	}
	if app.Source == nil {
		t.Fatal("expected source to be preserved")
	}
	if app.Source.CommitSHA != source.CommitSHA {
		t.Fatalf("expected commit %s, got %s", source.CommitSHA, app.Source.CommitSHA)
	}
	if app.Source.SourceDir != source.SourceDir {
		t.Fatalf("expected source dir %s, got %s", source.SourceDir, app.Source.SourceDir)
	}
}

func TestImportOperationClaimsAsManagedEvenForExternalRuntime(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("External Import")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	token, secret, err := s.CreateEnrollmentToken(tenant.ID, "worker", time.Hour)
	if err != nil {
		t.Fatalf("create enrollment token: %v", err)
	}
	if token.ID == "" {
		t.Fatal("expected enrollment token id")
	}
	externalRuntime, _, err := s.ConsumeEnrollmentToken(secret, "tenant-vps-1", "https://vps.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("consume enrollment token: %v", err)
	}

	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "",
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: externalRuntime.ID,
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 3000,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	source := *app.Source
	op, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeImport,
		AppID:         app.ID,
		DesiredSpec:   &spec,
		DesiredSource: &source,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim import operation: %v", err)
	}
	if !found {
		t.Fatal("expected claimed import operation")
	}
	if claimed.ID != op.ID {
		t.Fatalf("expected claimed id %s, got %s", op.ID, claimed.ID)
	}
	if claimed.ExecutionMode != model.ExecutionModeManaged || claimed.Status != model.OperationStatusRunning {
		t.Fatalf("expected managed running import, got mode=%s status=%s", claimed.ExecutionMode, claimed.Status)
	}
	if claimed.AssignedRuntimeID != "" {
		t.Fatalf("expected import to stay unassigned, got %q", claimed.AssignedRuntimeID)
	}
}

func TestDeployOperationUpdatesRouteServicePort(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Route Port")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.example.com/demo:old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	spec.Image = "registry.example.com/demo:new"
	spec.Ports = []int{3000}
	op, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &spec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected deploy operation")
	}
	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/demo.yaml", "deployed"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Route == nil {
		t.Fatal("expected route to remain present")
	}
	if app.Route.ServicePort != 3000 {
		t.Fatalf("expected route service port 3000, got %d", app.Route.ServicePort)
	}
}

func TestDeployOperationClearsRouteForBackgroundApps(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Background Route")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "workers", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.example.com/demo:old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	spec.Image = "registry.example.com/demo:new"
	spec.NetworkMode = model.AppNetworkModeBackground
	spec.Ports = nil
	op, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &spec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected deploy operation")
	}
	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/demo.yaml", "deployed"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Route != nil {
		t.Fatalf("expected background deploy to clear route, got %+v", app.Route)
	}
}

func TestFailedOperationMarksAppFailed(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Failed Operation")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyAuto,
	}, model.AppRoute{
		Hostname:   "demo.example.com",
		BaseDomain: "example.com",
		PublicURL:  "https://demo.example.com",
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	source := *app.Source
	op, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeImport,
		AppID:         app.ID,
		DesiredSpec:   &spec,
		DesiredSource: &source,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim operation: %v", err)
	} else if !found {
		t.Fatal("expected operation to be claimed")
	}

	if _, err := s.FailOperation(op.ID, "git clone failed"); err != nil {
		t.Fatalf("fail operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Status.Phase != "failed" {
		t.Fatalf("expected app phase failed, got %q", app.Status.Phase)
	}
	if app.Status.LastOperationID != op.ID {
		t.Fatalf("expected last operation %s, got %s", op.ID, app.Status.LastOperationID)
	}
	if app.Status.LastMessage != "git clone failed" {
		t.Fatalf("expected last message to be propagated, got %q", app.Status.LastMessage)
	}
}

func TestFailedRebuildKeepsDeployedPhaseWhenLiveVersionExists(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Failed Rebuild")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.example.com/demo:old",
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 3000,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	deploySpec := app.Spec
	deploySource := *app.Source
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &deploySpec,
		DesiredSource: &deploySource,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected deploy operation")
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "deployed"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	rebuildSpec := app.Spec
	rebuildSource := *app.Source
	rebuildOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeImport,
		AppID:         app.ID,
		DesiredSpec:   &rebuildSpec,
		DesiredSource: &rebuildSource,
	})
	if err != nil {
		t.Fatalf("create rebuild import operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim rebuild import operation: %v", err)
	} else if !found {
		t.Fatal("expected rebuild import operation")
	}
	if _, err := s.FailOperation(rebuildOp.ID, "kaniko failed"); err != nil {
		t.Fatalf("fail rebuild import operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Status.Phase != "deployed" {
		t.Fatalf("expected deployed phase to be preserved, got %q", app.Status.Phase)
	}
	if app.Status.LastOperationID != rebuildOp.ID {
		t.Fatalf("expected last operation %s, got %s", rebuildOp.ID, app.Status.LastOperationID)
	}
	if app.Status.LastMessage != "kaniko failed" {
		t.Fatalf("expected last message to contain rebuild failure, got %q", app.Status.LastMessage)
	}
	if app.Status.CurrentRuntimeID != "runtime_managed_shared" {
		t.Fatalf("expected current runtime to stay managed-shared, got %q", app.Status.CurrentRuntimeID)
	}
	if app.Status.CurrentReplicas != 1 {
		t.Fatalf("expected current replicas to stay 1, got %d", app.Status.CurrentReplicas)
	}
}

func TestInitRepairsFailedPhaseForLiveApp(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Repair Failed Phase")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	deploySpec := app.Spec
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected deploy operation")
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "deployed"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	if err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, app.ID)
		if index < 0 {
			t.Fatalf("app %s not found in state", app.ID)
		}
		state.Apps[index].Status.Phase = "failed"
		state.Apps[index].Status.LastMessage = "stale failure"
		return nil
	}); err != nil {
		t.Fatalf("corrupt app status: %v", err)
	}

	if err := s.Init(); err != nil {
		t.Fatalf("re-init store: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after repair: %v", err)
	}
	if app.Status.Phase != "deployed" {
		t.Fatalf("expected failed phase to be repaired to deployed, got %q", app.Status.Phase)
	}
	if app.Status.LastMessage != "stale failure" {
		t.Fatalf("expected last message to stay unchanged, got %q", app.Status.LastMessage)
	}
}

func TestCreateOperationImmediatelyRefreshesFailedAppStatus(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Immediate Refresh")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.example.com/demo:old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyStaticSite,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	importSpec := app.Spec
	importSource := *app.Source
	failedOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeImport,
		AppID:         app.ID,
		DesiredSpec:   &importSpec,
		DesiredSource: &importSource,
	})
	if err != nil {
		t.Fatalf("create failed import operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim failed import operation: %v", err)
	} else if !found {
		t.Fatal("expected failed import operation")
	}
	if _, err := s.FailOperation(failedOp.ID, "old build failed"); err != nil {
		t.Fatalf("fail old operation: %v", err)
	}

	deploySpec := app.Spec
	deploySpec.Image = "registry.example.com/demo:new"
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Status.Phase != "deploying" {
		t.Fatalf("expected phase deploying after create, got %q", app.Status.Phase)
	}
	if app.Status.LastOperationID != deployOp.ID {
		t.Fatalf("expected last operation %s, got %s", deployOp.ID, app.Status.LastOperationID)
	}
	if app.Status.LastMessage != "deploy queued" {
		t.Fatalf("expected deploy queued message, got %q", app.Status.LastMessage)
	}
}

func TestClaimAndRequeueManagedOperationRefreshAppStatus(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Claim Refresh")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:   500,
		MemoryMebibytes: 1024,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}

	replicas := 2
	op, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &replicas,
	})
	if err != nil {
		t.Fatalf("create scale operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after create: %v", err)
	}
	if app.Status.Phase != "scaling" || app.Status.LastMessage != "scale queued" {
		t.Fatalf("expected scaling/scale queued after create, got phase=%q message=%q", app.Status.Phase, app.Status.LastMessage)
	}

	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim scale operation: %v", err)
	}
	if !found {
		t.Fatal("expected claimed scale operation")
	}
	if claimed.ID != op.ID {
		t.Fatalf("expected claimed id %s, got %s", op.ID, claimed.ID)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after claim: %v", err)
	}
	if app.Status.Phase != "scaling" || app.Status.LastMessage != "scale in progress" {
		t.Fatalf("expected scaling/scale in progress after claim, got phase=%q message=%q", app.Status.Phase, app.Status.LastMessage)
	}

	requeued, err := s.RequeueManagedOperation(op.ID, "operation requeued after controller restart")
	if err != nil {
		t.Fatalf("requeue managed operation: %v", err)
	}
	if requeued.Status != model.OperationStatusPending {
		t.Fatalf("expected requeued status pending, got %q", requeued.Status)
	}
	if requeued.StartedAt != nil || requeued.CompletedAt != nil {
		t.Fatalf("expected cleared timestamps after requeue, got started=%v completed=%v", requeued.StartedAt, requeued.CompletedAt)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after requeue: %v", err)
	}
	if app.Status.Phase != "scaling" {
		t.Fatalf("expected phase scaling after requeue, got %q", app.Status.Phase)
	}
	if app.Status.LastOperationID != op.ID {
		t.Fatalf("expected last operation %s after requeue, got %s", op.ID, app.Status.LastOperationID)
	}
	if app.Status.LastMessage != "operation requeued after controller restart" {
		t.Fatalf("expected requeue message, got %q", app.Status.LastMessage)
	}
}

func TestScaleOperationAllowsZeroAndDisablesApp(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Disable App")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	replicas := 0
	op, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &replicas,
	})
	if err != nil {
		t.Fatalf("create scale operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim operation: %v", err)
	} else if !found {
		t.Fatal("expected pending operation")
	}
	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/demo-disabled.yaml", "disabled"); err != nil {
		t.Fatalf("complete operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Spec.Replicas != 0 || app.Status.CurrentReplicas != 0 {
		t.Fatalf("expected replicas=0 after disable, got spec=%d status=%d", app.Spec.Replicas, app.Status.CurrentReplicas)
	}
	if app.Status.Phase != "disabled" {
		t.Fatalf("expected phase disabled, got %s", app.Status.Phase)
	}
}

func TestDeleteOperationTombstonesAppAndFreesNameAndHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Delete App")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	spec := model.AppSpec{
		Image:     "registry.example.com/demo:latest",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	source := model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyStaticSite,
	}
	route := model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	}

	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", spec, source, route)
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	op, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeDelete,
		AppID:    app.ID,
	})
	if err != nil {
		t.Fatalf("create delete operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim delete operation: %v", err)
	} else if !found {
		t.Fatal("expected pending delete operation")
	}
	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/demo-delete.yaml", "deleted"); err != nil {
		t.Fatalf("complete delete operation: %v", err)
	}

	if _, err := s.GetApp(app.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted app to be hidden, got %v", err)
	}
	if _, err := s.GetAppByHostname(route.Hostname); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted app hostname to be released, got %v", err)
	}

	apps, err := s.ListApps(tenant.ID, false)
	if err != nil {
		t.Fatalf("list apps: %v", err)
	}
	if len(apps) != 0 {
		t.Fatalf("expected no visible apps after delete, got %+v", apps)
	}

	recreated, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", spec, source, route)
	if err != nil {
		t.Fatalf("recreate imported app after delete: %v", err)
	}
	if recreated.ID == app.ID {
		t.Fatalf("expected recreated app to have a new id, got %s", recreated.ID)
	}
}

func TestIdempotencyRecordLifecycle(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Idempotency")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	record, fresh, err := s.ReserveIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenant.ID, "key-1", "hash-a")
	if err != nil {
		t.Fatalf("reserve idempotency record: %v", err)
	}
	if !fresh || record.Status != model.IdempotencyStatusPending {
		t.Fatalf("expected fresh pending reservation, got fresh=%v record=%+v", fresh, record)
	}

	record, fresh, err = s.ReserveIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenant.ID, "key-1", "hash-a")
	if err != nil {
		t.Fatalf("reserve same idempotency record: %v", err)
	}
	if fresh {
		t.Fatalf("expected existing reservation, got fresh=%v", fresh)
	}

	if _, _, err := s.ReserveIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenant.ID, "key-1", "hash-b"); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("expected ErrIdempotencyMismatch, got %v", err)
	}

	record, err = s.CompleteIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenant.ID, "key-1", "app_demo", "op_demo")
	if err != nil {
		t.Fatalf("complete idempotency record: %v", err)
	}
	if record.Status != model.IdempotencyStatusCompleted || record.AppID != "app_demo" || record.OperationID != "op_demo" {
		t.Fatalf("unexpected completed idempotency record: %+v", record)
	}

	record, fresh, err = s.ReserveIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenant.ID, "key-1", "hash-a")
	if err != nil {
		t.Fatalf("reserve completed idempotency record: %v", err)
	}
	if fresh || record.Status != model.IdempotencyStatusCompleted {
		t.Fatalf("expected completed record replay, got fresh=%v record=%+v", fresh, record)
	}

	if _, fresh, err := s.ReserveIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenant.ID, "key-2", "hash-z"); err != nil {
		t.Fatalf("reserve second key: %v", err)
	} else if !fresh {
		t.Fatal("expected fresh second key")
	}
	if err := s.ReleaseIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenant.ID, "key-2"); err != nil {
		t.Fatalf("release idempotency record: %v", err)
	}
	if _, fresh, err := s.ReserveIdempotencyRecord(model.IdempotencyScopeAppImportGitHub, tenant.ID, "key-2", "hash-z"); err != nil {
		t.Fatalf("reserve released idempotency record: %v", err)
	} else if !fresh {
		t.Fatal("expected released key to be reservable again")
	}
}

func TestDeleteProjectConflictsUntilAppDeleted(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Project Delete Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appResources := model.DefaultManagedAppResources()
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		Resources: &appResources,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	if _, err := s.DeleteProject(project.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict while app exists, got %v", err)
	}

	deleteOp, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeDelete,
		AppID:    app.ID,
	})
	if err != nil {
		t.Fatalf("create delete operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim delete operation: %v", err)
	} else if !found {
		t.Fatal("expected pending delete operation")
	}
	if _, err := s.CompleteManagedOperation(deleteOp.ID, "/tmp/demo-delete.yaml", "deleted"); err != nil {
		t.Fatalf("complete delete operation: %v", err)
	}

	deletedProject, err := s.DeleteProject(project.ID)
	if err != nil {
		t.Fatalf("delete project: %v", err)
	}
	if deletedProject.ID != project.ID {
		t.Fatalf("expected deleted project id %s, got %s", project.ID, deletedProject.ID)
	}

	projects, err := s.ListProjects(tenant.ID)
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	if len(projects) != 0 {
		t.Fatalf("expected project to be removed, got %+v", projects)
	}
}

func TestPurgeAppRemovesImportedPlaceholderResources(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Purge Import Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "imports", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    750,
		MemoryMebibytes:  1536,
		StorageGibibytes: 1,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		BuildStrategy: model.AppBuildStrategyBuildpacks,
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	specCopy := cloneAppSpec(&app.Spec)
	sourceCopy := cloneAppSource(app.Source)
	if _, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeImport,
		AppID:         app.ID,
		DesiredSpec:   specCopy,
		DesiredSource: sourceCopy,
	}); err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	purgedApp, err := s.PurgeApp(app.ID)
	if err != nil {
		t.Fatalf("purge app: %v", err)
	}
	if purgedApp.ID != app.ID {
		t.Fatalf("expected purged app id %s, got %s", app.ID, purgedApp.ID)
	}

	if _, err := s.GetApp(app.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected app to be removed, got %v", err)
	}
	services, err := s.ListBackingServices(tenant.ID, false)
	if err != nil {
		t.Fatalf("list backing services: %v", err)
	}
	if len(services) != 0 {
		t.Fatalf("expected owned backing services to be removed, got %+v", services)
	}
	ops, err := s.ListOperations(tenant.ID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected operations to be removed with purged app, got %+v", ops)
	}
}

func TestManagedPostgresBindingIsExclusivePerService(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Binding Exclusivity Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appA, err := s.CreateApp(tenant.ID, project.ID, "demo-a", "", model.AppSpec{
		Image:     "ghcr.io/example/demo-a:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app a: %v", err)
	}
	appB, err := s.CreateApp(tenant.ID, project.ID, "demo-b", "", model.AppSpec{
		Image:     "ghcr.io/example/demo-b:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app b: %v", err)
	}
	service, err := s.CreateBackingService(tenant.ID, project.ID, "shared-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backing service: %v", err)
	}

	bindingA, err := s.BindBackingService(tenant.ID, appA.ID, service.ID, "", nil)
	if err != nil {
		t.Fatalf("bind service to app a: %v", err)
	}
	if _, err := s.BindBackingService(tenant.ID, appB.ID, service.ID, "", nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for second managed postgres binding, got %v", err)
	}

	if _, err := s.UnbindBackingService(bindingA.ID); err != nil {
		t.Fatalf("unbind service from app a: %v", err)
	}
	bindingB, err := s.BindBackingService(tenant.ID, appB.ID, service.ID, "", nil)
	if err != nil {
		t.Fatalf("bind service to app b after unbind: %v", err)
	}
	if bindingB.AppID != appB.ID {
		t.Fatalf("expected binding to app b, got %s", bindingB.AppID)
	}
}

func TestCreateAppAllowsPersistentWorkspaceOnManagedSharedRuntime(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Workspace Validation Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Workspace: &model.AppWorkspaceSpec{},
	})
	if err != nil {
		t.Fatalf("expected managed-shared workspace app to be valid, got %v", err)
	}
	if app.Spec.RuntimeID != "runtime_managed_shared" {
		t.Fatalf("expected runtime_managed_shared, got %q", app.Spec.RuntimeID)
	}
	if app.Spec.Workspace == nil {
		t.Fatal("expected workspace to be preserved")
	}
}

func TestCreateAppAllowsPersistentStorageOnManagedSharedRuntime(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Persistent Storage Validation Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mounts: []model.AppPersistentStorageMount{
				{
					Kind:        model.AppPersistentStorageMountKindFile,
					Path:        "/home/api.yaml",
					SeedContent: "providers: []\n",
				},
				{
					Kind: model.AppPersistentStorageMountKindDirectory,
					Path: "/home/data",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("expected managed-shared persistent storage app to be valid, got %v", err)
	}
	if app.Spec.PersistentStorage == nil || len(app.Spec.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected persistent storage to be preserved, got %+v", app.Spec.PersistentStorage)
	}
}

func TestBillingRejectsManagedScaleBeyondConfiguredEnvelope(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Billing Cap Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appResources := model.DefaultManagedAppResources()
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		Resources: &appResources,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpecFromResourceSpec(model.DefaultManagedAppResources())); err != nil {
		t.Fatalf("update billing: %v", err)
	}
	if _, err := s.TopUpTenantBilling(tenant.ID, 500, "seed"); err != nil {
		t.Fatalf("top up billing: %v", err)
	}

	deploySpec := app.Spec
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "done"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	replicas := 2
	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &replicas,
	}); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatalf("expected ErrBillingCapExceeded, got %v", err)
	}

	expectedEnvelope := model.BillingResourceSpecFromResourceSpec(model.DefaultManagedAppResources())
	if err := s.withLockedState(false, func(state *model.State) error {
		record := ensureTenantBillingRecord(state, tenant.ID, time.Now().UTC())
		if record.ManagedCap != expectedEnvelope {
			t.Fatalf("expected cap to stay at %+v, got %+v", expectedEnvelope, record.ManagedCap)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect billing state: %v", err)
	}
}

func TestCreateAppLeavesResourcesUnsetWhenNotSpecified(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Unbounded App Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if app.Spec.Resources != nil {
		t.Fatalf("expected app resources to remain unset, got %+v", *app.Spec.Resources)
	}
}

func TestNewTenantsStartWithSeededFreeTierBilling(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Free Tier Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	expectedCap := model.DefaultTenantFreeManagedCap()
	expectedBalance := billingMonthlyEstimateMicroCents(model.TenantBilling{
		ManagedCap: expectedCap,
		PriceBook:  model.DefaultBillingPriceBook(),
	})

	staleAccrual := time.Now().UTC().Add(-72 * time.Hour)
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findTenantBillingRecord(state, tenant.ID)
		if index < 0 {
			t.Fatalf("billing record for tenant %s not found", tenant.ID)
		}
		state.TenantBilling[index].LastAccruedAt = staleAccrual
		state.TenantBilling[index].UpdatedAt = staleAccrual
		return nil
	}); err != nil {
		t.Fatalf("seed idle billing state: %v", err)
	}

	if err := s.withLockedState(false, func(state *model.State) error {
		index := findTenantBillingRecord(state, tenant.ID)
		if index < 0 {
			t.Fatalf("billing record for tenant %s not found", tenant.ID)
		}
		record := state.TenantBilling[index]
		if record.ManagedCap != expectedCap {
			t.Fatalf("expected free-tier cap %+v, got %+v", expectedCap, record.ManagedCap)
		}
		if record.BalanceMicroCents != expectedBalance {
			t.Fatalf("expected seeded balance %d, got %d", expectedBalance, record.BalanceMicroCents)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect billing state: %v", err)
	}

	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("get billing summary: %v", err)
	}
	if summary.ManagedCap != expectedCap {
		t.Fatalf("expected summary cap %+v, got %+v", expectedCap, summary.ManagedCap)
	}
	if summary.Status != model.BillingStatusInactive {
		t.Fatalf("expected inactive billing status for an empty tenant, got %s", summary.Status)
	}
	if summary.HourlyRateMicroCents != 0 {
		t.Fatalf("expected zero hourly rate for an empty tenant, got %d", summary.HourlyRateMicroCents)
	}
	if summary.MonthlyEstimateMicroCents != 0 {
		t.Fatalf("expected zero monthly estimate for an empty tenant, got %d", summary.MonthlyEstimateMicroCents)
	}
	if summary.BalanceMicroCents != expectedBalance {
		t.Fatalf("expected seeded summary balance %d without idle drain, got %d", expectedBalance, summary.BalanceMicroCents)
	}
	if summary.DefaultAppResources != (model.BillingResourceSpec{}) {
		t.Fatalf("expected app default resources to remain unset, got %+v", summary.DefaultAppResources)
	}
}

func TestLegacyZeroBillingRecordBackfillsSeededFreeTier(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Legacy Billing Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	staleAccrual := time.Now().UTC().Add(-72 * time.Hour)
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findTenantBillingRecord(state, tenant.ID)
		if index < 0 {
			t.Fatalf("billing record for tenant %s not found", tenant.ID)
		}
		state.TenantBilling[index].ManagedCap = model.BillingResourceSpec{}
		state.TenantBilling[index].BalanceMicroCents = 0
		state.TenantBilling[index].LastAccruedAt = staleAccrual
		state.TenantBilling[index].UpdatedAt = staleAccrual
		return nil
	}); err != nil {
		t.Fatalf("seed legacy billing state: %v", err)
	}

	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("get billing summary: %v", err)
	}

	expectedCap := model.DefaultTenantFreeManagedCap()
	expectedBalance := billingMonthlyEstimateMicroCents(model.TenantBilling{
		ManagedCap: expectedCap,
		PriceBook:  model.DefaultBillingPriceBook(),
	})

	if summary.ManagedCap != expectedCap {
		t.Fatalf("expected backfilled cap %+v, got %+v", expectedCap, summary.ManagedCap)
	}
	if summary.Status != model.BillingStatusInactive {
		t.Fatalf("expected backfilled empty tenant to remain inactive, got %s", summary.Status)
	}
	if summary.HourlyRateMicroCents != 0 {
		t.Fatalf("expected backfilled empty tenant hourly rate 0, got %d", summary.HourlyRateMicroCents)
	}
	if summary.MonthlyEstimateMicroCents != 0 {
		t.Fatalf("expected backfilled empty tenant monthly estimate 0, got %d", summary.MonthlyEstimateMicroCents)
	}
	if summary.BalanceMicroCents != expectedBalance {
		t.Fatalf("expected backfilled balance %d without stale drain, got %d", expectedBalance, summary.BalanceMicroCents)
	}
}

func TestLegacyBillingPriceBookRecalibratesToDefault(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Legacy Pricing Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	managedCap := model.BillingResourceSpec{
		CPUMilliCores:    2000,
		MemoryMebibytes:  4096,
		StorageGibibytes: 30,
	}
	stalePriceBook := model.BillingPriceBook{
		Currency:                      model.DefaultBillingCurrency,
		HoursPerMonth:                 model.DefaultBillingHoursPerMonth,
		CPUMicroCentsPerMilliCoreHour: 3000,
		MemoryMicroCentsPerMiBHour:    900,
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findTenantBillingRecord(state, tenant.ID)
		if index < 0 {
			t.Fatalf("billing record for tenant %s not found", tenant.ID)
		}
		state.TenantBilling[index].ManagedCap = managedCap
		state.TenantBilling[index].PriceBook = stalePriceBook
		state.TenantBilling[index].LastAccruedAt = time.Now().UTC()
		state.TenantBilling[index].UpdatedAt = time.Now().UTC()
		return nil
	}); err != nil {
		t.Fatalf("seed legacy price book: %v", err)
	}

	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("get billing summary: %v", err)
	}

	expectedPriceBook := model.DefaultBillingPriceBook()

	if summary.PriceBook != expectedPriceBook {
		t.Fatalf("expected recalibrated price book %+v, got %+v", expectedPriceBook, summary.PriceBook)
	}
	if summary.Status != model.BillingStatusInactive {
		t.Fatalf("expected recalibrated empty tenant to remain inactive, got %s", summary.Status)
	}
	if summary.HourlyRateMicroCents != 0 {
		t.Fatalf("expected hourly rate 0 without active billable resources, got %d", summary.HourlyRateMicroCents)
	}
	if summary.MonthlyEstimateMicroCents != 0 {
		t.Fatalf("expected monthly estimate 0 without active billable resources, got %d", summary.MonthlyEstimateMicroCents)
	}

	if err := s.withLockedState(false, func(state *model.State) error {
		index := findTenantBillingRecord(state, tenant.ID)
		if index < 0 {
			t.Fatalf("billing record for tenant %s not found", tenant.ID)
		}
		if state.TenantBilling[index].PriceBook != expectedPriceBook {
			t.Fatalf("expected persisted price book %+v, got %+v", expectedPriceBook, state.TenantBilling[index].PriceBook)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect recalibrated billing state: %v", err)
	}
}

func TestBillingPriceBookCalibratesTwoCPUFourGiThirtyGiNearFourDollars(t *testing.T) {
	t.Parallel()

	monthlyEstimate := billingMonthlyEstimateMicroCents(model.TenantBilling{
		ManagedCap: model.BillingResourceSpec{
			CPUMilliCores:    2000,
			MemoryMebibytes:  4096,
			StorageGibibytes: 30,
		},
		PriceBook: model.DefaultBillingPriceBook(),
	})

	if monthlyEstimate < 399_000_000 || monthlyEstimate > 401_000_000 {
		t.Fatalf("expected 2 cpu / 4 GiB / 30 GiB monthly estimate near $4.00, got %d microcents", monthlyEstimate)
	}
}

func TestAppEffectiveResourcesIncludeWorkspaceStorage(t *testing.T) {
	t.Parallel()

	compute := model.DefaultManagedAppResources()
	got := appEffectiveResources(model.AppSpec{
		Resources: &compute,
		Workspace: &model.AppWorkspaceSpec{},
	})

	want := model.BillingResourceSpec{
		CPUMilliCores:    compute.CPUMilliCores,
		MemoryMebibytes:  compute.MemoryMebibytes,
		StorageGibibytes: 10,
	}
	if got != want {
		t.Fatalf("expected workspace billing resources %+v, got %+v", want, got)
	}
}

func TestAppEffectiveResourcesIncludePersistentStorage(t *testing.T) {
	t.Parallel()

	compute := model.DefaultManagedAppResources()
	got := appEffectiveResources(model.AppSpec{
		Resources: &compute,
		PersistentStorage: &model.AppPersistentStorageSpec{
			StorageSize: "12Gi",
			Mounts: []model.AppPersistentStorageMount{
				{
					Kind: model.AppPersistentStorageMountKindDirectory,
					Path: "/home/data",
				},
			},
		},
	})

	want := model.BillingResourceSpec{
		CPUMilliCores:    compute.CPUMilliCores,
		MemoryMebibytes:  compute.MemoryMebibytes,
		StorageGibibytes: 12,
	}
	if got != want {
		t.Fatalf("expected persistent storage billing resources %+v, got %+v", want, got)
	}
}

func TestSyncTenantBillingImageStorageContributesToCommittedStorageAndEstimate(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Image Billing Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	summary, err := s.SyncTenantBillingImageStorage(tenant.ID, 5)
	if err != nil {
		t.Fatalf("sync image storage: %v", err)
	}

	if got := summary.ManagedCommitted.StorageGibibytes; got != 5 {
		t.Fatalf("expected committed image storage 5 GiB, got %d", got)
	}
	if got := summary.HourlyRateMicroCents; got != billingHourlyRateMicroCentsWithCommittedStorage(model.TenantBilling{
		ManagedCap: model.DefaultTenantFreeManagedCap(),
		PriceBook:  model.DefaultBillingPriceBook(),
	}, 5) {
		t.Fatalf("expected hourly rate to include image storage, got %d", got)
	}
	if got := summary.MonthlyEstimateMicroCents; got != billingMonthlyEstimateMicroCentsWithCommittedStorage(model.TenantBilling{
		ManagedCap: model.DefaultTenantFreeManagedCap(),
		PriceBook:  model.DefaultBillingPriceBook(),
	}, 5) {
		t.Fatalf("expected monthly estimate to include image storage, got %d", got)
	}
	if summary.OverCap {
		t.Fatal("expected image storage matching the default saved envelope not to mark billing over-cap")
	}
}

func TestAnyActiveManagedResourceTurnsOnFullSavedEnvelopeBilling(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Image-Only Billing Gate Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := s.SyncTenantBillingImageStorage(tenant.ID, 1); err != nil {
		t.Fatalf("seed image storage: %v", err)
	}

	startBalance := int64(9_000_000_000)
	staleAccruedAt := time.Now().UTC().Add(-2 * time.Hour)
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findTenantBillingRecord(state, tenant.ID)
		if index < 0 {
			t.Fatalf("billing record for tenant %s not found", tenant.ID)
		}
		state.TenantBilling[index].BalanceMicroCents = startBalance
		state.TenantBilling[index].LastAccruedAt = staleAccruedAt
		state.TenantBilling[index].UpdatedAt = staleAccruedAt
		return nil
	}); err != nil {
		t.Fatalf("seed stale billing gate state: %v", err)
	}

	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("get billing summary: %v", err)
	}

	expectedHourly := billingHourlyRateMicroCents(model.TenantBilling{
		ManagedCap: model.DefaultTenantFreeManagedCap(),
		PriceBook:  model.DefaultBillingPriceBook(),
	})
	elapsedNanos := summary.LastAccruedAt.Sub(staleAccruedAt).Nanoseconds()
	expectedBalance := startBalance - expectedHourly*elapsedNanos/int64(time.Hour)

	if summary.Status != model.BillingStatusActive {
		t.Fatalf("expected active billing status once image storage exists, got %s", summary.Status)
	}
	if got := summary.ManagedCommitted.StorageGibibytes; got != 1 {
		t.Fatalf("expected committed image storage 1 GiB, got %d", got)
	}
	if summary.HourlyRateMicroCents != expectedHourly {
		t.Fatalf("expected full saved-envelope hourly rate %d, got %d", expectedHourly, summary.HourlyRateMicroCents)
	}
	if summary.BalanceMicroCents != expectedBalance {
		t.Fatalf("expected balance %d after gating on the full envelope, got %d", expectedBalance, summary.BalanceMicroCents)
	}
}

func TestSyncTenantBillingImageStorageAccruesUsingPreviousSnapshot(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Image Billing Accrual Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := s.SyncTenantBillingImageStorage(tenant.ID, 2); err != nil {
		t.Fatalf("seed image storage: %v", err)
	}

	startBalance := int64(9_000_000_000)
	staleAccruedAt := time.Now().UTC().Add(-2 * time.Hour)
	var seeded model.TenantBilling
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findTenantBillingRecord(state, tenant.ID)
		if index < 0 {
			t.Fatalf("billing record for tenant %s not found", tenant.ID)
		}
		state.TenantBilling[index].BalanceMicroCents = startBalance
		state.TenantBilling[index].LastAccruedAt = staleAccruedAt
		state.TenantBilling[index].UpdatedAt = staleAccruedAt
		seeded = state.TenantBilling[index]
		return nil
	}); err != nil {
		t.Fatalf("seed stale billing snapshot: %v", err)
	}

	summary, err := s.SyncTenantBillingImageStorage(tenant.ID, 8)
	if err != nil {
		t.Fatalf("sync updated image storage: %v", err)
	}

	elapsedNanos := summary.LastAccruedAt.Sub(staleAccruedAt).Nanoseconds()
	expectedBalance := startBalance - billingHourlyRateMicroCentsWithCommittedStorage(seeded, 2)*elapsedNanos/int64(time.Hour)
	if summary.BalanceMicroCents != expectedBalance {
		t.Fatalf("expected balance %d after accruing with previous image storage snapshot, got %d", expectedBalance, summary.BalanceMicroCents)
	}
	if got := summary.ManagedCommitted.StorageGibibytes; got != 8 {
		t.Fatalf("expected committed storage to refresh to 8 GiB, got %d", got)
	}
}

func TestExplicitZeroBillingConfigDoesNotBackfillAfterConfigEvent(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Paused Billing Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{}); err != nil {
		t.Fatalf("pause billing: %v", err)
	}

	staleAccrual := time.Now().UTC().Add(-72 * time.Hour)
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findTenantBillingRecord(state, tenant.ID)
		if index < 0 {
			t.Fatalf("billing record for tenant %s not found", tenant.ID)
		}
		state.TenantBilling[index].BalanceMicroCents = 0
		state.TenantBilling[index].LastAccruedAt = staleAccrual
		state.TenantBilling[index].UpdatedAt = staleAccrual
		return nil
	}); err != nil {
		t.Fatalf("seed paused billing state: %v", err)
	}

	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("get billing summary: %v", err)
	}

	if summary.ManagedCap != (model.BillingResourceSpec{}) {
		t.Fatalf("expected explicit zero cap to remain paused, got %+v", summary.ManagedCap)
	}
	if summary.Status != model.BillingStatusInactive {
		t.Fatalf("expected inactive billing status, got %s", summary.Status)
	}
	if summary.BalanceMicroCents != 0 {
		t.Fatalf("expected zero balance to remain unchanged, got %d", summary.BalanceMicroCents)
	}
}

func TestSetTenantBillingBalanceRecordsSignedAdjustments(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Balance Adjustment Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	metadata := map[string]string{
		"source":     "platform-admin",
		"actor_type": model.ActorTypeBootstrap,
		"actor_id":   "bootstrap-admin",
	}

	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{}); err != nil {
		t.Fatalf("pause managed billing before balance adjustments: %v", err)
	}

	if _, err := s.SetTenantBillingBalance(tenant.ID, 0, metadata); err != nil {
		t.Fatalf("set balance to zero: %v", err)
	}
	if _, err := s.SetTenantBillingBalance(tenant.ID, 1000, metadata); err != nil {
		t.Fatalf("set balance to 1000 cents: %v", err)
	}
	summary, err := s.SetTenantBillingBalance(tenant.ID, 300, metadata)
	if err != nil {
		t.Fatalf("set balance to 300 cents: %v", err)
	}

	if summary.BalanceMicroCents != 300*microCentsPerCent {
		t.Fatalf("expected final balance 300 cents, got %d microcents", summary.BalanceMicroCents)
	}
	if len(summary.Events) < 3 {
		t.Fatalf("expected at least 3 billing events, got %+v", summary.Events)
	}

	latest := summary.Events[0]
	if latest.Type != model.BillingEventTypeBalanceAdjusted {
		t.Fatalf("expected latest event type %q, got %q", model.BillingEventTypeBalanceAdjusted, latest.Type)
	}
	if latest.AmountMicroCents != -700*microCentsPerCent {
		t.Fatalf("expected latest delta -700 cents, got %d microcents", latest.AmountMicroCents)
	}
	if latest.BalanceAfterMicroCents != 300*microCentsPerCent {
		t.Fatalf("expected latest balance after 300 cents, got %d microcents", latest.BalanceAfterMicroCents)
	}
	if latest.Metadata["source"] != "platform-admin" {
		t.Fatalf("expected latest event source metadata, got %+v", latest.Metadata)
	}

	previous := summary.Events[1]
	if previous.Type != model.BillingEventTypeBalanceAdjusted {
		t.Fatalf("expected previous event type %q, got %q", model.BillingEventTypeBalanceAdjusted, previous.Type)
	}
	if previous.AmountMicroCents != 1000*microCentsPerCent {
		t.Fatalf("expected previous delta +1000 cents, got %d microcents", previous.AmountMicroCents)
	}
	if previous.BalanceAfterMicroCents != 1000*microCentsPerCent {
		t.Fatalf("expected previous balance after 1000 cents, got %d microcents", previous.BalanceAfterMicroCents)
	}

	noOpSummary, err := s.SetTenantBillingBalance(tenant.ID, 300, metadata)
	if err != nil {
		t.Fatalf("repeat balance set: %v", err)
	}
	if len(noOpSummary.Events) != len(summary.Events) {
		t.Fatalf("expected no-op balance set to avoid creating a new event, got %+v", noOpSummary.Events)
	}
	if noOpSummary.Events[0].ID != latest.ID {
		t.Fatalf("expected no-op balance set to keep latest event %s, got %s", latest.ID, noOpSummary.Events[0].ID)
	}
}

func TestSingleZeroBillingDimensionPausesManagedBilling(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		managedCap model.BillingResourceSpec
	}{
		{
			name: "cpu zero",
			managedCap: model.BillingResourceSpec{
				CPUMilliCores:   0,
				MemoryMebibytes: 2048,
			},
		},
		{
			name: "memory zero",
			managedCap: model.BillingResourceSpec{
				CPUMilliCores:   1000,
				MemoryMebibytes: 0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(filepath.Join(t.TempDir(), "store.json"))
			if err := s.Init(); err != nil {
				t.Fatalf("init store: %v", err)
			}

			tenant, err := s.CreateTenant("Paused Dimension Tenant")
			if err != nil {
				t.Fatalf("create tenant: %v", err)
			}

			if _, err := s.UpdateTenantBilling(tenant.ID, tc.managedCap); err != nil {
				t.Fatalf("update billing: %v", err)
			}

			staleAccrual := time.Now().UTC().Add(-48 * time.Hour)
			var previousBalance int64
			if err := s.withLockedState(true, func(state *model.State) error {
				index := findTenantBillingRecord(state, tenant.ID)
				if index < 0 {
					t.Fatalf("billing record for tenant %s not found", tenant.ID)
				}
				previousBalance = state.TenantBilling[index].BalanceMicroCents
				state.TenantBilling[index].LastAccruedAt = staleAccrual
				state.TenantBilling[index].UpdatedAt = staleAccrual
				return nil
			}); err != nil {
				t.Fatalf("seed paused single-dimension billing state: %v", err)
			}

			summary, err := s.GetTenantBillingSummary(tenant.ID)
			if err != nil {
				t.Fatalf("get billing summary: %v", err)
			}

			if summary.Status != model.BillingStatusInactive {
				t.Fatalf("expected inactive billing status, got %s", summary.Status)
			}
			if summary.HourlyRateMicroCents != 0 {
				t.Fatalf("expected zero hourly rate, got %d", summary.HourlyRateMicroCents)
			}
			if summary.MonthlyEstimateMicroCents != 0 {
				t.Fatalf("expected zero monthly estimate, got %d", summary.MonthlyEstimateMicroCents)
			}
			if summary.BalanceRestricted {
				t.Fatal("expected balance restriction to stay disabled while billing is paused")
			}
			if summary.RunwayHours != nil {
				t.Fatalf("expected no runway while billing is paused, got %v", *summary.RunwayHours)
			}
			if summary.BalanceMicroCents != previousBalance {
				t.Fatalf(
					"expected balance %d to remain unchanged while billing is paused, got %d",
					previousBalance,
					summary.BalanceMicroCents,
				)
			}
		})
	}
}

func TestBillingRejectsLoweringCapBelowCurrentCommitted(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Billing Overcap Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appResources := model.DefaultManagedAppResources()
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		Resources: &appResources,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	doubleAppCap := model.BillingResourceSpec{
		CPUMilliCores:   model.DefaultManagedAppResources().CPUMilliCores * 2,
		MemoryMebibytes: model.DefaultManagedAppResources().MemoryMebibytes * 2,
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, doubleAppCap); err != nil {
		t.Fatalf("update billing: %v", err)
	}
	if _, err := s.TopUpTenantBilling(tenant.ID, 500, "seed"); err != nil {
		t.Fatalf("top up billing: %v", err)
	}

	deploySpec := app.Spec
	deploySpec.Replicas = 2
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "done"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpecFromResourceSpec(model.DefaultManagedAppResources())); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatalf("expected ErrBillingCapExceeded when lowering cap below committed resources, got %v", err)
	}
}

func TestBillingAllowsScaleDownWhileOverCap(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Billing Overcap Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appResources := model.DefaultManagedAppResources()
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		Resources: &appResources,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	doubleAppCap := model.BillingResourceSpec{
		CPUMilliCores:   model.DefaultManagedAppResources().CPUMilliCores * 2,
		MemoryMebibytes: model.DefaultManagedAppResources().MemoryMebibytes * 2,
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, doubleAppCap); err != nil {
		t.Fatalf("update billing: %v", err)
	}
	if _, err := s.TopUpTenantBilling(tenant.ID, 500, "seed"); err != nil {
		t.Fatalf("top up billing: %v", err)
	}

	deploySpec := app.Spec
	deploySpec.Replicas = 2
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "done"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	if err := s.withLockedState(true, func(state *model.State) error {
		record := ensureTenantBillingRecord(state, tenant.ID, time.Now().UTC())
		record.ManagedCap = model.BillingResourceSpecFromResourceSpec(model.DefaultManagedAppResources())
		record.UpdatedAt = time.Now().UTC()
		return nil
	}); err != nil {
		t.Fatalf("seed legacy over-cap billing state: %v", err)
	}

	scaleDown := 1
	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &scaleDown,
	}); err != nil {
		t.Fatalf("expected scale-down to remain allowed, got %v", err)
	}

	scaleUp := 3
	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &scaleUp,
	}); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatalf("expected ErrBillingCapExceeded while legacy state remains over cap, got %v", err)
	}
}

func TestCreateBackingServiceRejectsManagedCapacityBeyondCap(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Backing Service Billing Cap Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{}); err != nil {
		t.Fatalf("set billing cap: %v", err)
	}

	if _, err := s.CreateBackingService(tenant.ID, project.ID, "shared-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database:  "demo",
			User:      "demo",
			Password:  "secret",
			RuntimeID: "runtime_managed_shared",
		},
	}); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatalf("expected ErrBillingCapExceeded, got %v", err)
	}
}

func TestBindBackingServiceRejectsManagedCapacityBeyondCap(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Binding Billing Cap Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appResources := model.DefaultManagedAppResources()
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		Resources: &appResources,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpecFromResourceSpec(appResources)); err != nil {
		t.Fatalf("set billing cap: %v", err)
	}

	deploySpec := app.Spec
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "done"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	service, err := s.CreateBackingService(tenant.ID, project.ID, "shared-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backing service: %v", err)
	}

	if _, err := s.BindBackingService(tenant.ID, app.ID, service.ID, "", nil); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatalf("expected ErrBillingCapExceeded, got %v", err)
	}
}

func TestCreateAppWithInlineManagedPostgresRejectsManagedCapacityBeyondCap(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Inline Postgres Billing Cap Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{}); err != nil {
		t.Fatalf("set billing cap: %v", err)
	}

	if _, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	}); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatalf("expected ErrBillingCapExceeded, got %v", err)
	}
}

func TestBillingDepletedBalanceOnlyBlocksCapacityIncrease(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Billing Balance Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appResources := model.DefaultManagedAppResources()
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		Resources: &appResources,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:   1000,
		MemoryMebibytes: 2048,
	}); err != nil {
		t.Fatalf("update billing: %v", err)
	}
	if _, err := s.TopUpTenantBilling(tenant.ID, 500, "seed"); err != nil {
		t.Fatalf("top up billing: %v", err)
	}

	deploySpec := app.Spec
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "done"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	if err := s.withLockedState(true, func(state *model.State) error {
		record := ensureTenantBillingRecord(state, tenant.ID, time.Now().UTC())
		record.BalanceMicroCents = 0
		record.UpdatedAt = time.Now().UTC()
		return nil
	}); err != nil {
		t.Fatalf("deplete balance: %v", err)
	}

	scaleUp := 2
	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &scaleUp,
	}); !errors.Is(err, ErrBillingBalanceDepleted) {
		t.Fatalf("expected ErrBillingBalanceDepleted, got %v", err)
	}

	scaleDown := 0
	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &scaleDown,
	}); err != nil {
		t.Fatalf("expected scale-down with depleted balance to remain allowed, got %v", err)
	}
}

func TestBillingDepletedBalanceBlocksDeployOntoPaidPublicRuntime(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Paid Public Runtime Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	consumer, err := s.CreateTenant("Paid Public Runtime Consumer")
	if err != nil {
		t.Fatalf("create consumer tenant: %v", err)
	}
	project, err := s.CreateProject(consumer.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "paid-public-node", "https://paid-public-node.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}
	if _, err := s.SetRuntimeAccessMode(runtimeObj.ID, owner.ID, model.RuntimeAccessModePublic); err != nil {
		t.Fatalf("set runtime public: %v", err)
	}
	if _, err := s.SetRuntimePublicOffer(runtimeObj.ID, owner.ID, model.RuntimePublicOffer{
		ReferenceBundle: model.BillingResourceSpec{
			CPUMilliCores:    1000,
			MemoryMebibytes:  1024,
			StorageGibibytes: 10,
		},
		ReferenceMonthlyPriceMicroCents: 200 * microCentsPerCent,
	}); err != nil {
		t.Fatalf("set runtime public offer: %v", err)
	}

	if err := s.withLockedState(true, func(state *model.State) error {
		record := ensureTenantBillingRecord(state, consumer.ID, time.Now().UTC())
		record.ManagedCap = model.BillingResourceSpec{}
		record.BalanceMicroCents = 0
		record.UpdatedAt = time.Now().UTC()
		appendTenantBillingEvent(state, newTenantBillingBalanceAdjustedEvent(
			consumer.ID,
			0,
			0,
			record.UpdatedAt,
			map[string]string{"source": "test-seed"},
		))
		return nil
	}); err != nil {
		t.Fatalf("deplete consumer balance: %v", err)
	}

	appSpec := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
		Resources: &model.ResourceSpec{
			CPUMilliCores:   1000,
			MemoryMebibytes: 1024,
		},
		Workspace: &model.AppWorkspaceSpec{
			StorageSize: "10Gi",
		},
	}
	app, err := s.CreateApp(consumer.ID, project.ID, "demo", "", appSpec)
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	if _, err := s.CreateOperation(model.Operation{
		TenantID:    consumer.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: cloneAppSpec(&appSpec),
	}); !errors.Is(err, ErrBillingBalanceDepleted) {
		t.Fatalf("expected ErrBillingBalanceDepleted for paid public runtime deploy, got %v", err)
	}
}
