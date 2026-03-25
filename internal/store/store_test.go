package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
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
	if got := app.Bindings[0].Env["DB_HOST"]; got != service.Spec.Postgres.ServiceName {
		t.Fatalf("expected binding DB_HOST=%q, got %q", service.Spec.Postgres.ServiceName, got)
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

	desiredSpec := app.Spec
	desiredSpec.Postgres = &model.AppPostgresSpec{
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
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
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
