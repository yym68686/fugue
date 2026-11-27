package operationqueue_test

import (
	"strconv"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/operationqueue"
	"fugue/internal/store"
	"fugue/internal/testpostgres"
)

func newQueueContractStore(t *testing.T, fixture string) *store.Store {
	t.Helper()
	s := store.New(testpostgres.Database(t, fixture))
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	return s
}

func TestImageReplicationRetryDoesNotStarveLaterOperation(t *testing.T) {
	t.Parallel()
	stateStore := newQueueContractStore(t, "")
	tenant, err := stateStore.CreateTenant("Retry Fairness Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "retry-fairness", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	firstApp, err := stateStore.CreateApp(tenant.ID, project.ID, "first", "", model.AppSpec{Image: "nginx:latest", Replicas: 1})
	if err != nil {
		t.Fatalf("create first app: %v", err)
	}
	secondApp, err := stateStore.CreateApp(tenant.ID, project.ID, "second", "", model.AppSpec{Image: "nginx:latest", Replicas: 1})
	if err != nil {
		t.Fatalf("create second app: %v", err)
	}
	first, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDelete, AppID: firstApp.ID})
	if err != nil {
		t.Fatalf("create first operation: %v", err)
	}
	var q operationqueue.Queue
	claimed, found, err := q.Claim(stateStore.OperationQueue(), operationqueue.ForegroundActivate, nil, nil)
	if err != nil || !found || claimed.ID != first.ID {
		t.Fatalf("claim first operation: found=%v op=%s err=%v", found, claimed.ID, err)
	}
	if _, err := stateStore.RequeueManagedOperation(first.ID, model.OperationResultDeployImageReplicationPending+": image is being replicated"); err != nil {
		t.Fatalf("requeue first operation: %v", err)
	}
	second, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDelete, AppID: secondApp.ID})
	if err != nil {
		t.Fatalf("create second operation: %v", err)
	}
	claimed, found, err = q.Claim(stateStore.OperationQueue(), operationqueue.ForegroundActivate, nil, nil)
	if err != nil || !found {
		t.Fatalf("claim later operation: found=%v err=%v", found, err)
	}
	if claimed.ID != second.ID {
		t.Fatalf("expected later operation %s while %s is backed off, got %s", second.ID, first.ID, claimed.ID)
	}
}

func TestForegroundActivateClaimsDependencyReadyDeployFirst(t *testing.T) {
	t.Parallel()

	stateStore := newQueueContractStore(t, "")

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-app", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-app:git-old",
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo-app:old",
		ComposeService:   "app",
		ComposeDependsOn: []string{"redis"},
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create app service: %v", err)
	}

	redis, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-redis", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-redis:git-old",
		Ports:     []int{6379},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeDockerImage,
		ImageRef:       "redis:7-alpine",
		ComposeService: "redis",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create redis service: %v", err)
	}

	appSpec := app.Spec
	appSpec.Image = "registry.push.example/fugue-apps/demo-app:new"
	appSource := *app.Source
	appDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &appSpec,
		DesiredSource: &appSource,
	})
	if err != nil {
		t.Fatalf("create app deploy: %v", err)
	}

	redisSpec := redis.Spec
	redisSpec.Image = "registry.push.example/fugue-apps/demo-redis:new"
	redisSource := *redis.Source
	redisDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         redis.ID,
		DesiredSpec:   &redisSpec,
		DesiredSource: &redisSource,
	})
	if err != nil {
		t.Fatalf("create redis deploy: %v", err)
	}

	var q operationqueue.Queue

	claimed, found, err := q.Claim(stateStore.OperationQueue(), operationqueue.ForegroundActivate, nil, nil)
	if err != nil {
		t.Fatalf("claim first deploy: %v", err)
	}
	if !found {
		t.Fatal("expected a deploy operation to be claimable")
	}
	if claimed.ID != redisDeploy.ID {
		t.Fatalf("expected dependency deploy %s to be claimed first, got %s", redisDeploy.ID, claimed.ID)
	}

	if _, found, err := q.Claim(stateStore.OperationQueue(), operationqueue.ForegroundActivate, nil, nil); err != nil {
		t.Fatalf("claim blocked deploy while dependency running: %v", err)
	} else if found {
		t.Fatal("expected dependent deploy to stay blocked while dependency is running")
	}

	if _, err := stateStore.CompleteManagedOperation(redisDeploy.ID, "", "done"); err != nil {
		t.Fatalf("complete dependency deploy: %v", err)
	}

	claimed, found, err = q.Claim(stateStore.OperationQueue(), operationqueue.ForegroundActivate, nil, nil)
	if err != nil {
		t.Fatalf("claim dependent deploy after dependency ready: %v", err)
	}
	if !found {
		t.Fatal("expected dependent deploy to become claimable")
	}
	if claimed.ID != appDeploy.ID {
		t.Fatalf("expected app deploy %s after dependency ready, got %s", appDeploy.ID, claimed.ID)
	}
}

func TestForegroundActivateFailsDeployWithMissingComposeDependency(t *testing.T) {
	t.Parallel()

	stateStore := newQueueContractStore(t, "")

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-app", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-app:git-old",
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo-app:old",
		ComposeService:   "app",
		ComposeDependsOn: []string{"redis"},
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create app service: %v", err)
	}

	appSpec := app.Spec
	appSpec.Image = "registry.push.example/fugue-apps/demo-app:new"
	appSource := *app.Source
	appDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &appSpec,
		DesiredSource: &appSource,
	})
	if err != nil {
		t.Fatalf("create app deploy: %v", err)
	}

	var q operationqueue.Queue

	if _, found, err := q.Claim(stateStore.OperationQueue(), operationqueue.ForegroundActivate, nil, nil); err != nil {
		t.Fatalf("claim blocked deploy: %v", err)
	} else if found {
		t.Fatal("expected blocked deploy to fail instead of being claimed")
	}

	op, err := stateStore.GetOperation(appDeploy.ID)
	if err != nil {
		t.Fatalf("get blocked deploy: %v", err)
	}
	if op.Status != model.OperationStatusFailed {
		t.Fatalf("expected blocked deploy status %q, got %q", model.OperationStatusFailed, op.Status)
	}
	if !strings.Contains(op.ErrorMessage, `compose dependency "redis"`) {
		t.Fatalf("expected compose dependency failure message, got %q", op.ErrorMessage)
	}
}

func TestForegroundActivateCancelsBlockedComposeDeployAndClaimsQueuedDelete(t *testing.T) {
	t.Parallel()

	stateStore := newQueueContractStore(t, "")

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-app", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-app:git-old",
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo-app:old",
		ComposeService:   "app",
		ComposeDependsOn: []string{"redis"},
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create app service: %v", err)
	}

	redis, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-redis", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-redis:git-old",
		Ports:     []int{6379},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeDockerImage,
		ImageRef:       "redis:7-alpine",
		ComposeService: "redis",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create redis service: %v", err)
	}

	redisSpec := redis.Spec
	redisSource := *redis.Source
	redisImport, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeImport,
		AppID:         redis.ID,
		DesiredSpec:   &redisSpec,
		DesiredSource: &redisSource,
	})
	if err != nil {
		t.Fatalf("create redis import: %v", err)
	}
	if _, found, err := stateStore.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim redis import: %v", err)
	} else if !found {
		t.Fatal("expected redis import to be claimed")
	}
	if _, err := stateStore.FailOperation(redisImport.ID, "resolve image digest: MANIFEST_UNKNOWN"); err != nil {
		t.Fatalf("fail redis import: %v", err)
	}

	appSpec := app.Spec
	appSpec.Image = "registry.push.example/fugue-apps/demo-app:new"
	appSource := *app.Source
	appDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &appSpec,
		DesiredSource: &appSource,
	})
	if err != nil {
		t.Fatalf("create app deploy: %v", err)
	}

	deleteOp, err := stateStore.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeDelete,
		AppID:    app.ID,
	})
	if err != nil {
		t.Fatalf("create queued delete: %v", err)
	}

	var q operationqueue.Queue

	claimed, found, err := q.Claim(stateStore.OperationQueue(), operationqueue.ForegroundActivate, nil, nil)
	if err != nil {
		t.Fatalf("claim after dependency failure: %v", err)
	}
	if !found {
		t.Fatal("expected queued delete to become claimable after blocked deploy is canceled")
	}
	if claimed.ID != deleteOp.ID {
		t.Fatalf("expected delete operation %s after dependency failure, got %s", deleteOp.ID, claimed.ID)
	}

	op, err := stateStore.GetOperation(appDeploy.ID)
	if err != nil {
		t.Fatalf("get blocked deploy: %v", err)
	}
	if op.Status != model.OperationStatusCanceled {
		t.Fatalf("expected blocked deploy status %q, got %q", model.OperationStatusCanceled, op.Status)
	}
	if !strings.Contains(op.ResultMessage, "superseded by app deletion") {
		t.Fatalf("expected deletion supersession reason, got %q", op.ResultMessage)
	}
}

func TestPendingDeleteCancelsStuckDeployBeforeClaim(t *testing.T) {
	for _, running := range []bool{false, true} {
		s := newQueueContractStore(t, strconv.FormatBool(running))
		tenant, err := s.CreateTenant("example")
		if err != nil {
			t.Fatal(err)
		}
		project, err := s.CreateProject(tenant.ID, "example", "")
		if err != nil {
			t.Fatal(err)
		}
		app, err := s.CreateApp(tenant.ID, project.ID, "example", "", model.AppSpec{RuntimeID: "runtime_managed_shared", Image: "example/image:latest", Replicas: 1, Ports: []int{8080}})
		if err != nil {
			t.Fatal(err)
		}
		deploy, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
		if err != nil {
			t.Fatal(err)
		}
		if running {
			if _, ok, err := s.TryClaimPendingOperation(deploy.ID); err != nil || !ok {
				t.Fatalf("claim %v %v", ok, err)
			}
		}
		deletion, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDelete})
		if err != nil {
			t.Fatal(err)
		}
		var q operationqueue.Queue
		if claimed, found, err := q.Claim(s.OperationQueue(), operationqueue.ForegroundActivate, nil, nil); err != nil || !found || claimed.ID != deletion.ID {
			t.Fatalf("deletion claim: %s, found=%v, err=%v", claimed.ID, found, err)
		}
		op, err := s.GetOperation(deploy.ID)
		if err != nil || op.Status != model.OperationStatusCanceled {
			t.Fatalf("deploy still active: %+v %v", op, err)
		}
	}
}
