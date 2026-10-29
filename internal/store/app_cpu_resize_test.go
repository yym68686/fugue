package store

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func cpuResizeStoreFixture(t *testing.T) (*Store, model.App) {
	t.Helper()
	s := New(t.TempDir() + "/state.json")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("Resize Tenant")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "services", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{
		Image: "registry.example/service:one", RuntimeID: "runtime_managed_shared", Replicas: 1,
		Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128, MemoryLimitMebibytes: 256},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := s.SyncManagedAppObservedStatus(app.ID, "ready", 1, "serving", &now, &now, nil); err != nil {
		t.Fatal(err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	return s, app
}

func TestAppCPUResizePreservesServingStateAndFencesOtherOperations(t *testing.T) {
	s, app := cpuResizeStoreFixture(t)
	desired := *cloneAppSpec(&app.Spec)
	desired.Image = "registry.example/untrusted:other"
	desired.Env = map[string]string{"UNRELATED": "change"}
	desired.Resources.CPUMilliCores, desired.Resources.MemoryMebibytes = 75, 999
	op, err := s.CreateAppCPUResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, DesiredSpec: &desired}, app.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if op.DesiredSpec.Image != app.Spec.Image || op.DesiredSpec.Resources.MemoryMebibytes != 128 || len(op.DesiredSpec.Env) != 0 {
		t.Fatal("resize accepted unrelated changes")
	}
	if _, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec}); !errors.Is(err, ErrConflict) {
		t.Fatalf("concurrent deploy: %v", err)
	}
	op, claimed, err := s.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	if _, err := s.CancelOperation(op.ID, "stop"); !errors.Is(err, ErrConflict) {
		t.Fatal("running mutation canceled", err)
	}
	if count, err := s.RequeueInFlightManagedOperations("controller restarted"); err != nil || count != 0 {
		t.Fatal("resize journal attempt requeued on restart", count, err)
	}
	if _, err := s.RequeueManagedOperationGuarded(op, "interrupted"); !errors.Is(err, ErrConflict) {
		t.Fatal("accepted resize became pending/cancellable", err)
	}
	if _, err := s.CompleteManagedOperationGuarded(op, &app, "", "verified", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Spec.Resources.CPUMilliCores != 75 || current.Spec.CPURequestTemplateMilliCores == nil || *current.Spec.CPURequestTemplateMilliCores != 100 {
		t.Fatalf("target/template: %+v", current.Spec)
	}
	if !reflect.DeepEqual(app.Status, current.Status) || current.Spec.Image != app.Spec.Image || !reflect.DeepEqual(app.Route, current.Route) {
		t.Fatal("serving identity/status changed")
	}
	deploy, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &current.Spec})
	if err != nil {
		t.Fatal(err)
	}
	if deploy.DesiredSpec.CPURequestTemplateMilliCores != nil || deploy.DesiredSpec.Resources.CPUMilliCores != 75 {
		t.Fatal("new release restored old CPU template")
	}
}

func TestAppCPUResizeRejectsStaleBaselineAndInvalidTargets(t *testing.T) {
	s, app := cpuResizeStoreFixture(t)
	for _, cpu := range []int64{0, -1} {
		desired := *cloneAppSpec(&app.Spec)
		desired.Resources.CPUMilliCores = cpu
		if _, err := s.CreateAppCPUResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, DesiredSpec: &desired}, app.Spec); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid CPU %d: %v", cpu, err)
		}
	}
	stale := *cloneAppSpec(&app.Spec)
	stale.Image = "stale"
	if _, err := s.CreateAppCPUResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, DesiredSpec: &app.Spec}, stale); !errors.Is(err, ErrConflict) {
		t.Fatal("stale baseline accepted", err)
	}
	copy := cloneAppSpec(&app.Spec)
	cpu := int64(100)
	copy.CPURequestTemplateMilliCores = &cpu
	clone := cloneAppSpec(copy)
	*clone.CPURequestTemplateMilliCores = 25
	if *copy.CPURequestTemplateMilliCores != 100 {
		t.Fatal("snapshot pointer aliases")
	}
}

func TestAppCPUResizeConfigurationRebaseFoldsVerifiedTarget(t *testing.T) {
	_, app := cpuResizeStoreFixture(t)
	base := *cloneAppSpec(&app.Spec)
	desired := *cloneAppSpec(&base)
	desired.Image = "registry.example/service:two"
	cpu := base.Resources.CPUMilliCores
	app.Spec.Resources.CPUMilliCores = 75
	app.Spec.CPURequestTemplateMilliCores = &cpu
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired}
	if err := rebaseImportDeployConfiguration(&op, app, &base); err != nil {
		t.Fatal(err)
	}
	if op.DesiredSpec.Resources.CPUMilliCores != 75 || op.DesiredSpec.CPURequestTemplateMilliCores != nil || op.DesiredSpec.Image != desired.Image {
		t.Fatal("release lost independently verified CPU target")
	}
}

func TestAppCPUResizeSurvivesStaleObservedBaseline(t *testing.T) {
	for _, observedPostgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "app baseline", true: "postgres observation"}[observedPostgres], func(t *testing.T) {
			s, app := cpuResizeStoreFixture(t)
			desired := *cloneAppSpec(&app.Spec)
			desired.Resources.CPUMilliCores = 75
			op, err := s.CreateAppCPUResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppCPUResize, DesiredSpec: &desired}, app.Spec)
			if err != nil {
				t.Fatal(err)
			}
			// An observation read before the resize cannot write through its
			// mutation interlock, including before the first Kubernetes PATCH.
			sync := func() (model.App, error) {
				if observedPostgres {
					return s.SyncObservedManagedPostgresSpec(app.ID, app.Spec)
				}
				return s.SyncObservedManagedAppBaseline(app.ID, app.Spec, nil)
			}
			if _, err := sync(); !errors.Is(err, ErrConflict) {
				t.Fatalf("active resize accepted observed baseline: %v", err)
			}
			op, claimed, err := s.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal(err)
			}
			if _, err := s.CompleteManagedOperationGuarded(op, &app, "", "verified", nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			// A source-less app continually recovers its deployment baseline.
			// That runtime snapshot owns neither the verified CPU target nor the
			// server-owned immutable CPU template marker.
			for i := 0; i < 2; i++ {
				current, err := sync()
				if err != nil {
					t.Fatal(err)
				}
				if current.Spec.Resources.CPUMilliCores != 75 || current.Spec.CPURequestTemplateMilliCores == nil || *current.Spec.CPURequestTemplateMilliCores != 100 {
					t.Fatal("observed snapshot overwrote verified CPU target")
				}
			}
		})
	}
}
