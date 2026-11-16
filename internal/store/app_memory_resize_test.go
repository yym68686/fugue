package store

import (
	"errors"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestAppMemoryResizeFencesMutationsAndPreservesVerifiedTarget(t *testing.T) {
	s, app := cpuResizeStoreFixture(t)
	desired := app.Spec.DeepCopy()
	desired.Resources.MemoryMebibytes = 96
	desired.Resources.CPUMilliCores = 999
	desired.Resources.MemoryLimitMebibytes = 999
	desired.Image = "registry.example/unrelated:other"
	op, err := s.CreateAppRequestResizeOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeAppMemoryResize, DesiredSpec: &desired}, app.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if op.DesiredSpec.Resources.CPUMilliCores != 100 || op.DesiredSpec.Resources.MemoryLimitMebibytes != 256 || op.DesiredSpec.Image != app.Spec.Image {
		t.Fatal("accepted unrelated mutation")
	}
	if _, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec}); !errors.Is(err, ErrConflict) {
		t.Fatal("concurrent deploy", err)
	}
	if _, err := s.SyncObservedManagedAppBaseline(app.ID, app.Spec, nil); !errors.Is(err, ErrConflict) {
		t.Fatal("observation overwrote active resize", err)
	}
	op, _, err = s.TryClaimPendingOperation(op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequeueManagedOperationGuarded(op, "restart"); !errors.Is(err, ErrConflict) {
		t.Fatal("accepted resize became cancellable", err)
	}
	if _, err := s.CompleteManagedOperationGuarded(op, &app, "", "verified", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	actual, _ := s.GetApp(app.ID)
	if actual.Spec.Resources.MemoryMebibytes != 96 || actual.Spec.MemoryRequestTemplateMebibytes == nil || *actual.Spec.MemoryRequestTemplateMebibytes != 128 || !reflect.DeepEqual(actual.Status, app.Status) {
		t.Fatal("verified target/status lost")
	}
	observed, err := s.SyncObservedManagedAppBaseline(app.ID, app.Spec, nil)
	if err != nil || observed.Spec.Resources.MemoryMebibytes != 96 || observed.Spec.MemoryRequestTemplateMebibytes == nil {
		t.Fatal("stale observation reverted verified request", err)
	}
	deploy, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &observed.Spec})
	if err != nil || deploy.DesiredSpec.MemoryRequestTemplateMebibytes != nil || deploy.DesiredSpec.Resources.MemoryMebibytes != 96 {
		t.Fatal("normal release did not fold verified memory", err)
	}
}
