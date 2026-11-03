package store

import (
	"testing"

	"fugue/internal/model"
)

func TestAppNodeLossDefaultAndConfigurationSurviveOldClientsAndObservation(t *testing.T) {
	s, app := cpuResizeStoreFixture(t)
	if app.Spec.NodeLoss == nil || app.Spec.NodeLoss.Mode != model.AppNodeLossWait {
		t.Fatal("new app did not default to wait")
	}
	desired := *cloneAppSpec(&app.Spec)
	desired.NodeLoss = nil
	op, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &desired})
	if err != nil {
		t.Fatal(err)
	}
	if op.DesiredSpec.NodeLoss == nil || op.DesiredSpec.NodeLoss.Mode != model.AppNodeLossWait {
		t.Fatal("old client reset existing policy")
	}
	if _, err = s.FailOperation(op.ID, "fixture"); err != nil {
		t.Fatal(err)
	}
	observed, err := s.SyncObservedManagedAppBaseline(app.ID, desired, nil)
	if err != nil || observed.Spec.NodeLoss == nil || observed.Spec.NodeLoss.Mode != model.AppNodeLossWait {
		t.Fatal("old observed snapshot reset durable policy", err)
	}
	desired.NodeLoss = &model.AppNodeLossPolicy{Mode: model.AppNodeLossLegacy}
	op, err = s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &desired})
	if err != nil || op.DesiredSpec.NodeLoss.Mode != model.AppNodeLossLegacy {
		t.Fatal("explicit legacy request lost", err)
	}
	seconds := int64(123)
	spec := model.AppSpec{NodeLoss: &model.AppNodeLossPolicy{Mode: model.AppNodeLossReplace, TolerationSeconds: &seconds}}
	cloned := cloneAppSpec(&spec)
	*cloned.NodeLoss.TolerationSeconds = 0
	if seconds != 123 {
		t.Fatal("cloned operation aliases user policy")
	}
}
