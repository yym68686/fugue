package controller

import (
	"context"
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"io"
	"log"
	"testing"
)

func TestConstraintPolicyCompletesWithoutKubernetesOrImageAvailability(t *testing.T) {
	st, app, _, pending := newSafeRolloutTestState(t)
	if _, err := st.CancelOperation(pending.ID, "fixture pending code release"); err != nil {
		t.Fatal(err)
	}
	app, err := st.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	desired := cloneControllerAppSpec(&app.Spec)
	desired.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{LatencyCohortModel: true}
	op, err := st.CreateOperation(model.Operation{AppID: app.ID, TenantID: app.TenantID, Type: model.OperationTypeDeploy, DesiredSpec: desired, ExecutionMode: model.ExecutionModeManaged})
	if err != nil {
		t.Fatal(err)
	}
	op, claimed, err := st.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	// KubectlApply is on and there is no usable Kubernetes configuration or
	// registry inspector. A constraint edit must not enter either dependency.
	s := &Service{Store: st, Config: config.ControllerConfig{KubectlApply: true}, Renderer: runtime.Renderer{BaseDir: t.TempDir()}, Logger: log.New(io.Discard, "", 0)}
	if err := s.executeManagedOperation(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetApp(app.ID)
	if err != nil || !stored.Spec.Continuity.ZeroDowntime.Gate.LatencyCohortModel {
		t.Fatal("policy absent", err)
	}
	if stored.Status.Phase != app.Status.Phase || stored.Status.CurrentReplicas != app.Status.CurrentReplicas {
		t.Fatal("policy claimed workload ready")
	}
}
