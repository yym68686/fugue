package controller

import (
	"context"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestManagedAppLiveGuardRecoversVerifiedServingScheduling(t *testing.T) {
	for _, tc := range []struct {
		name      string
		alter     func(*runtime.ManagedAppObject, *kubeDeployment)
		wantError string
	}{
		{name: "stored scheduling advanced before live deployment"},
		{name: "unproven serving key", alter: func(m *runtime.ManagedAppObject, _ *kubeDeployment) { m.Status.CurrentReleaseKey = "unproven" }, wantError: "matches neither"},
		{name: "different app snapshot", alter: func(m *runtime.ManagedAppObject, _ *kubeDeployment) {
			m.Spec.AppSpec.Image = "registry.example/live-guard:unrelated"
		}, wantError: "matches neither"},
		{name: "different live scheduling", alter: func(_ *runtime.ManagedAppObject, d *kubeDeployment) {
			d.Spec.Template.Spec.NodeSelector["pool"] = "other"
		}, wantError: "matches neither"},
		{name: "not ready still refuses rollout", alter: func(_ *runtime.ManagedAppObject, d *kubeDeployment) {
			d.Status.ReadyReplicas = 0
			d.Status.AvailableReplicas = 0
		}, wantError: "not fully ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := managedAppLiveGuardTestApp(nil)
			liveScheduling := runtime.SchedulingConstraints{NodeSelector: map[string]string{"pool": "shared"}}
			storedScheduling := runtime.SchedulingConstraints{NodeSelector: map[string]string{"pool": "shared", kubeHostnameLabelKey: "node-a"}}
			managed := managedAppLiveGuardObject(t, current, storedScheduling)
			svc := &Service{Renderer: runtime.Renderer{}}
			live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), liveScheduling)
			if !found {
				t.Fatal("expected live deployment")
			}
			managedAppLiveGuardMarkReady(&live, 1)
			managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseError, ReadyReplicas: 1, CurrentReleaseKey: live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey]}
			if tc.alter != nil {
				tc.alter(&managed, &live)
			}
			writes := 0
			client := managedAppLiveGuardClient(t, managed, live, true, true, &writes)
			desired := current
			desired.Spec.Image = "registry.example/live-guard:v2"
			prepared, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace, managed, desired, model.OperationTypeDeploy, storedScheduling)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("expected %q, got %v", tc.wantError, err)
				}
			} else {
				if err != nil {
					t.Fatalf("verified old scheduling must allow normal rollout checks: %v", err)
				}
				if prepared.Spec.Image != desired.Spec.Image || prepared.Spec.RolloutIntent != model.AppRolloutIntentOnlineImageUpdate {
					t.Fatalf("unexpected prepared rollout: image=%s intent=%s", prepared.Spec.Image, prepared.Spec.RolloutIntent)
				}
			}
			if writes != 0 {
				t.Fatalf("preflight must remain read-only: %d writes", writes)
			}
		})
	}
}
