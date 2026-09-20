package controller

import (
	"context"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestRolloutComparisonsPreserveFileSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name    string
		compare func(model.AppSpec) model.AppSpec
	}{
		{"config files", comparableConfigFileUpdateSpec},
		{"zero downtime restart", comparableZeroDowntimeRestartSpec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := managedAppLiveGuardTestApp(nil)
			model.ApplyAppSpecDefaults(&app.Spec)
			app.Spec.Files = []model.AppFile{{Path: "/etc/service.conf", Content: "mode=stable\n", Mode: 0o644}}
			before := *cloneControllerAppSpec(&app.Spec)
			key := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})

			comparison := tc.compare(app.Spec)
			if comparison.Files[0].Content != "" {
				t.Fatal("file contents must be excluded from the comparison")
			}
			if !reflect.DeepEqual(app.Spec, before) {
				t.Fatal("comparison mutated its input snapshot")
			}
			if got := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{}); got != key {
				t.Fatalf("comparison changed release identity: %s -> %s", key, got)
			}
		})
	}
}

func TestRolloutClassificationPreservesAcceptedAndRefusedFileSnapshots(t *testing.T) {
	for _, removeFiles := range []bool{false, true} {
		name := "content update"
		if removeFiles {
			name = "image update with removed file mounts"
		}
		t.Run(name, func(t *testing.T) {
			current := managedAppLiveGuardTestApp(nil)
			model.ApplyAppSpecDefaults(&current.Spec)
			current.Spec.Files = []model.AppFile{{Path: "/etc/service.conf", Content: "mode=stable\n", Mode: 0o644}}
			desired := current
			desired.Spec = *cloneControllerAppSpec(&current.Spec)
			desired.Spec.Files[0].Content = "mode=candidate\n"
			wantIntent := model.AppRolloutIntentOnlineConfigUpdate
			if removeFiles {
				desired.Spec.Image = "registry.example/live-guard:v2"
				desired.Spec.Files = nil
				wantIntent = ""
			}
			currentBefore := *cloneControllerAppSpec(&current.Spec)
			desiredBefore := *cloneControllerAppSpec(&desired.Spec)
			op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}
			for attempt := 0; attempt < 2; attempt++ {
				if got := rolloutIntentForManagedOperation(op, current, desired); got != wantIntent {
					t.Fatalf("classification %d: want %q, got %q", attempt, wantIntent, got)
				}
				if !reflect.DeepEqual(current.Spec, currentBefore) || !reflect.DeepEqual(desired.Spec, desiredBefore) {
					t.Fatal("classification mutated the serving or requested snapshot")
				}
			}
		})
	}
}

func TestManagedAppLiveGuardPreservesConfigFileUpdate(t *testing.T) {
	current := managedAppLiveGuardTestApp(nil)
	current.Spec.Files = []model.AppFile{{Path: "/etc/service.conf", Content: "mode=stable\n", Mode: 0o644}}
	desired := current
	desired.Spec = *cloneControllerAppSpec(&current.Spec)
	desired.Spec.Files[0].Content = "mode=candidate\n"
	managed := managedAppLiveGuardObject(t, current, runtime.SchedulingConstraints{})
	svc := &Service{Renderer: runtime.Renderer{}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), managed.Spec.Scheduling)
	if !found {
		t.Fatal("expected serving deployment")
	}
	managedAppLiveGuardMarkReady(&live, 1)
	managed.Status = runtime.ManagedAppStatus{
		Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1,
		CurrentReleaseKey: live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey],
	}
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)
	prepared, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, desired,
		model.OperationTypeDeploy, managed.Spec.Scheduling,
	)
	if err != nil {
		t.Fatalf("valid online file update must pass the live release guard: %v", err)
	}
	if prepared.Spec.RolloutIntent != model.AppRolloutIntentOnlineConfigUpdate {
		t.Fatalf("unexpected rollout intent %q", prepared.Spec.RolloutIntent)
	}
	if prepared.Spec.Files[0].Content != "mode=candidate\n" || current.Spec.Files[0].Content != "mode=stable\n" {
		t.Fatal("live preflight must preserve both serving and requested file contents")
	}
}
