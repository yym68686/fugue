package controller

import (
	"context"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrationSnapshotRecoveryIncludesRunningRollbackAndFailedRetry(t *testing.T) {
	for _, running := range []bool{true, false} {
		t.Run(map[bool]string{true: "running rollback", false: "failed retry"}[running], func(t *testing.T) {
			st := store.New(filepath.Join(t.TempDir(), "store.json"))
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := st.CreateTenant("snapshot fixture")
			if err != nil {
				t.Fatal(err)
			}
			project, err := st.CreateProject(tenant.ID, "fixture", "")
			if err != nil {
				t.Fatal(err)
			}
			source, _, err := st.CreateRuntime("", "source", model.RuntimeTypeManagedShared, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			target, _, err := st.CreateRuntime("", "target", model.RuntimeTypeManagedShared, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			app, err := st.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "worker", "", model.AppSpec{Image: "registry.example/worker:v1", Ports: []int{8080}, Replicas: 1, RuntimeID: source.ID}, model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/worker:v1"})
			if err != nil {
				t.Fatal(err)
			}
			next := app
			next.Spec = *cloneControllerAppSpec(&app.Spec)
			next.Spec.RuntimeID = target.ID
			op, err := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeMigrate, SourceRuntimeID: source.ID, TargetRuntimeID: target.ID, DesiredSpec: &next.Spec})
			if err != nil {
				t.Fatal(err)
			}
			op, claimed, err := st.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal(err, claimed)
			}
			started := op.CreatedAt
			if op.StartedAt != nil {
				started = *op.StartedAt
			}
			svc := &Service{Store: st, Renderer: runtime.Renderer{}}
			if err := svc.applyManagedMigrationOnlineRolloutIntent(op, app, &next); err != nil {
				t.Fatal(err)
			}
			sched := runtime.SchedulingForRuntime(target)
			key := svc.Renderer.ManagedAppReleaseKey(svc.Renderer.PrepareApp(next), sched)
			managed := managedAppLiveGuardObject(t, app, runtime.SchedulingForRuntime(source))
			managed.Metadata.Generation = 2
			managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseError, ObservedGeneration: 2, PendingReleaseKey: key, PendingReleaseStartedAt: started.UTC().Format(time.RFC3339Nano)}
			ctx := context.Background()
			if running {
				ctx = withManagedAppApplySource(ctx, managedAppApplySourceOperation, op.ID)
			} else {
				if _, err = st.FailOperation(op.ID, "initialization deadline"); err != nil {
					t.Fatal(err)
				}
				if _, ok, err := svc.recoverManagedAppPendingDeploySnapshot(ctx, managed, app, key); err != nil || ok {
					t.Fatal("background adopted retained migration", ok, err)
				}
				retry, err := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeMigrate, SourceRuntimeID: source.ID, TargetRuntimeID: target.ID, DesiredSpec: &next.Spec})
				if err != nil {
					t.Fatal(err)
				}
				if _, ok, err := st.TryClaimPendingOperation(retry.ID); err != nil || !ok {
					t.Fatal(err, ok)
				}
				ctx = withManagedAppApplySource(ctx, managedAppApplySourceOperation, retry.ID)
			}
			recovered, ok, err := svc.recoverManagedAppPendingDeploySnapshot(ctx, managed, app, key)
			if err != nil || !ok || recovered.Operation.ID != op.ID || recovered.App.Spec.RuntimeID != target.ID || recovered.App.Spec.RolloutIntent != model.AppRolloutIntentOnlineRestart {
				t.Fatalf("migration provenance not recovered: ok=%v err=%v operation=%s", ok, err, recovered.Operation.ID)
			}
			if running {
				live, _ := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(next), sched)
				live.Metadata.Generation = 2
				live.Status.ObservedGeneration = 2
				live.Status.Replicas, live.Status.UpdatedReplicas = 2, 1
				live.Status.ReadyReplicas, live.Status.AvailableReplicas, live.Status.UnavailableReplicas = 1, 1, 1
				live.Status.Conditions = []runtime.ManagedAppCondition{{Type: "Progressing", Status: "False", Reason: "ProgressDeadlineExceeded"}}
				previous, _ := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), runtime.SchedulingForRuntime(source))
				managed.Status.CurrentReleaseKey = svc.Renderer.ManagedAppReleaseKey(svc.Renderer.PrepareApp(app), runtime.SchedulingForRuntime(source))
				managed.Status.CurrentReleaseReadyAt = started.Add(-time.Minute).Format(time.RFC3339Nano)
				oldPod := readyTemplatePod("old-ready", previous, kubeResourceRequirements{})
				client := managedAppLiveGuardClientWithPods(t, managed, live, true, true, []kubePod{oldPod}, nil)
				got, err := svc.prepareManagedAppReconcileRolloutWithEvidence(ctx, client, managed.Metadata.Namespace, managed, app, model.OperationTypeMigrate, runtime.SchedulingForRuntime(source))
				if err != nil || got.Spec.RuntimeID != source.ID {
					t.Fatal("active migration rollback cannot reuse the exact ready old cohort", err)
				}
			}
			managed.Status.PendingReleaseStartedAt = started.Add(-time.Hour).Format(time.RFC3339Nano)
			if _, ok, err := svc.recoverManagedAppPendingDeploySnapshot(ctx, managed, app, key); err != nil || ok {
				t.Fatal("foreign timestamp accepted", ok, err)
			}
		})
	}
}
