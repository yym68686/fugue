package controller

import (
	"context"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// Exercise the real render -> ManagedApp -> live guard boundary. The durable
// snapshot omits generated env, while background reconciliation prepares desired.
func TestManagedAppReconcileScaleToZeroIgnoresInjectedEnv(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, ready := range []bool{false, true} {
			name := "stateless"
			if durable {
				name = "persistent"
			}
			if ready {
				name += "-ready"
			} else {
				name += "-pending"
			}
			t.Run(name, func(t *testing.T) {
				var storage *model.AppPersistentStorageSpec
				if durable {
					storage = &model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: "network-storage", StorageSize: "10Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}}
				}
				current := managedAppLiveGuardTestApp(storage)
				current.Spec.Env = map[string]string{"APP_MODE": "production", "FUGUE_CUSTOM_SETTING": "user-value"}
				current.Spec.Continuity = nil // Use the production service-default protection.
				svc := &Service{Renderer: runtime.Renderer{}}
				scheduling := runtime.SchedulingConstraints{NodeSelector: map[string]string{kubeHostnameLabelKey: "node-a"}}
				managed := managedAppLiveGuardObject(t, svc.Renderer.PrepareApp(current), scheduling)
				current = runtime.AppFromManagedApp(managed)
				desired := current
				desired.Spec = current.Spec.DeepCopy()
				desired.Spec.Replicas = 0
				if inferredManagedReconcileOperationType(current, desired) != model.OperationTypeScale {
					t.Fatal("raw replica-only change must infer scale")
				}
				prepared := svc.Renderer.PrepareApp(desired)
				if inferredManagedReconcileOperationType(current, prepared) != model.OperationTypeScale {
					t.Fatal("generated env must not turn scale-to-zero into deploy")
				}
				live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), scheduling)
				if !found {
					t.Fatal("missing rendered fixture deployment")
				}
				managedAppLiveGuardMarkReady(&live, 1)
				if ready {
					managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
				} else {
					live.Status.ReadyReplicas = 0
					live.Status.AvailableReplicas = 0
					managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseError, ReadyReplicas: 0}
				}
				writes := 0
				client := managedAppLiveGuardClient(t, managed, live, true, ready, &writes)
				_, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace, managed, prepared, "", scheduling)
				if err != nil {
					t.Fatalf("background scale-to-zero with generated env must pass: %v", err)
				}
				if _, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace, managed, prepared, "", runtime.SchedulingConstraints{}); err != nil {
					t.Fatalf("stopping without a new storage node pin must pass: %v", err)
				}
				if _, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace, managed, prepared, model.OperationTypeScale, scheduling); err != nil {
					t.Fatalf("explicit scale should pass: %v", err)
				}
				normalized := prepared
				normalized.Spec, _ = model.StripFugueInjectedAppEnvFromSpec(prepared.Spec)
				if inferredManagedReconcileOperationType(current, normalized) != model.OperationTypeScale {
					t.Fatal("removing only injected fields did not restore scale classification")
				}
				if _, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace, managed, normalized, "", scheduling); err != nil {
					t.Fatalf("normalized automatic classification should pass: %v", err)
				}
				userChange := normalized
				userChange.Spec = normalized.Spec.DeepCopy()
				userChange.Spec.Env["FUGUE_CUSTOM_SETTING"] = "changed-user-value"
				if inferredManagedReconcileOperationType(current, userChange) != model.OperationTypeDeploy {
					t.Fatal("real user env change must remain distinguishable")
				}
				if _, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace, managed, userChange, "", scheduling); err == nil {
					t.Fatal("true user configuration change must retain the deployment guard")
				}
				if writes != 0 {
					t.Fatalf("diagnostic wrote %d times", writes)
				}
			})
		}
	}
}
