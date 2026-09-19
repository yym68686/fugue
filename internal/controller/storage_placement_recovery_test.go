package controller

import (
	"context"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestUnstartedStoragePlacementRecoveryRequiresExactUnusedWorkload(t *testing.T) {
	for _, scenario := range []string{"pending-only", "ready-endpoint", "previously-serving", "started-init", "foreign-owner", "stale-generation", "attached-volume", "changed-user-env", "missing-pod-uid", "deleted-pod", "valid-old-node"} {
		t.Run(scenario, func(t *testing.T) {
			f := newStoragePlacementFixture(t)
			app := managedAppLiveGuardTestApp(&model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: "network-storage", StorageSize: "10Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}})
			svc := &Service{newKubeClient: func(string) (*kubeClient, error) { return f.client, nil }}
			before := schedulingPinnedToNode(runtime.SchedulingConstraints{}, "compute-only")
			after := schedulingPinnedToNode(runtime.SchedulingConstraints{}, "storage-ready")
			managed := managedAppLiveGuardObject(t, app, before)
			live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), before)
			if !found {
				t.Fatal("missing deployment")
			}
			managedAppLiveGuardMarkReady(&live, 1)
			live.Metadata.UID = "deployment-uid"
			live.Status.ReadyReplicas = 0
			live.Status.AvailableReplicas = 0
			f.deployment = &live
			f.rsDeploymentUID = live.Metadata.UID
			var pod kubePod
			pod.Metadata.Name = runtime.RuntimeAppResourceName(app) + "-pending"
			pod.ObservedUID = "pod-uid"
			pod.Metadata.Labels = live.Spec.Template.Metadata.Labels
			pod.Metadata.Annotations = live.Spec.Template.Metadata.Annotations
			pod.Spec.NodeName = "compute-only"
			pod.Spec.Containers = live.Spec.Template.Spec.Containers
			pod.Spec.InitContainers = live.Spec.Template.Spec.InitContainers
			pod.Spec.TerminationGracePeriodSeconds = live.Spec.Template.Spec.TerminationGracePeriodSeconds
			pod.Status.Phase = "Pending"
			pod.ObservedOwnerReferences = []kubePodOwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs-uid", Controller: true}}
			desired := app
			desired.Spec = app.Spec.DeepCopy()
			claim := runtime.PersistentStoragePVCName(app, *app.Spec.PersistentStorage)
			f.pvc.Name = claim
			f.pvc.Namespace = managed.Metadata.Namespace
			f.pv.Spec.ClaimRef.Name = claim
			f.pv.Spec.ClaimRef.Namespace = managed.Metadata.Namespace
			switch scenario {
			case "ready-endpoint":
				f.readyEndpoint = true
			case "previously-serving":
				managed.Status.CurrentReleaseReadyAt = "2026-01-01T00:00:00Z"
			case "started-init":
				pod.Status.InitContainerStatuses = []kubeContainerStatus{{}}
				pod.Status.InitContainerStatuses[0].State.Running = &kubeStateDetail{}
			case "foreign-owner":
				f.rsDeploymentUID = "foreign-deployment"
			case "stale-generation":
				live.Status.ObservedGeneration = 0
			case "attached-volume":
				f.volumeNode = "compute-only"
			case "changed-user-env":
				desired.Spec.Env["MODE"] = "new"
			case "missing-pod-uid":
				pod.ObservedUID = ""
			case "deleted-pod":
				pod.Metadata.DeletionTimestamp = "2026-01-01T00:00:00Z"
			case "valid-old-node":
				f.ready = append(f.ready, "compute-only")
			}
			f.pods = []kubePod{pod}
			ok, err := svc.unstartedStoragePlacementRecovery(context.Background(), f.client, managed.Metadata.Namespace, managed, live, app, desired, before, after)
			if scenario == "pending-only" {
				if err != nil || !ok {
					t.Fatalf("unused invalid placement was not recoverable: ok=%t err=%v", ok, err)
				}
				if _, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), f.client, managed.Metadata.Namespace, managed, svc.Renderer.PrepareApp(desired), "", after); err != nil {
					t.Fatalf("live guard blocked verified unstarted repair: %v", err)
				}
			} else if ok {
				t.Fatalf("unsafe recovery accepted: %v", err)
			}
		})
	}
}

func TestStorageNoopDoesNotReadUnavailableInventory(t *testing.T) {
	f := newStoragePlacementFixture(t)
	app := managedAppLiveGuardTestApp(&model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: "network-storage", StorageSize: "10Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}})
	svc := &Service{}
	scheduling := schedulingPinnedToNode(runtime.SchedulingConstraints{}, "storage-ready")
	live, _ := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), scheduling)
	managedAppLiveGuardMarkReady(&live, 1)
	f.deployment = &live
	f.failPath = "/persistentvolume"
	if err := svc.validateAppStoragePlacement(context.Background(), f.client, app, scheduling, svc.Renderer.BuildManagedAppChildObjects(app, scheduling, nil)); err != nil {
		t.Fatalf("healthy unchanged release depends on storage inventory: %v", err)
	}
	if f.reads["/apis/storage.k8s.io/v1/csinodes"] != 0 {
		t.Fatal("no-op queried attachment inventory")
	}
	objects := svc.Renderer.BuildManagedAppRevisionChildObjects(app, scheduling, nil, nil, safeRolloutCandidateRevision("candidate"))
	if err := svc.validateAppStoragePlacement(context.Background(), f.client, app, scheduling, objects); err == nil {
		t.Fatal("a new candidate inherited the serving release's no-op exemption")
	}
}
