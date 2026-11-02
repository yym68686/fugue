package controller

import (
	"context"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	corev1 "k8s.io/api/core/v1"
)

func TestLonghornOnlineRolloutRechecksLiveVolumeBeforeWriting(t *testing.T) {
	for _, scenario := range []string{"same-node", "other-node", "pv-driver", "class-driver", "pvc-rwop", "pv-rwop", "pvc-block", "pv-block", "unbound", "unclaimed-pv", "missing-pin", "observation-failure"} {
		t.Run(scenario, func(t *testing.T) {
			f := newStoragePlacementFixture(t)
			f.class.Name = model.AppStorageClassFugueLonghornRWO
			f.pvc.Spec.StorageClassName = &f.class.Name
			f.pvc.Status.Phase = corev1.ClaimBound
			f.pv.Spec.StorageClassName = f.class.Name
			f.pv.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
			f.volumeNode, f.volumeState = "storage-ready", "attached"
			app := managedAppLiveGuardTestApp(&model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeMovableRWO, StorageClassName: f.class.Name, StorageSize: "10Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}})
			app.Spec.RolloutIntent = model.AppRolloutIntentOnlineImageUpdate
			scheduling := schedulingPinnedToNode(runtime.SchedulingConstraints{}, "storage-ready")
			want := ""
			switch scenario {
			case "other-node":
				f.volumeNode = "compute-only"
				want = "different node"
			case "pv-driver":
				f.pv.Spec.CSI.Driver = "other.csi.example.test"
				want = "Longhorn CSI driver"
			case "class-driver":
				f.class.Provisioner = "other.csi.example.test"
				want = "Longhorn CSI driver"
			case "pvc-rwop":
				f.pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
				want = "filesystem ReadWriteOnce"
			case "pv-rwop":
				f.pv.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
				want = "filesystem ReadWriteOnce"
			case "pvc-block", "pv-block":
				block := corev1.PersistentVolumeBlock
				if scenario == "pvc-block" {
					f.pvc.Spec.VolumeMode = &block
				} else {
					f.pv.Spec.VolumeMode = &block
				}
				want = "filesystem ReadWriteOnce"
			case "unbound":
				f.pvc.Status.Phase = corev1.ClaimPending
				want = "bound matching PVC and PV"
			case "unclaimed-pv":
				f.pv.Spec.ClaimRef = nil
				want = "bound matching PVC and PV"
			case "missing-pin":
				scheduling = runtime.SchedulingConstraints{}
				want = "current-node placement"
			case "observation-failure":
				f.failPath = "persistentvolumes"
				want = "observe bound volume"
			}
			svc := &Service{}
			objects := svc.Renderer.BuildManagedAppChildObjects(app, scheduling, nil)
			claims, err := appStorageClaimRequests(objects)
			if err != nil || len(claims) != 1 {
				t.Fatalf("expected one rendered claim: %v", err)
			}
			f.pvc.Name = claims[0].Name
			f.pvc.Namespace = runtime.NamespaceForTenant(app.TenantID)
			if f.pv.Spec.ClaimRef != nil {
				f.pv.Spec.ClaimRef.Name, f.pv.Spec.ClaimRef.Namespace = f.pvc.Name, f.pvc.Namespace
			}
			err = svc.validateAppStoragePlacement(context.Background(), f.client, app, scheduling, objects)
			if want == "" && err != nil {
				t.Fatal(err)
			}
			if want != "" && (err == nil || !strings.Contains(err.Error(), want)) {
				t.Fatalf("want %q, got %v", want, err)
			}
			if scenario == "same-node" {
				// Fresh observations must invalidate a previously safe plan.
				f.pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
				if err := svc.validateAppStoragePlacement(context.Background(), f.client, app, scheduling, objects); err == nil {
					t.Fatal("stale RWO proof allowed a pod-exclusive claim")
				}
			}
		})
	}
}

func TestLonghornOnlineIntentDoesNotBlockInitialClaimCreation(t *testing.T) {
	f := newStoragePlacementFixture(t)
	f.class.Name = model.AppStorageClassFugueLonghornRWO
	f.pvc = corev1.PersistentVolumeClaim{}
	app := managedAppLiveGuardTestApp(&model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeMovableRWO, StorageClassName: f.class.Name, StorageSize: "10Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}})
	app.Spec.RolloutIntent = model.AppRolloutIntentOnlineImageUpdate
	svc := &Service{}
	scheduling := schedulingPinnedToNode(runtime.SchedulingConstraints{}, "storage-ready")
	objects := svc.Renderer.BuildManagedAppChildObjects(app, scheduling, nil)
	if err := svc.validateAppStoragePlacement(context.Background(), f.client, app, scheduling, objects); err != nil {
		t.Fatal(err)
	}
}
