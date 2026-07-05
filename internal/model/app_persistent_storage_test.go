package model

import "testing"

func TestRWOStorageClassSameNodeOnlineMountCapability(t *testing.T) {
	t.Parallel()

	if !AppRWOStorageClassSupportsSameNodeOnlineMount(AppStorageClassFugueLocalRWO) {
		t.Fatal("expected fugue-local-rwo to support same-node online mount")
	}
	if AppRWOStorageClassSupportsSameNodeOnlineMount(AppStorageClassFugueWorkspaceRWO) {
		t.Fatal("expected fugue-workspace-rwo not to support same-node online mount")
	}
	if AppRWOStorageClassSupportsSameNodeOnlineMount("fast-rwo") {
		t.Fatal("expected unknown RWO storage class not to advertise same-node online mount support")
	}
}

func TestPersistentStorageSpecSupportsSameNodeOnlineRollout(t *testing.T) {
	t.Parallel()

	spec := &AppPersistentStorageSpec{
		Mode:             AppPersistentStorageModeMovableRWO,
		StorageClassName: AppStorageClassFugueLocalRWO,
		Mounts: []AppPersistentStorageMount{
			{Kind: AppPersistentStorageMountKindDirectory, Path: "/workspace"},
		},
	}
	if !AppPersistentStorageSpecSupportsSameNodeOnlineRollout(spec) {
		t.Fatal("expected fugue-local-rwo movable RWO storage to support same-node online rollout")
	}

	unsupported := *spec
	unsupported.StorageClassName = AppStorageClassFugueWorkspaceRWO
	if AppPersistentStorageSpecSupportsSameNodeOnlineRollout(&unsupported) {
		t.Fatal("expected fugue-workspace-rwo movable RWO storage not to support same-node online rollout")
	}

	shared := *spec
	shared.Mode = AppPersistentStorageModeSharedProjectRWX
	if AppPersistentStorageSpecSupportsSameNodeOnlineRollout(&shared) {
		t.Fatal("shared RWX storage should not request same-node pinning")
	}
}

func TestStorageEventIndicatesSameNodeOnlineMountUnsupported(t *testing.T) {
	t.Parallel()

	message := "MountVolume.SetUp failed: verifyMount: device already mounted at /var/lib/kubelet/pods/demo"
	if !StorageEventIndicatesSameNodeOnlineMountUnsupported(message) {
		t.Fatal("expected verifyMount device already mounted message to be classified")
	}
	if StorageEventIndicatesSameNodeOnlineMountUnsupported("Unable to attach or mount volumes: timed out waiting for the condition") {
		t.Fatal("expected generic mount timeout not to be classified as same-node online mount unsupported")
	}
}
