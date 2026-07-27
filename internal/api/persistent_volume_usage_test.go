package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestLocalPathPersistentVolumeUsesDirectoryUsageWithoutCapacity(t *testing.T) {
	t.Parallel()

	const (
		directoryBytes = int64(26_900_922_368)
	)
	claimKey := clusterNamespacedResourceKey("tenant", "demo-data")
	nodeUsedBytes := uint64(229_870_346_240)
	nodeCapacity := uint64(439_634_501_632)
	app := model.App{ID: "app-demo", Name: "demo"}
	snapshot := resourceUsageSnapshot(
		"node-a",
		"tenant",
		"demo-pod",
		map[string]string{runtime.FugueLabelAppID: app.ID},
		kubeNodeSummaryVolume{
			Name: "data",
			PVCRef: &kubeNodeSummaryVolumeRef{
				Name:      "demo-data",
				Namespace: "tenant",
			},
			UsedBytes:     &nodeUsedBytes,
			CapacityBytes: &nodeCapacity,
		},
	)
	policies := persistentVolumeUsagePolicies{
		strict: true,
		byClaim: map[string]persistentVolumeUsagePolicy{
			claimKey: {usedBytes: int64Pointer(directoryBytes)},
		},
	}

	overlay := buildCurrentResourceUsageOverlayWithPolicies([]clusterNodeSnapshot{snapshot}, []model.App{app}, nil, policies)
	usage, ok := overlay.apps[app.ID]
	if !ok {
		t.Fatal("expected app resource usage")
	}
	if usage.PersistentStorageUsedBytes == nil || *usage.PersistentStorageUsedBytes != directoryBytes {
		t.Fatalf("expected directory usage %d, got %#v", directoryBytes, usage.PersistentStorageUsedBytes)
	}
	if usage.PersistentStorageCapacityBytes != nil {
		t.Fatalf("expected unenforced local-path capacity to be omitted, got %d", *usage.PersistentStorageCapacityBytes)
	}
}

func TestStrictPersistentVolumePoliciesSuppressUnclassifiedKubeletValues(t *testing.T) {
	t.Parallel()

	app := model.App{ID: "app-demo", Name: "demo"}
	nodeUsedBytes := uint64(229_870_346_240)
	nodeCapacity := uint64(439_634_501_632)
	snapshot := resourceUsageSnapshot(
		"node-a",
		"tenant",
		"demo-pod",
		map[string]string{runtime.FugueLabelAppID: app.ID},
		kubeNodeSummaryVolume{
			PVCRef:        &kubeNodeSummaryVolumeRef{Name: "unknown", Namespace: "tenant"},
			UsedBytes:     &nodeUsedBytes,
			CapacityBytes: &nodeCapacity,
		},
	)

	overlay := buildCurrentResourceUsageOverlayWithPolicies(
		[]clusterNodeSnapshot{snapshot},
		[]model.App{app},
		nil,
		persistentVolumeUsagePolicies{strict: true, byClaim: map[string]persistentVolumeUsagePolicy{}},
	)
	usage := overlay.apps[app.ID]
	if usage.PersistentStorageUsedBytes != nil || usage.PersistentStorageCapacityBytes != nil {
		t.Fatalf("expected unclassified persistent values to be omitted, got %#v", usage)
	}
}

func TestDedicatedPersistentVolumeKeepsKubeletUsageAndCapacity(t *testing.T) {
	t.Parallel()

	app := model.App{ID: "app-demo", Name: "demo"}
	usedBytes := uint64(3 * 1024 * 1024 * 1024)
	capacityBytes := uint64(20 * 1024 * 1024 * 1024)
	snapshot := resourceUsageSnapshot(
		"node-a",
		"tenant",
		"demo-pod",
		map[string]string{runtime.FugueLabelAppID: app.ID},
		kubeNodeSummaryVolume{
			PVCRef:        &kubeNodeSummaryVolumeRef{Name: "demo-data", Namespace: "tenant"},
			UsedBytes:     &usedBytes,
			CapacityBytes: &capacityBytes,
		},
	)
	policies := persistentVolumeUsagePolicies{
		strict: true,
		byClaim: map[string]persistentVolumeUsagePolicy{
			clusterNamespacedResourceKey("tenant", "demo-data"): {useKubelet: true},
		},
	}

	overlay := buildCurrentResourceUsageOverlayWithPolicies([]clusterNodeSnapshot{snapshot}, []model.App{app}, nil, policies)
	usage := overlay.apps[app.ID]
	if usage.PersistentStorageUsedBytes == nil || *usage.PersistentStorageUsedBytes != int64(usedBytes) {
		t.Fatalf("expected kubelet usage %d, got %#v", usedBytes, usage.PersistentStorageUsedBytes)
	}
	if usage.PersistentStorageCapacityBytes == nil || *usage.PersistentStorageCapacityBytes != int64(capacityBytes) {
		t.Fatalf("expected kubelet capacity %d, got %#v", capacityBytes, usage.PersistentStorageCapacityBytes)
	}
}

func TestPersistentVolumeIsCountedOnceAcrossWorkloads(t *testing.T) {
	t.Parallel()

	app := model.App{ID: "app-demo", Name: "demo"}
	service := model.BackingService{ID: "service-postgres", Name: "postgres"}
	usedBytes := uint64(7 * 1024 * 1024 * 1024)
	capacityBytes := uint64(20 * 1024 * 1024 * 1024)
	volume := kubeNodeSummaryVolume{
		PVCRef:        &kubeNodeSummaryVolumeRef{Name: "shared-data", Namespace: "tenant"},
		UsedBytes:     &usedBytes,
		CapacityBytes: &capacityBytes,
	}
	snapshots := []clusterNodeSnapshot{
		resourceUsageSnapshot("node-a", "tenant", "app-pod", map[string]string{runtime.FugueLabelAppID: app.ID}, volume),
		resourceUsageSnapshot("node-b", "tenant", "postgres-pod", map[string]string{runtime.FugueLabelBackingServiceID: service.ID}, volume),
	}
	policies := persistentVolumeUsagePolicies{
		strict: true,
		byClaim: map[string]persistentVolumeUsagePolicy{
			clusterNamespacedResourceKey("tenant", "shared-data"): {useKubelet: true},
		},
	}

	overlay := buildCurrentResourceUsageOverlayWithPolicies(snapshots, []model.App{app}, []model.BackingService{service}, policies)
	appUsage := overlay.apps[app.ID]
	serviceUsage := overlay.services[service.ID]
	if appUsage.PersistentStorageUsedBytes != nil {
		t.Fatalf("expected shared claim to be attributed only once, app got %#v", appUsage.PersistentStorageUsedBytes)
	}
	if serviceUsage.PersistentStorageUsedBytes == nil || *serviceUsage.PersistentStorageUsedBytes != int64(usedBytes) {
		t.Fatalf("expected backing service to own shared claim usage, got %#v", serviceUsage.PersistentStorageUsedBytes)
	}
}

func TestPlanPersistentVolumeUsagePoliciesClassifiesLocalPath(t *testing.T) {
	t.Parallel()

	observed := map[string]observedPersistentVolumeClaim{
		clusterNamespacedResourceKey("tenant", "local-data"): {
			namespace: "tenant",
			name:      "local-data",
			nodes:     map[string]struct{}{"node-a": {}},
		},
		clusterNamespacedResourceKey("tenant", "lvm-data"): {
			namespace: "tenant",
			name:      "lvm-data",
			nodes:     map[string]struct{}{"node-b": {}},
		},
	}
	localVolume := persistentVolumeForClaim("tenant", "local-data", legacyLocalPathProvisioner)
	localVolume.Metadata.Annotations[legacyLocalPathSelectedNodeAnnotation] = "node-a"
	localVolume.Spec.Local = &struct {
		Path string `json:"path,omitempty"`
	}{Path: "/var/lib/rancher/k3s/storage/pvc-local"}
	lvmVolume := persistentVolumeForClaim("tenant", "lvm-data", "local.csi.openebs.io")

	policies, targets := planPersistentVolumeUsagePolicies(observed, []kubePersistentVolume{localVolume, lvmVolume})
	if policy := policies[clusterNamespacedResourceKey("tenant", "local-data")]; policy.useKubelet || policy.usedBytes != nil {
		t.Fatalf("expected local-path directory policy before measurement, got %#v", policy)
	}
	if policy := policies[clusterNamespacedResourceKey("tenant", "lvm-data")]; !policy.useKubelet {
		t.Fatalf("expected dedicated LVM volume to use kubelet stats, got %#v", policy)
	}
	if len(targets["node-a"]) != 1 || targets["node-a"][0].hostPath != "/var/lib/rancher/k3s/storage/pvc-local" {
		t.Fatalf("unexpected local-path measurement targets: %#v", targets)
	}
}

func TestLegacyLocalPathClassificationSupportsHistoricalMetadata(t *testing.T) {
	t.Parallel()

	for _, annotation := range []string{
		persistentVolumeProvisionerAnnotation,
		persistentVolumeStorageAnnotation,
		persistentVolumeBetaStorageAnnotation,
	} {
		volume := kubePersistentVolume{}
		volume.Metadata.Annotations = map[string]string{annotation: legacyLocalPathProvisioner}
		if !isLegacyLocalPathPersistentVolume(volume) {
			t.Fatalf("expected annotation %q to classify local-path volume", annotation)
		}
	}

	volume := kubePersistentVolume{}
	volume.Spec.StorageClassName = legacyLocalPathStorageClass
	volume.Spec.HostPath = &struct {
		Path string `json:"path,omitempty"`
	}{Path: "/var/lib/rancher/k3s/storage/pvc-data"}
	if !isLegacyLocalPathPersistentVolume(volume) {
		t.Fatal("expected local-path storage class with a directory source to be classified")
	}
}

func TestPlanPersistentVolumeUsagePoliciesIgnoresReleasedVolume(t *testing.T) {
	t.Parallel()

	claimKey := clusterNamespacedResourceKey("tenant", "data")
	observed := map[string]observedPersistentVolumeClaim{
		claimKey: {namespace: "tenant", name: "data", nodes: map[string]struct{}{"node-a": {}}},
	}
	volume := persistentVolumeForClaim("tenant", "data", "local.csi.openebs.io")
	volume.Status.Phase = "Released"

	policies, targets := planPersistentVolumeUsagePolicies(observed, []kubePersistentVolume{volume})
	if _, ok := policies[claimKey]; ok || len(targets) != 0 {
		t.Fatalf("expected released PV to be ignored, policies=%#v targets=%#v", policies, targets)
	}
}

func TestPlanPersistentVolumeUsagePoliciesSuppressesSharedNFSFilesystemStats(t *testing.T) {
	t.Parallel()

	claimKey := clusterNamespacedResourceKey("tenant", "shared-workspace")
	observed := map[string]observedPersistentVolumeClaim{
		claimKey: {namespace: "tenant", name: "shared-workspace", nodes: map[string]struct{}{"node-a": {}}},
	}
	volume := persistentVolumeForClaim("tenant", "shared-workspace", "fugue.pro/nfs-rwx")
	volume.Spec.NFS = &struct {
		Server string `json:"server,omitempty"`
		Path   string `json:"path,omitempty"`
	}{Server: "10.43.0.10", Path: "/tenant/shared-workspace"}

	policies, targets := planPersistentVolumeUsagePolicies(observed, []kubePersistentVolume{volume})
	policy, ok := policies[claimKey]
	if !ok || policy.useKubelet || policy.usedBytes != nil {
		t.Fatalf("expected shared NFS stats to be suppressed, got %#v", policy)
	}
	if len(targets) != 0 {
		t.Fatalf("expected no host directory target for NFS volume, got %#v", targets)
	}
}

func TestMergePersistentVolumeUsagePoliciesKeepsOnlyRecentAccurateValue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	claimKey := clusterNamespacedResourceKey("tenant", "data")
	usedBytes := int64(1234)
	previous := persistentVolumeUsagePolicies{
		fingerprint: "same",
		strict:      true,
		byClaim: map[string]persistentVolumeUsagePolicy{
			claimKey: {usedBytes: &usedBytes, measuredAt: now.Add(-10 * time.Minute)},
		},
	}
	current := persistentVolumeUsagePolicies{
		fingerprint: "same",
		strict:      true,
		byClaim: map[string]persistentVolumeUsagePolicy{
			claimKey: {},
		},
	}

	merged := mergePersistentVolumeUsagePolicies(current, previous, now)
	if merged.byClaim[claimKey].usedBytes == nil || *merged.byClaim[claimKey].usedBytes != usedBytes {
		t.Fatalf("expected recent measured value to survive transient refresh failure, got %#v", merged.byClaim[claimKey])
	}

	current.byClaim[claimKey] = persistentVolumeUsagePolicy{}
	expired := mergePersistentVolumeUsagePolicies(current, previous, now.Add(persistentVolumeUsageCacheMaxStale+time.Second))
	if expired.byClaim[claimKey].usedBytes != nil {
		t.Fatalf("expected stale measured value to expire, got %#v", expired.byClaim[claimKey])
	}
}

func TestPersistentVolumeDirectoryUsageScriptAndParser(t *testing.T) {
	t.Parallel()

	targets := []persistentVolumeDirectoryTarget{
		{claimKey: "tenant/one", hostPath: "/var/lib/rancher/k3s/storage/pvc-one"},
		{claimKey: "tenant/two", hostPath: "/var/lib/rancher/k3s/storage/pvc-'two"},
	}
	script := buildPersistentVolumeDirectoryUsageScript(targets)
	for _, required := range []string{"chroot /host /bin/sh -lc", "du -x -s -B1", "timeout --kill-after=5s"} {
		if !strings.Contains(script, required) {
			t.Fatalf("expected script to contain %q, got %q", required, script)
		}
	}

	usage := parsePersistentVolumeDirectoryUsage([]byte("0\t123\n1\t456\n1\t400\ninvalid\n2\t999\n"), targets)
	if usage["tenant/one"] != 123 || usage["tenant/two"] != 456 || len(usage) != 2 {
		t.Fatalf("unexpected parsed directory usage: %#v", usage)
	}
}

func TestMeasurePersistentVolumeDirectoriesUsesReadyNodeJanitor(t *testing.T) {
	t.Parallel()

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/v1/pods" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "node-janitor-a", "namespace": "fugue-system"},
					"spec":     map[string]any{"nodeName": "node-a"},
					"status": map[string]any{
						"phase": "Running",
						"containerStatuses": []map[string]any{
							{
								"name":  clusterNodeJanitorContainer,
								"state": map[string]any{"running": map[string]any{}},
							},
						},
					},
				},
			},
		})
	}))
	defer kubeServer.Close()

	client := &clusterNodeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test-token",
	}
	runner := &fakeFilesystemExecRunner{outputs: [][]byte{[]byte("0\t26900922368\n")}}
	server := &Server{filesystemExecRunner: runner}
	targets := []persistentVolumeDirectoryTarget{
		{claimKey: clusterNamespacedResourceKey("tenant", "data"), hostPath: "/var/lib/rancher/k3s/storage/pvc-data"},
	}

	usage, err := server.measurePersistentVolumeDirectoriesOnNode(context.Background(), client, "node-a", targets)
	if err != nil {
		t.Fatalf("measure persistent volume directory: %v", err)
	}
	if got := usage[targets[0].claimKey]; got != 26_900_922_368 {
		t.Fatalf("expected measured usage 26900922368, got %d", got)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("expected one node-janitor exec, got %d", len(runner.calls))
	}
	call := runner.calls[0]
	if call.namespace != "fugue-system" || call.podName != "node-janitor-a" || call.container != clusterNodeJanitorContainer {
		t.Fatalf("unexpected node-janitor exec target: %#v", call)
	}
	if len(call.command) != 3 || call.command[0] != "/bin/bash" || call.command[1] != "-lc" || !strings.Contains(call.command[2], "chroot /host") {
		t.Fatalf("unexpected node-janitor command: %#v", call.command)
	}
}

func TestPersistentVolumeDirectoryPathRejectsUnsafeRoots(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", ".", "/", "relative/path", "/tmp/path\nnext"} {
		volume := kubePersistentVolume{}
		volume.Spec.Local = &struct {
			Path string `json:"path,omitempty"`
		}{Path: raw}
		if path, ok := persistentVolumeDirectoryPath(volume); ok {
			t.Fatalf("expected %q to be rejected, got %q", raw, path)
		}
	}
}

func resourceUsageSnapshot(
	nodeName string,
	namespace string,
	podName string,
	labels map[string]string,
	volume kubeNodeSummaryVolume,
) clusterNodeSnapshot {
	pod := clusterNodePod{}
	pod.Metadata.Name = podName
	pod.Metadata.Namespace = namespace
	pod.Metadata.Labels = labels
	pod.Spec.NodeName = nodeName
	summary := &kubeNodeSummary{
		Pods: []kubeNodeSummaryPod{
			{
				PodRef:  kubeNodeSummaryPodRef{Name: podName, Namespace: namespace},
				Volumes: []kubeNodeSummaryVolume{volume},
			},
		},
	}
	return clusterNodeSnapshot{
		node:    model.ClusterNode{Name: nodeName},
		pods:    []clusterNodePod{pod},
		summary: summary,
	}
}

func persistentVolumeForClaim(namespace, claimName, provisioner string) kubePersistentVolume {
	volume := kubePersistentVolume{}
	volume.Metadata.Name = "pv-" + claimName
	volume.Metadata.Annotations = map[string]string{persistentVolumeProvisionerAnnotation: provisioner}
	volume.Spec.ClaimRef = &struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}{Name: claimName, Namespace: namespace}
	volume.Status.Phase = "Bound"
	return volume
}
